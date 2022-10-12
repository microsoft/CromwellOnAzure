// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.RegularExpressions;
using Azure.Storage;
using Azure.Storage.Blobs;
using Microsoft.Azure.Management.Batch;
using Microsoft.Azure.Management.Batch.Models;
using Microsoft.Azure.Management.Compute.Fluent;
using Microsoft.Azure.Management.ContainerService;
using Microsoft.Azure.Management.ContainerService.Models;
using Microsoft.Azure.Management.Network.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Microsoft.Azure.Management.Storage.Fluent;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Rest;
using Microsoft.Rest.Azure;
using Microsoft.WindowsAzure.Storage.Blob;
using Polly;
using Polly.Retry;
using Renci.SshNet.Common;
using Renci.SshNet;
using Common.HostConfigs;
using CromwellOnAzureDeployer;
using TesApi.Web;

namespace HostConfigConsole
{
    // TODO: most of this should borrow from (or directly link to) the deployer, instead of copy
    internal class CromwellOnAzure
    {
        private static readonly AsyncRetryPolicy sshCommandRetryPolicy = Policy
            .Handle<Exception>(ex => !(ex is SshAuthenticationException && ex.Message.StartsWith("Permission")))
            .WaitAndRetryAsync(5, retryAttempt => System.TimeSpan.FromSeconds(5));

        private static readonly AsyncRetryPolicy generalRetryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(3, retryAttempt => System.TimeSpan.FromSeconds(1));

        public const string HostConfigConfigurationFileName = "host-configurations.json";
        public const string HostConfigBlobsContainerName = "host-config-blobs";
        public const string ConfigurationContainerName = "configuration";
        public const string PersonalizedSettingsFileName = "settings-user";
        public const string CromwellAzureRootDir = "/data/cromwellazure";
        public const string SettingsDelimiter = "=:=";
        public const string SshNsgRuleName = "SSH";

        private readonly CancellationTokenSource cts;

        private TokenCredentials tokenCredentials { get; }
        private BatchAccount batchAccount { get; }
        private string subscriptionId { get; }
        private IStorageAccount storageAccount { get; }

        #region Initialization
        public static async ValueTask<CromwellOnAzure> Create(CancellationTokenSource cts, string[] args)
        {
            var configuration = Configuration.BuildConfiguration(args);

            await ValidateTokenProviderAsync();

            var tokenCredentials = new TokenCredentials(new RefreshableAzureServiceTokenProvider("https://management.azure.com/"));
            var azureCredentials = new AzureCredentials(tokenCredentials, null, null, AzureEnvironment.AzureGlobalCloud);
            var azureClient = GetAzureClient(azureCredentials);
            var azureSubscriptionClient = azureClient.WithSubscription(configuration.SubscriptionId);
            var subscriptionIds = (await azureClient.Subscriptions.ListAsync()).Select(s => s.SubscriptionId);
            var resourceManagerClient = GetResourceManagerClient(azureCredentials);

            //await ValidateSubscriptionAndResourceGroupAsync(configuration);

            var resourceGroup = await azureSubscriptionClient.ResourceGroups.GetByNameAsync(configuration.ResourceGroupName);
            configuration.RegionName = resourceGroup.RegionName;

            var existingAksCluster = await ValidateAndGetExistingAKSClusterAsync();
            configuration.UseAks = existingAksCluster is not null;

            IStorageAccount storageAccount;
            Dictionary<string, string>? accountNames = null;
            if (configuration.UseAks)
            {
                if (!string.IsNullOrEmpty(configuration.StorageAccountName))
                {
                    storageAccount = await GetExistingStorageAccountAsync(configuration.StorageAccountName)
                        ?? throw new ValidationException($"Storage account {configuration.StorageAccountName}, does not exist in region {configuration.RegionName} or is not accessible to the current user.");
                }
                else
                {
                    var storageAccounts = await azureSubscriptionClient.StorageAccounts.ListByResourceGroupAsync(configuration.ResourceGroupName);

                    if (!storageAccounts.Any())
                    {
                        throw new ValidationException($"Update was requested but resource group {configuration.ResourceGroupName} does not contain any storage accounts.");
                    }
                    if (storageAccounts.Count() > 1)
                    {
                        throw new ValidationException($"Resource group {configuration.ResourceGroupName} contains multiple storage accounts. {nameof(configuration.StorageAccountName)} must be provided.");
                    }

                    storageAccount = storageAccounts.First();
                }

                accountNames = DelimitedTextToDictionary(await DownloadTextFromStorageAccountAsync(storageAccount, ConfigurationContainerName, PersonalizedSettingsFileName, cts), SettingsDelimiter);
            }
            else
            {
                var existingVms = await azureSubscriptionClient.VirtualMachines.ListByResourceGroupAsync(configuration.ResourceGroupName);

                if (!existingVms.Any())
                {
                    throw new ValidationException($"Update was requested but resource group {configuration.ResourceGroupName} does not contain any virtual machines.");
                }

                if (existingVms.Count() > 1 && string.IsNullOrWhiteSpace(configuration.VmName))
                {
                    throw new ValidationException($"Resource group {configuration.ResourceGroupName} contains multiple virtual machines. {nameof(configuration.VmName)} must be provided.");
                }

                IVirtualMachine? linuxVm;
                if (!string.IsNullOrWhiteSpace(configuration.VmName))
                {
                    linuxVm = existingVms.FirstOrDefault(vm => vm.Name.Equals(configuration.VmName, StringComparison.OrdinalIgnoreCase));

                    if (linuxVm is null)
                    {
                        throw new ValidationException($"Virtual machine {configuration.VmName} does not exist in resource group {configuration.ResourceGroupName}.");
                    }
                }
                else
                {
                    linuxVm = existingVms.Single();
                }

                configuration.VmName = linuxVm.Name;
                configuration.RegionName = linuxVm.RegionName;
                configuration.PrivateNetworking = linuxVm.GetPrimaryPublicIPAddress() is null;
                var networkSecurityGroup = (await azureSubscriptionClient.NetworkSecurityGroups.ListByResourceGroupAsync(configuration.ResourceGroupName)).FirstOrDefault(g => g.NetworkInterfaceIds.Contains(linuxVm.GetPrimaryNetworkInterface().Id));

                if (!configuration.PrivateNetworking.GetValueOrDefault() && networkSecurityGroup is null)
                {
                    if (string.IsNullOrWhiteSpace(configuration.NetworkSecurityGroupName))
                    {
                        configuration.NetworkSecurityGroupName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}", 15);
                    }

                    networkSecurityGroup = await CreateNetworkSecurityGroupAsync(resourceGroup, configuration.NetworkSecurityGroupName);
                    await AssociateNicWithNetworkSecurityGroupAsync(linuxVm.GetPrimaryNetworkInterface(), networkSecurityGroup);
                }

                try
                {
                    await EnableSsh(networkSecurityGroup);
                    var sshConnectionInfo = GetSshConnectionInfo(linuxVm, configuration.VmUsername, configuration.VmPassword);
                    await WaitForSshConnectivityAsync(sshConnectionInfo);

                    accountNames = DelimitedTextToDictionary((await ExecuteCommandOnVirtualMachineWithRetriesAsync(sshConnectionInfo, $"cat {CromwellAzureRootDir}/env-01-account-names.txt || echo ''")).Output);

                    if (!accountNames.TryGetValue("DefaultStorageAccountName", out var storageAccountName))
                    {
                        throw new ValidationException($"Could not retrieve the default storage account name from virtual machine {configuration.VmName}.");
                    }

                    storageAccount = await GetExistingStorageAccountAsync(storageAccountName)
                        ?? throw new ValidationException($"Storage account {storageAccountName}, referenced by the VM configuration, does not exist in region {configuration.RegionName} or is not accessible to the current user.");

                    configuration.StorageAccountName = storageAccountName;
                }
                finally
                {
                    await DisableSsh(networkSecurityGroup);
                }
            }

            if (!accountNames.Any())
            {
                throw new ValidationException($"Could not retrieve account names from virtual machine {configuration.VmName}.");
            }

            if (!accountNames.TryGetValue("BatchAccountName", out var batchAccountName))
            {
                throw new ValidationException($"Could not retrieve the Batch account name from virtual machine {configuration.VmName}.");
            }

            var (batchAccount, subscriptionId) = await GetExistingBatchAccountAsync(batchAccountName);
            if (batchAccount is null) throw new ValidationException($"Batch account {batchAccountName}, referenced by the VM configuration, does not exist in region {configuration.RegionName} or is not accessible to the current user.");

            configuration.BatchAccountName = batchAccountName;

            return new(cts/*, configuration*/, tokenCredentials/*, azureSubscriptionClient, azureClient, resourceManagerClient, azureCredentials, subscriptionIds*/, batchAccount, subscriptionId, storageAccount);

            // From CromwellOnAzureDeployer.Utility
            static Dictionary<string, string> DelimitedTextToDictionary(string text, string fieldDelimiter = "=", string rowDelimiter = "\n")
                => text.Trim().Split(rowDelimiter)
                    .Select(r => r.Trim().Split(fieldDelimiter))
                    .ToDictionary(f => f[0].Trim(), f => f[1].Trim());

            static Microsoft.Azure.Management.Fluent.Azure.IAuthenticated GetAzureClient(AzureCredentials azureCredentials)
                => Microsoft.Azure.Management.Fluent.Azure
                    .Configure()
                    .WithLogLevel(HttpLoggingDelegatingHandler.Level.Basic)
                    .Authenticate(azureCredentials);

            IResourceManager GetResourceManagerClient(AzureCredentials azureCredentials)
                => ResourceManager
                    .Configure()
                    .WithLogLevel(HttpLoggingDelegatingHandler.Level.Basic)
                    .Authenticate(azureCredentials)
                    .WithSubscription(configuration.SubscriptionId);

            async Task ValidateTokenProviderAsync()
            {
                try
                {
                    await Execute("Retrieving Azure management token...", () => new AzureServiceTokenProvider("RunAs=Developer; DeveloperTool=AzureCli").GetAccessTokenAsync("https://management.azure.com/"), cts);
                }
                catch (AzureServiceTokenProviderException ex)
                {
                    ConsoleEx.WriteLine("No access token found.  Please install the Azure CLI and login with 'az login'", ConsoleColor.Red);
                    ConsoleEx.WriteLine("Link: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli");
                    ConsoleEx.WriteLine($"Error details: {ex.Message}");
                    Environment.Exit(1);
                }
            }

            //async Task ValidateSubscriptionAndResourceGroupAsync()
            //{
            //    const string ownerRoleId = "8e3af657-a8ff-443c-a75c-2fe8c4bcb635";
            //    const string contributorRoleId = "b24988ac-6180-42a0-ab88-20f7382dd24c";
            //    const string userAccessAdministratorRoleId = "18d7d88d-d35e-4fb5-a5c3-7773c20a72d9";

            //    var azure = Microsoft.Azure.Management.Fluent.Azure
            //        .Configure()
            //        .WithLogLevel(HttpLoggingDelegatingHandler.Level.Basic)
            //        .Authenticate(azureCredentials);

            //    var subscriptionExists = (await azure.Subscriptions.ListAsync()).Any(sub => sub.SubscriptionId.Equals(configuration.SubscriptionId, StringComparison.OrdinalIgnoreCase));

            //    if (!subscriptionExists)
            //    {
            //        throw new ValidationException($"Invalid or inaccessible subcription id '{configuration.SubscriptionId}'. Make sure that subscription exists and that you are either an Owner or have Contributor and User Access Administrator roles on the subscription.", displayExample: false);
            //    }

            //    var rgExists = !string.IsNullOrEmpty(configuration.ResourceGroupName) && await azureSubscriptionClient.ResourceGroups.ContainAsync(configuration.ResourceGroupName);

            //    if (!string.IsNullOrEmpty(configuration.ResourceGroupName) && !rgExists)
            //    {
            //        throw new ValidationException($"If ResourceGroupName is provided, the resource group must already exist.", displayExample: false);
            //    }

            //    var token = await new AzureServiceTokenProvider().GetAccessTokenAsync("https://management.azure.com/");
            //    var currentPrincipalObjectId = new JwtSecurityTokenHandler().ReadJwtToken(token).Claims.FirstOrDefault(c => c.Type == "oid").Value;

            //    var currentPrincipalSubscriptionRoleIds = (await azureSubscriptionClient.AccessManagement.RoleAssignments.Inner.ListForScopeWithHttpMessagesAsync($"/subscriptions/{configuration.SubscriptionId}", new ODataQuery<RoleAssignmentFilter>($"atScope() and assignedTo('{currentPrincipalObjectId}')")))
            //        .Body.AsContinuousCollection(link => Extensions.Synchronize(() => azureSubscriptionClient.AccessManagement.RoleAssignments.Inner.ListForScopeNextWithHttpMessagesAsync(link)).Body)
            //        .Select(b => b.RoleDefinitionId.Split(new[] { '/' }).Last());

            //    var isuserAccessAdministrator = currentPrincipalSubscriptionRoleIds.Contains(userAccessAdministratorRoleId);

            //    if (!currentPrincipalSubscriptionRoleIds.Contains(ownerRoleId) && !(currentPrincipalSubscriptionRoleIds.Contains(contributorRoleId) && isuserAccessAdministrator))
            //    {
            //        if (!rgExists)
            //        {
            //            throw new ValidationException($"Insufficient access to deploy. You must be: 1) Owner of the subscription, or 2) Contributor and User Access Administrator of the subscription, or 3) Owner of the resource group", displayExample: false);
            //        }

            //        var currentPrincipalRgRoleIds = (await azureSubscriptionClient.AccessManagement.RoleAssignments.Inner.ListForScopeWithHttpMessagesAsync($"/subscriptions/{configuration.SubscriptionId}/resourceGroups/{configuration.ResourceGroupName}", new ODataQuery<RoleAssignmentFilter>($"atScope() and assignedTo('{currentPrincipalObjectId}')")))
            //            .Body.AsContinuousCollection(link => Extensions.Synchronize(() => azureSubscriptionClient.AccessManagement.RoleAssignments.Inner.ListForScopeNextWithHttpMessagesAsync(link)).Body)
            //            .Select(b => b.RoleDefinitionId.Split(new[] { '/' }).Last());

            //        if (!currentPrincipalRgRoleIds.Contains(ownerRoleId))
            //        {
            //            throw new ValidationException($"Insufficient access to deploy. You must be: 1) Owner of the subscription, or 2) Contributor and User Access Administrator of the subscription, or 3) Owner of the resource group", displayExample: false);
            //        }

            //        if (!isuserAccessAdministrator)
            //        {
            //            SkipBillingReaderRoleAssignment = true;
            //            DisplayBillingReaderInsufficientAccessLevelWarning();
            //        }
            //    }
            //}

#pragma warning disable CS8603 // Possible null reference return.
            async Task<IStorageAccount?> GetExistingStorageAccountAsync(string storageAccountName)
                => (await Task.WhenAll(subscriptionIds.Select(async s =>
                {
                    try
                    {
                        return await azureClient.WithSubscription(s).StorageAccounts.ListAsync();
                    }
                    catch (Exception)
                    {
                        // Ignore exception if a user does not have the required role to list storage accounts in a subscription
                        return null;
                    }
                })))
                    .Where(a => a is not null)
                    .SelectMany(a => a)
                    .SingleOrDefault(a => a.Name.Equals(storageAccountName, StringComparison.OrdinalIgnoreCase) && a.RegionName.Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase));
#pragma warning restore CS8603 // Possible null reference return.

            async Task<(BatchAccount BatchAccount, string SubscriptionId)> GetExistingBatchAccountAsync(string batchAccountName)
                => await (await Task.WhenAll(subscriptionIds.Select(async s =>
                {
                    IAsyncEnumerable<(BatchAccount BatchAccount, string SubscriptionId)> list;
                    try
                    {
                        var client = new BatchManagementClient(tokenCredentials) { SubscriptionId = s };
                        list = (await client.BatchAccount.ListAsync())
                            .ToAsyncEnumerable(listNextAsync: new Func<string, CancellationToken, Task<IPage<BatchAccount>>>(async (link, ct) => await client.BatchAccount.ListNextAsync(link, ct)))
                            .Select(b => (b, s));
                    }
                    catch (Exception e)
                    {
                        ConsoleEx.WriteLine(e.Message);
                        list = AsyncEnumerable.Empty<(BatchAccount, string)>();
                    }
                    return list;
                }
                    ))).ToAsyncEnumerable()
                    .SelectAwait(async a => await a.SingleOrDefaultAsync(a => a.BatchAccount.Name.Equals(batchAccountName, StringComparison.OrdinalIgnoreCase) && a.BatchAccount.Location.Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase)))
                    .Where(a => a.BatchAccount is not null)
                    .SingleOrDefaultAsync();

            async Task<ManagedCluster?> ValidateAndGetExistingAKSClusterAsync()
            {
                if (string.IsNullOrWhiteSpace(configuration.AksClusterName))
                {
                    return null;
                }

                return (await GetExistingAKSClusterAsync(configuration.AksClusterName))
                    ?? throw new ValidationException($"If AKS cluster name is provided, the cluster must already exist in region {configuration.RegionName}, and be accessible to the current user.", displayExample: false);
            }

            async Task<ManagedCluster?> GetExistingAKSClusterAsync(string aksClusterName)
            {
#pragma warning disable CS8603 // Possible null reference return.
                return (await Task.WhenAll(subscriptionIds.Select(async s =>
                {
                    try
                    {
                        var client = new ContainerServiceClient(tokenCredentials) { SubscriptionId = s };
                        return await client.ManagedClusters.ListAsync();
                    }
                    catch (Exception e)
                    {
                        ConsoleEx.WriteLine(e.Message);
                        return null;
                    }
                })))
                    .Where(a => a is not null)
                    .SelectMany(a => a)
                    .SingleOrDefault(a => a.Name.Equals(aksClusterName, StringComparison.OrdinalIgnoreCase) && a.Location.Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase));
#pragma warning restore CS8603 // Possible null reference return.
            }

            Task<INetworkSecurityGroup> CreateNetworkSecurityGroupAsync(IResourceGroup resourceGroup, string networkSecurityGroupName)
            {
                const int SshAllowedPort = 22;
                const int SshDefaultPriority = 300;

                return Execute(
                    $"Creating Network Security Group: {networkSecurityGroupName}...",
                    () => azureSubscriptionClient.NetworkSecurityGroups.Define(networkSecurityGroupName)
                        .WithRegion(configuration.RegionName)
                        .WithExistingResourceGroup(resourceGroup)
                        .DefineRule(SshNsgRuleName)
                        .AllowInbound()
                        .FromAnyAddress()
                        .FromAnyPort()
                        .ToAnyAddress()
                        .ToPort(SshAllowedPort)
                        .WithProtocol(Microsoft.Azure.Management.Network.Fluent.Models.SecurityRuleProtocol.Tcp)
                        .WithPriority(SshDefaultPriority)
                        .Attach()
                        .CreateAsync(cts.Token),
                    cts
                );
            }

            Task EnableSsh(INetworkSecurityGroup? networkSecurityGroup)
            {
                try
                {
                    return networkSecurityGroup?.SecurityRules[SshNsgRuleName]?.Access switch
                    {
                        null => Task.CompletedTask,
                        var x when Microsoft.Azure.Management.Network.Fluent.Models.SecurityRuleAccess.Allow.Equals(x) => SetKeepSshPortOpen(true),
                        _ => Execute(
                            "Enabling SSH on VM...",
                            () => EnableSshPort(),
                            cts),
                    };
                }
                catch (KeyNotFoundException)
                {
                    return Task.CompletedTask;
                }

                Task<INetworkSecurityGroup> EnableSshPort()
                {
                    _ = SetKeepSshPortOpen(false);
                    return networkSecurityGroup.Update().UpdateRule(SshNsgRuleName).AllowInbound().Parent().ApplyAsync();
                }

                Task SetKeepSshPortOpen(bool value)
                {
                    configuration.KeepSshPortOpen = value;
                    return Task.CompletedTask;
                }
            }

            Task DisableSsh(INetworkSecurityGroup? networkSecurityGroup)
                => networkSecurityGroup is null ? Task.CompletedTask : Execute(
                    "Disabling SSH on VM...",
                    () => networkSecurityGroup.Update().UpdateRule(SshNsgRuleName).DenyInbound().Parent().ApplyAsync(),
                    cts
                );

            Task<INetworkInterface> AssociateNicWithNetworkSecurityGroupAsync(INetworkInterface networkInterface, INetworkSecurityGroup networkSecurityGroup)
                => Execute(
                    $"Associating VM NIC with Network Security Group {networkSecurityGroup.Name}...",
                    () => networkInterface.Update().WithExistingNetworkSecurityGroup(networkSecurityGroup).ApplyAsync(),
                    cts
                );

            static ConnectionInfo GetSshConnectionInfo(IVirtualMachine linuxVm, string vmUsername, string vmPassword)
            {
                var publicIPAddress = linuxVm.GetPrimaryPublicIPAddress();

                return new ConnectionInfo(
                    publicIPAddress is not null ? publicIPAddress.Fqdn : linuxVm.GetPrimaryNetworkInterface().PrimaryPrivateIP,
                    vmUsername,
                    new PasswordAuthenticationMethod(vmUsername, vmPassword));
            }

            Task WaitForSshConnectivityAsync(ConnectionInfo sshConnectionInfo)
            {
                var timeout = System.TimeSpan.FromMinutes(10);

                return Execute(
                    $"Waiting for VM to accept SSH connections at {sshConnectionInfo.Host}...",
                    async () =>
                    {
                        var startTime = DateTime.UtcNow;

                        while (!cts.IsCancellationRequested)
                        {
                            try
                            {
                                using var sshClient = new SshClient(sshConnectionInfo);
                                sshClient.ConnectWithRetries();
                                sshClient.Disconnect();
                            }
                            catch (SshAuthenticationException ex) when (ex.Message.StartsWith("Permission"))
                            {
                                throw new ValidationException($"Could not connect to VM '{sshConnectionInfo.Host}'. Reason: {ex.Message}", false);
                            }
                            catch
                            {
                                if (DateTime.UtcNow.Subtract(startTime) > timeout)
                                {
                                    throw new Exception("Timeout occurred while waiting for VM to accept SSH connections");
                                }
                                else
                                {
                                    await Task.Delay(System.TimeSpan.FromSeconds(5), cts.Token);
                                    continue;
                                }
                            }

                            break;
                        }
                    },
                    cts);
            }

            static async Task<(string Output, string Error, int ExitStatus)> ExecuteCommandOnVirtualMachineAsync(ConnectionInfo sshConnectionInfo, string command)
            {
                using var sshClient = new SshClient(sshConnectionInfo);
                sshClient.ConnectWithRetries();
                var (output, error, exitStatus) = await sshClient.ExecuteCommandAsync(command);
                sshClient.Disconnect();

                return (output, error, exitStatus);
            }

            static Task<(string Output, string Error, int ExitStatus)> ExecuteCommandOnVirtualMachineWithRetriesAsync(ConnectionInfo sshConnectionInfo, string command)
                => sshCommandRetryPolicy.ExecuteAsync(() => ExecuteCommandOnVirtualMachineAsync(sshConnectionInfo, command));
        }
        #endregion

        private CromwellOnAzure(CancellationTokenSource cts, TokenCredentials tokenCredentials, BatchAccount batchAccount, string subscriptionId, IStorageAccount storageAccount)
        {
            ArgumentNullException.ThrowIfNull(cts);
            ArgumentNullException.ThrowIfNull(tokenCredentials);
            ArgumentNullException.ThrowIfNull(batchAccount);
            ArgumentNullException.ThrowIfNull(subscriptionId);
            ArgumentNullException.ThrowIfNull(storageAccount);

            this.cts = cts;
            this.tokenCredentials = tokenCredentials;
            this.batchAccount = batchAccount;
            this.subscriptionId = subscriptionId;
            this.storageAccount = storageAccount;
        }

        #region Public Methods
        public async ValueTask<HostConfig> GetHostConfig()
        {
            string? configText;
            try
            {
                configText = await DownloadTextFromStorageAccountAsync(storageAccount, ConfigurationContainerName, HostConfigConfigurationFileName, cts);
            }
            catch
            {
                configText = default;
            }

            return Updater.ReadJson(string.IsNullOrWhiteSpace(configText) ? default : new StringReader(configText), () => new HostConfig());
        }

        public async ValueTask WriteHostConfig(HostConfig config)
            => await UploadTextToStorageAccountAsync(storageAccount, ConfigurationContainerName, HostConfigConfigurationFileName, Updater.WriteJson(config));

        public async ValueTask AddApplications(IEnumerable<(string Name, string Version, Func<Stream> Open)> packages)
            => await Execute(
                "Adding Batch Applications.",
                async () =>
                {
                    var resourceGroupRegex = new Regex("/*/resourceGroups/([^/]*)/*");
                    var batchManagementClient = new BatchManagementClient(tokenCredentials) { SubscriptionId = subscriptionId };
                    var resourceGroupName = resourceGroupRegex.Match(batchAccount.Id).Groups[1].Value;
                    var list = packages.ToList();

                    list.Select(t => t.Name).Distinct(StringComparer.OrdinalIgnoreCase).ForEach(async app =>
                    {
                        if (!await (await batchManagementClient.Application.ListAsync(resourceGroupName, batchAccount.Name))
                            .ToAsyncEnumerable(listNextAsync: new(async (link, ct) => await batchManagementClient.Application.ListNextAsync(link, ct)))
                            .AnyAsync(a => a.Name.Equals(app, StringComparison.OrdinalIgnoreCase), cts.Token))
                        {
                            _ = await batchManagementClient.Application.CreateAsync(resourceGroupName, batchAccount.Name, app);
                        }
                    });

                    await Task.WhenAll(list.Select(async app =>
                    {
                        var applicationPackage = await batchManagementClient.ApplicationPackage.CreateAsync(resourceGroupName, batchAccount.Name, app.Name, app.Version);
                        using var package = app.Open();
                        await new CloudBlockBlob(new Uri(applicationPackage.StorageUrl, UriKind.Absolute)).UploadFromStreamAsync(package);
                        _ = await batchManagementClient.ApplicationPackage.ActivateAsync(resourceGroupName, batchAccount.Name, app.Name, app.Version, "zip");
                        _ = (await batchManagementClient.Application.UpdateAsync(resourceGroupName, batchAccount.Name, app.Name, new Application(allowUpdates: false))).Id;
                    }).ToArray());
                });

        public async ValueTask RemoveApplications(IEnumerable<(string Name, string Version)> packages)
            => await Execute(
                "Removing Batch Applications.",
                async () =>
                {
                    var resourceGroupRegex = new Regex("/*/resourceGroups/([^/]*)/*");
                    var batchManagementClient = new BatchManagementClient(tokenCredentials) { SubscriptionId = subscriptionId };
                    var resourceGroupName = resourceGroupRegex.Match(batchAccount.Id).Groups[1].Value;
                    var list = packages.ToList();

                    await Task.WhenAll(list.Select(async app =>
                    {
                        await batchManagementClient.ApplicationPackage.DeleteAsync(resourceGroupName, batchAccount.Name, app.Name, app.Version);
                    }).ToArray());

                    list.Select(t => t.Name).Distinct(StringComparer.OrdinalIgnoreCase).ForEach(async app =>
                    {
                        if (!await (await batchManagementClient.ApplicationPackage.ListAsync(resourceGroupName, batchAccount.Name, app))
                            .ToAsyncEnumerable(listNextAsync: new Func<string, CancellationToken, Task<IPage<ApplicationPackage>>>(async (link, ct) => await batchManagementClient.ApplicationPackage.ListNextAsync(link, ct)))
                            .AnyAsync(cts.Token))
                        {
                            await batchManagementClient.Application.DeleteAsync(resourceGroupName, batchAccount.Name, app);
                        }
                    });
                });

        public async ValueTask AddHostConfigBlobs(IEnumerable<(string Hash, Func<Stream> Open)> blobs)
            => await Execute(
                "Writing host configuration resource blobs (task scripts).",
                async () =>
                {
                    var blobClient = await GetBlobClientAsync(storageAccount);
                    var container = blobClient.GetBlobContainerClient(HostConfigBlobsContainerName);
                    await container.CreateIfNotExistsAsync();
                    await Task.WhenAll(blobs.Select(async start =>
                    {
                        using var stream = start.Open();
                        await container.GetBlobClient(start.Hash).UploadAsync(stream, cts.Token);
                    }).ToArray());
                });

        public async ValueTask RemoveHostConfigBlobs(IEnumerable<string> blobs)
            => await Execute(
                "Removing host configuration resource blobs (task scripts).",
                async () =>
                {
                    var blobClient = await GetBlobClientAsync(storageAccount);
                    var container = blobClient.GetBlobContainerClient(HostConfigBlobsContainerName);
                    await Task.WhenAll(blobs.Select(async hash =>
                            await container.GetBlobClient(hash).DeleteAsync(snapshotsOption: Azure.Storage.Blobs.Models.DeleteSnapshotsOption.IncludeSnapshots, cancellationToken: cts.Token))
                        .ToArray());
                });
        #endregion

        #region Implementation
        private Task Execute(string message, Func<Task> func)
            => Execute(message, func, cts);

        private Task<T> Execute<T>(string message, Func<Task<T>> func)
            => Execute(message, func, cts);

        private static Task Execute(string message, Func<Task> func, CancellationTokenSource cts)
            => Execute(message, async () => { await func(); return false; }, cts);

        private static async Task<T> Execute<T>(string message, Func<Task<T>> func, CancellationTokenSource cts)
        {
            const int retryCount = 3;

            var startTime = DateTime.UtcNow;
            var line = ConsoleEx.WriteLine(message);

            for (var i = 0; i < retryCount; i++)
            {
                try
                {
                    cts.Token.ThrowIfCancellationRequested();
                    var result = await func();
                    WriteExecutionTime(line, startTime);
                    return result;
                }
                catch (CloudException cloudException) when (cloudException.ToCloudErrorType() == CloudErrorType.ExpiredAuthenticationToken)
                {
                }
                catch (OperationCanceledException) when (cts.Token.IsCancellationRequested)
                {
                    line.Write(" Cancelled", ConsoleColor.Red);
                    return await Task.FromCanceled<T>(cts.Token);
                }
                catch (Exception ex)
                {
                    line.Write($" Failed. {ex.GetType().Name}: {ex.Message}", ConsoleColor.Red);
                    cts.Cancel();
                    throw;
                }
            }

            line.Write($" Failed", ConsoleColor.Red);
            cts.Cancel();
            throw new Exception($"Failed after {retryCount} attempts");
        }

        private static void WriteExecutionTime(ConsoleEx.Line line, DateTime startTime)
            => line.Write($" Completed in {DateTime.UtcNow.Subtract(startTime).TotalSeconds:n0}s", ConsoleColor.Green);

        private static async Task<string> DownloadTextFromStorageAccountAsync(IStorageAccount storageAccount, string containerName, string blobName, CancellationTokenSource cts)
        {
            var blobClient = await GetBlobClientAsync(storageAccount);
            var container = blobClient.GetBlobContainerClient(containerName);

            return (await container.GetBlobClient(blobName).DownloadContentAsync(cts.Token)).Value.Content.ToString();
        }

        private Task UploadTextToStorageAccountAsync(IStorageAccount storageAccount, string containerName, string blobName, string content)
            => UploadBinaryToStorageAccountAsync(storageAccount, containerName, blobName, BinaryData.FromString(content));

        private async Task UploadBinaryToStorageAccountAsync(IStorageAccount storageAccount, string containerName, string blobName, BinaryData content)
        {
            var blobClient = await GetBlobClientAsync(storageAccount);
            var container = blobClient.GetBlobContainerClient(containerName);

            await container.CreateIfNotExistsAsync();
            await container.GetBlobClient(blobName).UploadAsync(content, true, cts.Token);
        }

        private static async Task<BlobServiceClient> GetBlobClientAsync(IStorageAccount storageAccount)
            => new(
                new Uri($"https://{storageAccount.Name}.blob.core.windows.net"),
                new StorageSharedKeyCredential(
                    storageAccount.Name,
                    (await storageAccount.GetKeysAsync())[0].Value));

        private class ValidationException : Exception
        {
            public string Reason { get; set; }
            public bool DisplayExample { get; set; }

            public ValidationException(string reason, bool displayExample = true)
            {
                Reason = reason;
                DisplayExample = displayExample;
            }
        }
        #endregion
    }
}
