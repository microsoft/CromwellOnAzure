// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IdentityModel.Tokens.Jwt;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage;
using Azure.Storage.Blobs;
using Microsoft.Azure.Management.Batch;
using Microsoft.Azure.Management.Batch.Models;
using Microsoft.Azure.Management.Compute.Fluent;
using Microsoft.Azure.Management.Compute.Fluent.Models;
using Microsoft.Azure.Management.ContainerRegistry.Fluent;
using Microsoft.Azure.Management.CosmosDB.Fluent;
using Microsoft.Azure.Management.CosmosDB.Fluent.Models;
using Microsoft.Azure.Management.Fluent;
using Microsoft.Azure.Management.Graph.RBAC.Fluent;
using Microsoft.Azure.Management.Graph.RBAC.Fluent.Models;
using Microsoft.Azure.Management.Msi.Fluent;
using Microsoft.Azure.Management.Network.Fluent;
using Microsoft.Azure.Management.Network.Fluent.Models;
using Microsoft.Azure.Management.PostgreSQL.FlexibleServers;
using Microsoft.Azure.Management.PostgreSQL.FlexibleServers.Models;
using Microsoft.Azure.Management.PrivateDns.Fluent;
using Microsoft.Azure.Management.ResourceGraph;
using Microsoft.Azure.Management.ResourceGraph.Models;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Microsoft.Azure.Management.Storage.Fluent;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Rest;
using Microsoft.Rest.Azure.OData;
using Newtonsoft.Json;
using Polly;
using Polly.Retry;
using Renci.SshNet;
using Renci.SshNet.Common;
using Common;
using System.Net;

using Sku = Microsoft.Azure.Management.PostgreSQL.FlexibleServers.Models.Sku;

namespace CromwellOnAzureDeployer
{
    public class Deployer
    {
        private static readonly AsyncRetryPolicy roleAssignmentHashConflictRetryPolicy = Policy
            .Handle<Microsoft.Rest.Azure.CloudException>(cloudException => cloudException.Body.Code.Equals("HashConflictOnDifferentRoleAssignmentIds"))
            .RetryAsync();

        private static readonly AsyncRetryPolicy sshCommandRetryPolicy = Policy
            .Handle<Exception>(ex => !(ex is SshAuthenticationException && ex.Message.StartsWith("Permission")))
            .WaitAndRetryAsync(5, retryAttempt => TimeSpan.FromSeconds(5));

        private const string WorkflowsContainerName = "workflows";
        private const string ConfigurationContainerName = "configuration";
        private const string CromwellConfigurationFileName = "cromwell-application.conf";
        private const string ContainersToMountFileName = "containers-to-mount";
        private const string AllowedVmSizesFileName = "allowed-vm-sizes";
        private const string InputsContainerName = "inputs";
        private const string CromwellAzureRootDir = "/data/cromwellazure";
        private const string CromwellAzureRootDirSymLink = "/cromwellazure";    // This path is present in all CoA versions

        private const string SshNsgRuleName = "SSH";

        private readonly CancellationTokenSource cts = new();

        private readonly List<string> requiredResourceProviders = new()
        {
                "Microsoft.Authorization",
                "Microsoft.Batch",
                "Microsoft.Compute",
                "Microsoft.DocumentDB",
                "Microsoft.OperationalInsights",
                "Microsoft.insights",
                "Microsoft.Network",
                "Microsoft.Storage"
            };

        private Configuration configuration { get; set; }
        private TokenCredentials tokenCredentials;
        private IAzure azureSubscriptionClient { get; set; }
        private Microsoft.Azure.Management.Fluent.Azure.IAuthenticated azureClient { get; set; }
        private IResourceManager resourceManagerClient { get; set; }
        private AzureCredentials azureCredentials { get; set; }
        private IPostgreSQLManagementClient postgreSqlManagementClient { get; set; }
        private IEnumerable<string> subscriptionIds { get; set; }
        private bool SkipBillingReaderRoleAssignment { get; set; }
        private bool isResourceGroupCreated { get; set; }

        public Deployer(Configuration configuration)
        {
            this.configuration = configuration;
        }

        public async Task<int> DeployAsync()
        {
            var mainTimer = Stopwatch.StartNew();

            try
            {
                ValidateInitialCommandLineArgsAsync();

                ConsoleEx.WriteLine("Running...");

                await ValidateTokenProviderAsync();

                tokenCredentials = new TokenCredentials(new RefreshableAzureServiceTokenProvider("https://management.azure.com/"));
                azureCredentials = new AzureCredentials(tokenCredentials, null, null, AzureEnvironment.AzureGlobalCloud);
                azureClient = GetAzureClient(azureCredentials);
                azureSubscriptionClient = azureClient.WithSubscription(configuration.SubscriptionId);
                subscriptionIds = (await azureClient.Subscriptions.ListAsync()).Select(s => s.SubscriptionId);
                resourceManagerClient = GetResourceManagerClient(azureCredentials);
                postgreSqlManagementClient = new PostgreSQLManagementClient(azureCredentials) { SubscriptionId = configuration.SubscriptionId };

                await ValidateSubscriptionAndResourceGroupAsync(configuration);

                IResourceGroup resourceGroup = null;
                BatchAccount batchAccount = null;
                IGenericResource logAnalyticsWorkspace = null;
                IGenericResource appInsights = null;
                ICosmosDBAccount cosmosDb = null;
                Server postgreSqlServer = null;
                IStorageAccount storageAccount = null;
                IVirtualMachine linuxVm = null;
                INetworkSecurityGroup networkSecurityGroup = null;
                IIdentity managedIdentity = null;
                ConnectionInfo sshConnectionInfo = null;
                IPrivateDnsZone postgreSqlDnsZone = null;

                try
                {
                    if (configuration.Update)
                    {
                        resourceGroup = await azureSubscriptionClient.ResourceGroups.GetByNameAsync(configuration.ResourceGroupName);

                        var targetVersion = Utility.DelimitedTextToDictionary(Utility.GetFileContent("scripts", "env-00-coa-version.txt")).GetValueOrDefault("CromwellOnAzureVersion");

                        ConsoleEx.WriteLine($"Upgrading Cromwell on Azure instance in resource group '{resourceGroup.Name}' to version {targetVersion}...");

                        var existingVms = await azureSubscriptionClient.VirtualMachines.ListByResourceGroupAsync(configuration.ResourceGroupName);

                        if (!existingVms.Any())
                        {
                            throw new ValidationException($"Update was requested but resource group {configuration.ResourceGroupName} does not contain any virtual machines.");
                        }

                        if (existingVms.Count() > 1 && string.IsNullOrWhiteSpace(configuration.VmName))
                        {
                            throw new ValidationException($"Resource group {configuration.ResourceGroupName} contains multiple virtual machines. {nameof(configuration.VmName)} must be provided.");
                        }

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

                        networkSecurityGroup = (await azureSubscriptionClient.NetworkSecurityGroups.ListByResourceGroupAsync(configuration.ResourceGroupName)).FirstOrDefault(g => g.NetworkInterfaceIds.Contains(linuxVm.GetPrimaryNetworkInterface().Id));

                        if (!configuration.PrivateNetworking.GetValueOrDefault())
                        {
                            if (networkSecurityGroup is null)
                            {
                                if (string.IsNullOrWhiteSpace(configuration.NetworkSecurityGroupName))
                                {
                                    configuration.NetworkSecurityGroupName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}", 15);
                                }

                                networkSecurityGroup = await CreateNetworkSecurityGroupAsync(resourceGroup, configuration.NetworkSecurityGroupName);
                                await AssociateNicWithNetworkSecurityGroupAsync(linuxVm.GetPrimaryNetworkInterface(), networkSecurityGroup);
                            }

                            await EnableSsh(networkSecurityGroup);
                        }



                        sshConnectionInfo = GetSshConnectionInfo(linuxVm, configuration.VmUsername, configuration.VmPassword);

                        await WaitForSshConnectivityAsync(sshConnectionInfo);

                        await ConfigureVmAsync(sshConnectionInfo, managedIdentity);

                        var accountNames = Utility.DelimitedTextToDictionary((await ExecuteCommandOnVirtualMachineWithRetriesAsync(sshConnectionInfo, $"cat {CromwellAzureRootDir}/env-01-account-names.txt || echo ''")).Output);

                        if (!accountNames.Any())
                        {
                            throw new ValidationException($"Could not retrieve account names from virtual machine {configuration.VmName}.");
                        }

                        if (!accountNames.TryGetValue("ManagedIdentityClientId", out var existingUserManagedIdentity))
                        {
                            managedIdentity = await ReplaceSystemManagedIdentityWithUserManagedIdentityAsync(resourceGroup, linuxVm);
                        }
                        else
                        {
                            managedIdentity = await linuxVm.UserAssignedManagedServiceIdentityIds.Select(GetIdentityByIdAsync).FirstOrDefault(MatchesClientId);

                            if (managedIdentity is null)
                            {
                                throw new ValidationException($"The managed identity, referenced by the VM configuration retrieved from virtual machine {configuration.VmName}, does not exist or is not accessible to the current user. ");
                            }

                            Task<IIdentity> GetIdentityByIdAsync(string id) => azureSubscriptionClient.Identities.GetByIdAsync(id);

                            bool MatchesClientId(Task<IIdentity> identity)
                            {
                                try
                                {
                                    return existingUserManagedIdentity.Equals(identity.Result.ClientId, StringComparison.OrdinalIgnoreCase);
                                }
                                catch
                                {
                                    _ = identity.Exception;
                                    return false;
                                }
                            }
                        }

                        if (!accountNames.TryGetValue("BatchAccountName", out var batchAccountName))
                        {
                            throw new ValidationException($"Could not retrieve the Batch account name from virtual machine {configuration.VmName}.");
                        }

                        batchAccount = await GetExistingBatchAccountAsync(batchAccountName)
                            ?? throw new ValidationException($"Batch account {batchAccountName}, referenced by the VM configuration, does not exist in region {configuration.RegionName} or is not accessible to the current user.");

                        configuration.BatchAccountName = batchAccountName;

                        if (!accountNames.TryGetValue("DefaultStorageAccountName", out var storageAccountName))
                        {
                            throw new ValidationException($"Could not retrieve the default storage account name from virtual machine {configuration.VmName}.");
                        }

                        storageAccount = await GetExistingStorageAccountAsync(storageAccountName)
                            ?? throw new ValidationException($"Storage account {storageAccountName}, referenced by the VM configuration, does not exist in region {configuration.RegionName} or is not accessible to the current user.");

                        configuration.StorageAccountName = storageAccountName;

                        if (!accountNames.TryGetValue("CosmosDbAccountName", out var cosmosDbAccountName))
                        {
                            throw new ValidationException($"Could not retrieve the CosmosDb account name from virtual machine {configuration.VmName}.");
                        }

                        cosmosDb = (await azureSubscriptionClient.CosmosDBAccounts.ListByResourceGroupAsync(configuration.ResourceGroupName))
                            .FirstOrDefault(a => a.Name.Equals(cosmosDbAccountName, StringComparison.OrdinalIgnoreCase))
                                ?? throw new ValidationException($"CosmosDb account {cosmosDbAccountName} does not exist in resource group {configuration.ResourceGroupName}.");

                        configuration.CosmosDbAccountName = cosmosDbAccountName;

                        await WriteNonPersonalizedFilesToStorageAccountAsync(storageAccount);

                        // Note: Current behavior is to block switching from Docker MySQL to Azure PostgreSql on Update.
                        // However we do ancitipate including this change, this code is here to facilitate this future behavior.
                        configuration.PostgreSqlServerName = accountNames.GetValueOrDefault("PostgreSqlServerName");

                        var installedVersion = await GetInstalledCromwellOnAzureVersionAsync(sshConnectionInfo);

                        if (installedVersion is null)
                        {
                            // If upgrading from pre-2.1 version, patch the installed Cromwell configuration file (disable call caching and default to preemptible)
                            await PatchCromwellConfigurationFileV200Async(storageAccount);
                            await SetCosmosDbContainerAutoScaleAsync(cosmosDb);
                        }

                        if (installedVersion is null || installedVersion < new Version(2, 1))
                        {
                            await PatchContainersToMountFileV210Async(storageAccount, managedIdentity.Name);
                        }

                        if (installedVersion is null || installedVersion < new Version(2, 2))
                        {
                            await PatchContainersToMountFileV220Async(storageAccount);
                        }

                        if (installedVersion is null || installedVersion < new Version(2, 4))
                        {
                            await PatchContainersToMountFileV240Async(storageAccount);
                            await PatchAccountNamesFileV240Async(sshConnectionInfo, managedIdentity);
                        }

                        if (installedVersion is null || installedVersion < new Version(2, 5))
                        {
                            await MitigateChaosDbV250Async(cosmosDb);
                        }

                        if (installedVersion is null || installedVersion < new Version(3, 0))
                        {
                            await PatchCromwellConfigurationFileV300Async(storageAccount);
                            await AddNewSettingsV300Async(sshConnectionInfo);
                            await UpgradeBlobfuseV300Async(sshConnectionInfo);
                            await DisableDockerServiceV300Async(sshConnectionInfo);

                            ConsoleEx.WriteLine($"It's recommended to update the default CoA storage account to a General Purpose v2 account.", ConsoleColor.Yellow);
                            ConsoleEx.WriteLine($"To do that, navigate to the storage account in the Azure Portal,", ConsoleColor.Yellow);
                            ConsoleEx.WriteLine($"Configuration tab, and click 'Upgrade.'", ConsoleColor.Yellow);
                        }

                        if (installedVersion is null || installedVersion < new Version(3, 1))
                        {
                            await PatchCromwellConfigurationFileV310Async(storageAccount);
                        }
                    }

                    if (!configuration.Update)
                    {
                        ValidateRegionName(configuration.RegionName);
                        ValidateMainIdentifierPrefix(configuration.MainIdentifierPrefix);
                        storageAccount = await ValidateAndGetExistingStorageAccountAsync();
                        batchAccount = await ValidateAndGetExistingBatchAccountAsync();

                        // Configuration preferences not currently settable by user.
                        configuration.PostgreSqlServerName = configuration.ProvisionPostgreSqlOnAzure.GetValueOrDefault() ? SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}-", 15) : String.Empty;
                        configuration.PostgreSqlAdministratorPassword = Utility.GeneratePassword();
                        configuration.PostgreSqlUserPassword = Utility.GeneratePassword();

                        if (string.IsNullOrWhiteSpace(configuration.BatchAccountName))
                        {
                            configuration.BatchAccountName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}", 15);
                        }

                        if (string.IsNullOrWhiteSpace(configuration.StorageAccountName))
                        {
                            configuration.StorageAccountName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}", 24);
                        }

                        if (string.IsNullOrWhiteSpace(configuration.NetworkSecurityGroupName))
                        {
                            configuration.NetworkSecurityGroupName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}", 15);
                        }

                        if (string.IsNullOrWhiteSpace(configuration.CosmosDbAccountName))
                        {
                            configuration.CosmosDbAccountName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}-", 15);
                        }

                        if (string.IsNullOrWhiteSpace(configuration.ApplicationInsightsAccountName))
                        {
                            configuration.ApplicationInsightsAccountName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}-", 15);
                        }

                        if (string.IsNullOrWhiteSpace(configuration.VmName))
                        {
                            configuration.VmName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}-", 25);
                        }

                        if (string.IsNullOrWhiteSpace(configuration.VmPassword))
                        {
                            configuration.VmPassword = Utility.GeneratePassword();
                        }

                        await RegisterResourceProvidersAsync();
                        await ValidateVmAsync();

                        if (batchAccount is null)
                        {
                            await ValidateBatchAccountQuotaAsync();
                        }

                        var vnetAndSubnet = await ValidateAndGetExistingVirtualNetworkAsync();

                        ConsoleEx.WriteLine();
                        ConsoleEx.WriteLine($"VM host: {configuration.VmName}.{configuration.RegionName}.cloudapp.azure.com");
                        ConsoleEx.WriteLine($"VM username: {configuration.VmUsername}");
                        ConsoleEx.WriteLine($"VM password: {configuration.VmPassword}");
                        ConsoleEx.WriteLine();

                        if (string.IsNullOrWhiteSpace(configuration.ResourceGroupName))
                        {
                            configuration.ResourceGroupName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}-", 15);
                            resourceGroup = await CreateResourceGroupAsync();
                            isResourceGroupCreated = true;
                        }
                        else
                        {
                            resourceGroup = await azureSubscriptionClient.ResourceGroups.GetByNameAsync(configuration.ResourceGroupName);
                        }

                        managedIdentity = await CreateUserManagedIdentityAsync(resourceGroup);

                        if (vnetAndSubnet is not null)
                        {
                            ConsoleEx.WriteLine($"Creating VM in existing virtual network {vnetAndSubnet.Value.virtualNetwork.Name} and subnet {vnetAndSubnet.Value.vmSubnet.Name}");
                        }

                        if (storageAccount is not null)
                        {
                            ConsoleEx.WriteLine($"Using existing Storage Account {storageAccount.Name}");
                        }
                        
                        if (batchAccount is not null)
                        {
                            ConsoleEx.WriteLine($"Using existing Batch Account {batchAccount.Name}");
                        }

                        if (vnetAndSubnet is null)
                        {
                            configuration.VnetName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}-", 15);
                            configuration.PostgreSqlSubnetName = String.IsNullOrEmpty(configuration.PostgreSqlSubnetName) && configuration.ProvisionPostgreSqlOnAzure.GetValueOrDefault() ? configuration.DefaultPostgreSqlSubnetName : configuration.PostgreSqlSubnetName;
                            configuration.VmSubnetName = String.IsNullOrEmpty(configuration.VmSubnetName) ? configuration.DefaultVmSubnetName : configuration.VmSubnetName;
                            vnetAndSubnet = await CreateVnetAndSubnetsAsync(resourceGroup);
                        }

                        if (string.IsNullOrWhiteSpace(configuration.LogAnalyticsArmId))
                        {
                            var workspaceName = SdkContext.RandomResourceName(configuration.MainIdentifierPrefix, 15);
                            logAnalyticsWorkspace = await CreateLogAnalyticsWorkspaceResourceAsync(workspaceName);
                            configuration.LogAnalyticsArmId = logAnalyticsWorkspace.Id;
                        }

                        if (configuration.ProvisionPostgreSqlOnAzure.GetValueOrDefault())
                        {
                            postgreSqlDnsZone = await CreatePrivateDnsZoneAsync(vnetAndSubnet.Value.virtualNetwork);
                        }

                        await Task.WhenAll(new Task[]
                        {
                            Task.Run(async () => appInsights = await CreateAppInsightsResourceAsync(configuration.LogAnalyticsArmId)),
                            Task.Run(async () => cosmosDb = await CreateCosmosDbAsync()),
                            Task.Run(async () => { 
                                if(configuration.ProvisionPostgreSqlOnAzure== true) { postgreSqlServer = await CreatePostgreSqlServerAndDatabaseAsync(postgreSqlManagementClient, vnetAndSubnet.Value.postgreSqlSubnet, postgreSqlDnsZone); }
                            }),

                            Task.Run(async () =>
                            {
                                storageAccount ??= await CreateStorageAccountAsync();

                                await Task.WhenAll(new Task[]
                                {
                                    Task.Run(async () => batchAccount ??= await CreateBatchAccountAsync(storageAccount.Id)),
                                    Task.Run(async () =>
                                    {
                                        await CreateDefaultStorageContainersAsync(storageAccount);
                                        await WriteNonPersonalizedFilesToStorageAccountAsync(storageAccount);
                                        await WritePersonalizedFilesToStorageAccountAsync(storageAccount, managedIdentity.Name);
                                    })
                                });
                            }),

                            Task.Run(() => CreateVirtualMachineAsync(managedIdentity, vnetAndSubnet?.virtualNetwork, vnetAndSubnet?.vmSubnet.Name)
                                .ContinueWith(async t =>
                                    {
                                        linuxVm = t.Result;

                                        if (!configuration.PrivateNetworking.GetValueOrDefault())
                                        {
                                            networkSecurityGroup = await CreateNetworkSecurityGroupAsync(resourceGroup, configuration.NetworkSecurityGroupName);
                                            await AssociateNicWithNetworkSecurityGroupAsync(linuxVm.GetPrimaryNetworkInterface(), networkSecurityGroup);
                                        }

                                        sshConnectionInfo = GetSshConnectionInfo(linuxVm, configuration.VmUsername, configuration.VmPassword);
                                        await WaitForSshConnectivityAsync(sshConnectionInfo);
                                        await ConfigureVmAsync(sshConnectionInfo, managedIdentity);
                                    },
                                    TaskContinuationOptions.OnlyOnRanToCompletion)
                                .Unwrap())
                        });

                        await AssignVmAsContributorToAppInsightsAsync(managedIdentity, appInsights);
                        await AssignVmAsContributorToCosmosDb(managedIdentity, cosmosDb);
                        await AssignVmAsContributorToBatchAccountAsync(managedIdentity, batchAccount);
                        await AssignVmAsContributorToStorageAccountAsync(managedIdentity, storageAccount);
                        await AssignVmAsDataReaderToStorageAccountAsync(managedIdentity, storageAccount);

                        if (!SkipBillingReaderRoleAssignment)
                        {
                            await AssignVmAsBillingReaderToSubscriptionAsync(managedIdentity);
                        }

                        if (configuration.ProvisionPostgreSqlOnAzure.GetValueOrDefault())
                        {
                            await CreatePostgreSqlDatabaseUser(sshConnectionInfo);
                        }
                    }

                    await WriteCoaVersionToVmAsync(sshConnectionInfo);
                    await RebootVmAsync(sshConnectionInfo);
                    await WaitForSshConnectivityAsync(sshConnectionInfo);

                    if (!await IsStartupSuccessfulAsync(sshConnectionInfo))
                    {
                        ConsoleEx.WriteLine($"Startup script on the VM failed. Check {CromwellAzureRootDir}/startup.log for details", ConsoleColor.Red);
                        return 1;
                    }

                    if (await MountWarningsExistAsync(sshConnectionInfo))
                    {
                        ConsoleEx.WriteLine($"Found warnings in {CromwellAzureRootDir}/mount.blobfuse.log. Some storage containers may have failed to mount on the VM. Check the file for details.", ConsoleColor.Yellow);
                    }

                    await WaitForDockerComposeAsync(sshConnectionInfo);
                    await WaitForCromwellAsync(sshConnectionInfo);
                }
                finally
                {
                    if (!configuration.KeepSshPortOpen.GetValueOrDefault())
                    {
                        await DisableSsh(networkSecurityGroup);
                    }
                }

                var maxPerFamilyQuota = batchAccount.DedicatedCoreQuotaPerVMFamilyEnforced ? batchAccount.DedicatedCoreQuotaPerVMFamily.Select(q => q.CoreQuota).Where(q => 0 != q) : Enumerable.Repeat(batchAccount.DedicatedCoreQuota ?? 0, 1);
                var isBatchQuotaAvailable = batchAccount.LowPriorityCoreQuota > 0 || (batchAccount.DedicatedCoreQuota > 0 && maxPerFamilyQuota.Append(0).Max() > 0);

                int exitCode;

                if (isBatchQuotaAvailable)
                {
                    if (configuration.SkipTestWorkflow)
                    {
                        exitCode = 0;
                    }
                    else
                    {
                        var isTestWorkflowSuccessful = await RunTestWorkflow(storageAccount, usePreemptibleVm: batchAccount.LowPriorityCoreQuota > 0);

                        if (!isTestWorkflowSuccessful)
                        {
                            await DeleteResourceGroupIfUserConsentsAsync();
                        }

                        exitCode = isTestWorkflowSuccessful ? 0 : 1;
                    }
                }
                else
                {
                    if (!configuration.SkipTestWorkflow)
                    {
                        ConsoleEx.WriteLine($"Could not run the test workflow.", ConsoleColor.Yellow);
                    }

                    ConsoleEx.WriteLine($"Deployment was successful, but Batch account {configuration.BatchAccountName} does not have sufficient core quota to run workflows.", ConsoleColor.Yellow);
                    ConsoleEx.WriteLine($"Request Batch core quota: https://docs.microsoft.com/en-us/azure/batch/batch-quota-limit", ConsoleColor.Yellow);
                    ConsoleEx.WriteLine($"After receiving the quota, read the docs to run a test workflow and confirm successful deployment.", ConsoleColor.Yellow);
                    exitCode = 2;
                }

                ConsoleEx.WriteLine($"Completed in {mainTimer.Elapsed.TotalMinutes:n1} minutes.");

                return exitCode;
            }
            catch (ValidationException validationException)
            {
                DisplayValidationExceptionAndExit(validationException);
                return 1;
            }
            catch (Exception exc)
            {
                if (!(exc is OperationCanceledException && cts.Token.IsCancellationRequested))
                {
                    ConsoleEx.WriteLine();
                    ConsoleEx.WriteLine($"{exc.GetType().Name}: {exc.Message}", ConsoleColor.Red);
                }

                ConsoleEx.WriteLine();
                Debugger.Break();
                WriteGeneralRetryMessageToConsole();
                await DeleteResourceGroupIfUserConsentsAsync();
                return 1;
            }
        }

        private Task WaitForSshConnectivityAsync(ConnectionInfo sshConnectionInfo)
        {
            var timeout = TimeSpan.FromMinutes(10);

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
                                await Task.Delay(TimeSpan.FromSeconds(5), cts.Token);
                                continue;
                            }
                        }

                        break;
                    }
                });
        }

        private Task WaitForDockerComposeAsync(ConnectionInfo sshConnectionInfo)
            => Execute(
                "Waiting for docker containers to download and start...",
                async () =>
                {
                    while (!cts.IsCancellationRequested)
                    {
                        var totalNumberOfRunningDockerContainers = configuration.ProvisionPostgreSqlOnAzure.GetValueOrDefault() ? "3" : "4";
                        var (numberOfRunningContainers, _, _) = await ExecuteCommandOnVirtualMachineWithRetriesAsync(sshConnectionInfo, "sudo docker ps -a | grep -c 'Up ' || :");

                        if (numberOfRunningContainers == totalNumberOfRunningDockerContainers)
                        {
                            break;
                        }

                        await Task.Delay(5000, cts.Token);
                    }
                });

        private Task WaitForCromwellAsync(ConnectionInfo sshConnectionInfo)
            => Execute(
                "Waiting for Cromwell to perform one-time database preparation...",
                async () =>
                {
                    while (!cts.IsCancellationRequested)
                    {
                        var (isCromwellAvailable, _, _) = await ExecuteCommandOnVirtualMachineWithRetriesAsync(sshConnectionInfo, $"[ $(sudo docker logs cromwellazure_triggerservice_1 | grep -c '{AvailabilityTracker.GetAvailabilityMessage(Constants.CromwellSystemName)}') -gt 0 ] && echo 1 || echo 0");

                        if (isCromwellAvailable == "1")
                        {
                            break;
                        }

                        await Task.Delay(5000, cts.Token);
                    }
                });

        private Task<bool> IsStartupSuccessfulAsync(ConnectionInfo sshConnectionInfo)
            => Execute(
                "Waiting for startup script completion...",
                async () =>
                {
                    while (!cts.IsCancellationRequested)
                    {
                        var (startupLogContent, _, _) = await ExecuteCommandOnVirtualMachineWithRetriesAsync(sshConnectionInfo, $"cat {CromwellAzureRootDir}/startup.log || echo ''");

                        if (startupLogContent.Contains("Startup complete"))
                        {
                            return true;
                        }

                        if (startupLogContent.Contains("Startup failed"))
                        {
                            return false;
                        }

                        await Task.Delay(5000, cts.Token);
                    }

                    return false;
                });

        private static async Task<bool> MountWarningsExistAsync(ConnectionInfo sshConnectionInfo)
            => int.Parse((await ExecuteCommandOnVirtualMachineWithRetriesAsync(sshConnectionInfo, $"grep -c 'WARNING' {CromwellAzureRootDir}/mount.blobfuse.log || :")).Output) > 0;

        private static Microsoft.Azure.Management.Fluent.Azure.IAuthenticated GetAzureClient(AzureCredentials azureCredentials)
            => Microsoft.Azure.Management.Fluent.Azure
                .Configure()
                .WithLogLevel(HttpLoggingDelegatingHandler.Level.Basic)
                .Authenticate(azureCredentials);

        private IResourceManager GetResourceManagerClient(AzureCredentials azureCredentials)
            => ResourceManager
                .Configure()
                .WithLogLevel(HttpLoggingDelegatingHandler.Level.Basic)
                .Authenticate(azureCredentials)
                .WithSubscription(configuration.SubscriptionId);

        private async Task RegisterResourceProvidersAsync()
        {
            var unregisteredResourceProviders = await GetRequiredResourceProvidersNotRegisteredAsync();

            if (unregisteredResourceProviders.Count == 0)
            {
                return;
            }

            try
            {
                await Execute(
                    $"Registering resource providers...",
                    async () =>
                    {
                        await Task.WhenAll(
                            unregisteredResourceProviders.Select(rp =>
                                resourceManagerClient.Providers.RegisterAsync(rp))
                        );

                        // RP registration takes a few minutes; poll until done registering

                        while (!cts.IsCancellationRequested)
                        {
                            unregisteredResourceProviders = await GetRequiredResourceProvidersNotRegisteredAsync();

                            if (unregisteredResourceProviders.Count == 0)
                            {
                                break;
                            }

                            await Task.Delay(TimeSpan.FromSeconds(15));
                        }
                    });
            }
            catch (Microsoft.Rest.Azure.CloudException ex) when (ex.ToCloudErrorType() == CloudErrorType.AuthorizationFailed)
            {
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine("Unable to programatically register the required resource providers.", ConsoleColor.Red);
                ConsoleEx.WriteLine("This can happen if you don't have the Owner or Contributor role assignment for the subscription.", ConsoleColor.Red);
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine("Please contact the Owner or Contributor of your Azure subscription, and have them:", ConsoleColor.Yellow);
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine("1. Navigate to https://portal.azure.com", ConsoleColor.Yellow);
                ConsoleEx.WriteLine("2. Select Subscription -> Resource Providers", ConsoleColor.Yellow);
                ConsoleEx.WriteLine("3. Select each of the following and click Register:", ConsoleColor.Yellow);
                ConsoleEx.WriteLine();
                unregisteredResourceProviders.ForEach(rp => ConsoleEx.WriteLine($"- {rp}", ConsoleColor.Yellow));
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine("After completion, please re-attempt deployment.");

                Environment.Exit(1);
            }
        }

        private async Task<List<string>> GetRequiredResourceProvidersNotRegisteredAsync()
        {
            var cloudResourceProviders = await resourceManagerClient.Providers.ListAsync();

            var notRegisteredResourceProviders = requiredResourceProviders
                .Intersect(cloudResourceProviders
                    .Where(rp => !rp.RegistrationState.Equals("Registered", StringComparison.OrdinalIgnoreCase))
                    .Select(rp => rp.Namespace), StringComparer.OrdinalIgnoreCase)
                .ToList();

            return notRegisteredResourceProviders;
        }

        private async Task ConfigureVmAsync(ConnectionInfo sshConnectionInfo, IIdentity managedIdentity)
        {
            // If the user was added or password reset via Azure portal, assure that the user will not be prompted
            // for password on each command and make bash the default shell.
            await ExecuteCommandOnVirtualMachineAsync(sshConnectionInfo, $"echo '{configuration.VmPassword}' | sudo -S -p '' /bin/bash -c \"echo '{configuration.VmUsername} ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/z_{configuration.VmUsername}\"");
            await ExecuteCommandOnVirtualMachineAsync(sshConnectionInfo, $"sudo usermod --shell /bin/bash {configuration.VmUsername}");

            if (configuration.Update)
            {
                await ExecuteCommandOnVirtualMachineAsync(sshConnectionInfo, $"sudo docker-compose -f {CromwellAzureRootDirSymLink}/docker-compose.yml down");
            }

            await MountDataDiskOnTheVirtualMachineAsync(sshConnectionInfo);
            await ExecuteCommandOnVirtualMachineAsync(sshConnectionInfo, $"sudo mkdir -p {CromwellAzureRootDir} && sudo chown {configuration.VmUsername} {CromwellAzureRootDir} && sudo chmod ug=rwx,o= {CromwellAzureRootDir}");
            await WriteNonPersonalizedFilesToVmAsync(sshConnectionInfo);
            await RunInstallationScriptAsync(sshConnectionInfo);
            await HandleCustomImagesAsync(sshConnectionInfo);
            await HandleConfigurationPropertiesAsync(sshConnectionInfo);

            if (!configuration.Update)
            {
                await WritePersonalizedFilesToVmAsync(sshConnectionInfo, managedIdentity);
            }
        }

        private static async Task<Version> GetInstalledCromwellOnAzureVersionAsync(ConnectionInfo sshConnectionInfo)
        {
            var versionString = (await ExecuteCommandOnVirtualMachineWithRetriesAsync(sshConnectionInfo, $@"grep -sPo 'CromwellOnAzureVersion=\K(.*)$' {CromwellAzureRootDir}/env-00-coa-version.txt || :")).Output;

            return !string.IsNullOrEmpty(versionString) && Version.TryParse(versionString, out var version) ? version : null;
        }

        private Task MountDataDiskOnTheVirtualMachineAsync(ConnectionInfo sshConnectionInfo)
            => Execute(
                $"Mounting data disk to the VM...",
                async () =>
                {
                    await UploadFilesToVirtualMachineAsync(sshConnectionInfo, (Utility.GetFileContent("scripts", "mount-data-disk.sh"), $"/tmp/mount-data-disk.sh", true));
                    await ExecuteCommandOnVirtualMachineWithRetriesAsync(sshConnectionInfo, $"/tmp/mount-data-disk.sh");
                });

        private Task WriteNonPersonalizedFilesToVmAsync(ConnectionInfo sshConnectionInfo)
            => Execute(
                $"Writing files to the VM...",
                () => UploadFilesToVirtualMachineAsync(
                    sshConnectionInfo,
                    new[] {
                        (Utility.GetFileContent("scripts", "startup.sh"), $"{CromwellAzureRootDir}/startup.sh", true),
                        (Utility.GetFileContent("scripts", "wait-for-it.sh"), $"{CromwellAzureRootDir}/wait-for-it/wait-for-it.sh", true),
                        (Utility.GetFileContent("scripts", "install-cromwellazure.sh"), $"{CromwellAzureRootDir}/install-cromwellazure.sh", true),
                        (Utility.GetFileContent("scripts", "mount_containers.sh"), $"{CromwellAzureRootDir}/mount_containers.sh", true),
                        (Utility.GetFileContent("scripts", "env-02-internal-images.txt"), $"{CromwellAzureRootDir}/env-02-internal-images.txt", false),
                        (Utility.GetFileContent("scripts", "env-03-external-images.txt"), $"{CromwellAzureRootDir}/env-03-external-images.txt", false),
                        (Utility.GetFileContent("scripts", "docker-compose.yml"), $"{CromwellAzureRootDir}/docker-compose.yml", false),
                        (Utility.GetFileContent("scripts", "cromwellazure.service"), "/lib/systemd/system/cromwellazure.service", false),
                        (Utility.GetFileContent("scripts", "mount.blobfuse"), "/usr/sbin/mount.blobfuse", true)
                    }.Concat(!configuration.ProvisionPostgreSqlOnAzure.GetValueOrDefault()
                        ? new[] {
                        (Utility.GetFileContent("scripts", "env-12-local-my-sql-db.txt"), $"{CromwellAzureRootDir}/env-12-local-my-sql-db.txt", false),
                        (Utility.GetFileContent("scripts", "docker-compose.mysql.yml"), $"{CromwellAzureRootDir}/docker-compose.mysql.yml", false),
                        (Utility.GetFileContent("scripts", "mysql", "init-user.sql"), $"{CromwellAzureRootDir}/mysql-init/init-user.sql", false),
                        (Utility.GetFileContent("scripts", "mysql", "unlock-change-log.sql"), $"{CromwellAzureRootDir}/mysql-init/unlock-change-log.sql", false)
                        }
                        : Array.Empty<(string, string, bool)>())
                     .ToArray()
                    ));

        private Task WriteCoaVersionToVmAsync(ConnectionInfo sshConnectionInfo)
            => Execute(
                $"Writing CoA version file to the VM...",
                () => UploadFilesToVirtualMachineAsync(sshConnectionInfo, (Utility.GetFileContent("scripts", "env-00-coa-version.txt"), $"{CromwellAzureRootDir}/env-00-coa-version.txt", false)));

        private Task RunInstallationScriptAsync(ConnectionInfo sshConnectionInfo)
            => Execute(
                $"Running installation script on the VM...",
                async () =>
                {
                    await ExecuteCommandOnVirtualMachineAsync(sshConnectionInfo, $"sudo {CromwellAzureRootDir}/install-cromwellazure.sh");
                    await ExecuteCommandOnVirtualMachineAsync(sshConnectionInfo, $"sudo usermod -aG docker {configuration.VmUsername}");

                    if(configuration.ProvisionPostgreSqlOnAzure.GetValueOrDefault())
                    {
                        await ExecuteCommandOnVirtualMachineAsync(sshConnectionInfo, $"sudo apt install -y postgresql-client");
                    }
                });

        private async Task WritePersonalizedFilesToVmAsync(ConnectionInfo sshConnectionInfo, IIdentity managedIdentity)
            => await UploadFilesToVirtualMachineAsync(
                sshConnectionInfo,
                new[] {
                    (Utility.PersonalizeContent(new []
                    {
                        new Utility.ConfigReplaceTextItem("{DefaultStorageAccountName}", configuration.StorageAccountName),
                        new Utility.ConfigReplaceTextItem("{CosmosDbAccountName}", configuration.CosmosDbAccountName),
                        new Utility.ConfigReplaceTextItem("{BatchAccountName}", configuration.BatchAccountName),
                        new Utility.ConfigReplaceTextItem("{ApplicationInsightsAccountName}", configuration.ApplicationInsightsAccountName),
                        new Utility.ConfigReplaceTextItem("{ManagedIdentityClientId}", managedIdentity.ClientId),
                        new Utility.ConfigReplaceTextItem("{PostgreSqlServerName}", configuration.ProvisionPostgreSqlOnAzure.GetValueOrDefault() ? configuration.PostgreSqlServerName : String.Empty),
                    }, "scripts", "env-01-account-names.txt"),
                    $"{CromwellAzureRootDir}/env-01-account-names.txt", false),

                    (Utility.GetFileContent("scripts", "env-04-settings.txt"), $"{CromwellAzureRootDir}/env-04-settings.txt", false),

                    (Utility.PersonalizeContent(new []
                    {
                        new Utility.ConfigReplaceTextItem("{PostgreSqlDatabaseName}", configuration.PostgreSqlDatabaseName),
                        new Utility.ConfigReplaceTextItem("{PostgreSqlUserLogin}", configuration.PostgreSqlUserLogin),
                        new Utility.ConfigReplaceTextItem("{PostgreSqlUserPassword}", configuration.PostgreSqlUserPassword),
                    }, "scripts", "env-13-my-sql-db.txt"),
                    $"{CromwellAzureRootDir}/env-13-my-sql-db.txt", false),
                }.Concat(configuration.ProvisionPostgreSqlOnAzure.GetValueOrDefault()
                    ? new[] {
                        (Utility.PersonalizeContent(new []
                        {
                            new Utility.ConfigReplaceTextItem("{PostgreSqlDatabaseName}", configuration.PostgreSqlDatabaseName),
                            new Utility.ConfigReplaceTextItem("{PostgreSqlUserLogin}", configuration.PostgreSqlUserLogin),
                            new Utility.ConfigReplaceTextItem("{PostgreSqlUserPassword}", configuration.PostgreSqlUserPassword),
                        }, "scripts", "mysql", "init-user-postgre.sql"),
                        $"{CromwellAzureRootDir}/mysql-init/init-user-postgre.sql", false)
                    }
                    : Array.Empty<(string, string, bool)>())
                    .ToArray()
                );

        private async Task HandleCustomImagesAsync(ConnectionInfo sshConnectionInfo)
        {
            await HandleCustomImageAsync(sshConnectionInfo, configuration.CromwellVersion, configuration.CustomCromwellImagePath, "env-05-custom-cromwell-image-name.txt", "CromwellImageName", cromwellVersion => $"broadinstitute/cromwell:{cromwellVersion}");
            await HandleCustomImageAsync(sshConnectionInfo, configuration.TesImageName, configuration.CustomTesImagePath, "env-06-custom-tes-image-name.txt", "TesImageName");
            await HandleCustomImageAsync(sshConnectionInfo, configuration.TriggerServiceImageName, configuration.CustomTriggerServiceImagePath, "env-07-custom-trigger-service-image-name.txt", "TriggerServiceImageName");
        }

        private static async Task HandleCustomImageAsync(ConnectionInfo sshConnectionInfo, string imageNameOrTag, string customImagePath, string envFileName, string envFileKey, Func<string, string> imageNameFactory = null)
        {
            async Task CopyCustomDockerImageAsync(string customImagePath)
            {
                var startTime = DateTime.UtcNow;
                var line = ConsoleEx.WriteLine($"Copying custom image from {customImagePath} to the VM...");
                var remotePath = $"{CromwellAzureRootDir}/{Path.GetFileName(customImagePath)}";
                await UploadFilesToVirtualMachineAsync(sshConnectionInfo, (File.OpenRead(customImagePath), remotePath, false));
                WriteExecutionTime(line, startTime);
            }

            async Task<string> LoadCustomDockerImageAsync(string customImagePath)
            {
                var startTime = DateTime.UtcNow;
                var line = ConsoleEx.WriteLine($"Loading custom image {customImagePath} on the VM...");
                var remotePath = $"{CromwellAzureRootDir}/{Path.GetFileName(customImagePath)}";
                var (loadedImageName, _, _) = await ExecuteCommandOnVirtualMachineAsync(sshConnectionInfo, $"imageName=$(sudo docker load -i {remotePath}) && rm {remotePath} && imageName=$(expr \"$imageName\" : 'Loaded.*: \\(.*\\)') && echo $imageName");
                WriteExecutionTime(line, startTime);

                return loadedImageName;
            }

            if (imageNameOrTag is not null && imageNameOrTag.Equals(string.Empty))
            {
                await DeleteFileFromVirtualMachineAsync(sshConnectionInfo, $"{CromwellAzureRootDir}/{envFileName}");
            }
            else if (!string.IsNullOrEmpty(imageNameOrTag))
            {
                var actualImageName = imageNameFactory is not null ? imageNameFactory(imageNameOrTag) : imageNameOrTag;
                await UploadFilesToVirtualMachineAsync(sshConnectionInfo, ($"{envFileKey}={actualImageName}", $"{CromwellAzureRootDir}/{envFileName}", false));
            }
            else if (!string.IsNullOrEmpty(customImagePath))
            {
                await CopyCustomDockerImageAsync(customImagePath);
                var loadedImageName = await LoadCustomDockerImageAsync(customImagePath);
                await UploadFilesToVirtualMachineAsync(sshConnectionInfo, ($"{envFileKey}={loadedImageName}", $"{CromwellAzureRootDir}/{envFileName}", false));
            }
        }

        private async Task HandleConfigurationPropertiesAsync(ConnectionInfo sshConnectionInfo)
        {
            await HandleConfigurationPropertyAsync(sshConnectionInfo, "BatchNodesSubnetId", configuration.BatchNodesSubnetId, "env-08-batch-nodes-subnet-id.txt");
            await HandleConfigurationPropertyAsync(sshConnectionInfo, "DockerInDockerImageName", configuration.DockerInDockerImageName, "env-09-docker-in-docker-image-name.txt");
            await HandleConfigurationPropertyAsync(sshConnectionInfo, "BlobxferImageName", configuration.BlobxferImageName, "env-10-blobxfer-image-name.txt");
            await HandleConfigurationPropertyAsync(sshConnectionInfo, "DisableBatchNodesPublicIpAddress", configuration.DisableBatchNodesPublicIpAddress, "env-11-disable-batch-nodes-public-ip-address.txt");
        }

        private static async Task HandleConfigurationPropertyAsync(ConnectionInfo sshConnectionInfo, string key, string value, string envFileName)
        {
            // If the value is provided and empty, remove the property from the VM
            // If the value is not empty, create/update the property on the VM
            // If the value is not provided, don't do anything, the property may or may not exist on the VM
            // Properties are kept in env-* files, aggregated to .env file at VM startup, and used in docker-compose.yml as environment variables
            if (!string.IsNullOrEmpty(value))
            {
                await UploadFilesToVirtualMachineAsync(sshConnectionInfo, ($"{key}={value}", $"{CromwellAzureRootDir}/{envFileName}", false));
            }
            else if (value is not null)
            {
                await DeleteFileFromVirtualMachineAsync(sshConnectionInfo, $"{CromwellAzureRootDir}/{envFileName}");
            }
        }

        private static async Task HandleConfigurationPropertyAsync(ConnectionInfo sshConnectionInfo, string key, bool? value, string envFileName)
        {
            if (value.HasValue)
            {
                await HandleConfigurationPropertyAsync(sshConnectionInfo, key, value.Value.ToString(), envFileName);
            }
        }

        private Task RebootVmAsync(ConnectionInfo sshConnectionInfo)
            => Execute(
                "Rebooting VM...",
                async () =>
                {
                    try
                    {
                        await ExecuteCommandOnVirtualMachineAsync(sshConnectionInfo, "nohup sudo -b bash -c 'reboot' &>/dev/null");
                    }
                    catch (SshConnectionException)
                    {
                        return;
                    }
                });

        private Task AssignVmAsDataReaderToStorageAccountAsync(IIdentity managedIdentity, IStorageAccount storageAccount)
        {
            // https://docs.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#storage-blob-data-reader
            var roleDefinitionId = $"/subscriptions/{configuration.SubscriptionId}/providers/Microsoft.Authorization/roleDefinitions/2a2b9908-6ea1-4ae2-8e65-a410df84e7d1";

            return Execute(
                $"Assigning Storage Blob Data Reader role for VM to Storage Account resource scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    () => azureSubscriptionClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithRoleDefinition(roleDefinitionId)
                        .WithResourceScope(storageAccount)
                        .CreateAsync(cts.Token)));
        }

        private Task AssignVmAsContributorToStorageAccountAsync(IIdentity managedIdentity, IResource storageAccount)
            => Execute(
                $"Assigning {BuiltInRole.Contributor} role for VM to Storage Account resource scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    () => azureSubscriptionClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithBuiltInRole(BuiltInRole.Contributor)
                        .WithResourceScope(storageAccount)
                        .CreateAsync(cts.Token)));

        private Task<IStorageAccount> CreateStorageAccountAsync()
            => Execute(
                $"Creating Storage Account: {configuration.StorageAccountName}...",
                () => azureSubscriptionClient.StorageAccounts
                    .Define(configuration.StorageAccountName)
                    .WithRegion(configuration.RegionName)
                    .WithExistingResourceGroup(configuration.ResourceGroupName)
                    .WithGeneralPurposeAccountKindV2()
                    .WithOnlyHttpsTraffic()
                    .CreateAsync(cts.Token));

        private async Task<IStorageAccount> GetExistingStorageAccountAsync(string storageAccountName)
            => (await Task.WhenAll(subscriptionIds.Select(s =>
            {
                try
                {
                    return azureClient.WithSubscription(s).StorageAccounts.ListAsync();
                }
                catch (Exception)
                {
                    // Ignore exception if a user does not have the required role to list storage accounts in a subscription
                    return null;
                }
            })))
                .SelectMany(a => a)
                .Where(a => a is not null)
                .SingleOrDefault(a => a.Name.Equals(storageAccountName, StringComparison.OrdinalIgnoreCase) && a.RegionName.Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase));

        private async Task<BatchAccount> GetExistingBatchAccountAsync(string batchAccountName)
            => (await Task.WhenAll(subscriptionIds.Select(s => new BatchManagementClient(tokenCredentials) { SubscriptionId = s }.BatchAccount.ListAsync())))
                .SelectMany(a => a)
                .SingleOrDefault(a => a.Name.Equals(batchAccountName, StringComparison.OrdinalIgnoreCase) && a.Location.Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase));

        private async Task CreateDefaultStorageContainersAsync(IStorageAccount storageAccount)
        {
            var blobClient = await GetBlobClientAsync(storageAccount);

            var defaultContainers = new List<string> { WorkflowsContainerName, InputsContainerName, "cromwell-executions", "cromwell-workflow-logs", "outputs", ConfigurationContainerName };
            await Task.WhenAll(defaultContainers.Select(c => blobClient.GetBlobContainerClient(c).CreateIfNotExistsAsync(cancellationToken: cts.Token)));
        }

        private Task WriteNonPersonalizedFilesToStorageAccountAsync(IStorageAccount storageAccount)
            => Execute(
                $"Writing readme.txt files to '{WorkflowsContainerName}' storage container...",
                async () =>
                {
                    await UploadTextToStorageAccountAsync(storageAccount, WorkflowsContainerName, "new/readme.txt", "Upload a trigger file to this virtual directory to create a new workflow. Additional information here: https://github.com/microsoft/CromwellOnAzure");
                    await UploadTextToStorageAccountAsync(storageAccount, WorkflowsContainerName, "abort/readme.txt", "Upload an empty file to this virtual directory to abort an existing workflow. The empty file's name shall be the Cromwell workflow ID you wish to cancel.  Additional information here: https://github.com/microsoft/CromwellOnAzure");
                });

        private Task WritePersonalizedFilesToStorageAccountAsync(IStorageAccount storageAccount, string managedIdentityName)
            => Execute(
                $"Writing {ContainersToMountFileName} and {CromwellConfigurationFileName} files to '{ConfigurationContainerName}' storage container...",
                async () =>
                {
                    await UploadTextToStorageAccountAsync(storageAccount, ConfigurationContainerName, ContainersToMountFileName, Utility.PersonalizeContent(new[]
                    {
                        new Utility.ConfigReplaceTextItem("{DefaultStorageAccountName}", configuration.StorageAccountName),
                        new Utility.ConfigReplaceTextItem("{ManagedIdentityName}", managedIdentityName)
                    }, "scripts", ContainersToMountFileName));

                    // Configure Cromwell config file for Docker Mysql or PostgreSQL on Azure.
                    var databaseConfig = new List<String>();
                    if(configuration.ProvisionPostgreSqlOnAzure.GetValueOrDefault())
                    {
                        databaseConfig.Add($"\"jdbc:postgresql://{configuration.PostgreSqlServerName}.postgres.database.azure.com/{configuration.PostgreSqlDatabaseName}?sslmode=require\"");
                        databaseConfig.Add($"\"{configuration.PostgreSqlUserLogin}\"");
                        databaseConfig.Add($"\"{configuration.PostgreSqlUserPassword}\"");
                        databaseConfig.Add($"\"org.postgresql.Driver\"");
                        databaseConfig.Add("\"slick.jdbc.PostgresProfile$\"");
                    } else
                    {
                        databaseConfig.Add($"\"jdbc:mysql://mysqldb/cromwell_db?useSSL=false&rewriteBatchedStatements=true&allowPublicKeyRetrieval=true\"");
                        databaseConfig.Add($"\"cromwell\"");
                        databaseConfig.Add($"\"cromwell\"");
                        databaseConfig.Add($"\"com.mysql.cj.jdbc.Driver\"");
                        databaseConfig.Add("\"slick.jdbc.MySQLProfile$\"");
                    }
                    await UploadTextToStorageAccountAsync(storageAccount, ConfigurationContainerName, CromwellConfigurationFileName, Utility.PersonalizeContent(new[]
                    {
                        new Utility.ConfigReplaceTextItem("{DatabaseUrl}", databaseConfig[0]),
                        new Utility.ConfigReplaceTextItem("{DatabaseUser}", databaseConfig[1]),
                        new Utility.ConfigReplaceTextItem("{DatabasePassword}", databaseConfig[2]),
                        new Utility.ConfigReplaceTextItem("{DatabaseDriver}", databaseConfig[3]),
                        new Utility.ConfigReplaceTextItem("{DatabaseProfile}", databaseConfig[4]),
                    }, "scripts", CromwellConfigurationFileName));
                    await UploadTextToStorageAccountAsync(storageAccount, ConfigurationContainerName, AllowedVmSizesFileName, Utility.GetFileContent("scripts", AllowedVmSizesFileName));
                });

        private Task AssignVmAsContributorToBatchAccountAsync(IIdentity managedIdentity, BatchAccount batchAccount)
            => Execute(
                $"Assigning {BuiltInRole.Contributor} role for VM to Batch Account resource scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    () => azureSubscriptionClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithBuiltInRole(BuiltInRole.Contributor)
                        .WithScope(batchAccount.Id)
                        .CreateAsync(cts.Token)));

        private Task AssignVmAsContributorToCosmosDb(IIdentity managedIdentity, IResource cosmosDb)
            => Execute(
                $"Assigning {BuiltInRole.Contributor} role for VM to Cosmos DB resource scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    () => azureSubscriptionClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithBuiltInRole(BuiltInRole.Contributor)
                        .WithResourceScope(cosmosDb)
                        .CreateAsync(cts.Token)));

        private Task<ICosmosDBAccount> CreateCosmosDbAsync()
            => Execute(
                $"Creating Cosmos DB: {configuration.CosmosDbAccountName}...",
                () => azureSubscriptionClient.CosmosDBAccounts
                    .Define(configuration.CosmosDbAccountName)
                    .WithRegion(configuration.RegionName)
                    .WithExistingResourceGroup(configuration.ResourceGroupName)
                    .WithDataModelSql()
                    .WithSessionConsistency()
                    .WithWriteReplication(Region.Create(configuration.RegionName))
                    .CreateAsync(cts.Token));

        private async Task<Server> CreatePostgreSqlServerAndDatabaseAsync(IPostgreSQLManagementClient postgresManagementClient, ISubnet subnet, IPrivateDnsZone postgreSqlDnsZone)
        {
            if (!subnet.Inner.Delegations.Any())
            {
                subnet.Parent.Update().UpdateSubnet(subnet.Name).WithDelegation("Microsoft.DBforPostgreSQL/flexibleServers");
                await subnet.Parent.Update().ApplyAsync();
            }

            Server server = null;

            await Execute(
                $"Creating Azure Server for PostgreSQL: {configuration.PostgreSqlServerName} ...",
                async () =>
                {
                    server = await postgresManagementClient.Servers.CreateAsync(
                        configuration.ResourceGroupName, configuration.PostgreSqlServerName,
                        new Server(
                           location: configuration.RegionName,
                           version: configuration.PostgreSqlVersion,
                           sku: new Sku(configuration.PostgreSqlSkuName, configuration.PostgreSqlTier),
                           storage: new Storage(configuration.PostgreSqlStorageSize),
                           administratorLogin: configuration.PostgreSqlAdministratorLogin,
                           administratorLoginPassword: configuration.PostgreSqlAdministratorPassword,
                           network: new Network(publicNetworkAccess: "Disabled", delegatedSubnetResourceId: subnet.Inner.Id, privateDnsZoneArmResourceId: postgreSqlDnsZone.Id)
                        ));
                });

            await Execute(
                $"Creating PostgreSQL cromwell database: {configuration.PostgreSqlDatabaseName}...",
                () => postgresManagementClient.Databases.CreateAsync(
                    configuration.ResourceGroupName, configuration.PostgreSqlServerName, configuration.PostgreSqlDatabaseName,
                    new Database()));
 
            return server;
        }

        private Task AssignVmAsBillingReaderToSubscriptionAsync(IIdentity managedIdentity)
        {
            try
            {
                return Execute(
                    $"Assigning {BuiltInRole.BillingReader} role for VM to Subscription scope...",
                    () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                        () => azureSubscriptionClient.AccessManagement.RoleAssignments
                            .Define(Guid.NewGuid().ToString())
                            .ForObjectId(managedIdentity.PrincipalId)
                            .WithBuiltInRole(BuiltInRole.BillingReader)
                            .WithSubscriptionScope(configuration.SubscriptionId)
                            .CreateAsync(cts.Token)));
            }
            catch (Microsoft.Rest.Azure.CloudException)
            {
                DisplayBillingReaderInsufficientAccessLevelWarning();
                return Task.CompletedTask;
            }
        }

        private Task AssignVmAsContributorToAppInsightsAsync(IIdentity managedIdentity, IResource appInsights)
            => Execute(
                $"Assigning {BuiltInRole.Contributor} role for VM to App Insights resource scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    () => azureSubscriptionClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithBuiltInRole(BuiltInRole.Contributor)
                        .WithResourceScope(appInsights)
                        .CreateAsync(cts.Token)));

        private Task<IVirtualMachine> CreateVirtualMachineAsync(IIdentity managedIdentity, INetwork vnet, string subnetName)
        {
            const int dataDiskSizeGiB = 32;
            const int dataDiskLun = 0;

            var vmDefinitionPart1 = azureSubscriptionClient.VirtualMachines.Define(configuration.VmName)
                .WithRegion(configuration.RegionName)
                .WithExistingResourceGroup(configuration.ResourceGroupName)
                .WithExistingPrimaryNetwork(vnet)
                .WithSubnet(subnetName)
                .WithPrimaryPrivateIPAddressDynamic();

            var vmDefinitionPart2 = (configuration.PrivateNetworking.GetValueOrDefault() ? vmDefinitionPart1.WithoutPrimaryPublicIPAddress() : vmDefinitionPart1.WithNewPrimaryPublicIPAddress(configuration.VmName))
                .WithLatestLinuxImage(configuration.VmOsProvider, configuration.VmOsName, configuration.VmOsVersion)
                .WithRootUsername(configuration.VmUsername)
                .WithRootPassword(configuration.VmPassword)
                .WithNewDataDisk(dataDiskSizeGiB, dataDiskLun, CachingTypes.None)
                .WithSize(configuration.VmSize)
                .WithExistingUserAssignedManagedServiceIdentity(managedIdentity);

            return Execute($"Creating Linux VM: {configuration.VmName}...", () => vmDefinitionPart2.CreateAsync(cts.Token));
        }

        private Task<(INetwork virtualNetwork, ISubnet vmSubnet, ISubnet postgreSqlSubnet)> CreateVnetAndSubnetsAsync(IResourceGroup resourceGroup)
          => Execute(
                $"Creating virtual network and subnets: {configuration.VnetName} ...",
            async () =>
            {
                var vnetDefinition = azureSubscriptionClient.Networks
                    .Define(configuration.VnetName)
                    .WithRegion(configuration.RegionName)
                    .WithExistingResourceGroup(resourceGroup)
                    .WithAddressSpace(configuration.VnetAddressSpace)
                    .DefineSubnet(configuration.VmSubnetName).WithAddressPrefix(configuration.VmSubnetAddressSpace).Attach();

                vnetDefinition = configuration.ProvisionPostgreSqlOnAzure.GetValueOrDefault()
                    ? vnetDefinition.DefineSubnet(configuration.PostgreSqlSubnetName).WithAddressPrefix(configuration.PostgreSqlSubnetAddressSpace).WithDelegation("Microsoft.DBforPostgreSQL/flexibleServers").Attach()
                    : vnetDefinition;

                var vnet = await vnetDefinition.CreateAsync();

                return (vnet, vnet.Subnets.FirstOrDefault(s => s.Key.Equals(configuration.VmSubnetName, StringComparison.OrdinalIgnoreCase)).Value, vnet.Subnets.FirstOrDefault(s => s.Key.Equals(configuration.PostgreSqlSubnetName, StringComparison.OrdinalIgnoreCase)).Value);
            });

        private Task<INetworkSecurityGroup> CreateNetworkSecurityGroupAsync(IResourceGroup resourceGroup, string networkSecurityGroupName)
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
                    .WithProtocol(SecurityRuleProtocol.Tcp)
                    .WithPriority(SshDefaultPriority)
                    .Attach()
                    .CreateAsync(cts.Token)
            );
        }

        private Task EnableSsh(INetworkSecurityGroup networkSecurityGroup)
        {
            try
            {
                return networkSecurityGroup?.SecurityRules[SshNsgRuleName]?.Access switch
                {
                    null => Task.CompletedTask,
                    var x when SecurityRuleAccess.Allow.Equals(x) => SetKeepSshPortOpen(true),
                    _ => Execute(
                        "Enabling SSH on VM...",
                        () => EnableSshPort()),
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

        private Task DisableSsh(INetworkSecurityGroup networkSecurityGroup)
            => networkSecurityGroup is null ? Task.CompletedTask : Execute(
                "Disabling SSH on VM...",
                () => networkSecurityGroup.Update().UpdateRule(SshNsgRuleName).DenyInbound().Parent().ApplyAsync()
            );

        private Task<INetworkInterface> AssociateNicWithNetworkSecurityGroupAsync(INetworkInterface networkInterface, INetworkSecurityGroup networkSecurityGroup)
            => Execute(
                $"Associating VM NIC with Network Security Group {networkSecurityGroup.Name}...",
                () => networkInterface.Update().WithExistingNetworkSecurityGroup(networkSecurityGroup).ApplyAsync()
            );

        private Task CreatePostgreSqlDatabaseUser(ConnectionInfo sshConnectionInfo)
            => Execute(
                $"Creating PostgreSQL database user...",
                () => ExecuteCommandOnVirtualMachineAsync(sshConnectionInfo, $"psql postgresql://{configuration.PostgreSqlAdministratorLogin}:{configuration.PostgreSqlAdministratorPassword}@{configuration.PostgreSqlServerName}.postgres.database.azure.com/{configuration.PostgreSqlDatabaseName} < {CromwellAzureRootDir}/mysql-init/init-user-postgre.sql")
            );

        private Task<IPrivateDnsZone> CreatePrivateDnsZoneAsync(INetwork virtualNetwork)
            => Execute(
                "Creating private DNS Zone for PostgreSQL Server",
                async () =>
                {
                    // Note: upon refactoring out of fluent see commit cbffa28 in #392. 
                    var postgreSqlDnsZone = await azureSubscriptionClient.PrivateDnsZones
                        .Define($"{configuration.PostgreSqlServerName}.private.postgres.database.azure.com")
                        .WithExistingResourceGroup(configuration.ResourceGroupName)
                        .DefineVirtualNetworkLink($"{virtualNetwork.Name}-link").WithReferencedVirtualNetworkId(virtualNetwork.Id).DisableAutoRegistration().Attach()
                        .CreateAsync();
                    return postgreSqlDnsZone;
                });

        private Task<IGenericResource> CreateLogAnalyticsWorkspaceResourceAsync(string workspaceName)
            => Execute(
                $"Creating Log Analytics Workspace: {workspaceName}...",
                () => ResourceManager
                    .Configure()
                    .Authenticate(azureCredentials)
                    .WithSubscription(configuration.SubscriptionId)
                    .GenericResources.Define(workspaceName)
                    .WithRegion(configuration.RegionName)
                    .WithExistingResourceGroup(configuration.ResourceGroupName)
                    .WithResourceType("workspaces")
                    .WithProviderNamespace("Microsoft.OperationalInsights")
                    .WithoutPlan()
                    .WithApiVersion("2020-08-01")
                    .WithParentResource(string.Empty)
                    .CreateAsync(cts.Token));

        private Task<IGenericResource> CreateAppInsightsResourceAsync(string logAnalyticsArmId)
            => Execute(
                $"Creating Application Insights: {configuration.ApplicationInsightsAccountName}...",
                () => ResourceManager
                    .Configure()
                    .Authenticate(azureCredentials)
                    .WithSubscription(configuration.SubscriptionId)
                    .GenericResources.Define(configuration.ApplicationInsightsAccountName)
                    .WithRegion(configuration.RegionName)
                    .WithExistingResourceGroup(configuration.ResourceGroupName)
                    .WithResourceType("components")
                    .WithProviderNamespace("microsoft.insights")
                    .WithoutPlan()
                    .WithApiVersion("2020-02-02")
                    .WithParentResource(string.Empty)
                    .WithProperties(new Dictionary<string, string>() { 
                        { "Application_Type", "other" } ,
                        { "WorkspaceResourceId", logAnalyticsArmId }
                    })
                    .CreateAsync(cts.Token));

        private Task<BatchAccount> CreateBatchAccountAsync(string storageAccountId)
            => Execute(
                $"Creating Batch Account: {configuration.BatchAccountName}...",
                () => new BatchManagementClient(tokenCredentials) { SubscriptionId = configuration.SubscriptionId }
                    .BatchAccount
                    .CreateAsync(
                        configuration.ResourceGroupName,
                        configuration.BatchAccountName,
                        new BatchAccountCreateParameters(
                            configuration.RegionName,
                            autoStorage: configuration.PrivateNetworking.GetValueOrDefault() ? new AutoStorageBaseProperties { StorageAccountId = storageAccountId } : null),
                        cts.Token));

        private Task<IResourceGroup> CreateResourceGroupAsync()
        {
            var tags = !string.IsNullOrWhiteSpace(configuration.Tags) ? Utility.DelimitedTextToDictionary(configuration.Tags, "=", ",") : null;

            var resourceGroupDefinition = azureSubscriptionClient
                .ResourceGroups
                .Define(configuration.ResourceGroupName)
                .WithRegion(configuration.RegionName);

            resourceGroupDefinition = tags is not null ? resourceGroupDefinition.WithTags(tags) : resourceGroupDefinition;

            return Execute(
                $"Creating Resource Group: {configuration.ResourceGroupName}...",
                () => resourceGroupDefinition.CreateAsync());
        }

        private Task<IIdentity> CreateUserManagedIdentityAsync(IResourceGroup resourceGroup)
        {
            // Resource group name supports periods and parenthesis but identity doesn't. Replacing them with hyphens.
            var managedIdentityName = $"{resourceGroup.Name.Replace(".", "-").Replace("(", "-").Replace(")", "-")}-identity";

            return Execute(
                $"Creating user-managed identity: {managedIdentityName}...",
                () => azureSubscriptionClient.Identities.Define(managedIdentityName)
                    .WithRegion(configuration.RegionName)
                    .WithExistingResourceGroup(resourceGroup)
                    .CreateAsync());
        }

        private Task<IIdentity> ReplaceSystemManagedIdentityWithUserManagedIdentityAsync(IResourceGroup resourceGroup, IVirtualMachine linuxVm)
            => Execute(
                "Replacing VM system-managed identity with user-managed identity for easier VM upgrades in the future...",
                async () =>
                {
                    var userManagedIdentity = await CreateUserManagedIdentityAsync(resourceGroup);

                    var existingVmRoles = (await azureSubscriptionClient.AccessManagement.RoleAssignments.Inner.ListForScopeWithHttpMessagesAsync(
                            $"/subscriptions/{configuration.SubscriptionId}",
                            new ODataQuery<RoleAssignmentFilter>($"assignedTo('{linuxVm.SystemAssignedManagedServiceIdentityPrincipalId}')"))).Body
                        .AsContinuousCollection(link => Extensions.Synchronize(() => azureSubscriptionClient.AccessManagement.RoleAssignments.Inner.ListForScopeNextWithHttpMessagesAsync(link)).Body)
                        .ToList();

                    foreach (var role in existingVmRoles)
                    {
                        await azureSubscriptionClient.AccessManagement.RoleAssignments
                            .Define(Guid.NewGuid().ToString())
                            .ForObjectId(userManagedIdentity.PrincipalId)
                            .WithRoleDefinition(role.RoleDefinitionId)
                            .WithScope(role.Scope)
                            .CreateAsync();
                    }

                    foreach (var role in existingVmRoles)
                    {
                        await azureSubscriptionClient.AccessManagement.RoleAssignments.DeleteByIdAsync(role.Id);
                    }

                    await Execute(
                        "Removing existing system-managed identity and assigning new user-managed identity to the VM...",
                        () => linuxVm.Update().WithoutSystemAssignedManagedServiceIdentity().WithExistingUserAssignedManagedServiceIdentity(userManagedIdentity).ApplyAsync());

                    return userManagedIdentity;
                });

        private async Task DeleteResourceGroupAsync()
        {
            var startTime = DateTime.UtcNow;
            var line = ConsoleEx.WriteLine("Deleting resource group...");
            await azureSubscriptionClient.ResourceGroups.DeleteByNameAsync(configuration.ResourceGroupName, CancellationToken.None);
            WriteExecutionTime(line, startTime);
        }

        private Task PatchCromwellConfigurationFileV200Async(IStorageAccount storageAccount)
            => Execute(
                $"Patching '{CromwellConfigurationFileName}' in '{ConfigurationContainerName}' storage container...",
                async () =>
                {
                    await UploadTextToStorageAccountAsync(storageAccount, ConfigurationContainerName, CromwellConfigurationFileName, Utility.PersonalizeContent(new[]
                    {
                        // Replace "enabled = true" with "enabled = false" in call-caching element
                        (Utility.ConfigReplaceTextItemBase)new Utility.ConfigReplaceRegExItemText(@"^(\s*call-caching\s*{[^}]*enabled\s*[=:]{1}\s*)(true)$", "$1false", RegexOptions.Multiline),
                        // Add "preemptible: true" to default-runtime-attributes element, if preemptible is not already present
                        new Utility.ConfigReplaceRegExItemEvaluator(@"(?![\s\S]*preemptible)^(\s*default-runtime-attributes\s*{)([^}]*$)(\s*})$", match => $"{match.Groups[1].Value}{match.Groups[2].Value}\n          preemptible: true{match.Groups[3].Value}", RegexOptions.Multiline),
                    }, (await DownloadTextFromStorageAccountAsync(storageAccount, ConfigurationContainerName, CromwellConfigurationFileName)) ?? Utility.GetFileContent("scripts", CromwellConfigurationFileName)));
                });

        private Task PatchContainersToMountFileV210Async(IStorageAccount storageAccount, string managedIdentityName)
            => Execute(
                $"Adding public datasettestinputs/dataset container to '{ContainersToMountFileName}' file in '{ConfigurationContainerName}' storage container...",
                async () =>
                {
                    var containersToMountText = await DownloadTextFromStorageAccountAsync(storageAccount, ConfigurationContainerName, ContainersToMountFileName);

                    if (containersToMountText is not null)
                    {
                        // Add datasettestinputs container if not already present
                        if (!containersToMountText.Contains("datasettestinputs.blob.core.windows.net/dataset"))
                        {
                            // [SuppressMessage("Microsoft.Security", "CS002:SecretInNextLine", Justification="SAS token for public use")]
                            var dataSetUrl = "https://datasettestinputs.blob.core.windows.net/dataset?sv=2018-03-28&sr=c&si=coa&sig=nKoK6dxjtk5172JZfDH116N6p3xTs7d%2Bs5EAUE4qqgM%3D";
                            containersToMountText = $"{containersToMountText.TrimEnd()}\n{dataSetUrl}";
                        }

                        containersToMountText = containersToMountText
                            .Replace("where the VM has Contributor role", $"where the identity '{managedIdentityName}' has 'Contributor' role")
                            .Replace("where VM's identity", "where CoA VM")
                            .Replace("that the VM's identity has Contributor role", $"that the identity '{managedIdentityName}' has 'Contributor' role");

                        await UploadTextToStorageAccountAsync(storageAccount, ConfigurationContainerName, ContainersToMountFileName, containersToMountText);
                    }
                });

        private Task PatchContainersToMountFileV220Async(IStorageAccount storageAccount)
            => Execute(
                $"Commenting out msgenpublicdata/inputs in '{ContainersToMountFileName}' file in '{ConfigurationContainerName}' storage container. It will be removed in v2.3",
                async () =>
                {
                    var containersToMountText = await DownloadTextFromStorageAccountAsync(storageAccount, ConfigurationContainerName, ContainersToMountFileName);

                    if (containersToMountText is not null)
                    {
                        containersToMountText = containersToMountText.Replace("https://msgenpublicdata", $"#https://msgenpublicdata");

                        await UploadTextToStorageAccountAsync(storageAccount, ConfigurationContainerName, ContainersToMountFileName, containersToMountText);
                    }
                });

        private Task PatchContainersToMountFileV240Async(IStorageAccount storageAccount)
            => Execute(
                $"Removing reference to msgenpublicdata/inputs in '{ContainersToMountFileName}' file in '{ConfigurationContainerName}' storage container.",
                async () =>
                {
                    var containersToMountText = await DownloadTextFromStorageAccountAsync(storageAccount, ConfigurationContainerName, ContainersToMountFileName);

                    if (containersToMountText is not null)
                    {
                        var regex = new Regex("^.*msgenpublicdata.blob.core.windows.net/inputs.*(\n|\r|\r\n)", RegexOptions.Multiline);
                        containersToMountText = regex.Replace(containersToMountText, string.Empty);

                        await UploadTextToStorageAccountAsync(storageAccount, ConfigurationContainerName, ContainersToMountFileName, containersToMountText);
                    }
                });

        private Task PatchAccountNamesFileV240Async(ConnectionInfo sshConnectionInfo, IIdentity managedIdentity)
            => Execute(
                $"Adding Managed Identity ClientId to 'env-01-account-names.txt' file on the VM...",
                async () =>
                {
                    var accountNames = Utility.DelimitedTextToDictionary((await ExecuteCommandOnVirtualMachineWithRetriesAsync(sshConnectionInfo, $"cat {CromwellAzureRootDir}/env-01-account-names.txt")).Output);
                    accountNames["ManagedIdentityClientId"] = managedIdentity.ClientId;

                    await UploadFilesToVirtualMachineAsync(sshConnectionInfo, (Utility.DictionaryToDelimitedText(accountNames), $"{CromwellAzureRootDir}/env-01-account-names.txt", false));
                });

        private Task PatchCromwellConfigurationFileV300Async(IStorageAccount storageAccount)
            => Execute(
                $"Patching '{CromwellConfigurationFileName}' in '{ConfigurationContainerName}' storage container...",
                async () =>
                {
                    var cromwellConfigText = await DownloadTextFromStorageAccountAsync(storageAccount, ConfigurationContainerName, CromwellConfigurationFileName);
                    var tesBackendParametersRegex = new Regex(@"^(\s*endpoint.*)([\s\S]*)", RegexOptions.Multiline);

                    // Add "use_tes_11_preview_backend_parameters = true" to TES config, after endpoint setting, if use_tes_11_preview_backend_parameters is not already present
                    if (!cromwellConfigText.Contains("use_tes_11_preview_backend_parameters", StringComparison.OrdinalIgnoreCase))
                    {
                        cromwellConfigText = tesBackendParametersRegex.Replace(cromwellConfigText, match => $"{match.Groups[1].Value}\n        use_tes_11_preview_backend_parameters = true{match.Groups[2].Value}");
                    }

                    await UploadTextToStorageAccountAsync(storageAccount, ConfigurationContainerName, CromwellConfigurationFileName, cromwellConfigText);
                });

        private Task PatchCromwellConfigurationFileV310Async(IStorageAccount storageAccount)
            => Execute(
                $"Patching '{CromwellConfigurationFileName}' in '{ConfigurationContainerName}' storage container...",
                async () =>
                {
                    var cromwellConfigText = await DownloadTextFromStorageAccountAsync(storageAccount, ConfigurationContainerName, CromwellConfigurationFileName);
                    var akkaHttpRegex = new Regex(@"(\s*akka\.http\.host-connection-pool\.pool-implementation.*$)", RegexOptions.Multiline);
                    cromwellConfigText = akkaHttpRegex.Replace(cromwellConfigText, match => string.Empty);

                    await UploadTextToStorageAccountAsync(storageAccount, ConfigurationContainerName, CromwellConfigurationFileName, cromwellConfigText);
                });

        private Task AddNewSettingsV300Async(ConnectionInfo sshConnectionInfo)
            => Execute(
                $"Adding new settings to 'env-04-settings.txt' file on the VM...",
                async () =>
                {
                    var existingFileContent = await ExecuteCommandOnVirtualMachineAsync(sshConnectionInfo, $"cat {CromwellAzureRootDir}/env-04-settings.txt");
                    var existingSettings = Utility.DelimitedTextToDictionary(existingFileContent.Output.Trim());
                    var newSettings = Utility.DelimitedTextToDictionary(Utility.GetFileContent("scripts", "env-04-settings.txt"));

                    foreach (var key in newSettings.Keys.Except(existingSettings.Keys))
                    {
                        existingSettings.Add(key, newSettings[key]);
                    }

                    var newFileContent = Utility.DictionaryToDelimitedText(existingSettings);

                    await UploadFilesToVirtualMachineAsync(
                        sshConnectionInfo,
                        new[] {
                            (newFileContent, $"{CromwellAzureRootDir}/env-04-settings.txt", false)
                        });
                });

        private async Task MitigateChaosDbV250Async(ICosmosDBAccount cosmosDb)
            => await Execute("#ChaosDB remedition (regenerating CosmosDB primary key)",
                () => cosmosDb.RegenerateKeyAsync(KeyKind.Primary.Value));

        private async Task UpgradeBlobfuseV300Async(ConnectionInfo sshConnectionInfo)
            => await Execute("Upgrading blobfuse to 1.4.3...",
                () => ExecuteCommandOnVirtualMachineWithRetriesAsync(sshConnectionInfo, "sudo apt-get update ; sudo apt-get --only-upgrade install blobfuse=1.4.3"));

        private async Task DisableDockerServiceV300Async(ConnectionInfo sshConnectionInfo)
            => await Execute("Disabling auto-start of Docker service...",
                () => ExecuteCommandOnVirtualMachineWithRetriesAsync(sshConnectionInfo, "sudo systemctl disable docker"));

        private async Task SetCosmosDbContainerAutoScaleAsync(ICosmosDBAccount cosmosDb)
        {
            var tesDb = await cosmosDb.GetSqlDatabaseAsync(Constants.CosmosDbDatabaseId);
            var taskContainer = await tesDb.GetSqlContainerAsync(Constants.CosmosDbContainerId);
            var requestThroughput = await taskContainer.GetThroughputSettingsAsync();

            if (requestThroughput is not null && requestThroughput.Throughput is not null && requestThroughput.AutopilotSettings?.MaxThroughput is null)
            {
                var key = (await cosmosDb.ListKeysAsync()).PrimaryMasterKey;
                var cosmosClient = new CosmosRestClient(cosmosDb.DocumentEndpoint, key);

                // If the container has request throughput setting configured, and it is currently manual, set it to auto
                await Execute(
                    $"Switching the throughput setting for CosmosDb container 'Tasks' in database 'TES' from Manual to Autoscale...",
                    () => cosmosClient.SwitchContainerRequestThroughputToAutoAsync(Constants.CosmosDbDatabaseId, Constants.CosmosDbContainerId));
            }
        }

        private static ConnectionInfo GetSshConnectionInfo(IVirtualMachine linuxVm, string vmUsername, string vmPassword)
        {
            var publicIPAddress = linuxVm.GetPrimaryPublicIPAddress();

            return new ConnectionInfo(
                publicIPAddress is not null ? publicIPAddress.Fqdn : linuxVm.GetPrimaryNetworkInterface().PrimaryPrivateIP,
                vmUsername,
                new PasswordAuthenticationMethod(vmUsername, vmPassword));
        }

        private static void ValidateMainIdentifierPrefix(string prefix)
        {
            const int maxLength = 12;

            if (prefix.Any(c => !char.IsLetter(c)))
            {
                throw new ValidationException($"MainIdentifierPrefix must only contain letters.");
            }

            if (prefix.Length > maxLength)
            {
                throw new ValidationException($"MainIdentifierPrefix too long - must be {maxLength} characters or less.");
            }
        }

        private void ValidateRegionName(string regionName)
        {
            var validRegionNames = azureSubscriptionClient.GetCurrentSubscription().ListLocations().Select(loc => loc.Region.Name);

            if (!validRegionNames.Contains(regionName, StringComparer.OrdinalIgnoreCase))
            {
                throw new ValidationException($"Invalid region name '{regionName}'. Valid names are: {string.Join(", ", validRegionNames)}");
            }
        }

        private async Task ValidateSubscriptionAndResourceGroupAsync(Configuration configuration)
        {
            const string ownerRoleId = "8e3af657-a8ff-443c-a75c-2fe8c4bcb635";
            const string contributorRoleId = "b24988ac-6180-42a0-ab88-20f7382dd24c";
            const string userAccessAdministratorRoleId = "18d7d88d-d35e-4fb5-a5c3-7773c20a72d9";

            var azure = Microsoft.Azure.Management.Fluent.Azure
                .Configure()
                .WithLogLevel(HttpLoggingDelegatingHandler.Level.Basic)
                .Authenticate(azureCredentials);

            var subscriptionExists = (await azure.Subscriptions.ListAsync()).Any(sub => sub.SubscriptionId.Equals(configuration.SubscriptionId, StringComparison.OrdinalIgnoreCase));

            if (!subscriptionExists)
            {
                throw new ValidationException($"Invalid or inaccessible subcription id '{configuration.SubscriptionId}'. Make sure that subscription exists and that you are either an Owner or have Contributor and User Access Administrator roles on the subscription.", displayExample: false);
            }

            var rgExists = !string.IsNullOrEmpty(configuration.ResourceGroupName) && await azureSubscriptionClient.ResourceGroups.ContainAsync(configuration.ResourceGroupName);

            if (!string.IsNullOrEmpty(configuration.ResourceGroupName) && !rgExists)
            {
                throw new ValidationException($"If ResourceGroupName is provided, the resource group must already exist.", displayExample: false);
            }

            var token = await new AzureServiceTokenProvider().GetAccessTokenAsync("https://management.azure.com/");
            var currentPrincipalObjectId = new JwtSecurityTokenHandler().ReadJwtToken(token).Claims.FirstOrDefault(c => c.Type == "oid").Value;

            var currentPrincipalSubscriptionRoleIds = (await azureSubscriptionClient.AccessManagement.RoleAssignments.Inner.ListForScopeWithHttpMessagesAsync($"/subscriptions/{configuration.SubscriptionId}", new ODataQuery<RoleAssignmentFilter>($"atScope() and assignedTo('{currentPrincipalObjectId}')")))
                .Body.AsContinuousCollection(link => Extensions.Synchronize(() => azureSubscriptionClient.AccessManagement.RoleAssignments.Inner.ListForScopeNextWithHttpMessagesAsync(link)).Body)
                .Select(b => b.RoleDefinitionId.Split(new[] { '/' }).Last());

            var isuserAccessAdministrator = currentPrincipalSubscriptionRoleIds.Contains(userAccessAdministratorRoleId);

            if (!currentPrincipalSubscriptionRoleIds.Contains(ownerRoleId) && !(currentPrincipalSubscriptionRoleIds.Contains(contributorRoleId) && isuserAccessAdministrator))
            {
                if (!rgExists)
                {
                    throw new ValidationException($"Insufficient access to deploy. You must be: 1) Owner of the subscription, or 2) Contributor and User Access Administrator of the subscription, or 3) Owner of the resource group", displayExample: false);
                }

                var currentPrincipalRgRoleIds = (await azureSubscriptionClient.AccessManagement.RoleAssignments.Inner.ListForScopeWithHttpMessagesAsync($"/subscriptions/{configuration.SubscriptionId}/resourceGroups/{configuration.ResourceGroupName}", new ODataQuery<RoleAssignmentFilter>($"atScope() and assignedTo('{currentPrincipalObjectId}')")))
                    .Body.AsContinuousCollection(link => Extensions.Synchronize(() => azureSubscriptionClient.AccessManagement.RoleAssignments.Inner.ListForScopeNextWithHttpMessagesAsync(link)).Body)
                    .Select(b => b.RoleDefinitionId.Split(new[] { '/' }).Last());

                if (!currentPrincipalRgRoleIds.Contains(ownerRoleId))
                {
                    throw new ValidationException($"Insufficient access to deploy. You must be: 1) Owner of the subscription, or 2) Contributor and User Access Administrator of the subscription, or 3) Owner of the resource group", displayExample: false);
                }

                if (!isuserAccessAdministrator)
                {
                    SkipBillingReaderRoleAssignment = true;
                    DisplayBillingReaderInsufficientAccessLevelWarning();
                }
            }
        }

        private async Task<IStorageAccount> ValidateAndGetExistingStorageAccountAsync()
        {
            if (configuration.StorageAccountName is null)
            {
                return null;
            }

            return (await GetExistingStorageAccountAsync(configuration.StorageAccountName))
                ?? throw new ValidationException($"If StorageAccountName is provided, the storage account must already exist in region {configuration.RegionName}, and be accessible to the current user.", displayExample: false);
        }

        private async Task<BatchAccount> ValidateAndGetExistingBatchAccountAsync()
        {
            if (configuration.BatchAccountName is null)
            {
                return null;
            }

            return (await GetExistingBatchAccountAsync(configuration.BatchAccountName))
                ?? throw new ValidationException($"If BatchAccountName is provided, the batch account must already exist in region {configuration.RegionName}, and be accessible to the current user.", displayExample: false);
        }

        private async Task<(INetwork virtualNetwork, ISubnet vmSubnet, ISubnet postgreSqlSubnet)?> ValidateAndGetExistingVirtualNetworkAsync()
        {
            static bool AllOrNoneSet(params string[] values) => values.All(v => !string.IsNullOrEmpty(v)) || values.All(v => string.IsNullOrEmpty(v));
            static bool NoneSet(params string[] values) => values.All(v => string.IsNullOrEmpty(v));

            if (NoneSet(configuration.VnetResourceGroupName, configuration.VnetName, configuration.VmSubnetName))
            {
                if (configuration.PrivateNetworking.GetValueOrDefault())
                {
                    throw new ValidationException($"{nameof(configuration.VnetResourceGroupName)}, {nameof(configuration.VnetName)} and {nameof(configuration.VmSubnetName)} are required when using private networking.");
                }

                return null;
            }

            if (configuration.ProvisionPostgreSqlOnAzure == true && !AllOrNoneSet(configuration.VnetResourceGroupName, configuration.VnetName, configuration.VmSubnetName, configuration.PostgreSqlSubnetName))
            {
                throw new ValidationException($"{nameof(configuration.VnetResourceGroupName)}, {nameof(configuration.VnetName)}, {nameof(configuration.VmSubnetName)} and {nameof(configuration.PostgreSqlSubnetName)} are required when using an existing virtual network and {nameof(configuration.ProvisionPostgreSqlOnAzure)} is set.");
            }

            if (!AllOrNoneSet(configuration.VnetResourceGroupName, configuration.VnetName, configuration.VmSubnetName))
            {
                throw new ValidationException($"{nameof(configuration.VnetResourceGroupName)}, {nameof(configuration.VnetName)} and {nameof(configuration.VmSubnetName)} are required when using an existing virtual network.");
            }

            if (!(await azureSubscriptionClient.ResourceGroups.ListAsync(true)).Any(rg => rg.Name.Equals(configuration.VnetResourceGroupName, StringComparison.OrdinalIgnoreCase)))
            {
                throw new ValidationException($"Resource group '{configuration.VnetResourceGroupName}' does not exist.");
            }

            var vnet = await azureSubscriptionClient.Networks.GetByResourceGroupAsync(configuration.VnetResourceGroupName, configuration.VnetName);

            if (vnet is null)
            {
                throw new ValidationException($"Virtual network '{configuration.VnetName}' does not exist in resource group '{configuration.VnetResourceGroupName}'.");
            }

            if (!vnet.RegionName.Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase))
            {
                throw new ValidationException($"Virtual network '{configuration.VnetName}' must be in the same region that you are deploying to ({configuration.RegionName}).");
            }

            var vmSubnet = vnet.Subnets.FirstOrDefault(s => s.Key.Equals(configuration.VmSubnetName, StringComparison.OrdinalIgnoreCase)).Value;

            if (vmSubnet == null)
            {
                throw new ValidationException($"Virtual network '{configuration.VnetName}' does not contain subnet '{configuration.VmSubnetName}'");
            }

            var resourceGraphClient = new ResourceGraphClient(tokenCredentials);
            var postgreSqlSubnet = vnet.Subnets.FirstOrDefault(s => s.Key.Equals(configuration.PostgreSqlSubnetName, StringComparison.OrdinalIgnoreCase)).Value;

            if (configuration.ProvisionPostgreSqlOnAzure== true)
            {
                if (postgreSqlSubnet == null)
                {
                    throw new ValidationException($"Virtual network '{configuration.VnetName}' does not contain subnet '{configuration.PostgreSqlSubnetName}'");
                }

                var delegatedServices = postgreSqlSubnet.Inner.Delegations.Select(d => d.ServiceName);
                var hasOtherDelegations = delegatedServices.Any(s => s != "Microsoft.DBforPostgreSQL/flexibleServers");
                var hasNoDelegations = delegatedServices.Count() == 0;

                if (hasOtherDelegations)
                {
                    throw new ValidationException($"Subnet '{configuration.PostgreSqlSubnetName}' can have 'Microsoft.DBforPostgreSQL/flexibleServers' delegation only.");
                }

                var resourcesInPostgreSqlSubnetQuery = $"where type =~ 'Microsoft.Network/networkInterfaces' | where properties.ipConfigurations[0].properties.subnet.id == '{postgreSqlSubnet.Inner.Id}'";
                var resourcesExist = (await resourceGraphClient.ResourcesAsync(new QueryRequest(new[] { configuration.SubscriptionId }, resourcesInPostgreSqlSubnetQuery))).TotalRecords > 0;

                if (hasNoDelegations && resourcesExist)
                {
                    throw new ValidationException($"Subnet '{configuration.PostgreSqlSubnetName}' must be either empty or have 'Microsoft.DBforPostgreSQL/flexibleServers' delegation.");
                }
            }

            return (vnet, vmSubnet, postgreSqlSubnet);
        }

        private async Task ValidateBatchAccountQuotaAsync()
        {
            var accountQuota = (await new BatchManagementClient(tokenCredentials) { SubscriptionId = configuration.SubscriptionId }.Location.GetQuotasAsync(configuration.RegionName)).AccountQuota;
            var existingBatchAccountCount = (await new BatchManagementClient(tokenCredentials) { SubscriptionId = configuration.SubscriptionId }.BatchAccount.ListAsync()).AsEnumerable().Count(b => b.Location.Equals(configuration.RegionName));

            if (existingBatchAccountCount >= accountQuota)
            {
                throw new ValidationException($"The regional Batch account quota ({accountQuota} account(s) per region) for the specified subscription has been reached. Submit a support request to increase the quota or choose another region.", displayExample: false);
            }
        }

        private async Task ValidateVmAsync()
        {
            var computeSkus = (await azureSubscriptionClient.ComputeSkus.ListByRegionAsync(configuration.RegionName))
                .Where(s => s.ResourceType == ComputeResourceType.VirtualMachines && !s.Restrictions.Any())
                .Select(s => s.Name.ToString())
                .ToList();

            if (!computeSkus.Any())
            {
                throw new ValidationException($"Your subscription doesn't support virtual machine creation in {configuration.RegionName}.  Please create an Azure Support case: https://docs.microsoft.com/en-us/azure/azure-portal/supportability/how-to-create-azure-support-request", displayExample: false);
            }
            else if (!computeSkus.Any(s => s.Equals(configuration.VmSize, StringComparison.OrdinalIgnoreCase)))
            {
                throw new ValidationException($"The VmSize {configuration.VmSize} is not available or does not exist in {configuration.RegionName}.  You can use 'az vm list-skus --location {configuration.RegionName} --output table' to find an available VM.", displayExample: false);
            }
        }

        private static async Task<BlobServiceClient> GetBlobClientAsync(IStorageAccount storageAccount)
            => new(
                new Uri($"https://{storageAccount.Name}.blob.core.windows.net"),
                new StorageSharedKeyCredential(
                    storageAccount.Name,
                    (await storageAccount.GetKeysAsync())[0].Value));

        private async Task ValidateTokenProviderAsync()
        {
            try
            {
                await Execute("Retrieving Azure management token...", () => new AzureServiceTokenProvider().GetAccessTokenAsync("https://management.azure.com/"));
            }
            catch (AzureServiceTokenProviderException ex)
            {
                ConsoleEx.WriteLine("No access token found.  Please install the Azure CLI and login with 'az login'", ConsoleColor.Red);
                ConsoleEx.WriteLine("Link: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli");
                ConsoleEx.WriteLine($"Error details: {ex.Message}");
                Environment.Exit(1);
            }
        }

        private void ValidateInitialCommandLineArgsAsync()
        {
            void ThrowIfProvidedForUpdate(object attributeValue, string attributeName)
            {
                if (configuration.Update && attributeValue is not null)
                {
                    throw new ValidationException($"{attributeName} must not be provided when updating", false);
                }
            }

            void ThrowIfNotProvidedForUpdate(string attributeValue, string attributeName)
            {
                if (configuration.Update && string.IsNullOrWhiteSpace(attributeValue))
                {
                    throw new ValidationException($"{attributeName} is required for update.", false);
                }
            }

            void ThrowIfNotProvided(string attributeValue, string attributeName)
            {
                if (string.IsNullOrWhiteSpace(attributeValue))
                {
                    throw new ValidationException($"{attributeName} is required.", false);
                }
            }

            void ThrowIfNotProvidedForInstall(string attributeValue, string attributeName)
            {
                if (!configuration.Update && string.IsNullOrWhiteSpace(attributeValue))
                {
                    throw new ValidationException($"{attributeName} is required.", false);
                }
            }

            void ThrowIfTagsFormatIsUnacceptable(string attributeValue, string attributeName)
            {
                if (string.IsNullOrWhiteSpace(attributeValue))
                {
                    return;
                }

                try
                {
                    Utility.DelimitedTextToDictionary(attributeValue, "=", ",");
                }
                catch
                {
                    throw new ValidationException($"{attributeName} is specified in incorrect format. Try as TagName=TagValue,TagName=TagValue in double quotes", false);
                }
            }

            ThrowIfNotProvided(configuration.SubscriptionId, nameof(configuration.SubscriptionId));

            ThrowIfNotProvidedForInstall(configuration.RegionName, nameof(configuration.RegionName));

            ThrowIfNotProvidedForUpdate(configuration.ResourceGroupName, nameof(configuration.ResourceGroupName));
            ThrowIfNotProvidedForUpdate(configuration.VmPassword, nameof(configuration.VmPassword));

            ThrowIfProvidedForUpdate(configuration.RegionName, nameof(configuration.RegionName));
            ThrowIfProvidedForUpdate(configuration.StorageAccountName, nameof(configuration.StorageAccountName));
            ThrowIfProvidedForUpdate(configuration.BatchAccountName, nameof(configuration.BatchAccountName));
            ThrowIfProvidedForUpdate(configuration.CosmosDbAccountName, nameof(configuration.CosmosDbAccountName));
            ThrowIfProvidedForUpdate(configuration.ProvisionPostgreSqlOnAzure, nameof(configuration.ProvisionPostgreSqlOnAzure));
            ThrowIfProvidedForUpdate(configuration.ApplicationInsightsAccountName, nameof(configuration.ApplicationInsightsAccountName));
            ThrowIfProvidedForUpdate(configuration.PrivateNetworking, nameof(configuration.PrivateNetworking));
            ThrowIfProvidedForUpdate(configuration.VnetName, nameof(configuration.VnetName));
            ThrowIfProvidedForUpdate(configuration.VnetResourceGroupName, nameof(configuration.VnetResourceGroupName));
            ThrowIfProvidedForUpdate(configuration.SubnetName, nameof(configuration.SubnetName));
            ThrowIfProvidedForUpdate(configuration.Tags, nameof(configuration.Tags));
            ThrowIfTagsFormatIsUnacceptable(configuration.Tags, nameof(configuration.Tags));
        }

        private static void DisplayBillingReaderInsufficientAccessLevelWarning()
        {
            ConsoleEx.WriteLine("Warning: insufficient subscription access level to assign the Billing Reader", ConsoleColor.Yellow);
            ConsoleEx.WriteLine("role for the VM to your Azure Subscription.", ConsoleColor.Yellow);
            ConsoleEx.WriteLine("Deployment will continue, but only default VM prices will be used for your workflows,", ConsoleColor.Yellow);
            ConsoleEx.WriteLine("since the Billing Reader role is required to access RateCard API pricing data.", ConsoleColor.Yellow);
            ConsoleEx.WriteLine("To resolve this in the future, have your Azure subscription Owner or Contributor", ConsoleColor.Yellow);
            ConsoleEx.WriteLine("assign the Billing Reader role for the VM's managed identity to your Azure Subscription scope.", ConsoleColor.Yellow);
            ConsoleEx.WriteLine("More info: https://github.com/microsoft/CromwellOnAzure/blob/master/docs/troubleshooting-guide.md#dynamic-cost-optimization-and-ratecard-api-access", ConsoleColor.Yellow);
        }

        private static void DisplayValidationExceptionAndExit(ValidationException validationException)
        {
            ConsoleEx.WriteLine(validationException.Reason, ConsoleColor.Red);

            if (validationException.DisplayExample)
            {
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine($"Example: ", ConsoleColor.Green).Write($"deploy-cromwell-on-azure --subscriptionid {Guid.NewGuid()} --regionname westus2 --mainidentifierprefix coa", ConsoleColor.White);
            }

            Environment.Exit(1);
        }

        private async Task DeleteResourceGroupIfUserConsentsAsync()
        {
            if (!isResourceGroupCreated)
            {
                return;
            }

            var userResponse = string.Empty;

            if (!configuration.Silent)
            {
                ConsoleEx.WriteLine();
                ConsoleEx.Write("Delete the resource group?  Type 'yes' and press enter, or, press any key to exit: ");
                userResponse = ConsoleEx.ReadLine();
            }

            if (userResponse.Equals("yes", StringComparison.OrdinalIgnoreCase) || (configuration.Silent && configuration.DeleteResourceGroupOnFailure))
            {
                await DeleteResourceGroupAsync();
            }
        }

        private async Task<bool> RunTestWorkflow(IStorageAccount storageAccount, bool usePreemptibleVm = true)
        {
            var startTime = DateTime.UtcNow;
            var line = ConsoleEx.WriteLine("Running a test workflow...");
            var isTestWorkflowSuccessful = await TestWorkflowAsync(storageAccount, usePreemptibleVm);
            WriteExecutionTime(line, startTime);

            if (isTestWorkflowSuccessful)
            {
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine($"Test workflow succeeded.", ConsoleColor.Green);
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine("Learn more about how to use Cromwell on Azure: https://github.com/microsoft/CromwellOnAzure");
                ConsoleEx.WriteLine();
            }
            else
            {
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine($"Test workflow failed.", ConsoleColor.Red);
                ConsoleEx.WriteLine();
                WriteGeneralRetryMessageToConsole();
                ConsoleEx.WriteLine();
            }

            return isTestWorkflowSuccessful;
        }

        private static void WriteGeneralRetryMessageToConsole()
            => ConsoleEx.WriteLine("Please try deployment again, and create an issue if this continues to fail: https://github.com/microsoft/CromwellOnAzure/issues");

        private async Task<bool> TestWorkflowAsync(IStorageAccount storageAccount, bool usePreemptibleVm = true)
        {
            const string testDirectoryName = "test";
            const string wdlFileName = "test.wdl";
            const string workflowInputsFileName = "testInputs.json";
            const string inputFileName = "inputFile.txt";
            const string inputFileContent = "Hello from inputFile.txt!";

            var id = Guid.NewGuid();
            var wdlFileContent = Utility.GetFileContent(wdlFileName);
            var workflowInputsFileContent = Utility.GetFileContent(workflowInputsFileName).Replace("{InputFilePath}", $"/{storageAccount.Name}/{InputsContainerName}/{testDirectoryName}/{inputFileName}");

            if (!usePreemptibleVm)
            {
                wdlFileContent = wdlFileContent.Replace("preemptible: true", "preemptible: false", StringComparison.OrdinalIgnoreCase);
            }

            var workflowTrigger = new Workflow
            {
                WorkflowUrl = $"/{storageAccount.Name}/{InputsContainerName}/{testDirectoryName}/{wdlFileName}",
                WorkflowInputsUrl = $"/{storageAccount.Name}/{InputsContainerName}/{testDirectoryName}/{workflowInputsFileName}"
            };

            await UploadTextToStorageAccountAsync(storageAccount, InputsContainerName, $"{testDirectoryName}/{wdlFileName}", wdlFileContent);
            await UploadTextToStorageAccountAsync(storageAccount, InputsContainerName, $"{testDirectoryName}/{workflowInputsFileName}", workflowInputsFileContent);
            await UploadTextToStorageAccountAsync(storageAccount, InputsContainerName, $"{testDirectoryName}/{inputFileName}", inputFileContent);
            await UploadTextToStorageAccountAsync(storageAccount, WorkflowsContainerName, $"new/{id}.json", JsonConvert.SerializeObject(workflowTrigger, Formatting.Indented));

            return await IsWorkflowSuccessfulAfterLongPollingAsync(storageAccount, WorkflowsContainerName, id);
        }

        private static async Task<bool> IsWorkflowSuccessfulAfterLongPollingAsync(IStorageAccount storageAccount, string containerName, Guid id)
        {
            var container = (await GetBlobClientAsync(storageAccount)).GetBlobContainerClient(containerName);

            while (true)
            {
                try
                {
                    var succeeded = container.GetBlobs(prefix: $"succeeded/{id}").Count() == 1;
                    var failed = container.GetBlobs(prefix: $"failed/{id}").Count() == 1;

                    if (succeeded || failed)
                    {
                        return succeeded && !failed;
                    }
                }
                catch (Exception exc)
                {
                    // "Server is busy" occasionally can be ignored
                    ConsoleEx.WriteLine(exc.Message);
                }

                await Task.Delay(TimeSpan.FromSeconds(10));
            }
        }

        private Task Execute(string message, Func<Task> func)
            => Execute(message, async () => { await func(); return false; });

        private async Task<T> Execute<T>(string message, Func<Task<T>> func)
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
                catch (Microsoft.Rest.Azure.CloudException cloudException) when (cloudException.ToCloudErrorType() == CloudErrorType.ExpiredAuthenticationToken)
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

        private static async Task<(string Output, string Error, int ExitStatus)> ExecuteCommandOnVirtualMachineAsync(ConnectionInfo sshConnectionInfo, string command)
        {
            using var sshClient = new SshClient(sshConnectionInfo);
            sshClient.ConnectWithRetries();
            var (output, error, exitStatus) = await sshClient.ExecuteCommandAsync(command);
            sshClient.Disconnect();

            return (output, error, exitStatus);
        }

        private static Task<(string Output, string Error, int ExitStatus)> ExecuteCommandOnVirtualMachineWithRetriesAsync(ConnectionInfo sshConnectionInfo, string command)
            => sshCommandRetryPolicy.ExecuteAsync(() => ExecuteCommandOnVirtualMachineAsync(sshConnectionInfo, command));

        private static async Task UploadFilesToVirtualMachineAsync(ConnectionInfo sshConnectionInfo, params (string fileContent, string remoteFilePath, bool makeExecutable)[] files)
            => await UploadFilesToVirtualMachineAsync(sshConnectionInfo, files.Select(f => ((Stream)new MemoryStream(Encoding.UTF8.GetBytes(f.fileContent)), f.remoteFilePath, f.makeExecutable)).ToArray());

        private static async Task UploadFilesToVirtualMachineAsync(ConnectionInfo sshConnectionInfo, params (Stream input, string remoteFilePath, bool makeExecutable)[] files)
        {
            using var sshClient = new SshClient(sshConnectionInfo);
            using var sftpClient = new SftpClient(sshConnectionInfo);

            try
            {
                sshClient.ConnectWithRetries();
                sftpClient.Connect();

                foreach (var (input, remoteFilePath, makeExecutable) in files)
                {
                    var dir = GetLinuxParentPath(remoteFilePath);

                    // Create destination directory if needed and make it writable for the current user
                    var (output, _, _) = await sshClient.ExecuteCommandAsync($"sudo mkdir -p {dir} && owner=$(stat -c '%U' {dir}) && mask=$(stat -c '%a' {dir}) && ownerCanWrite=$(( (16#$mask & 16#200) > 0 )) && othersCanWrite=$(( (16#$mask & 16#002) > 0 )) && ( [[ $owner == $(whoami) && $ownerCanWrite == 1 || $othersCanWrite == 1 ]] && echo 0 || ( sudo chmod o+w {dir} && echo 1 ))");
                    var dirWasMadeWritableToOthers = output == "1";

                    // Make the destination file writable for the current user. The user running the update might not be the same user that created the file.
                    await sshClient.ExecuteCommandAsync($"sudo touch {remoteFilePath} && sudo chmod o+w {remoteFilePath}");
                    await sftpClient.UploadFileAsync(input, remoteFilePath, true);
                    await sshClient.ExecuteCommandAsync($"sudo chmod o-w {remoteFilePath}");

                    if (makeExecutable)
                    {
                        await sshClient.ExecuteCommandAsync($"sudo chmod +x {remoteFilePath}");
                    }

                    if (dirWasMadeWritableToOthers)
                    {
                        await sshClient.ExecuteCommandAsync($"sudo chmod o-w {dir}");
                    }
                }
            }
            finally
            {
                sshClient.Disconnect();
                sftpClient.Disconnect();

                foreach (var (input, _, _) in files)
                {
                    await input.DisposeAsync();
                }

                sshClient.Dispose();
                sftpClient.Dispose();
            }
        }

        private static async Task DeleteFileFromVirtualMachineAsync(ConnectionInfo sshConnectionInfo, string filePath)
        {
            using var sshClient = new SshClient(sshConnectionInfo);
            sshClient.ConnectWithRetries();
            await sshClient.ExecuteCommandAsync($"sudo rm -f {filePath}");
            sshClient.Disconnect();
        }

        private async Task<string> DownloadTextFromStorageAccountAsync(IStorageAccount storageAccount, string containerName, string blobName)
        {
            var blobClient = await GetBlobClientAsync(storageAccount);
            var container = blobClient.GetBlobContainerClient(containerName);

            return (await container.GetBlobClient(blobName).DownloadContentAsync(cts.Token)).Value.Content.ToString();
        }

        private async Task UploadTextToStorageAccountAsync(IStorageAccount storageAccount, string containerName, string blobName, string content)
        {
            var blobClient = await GetBlobClientAsync(storageAccount);
            var container = blobClient.GetBlobContainerClient(containerName);

            await container.CreateIfNotExistsAsync();
            await container.GetBlobClient(blobName).UploadAsync(BinaryData.FromString(content), true, cts.Token);
        }

        private static string GetLinuxParentPath(string path)
        {
            const char dirSeparator = '/';

            if (string.IsNullOrEmpty(path))
            {
                return null;
            }

            var pathComponents = path.TrimEnd(dirSeparator).Split(dirSeparator);

            return string.Join(dirSeparator, pathComponents.Take(pathComponents.Length - 1));
        }

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
    }
}
