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
using Common;
using Microsoft.Azure.Management.Batch;
using Microsoft.Azure.Management.Batch.Models;
using Microsoft.Azure.Management.Compute.Fluent;
using Microsoft.Azure.Management.Compute.Fluent.Models;
using Microsoft.Azure.Management.CosmosDB.Fluent;
using Microsoft.Azure.Management.Fluent;
using Microsoft.Azure.Management.Graph.RBAC.Fluent;
using Microsoft.Azure.Management.Graph.RBAC.Fluent.Models;
using Microsoft.Azure.Management.Msi.Fluent;
using Microsoft.Azure.Management.Network.Fluent;
using Microsoft.Azure.Management.Network.Fluent.Models;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Microsoft.Azure.Management.Storage.Fluent;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Auth;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Rest;
using Microsoft.Rest.Azure.OData;
using Newtonsoft.Json;
using Polly;
using Polly.Retry;
using Renci.SshNet;

namespace CromwellOnAzureDeployer
{
    public class Deployer
    {
        private static readonly AsyncRetryPolicy roleAssignmentHashConflictRetryPolicy = Policy
            .Handle<Microsoft.Rest.Azure.CloudException>(cloudException => cloudException.Body.Code.Equals("HashConflictOnDifferentRoleAssignmentIds"))
            .RetryAsync();
        private const string WorkflowsContainerName = "workflows";
        private const string ConfigurationContainerName = "configuration";
        private const string InputsContainerName = "inputs";
        private const string CromwellAzureRootDir = "/data/cromwellazure";

        private readonly CancellationTokenSource cts = new CancellationTokenSource();

        private readonly List<string> requiredResourceProviders = new List<string>
            {
                "Microsoft.Authorization",
                "Microsoft.Batch",
                "Microsoft.Compute",
                "Microsoft.DocumentDB",
                "Microsoft.insights",
                "Microsoft.Network",
                "Microsoft.Storage"
            };

        private Configuration configuration { get; set; }
        private TokenCredentials tokenCredentials;
        private IAzure azureClient { get; set; }
        private IResourceManager resourceManagerClient { get; set; }
        private AzureCredentials azureCredentials { get; set; }
        private bool SkipBillingReaderRoleAssignment { get; set; }

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

                RefreshableConsole.WriteLine("Running...");

                await ValidateTokenProviderAsync();

                tokenCredentials = new TokenCredentials(new RefreshableAzureServiceTokenProvider("https://management.azure.com/"));
                azureCredentials = new AzureCredentials(tokenCredentials, null, null, AzureEnvironment.AzureGlobalCloud);
                azureClient = GetAzureClient(azureCredentials);
                resourceManagerClient = GetResourceManagerClient(azureCredentials);

                await ValidateSubscriptionAndResourceGroupAsync(configuration.SubscriptionId, configuration.ResourceGroupName, configuration.Update);

                IResourceGroup resourceGroup = null;
                BatchAccount batchAccount = null;
                IGenericResource appInsights = null;
                ICosmosDBAccount cosmosDb = null;
                IStorageAccount storageAccount = null;
                IVirtualMachine linuxVm = null;
                INetworkSecurityGroup networkSecurityGroup = null;
                IIdentity managedIdentity = null;
                ConnectionInfo sshConnectionInfo = null;

                if (configuration.Update)
                {
                    resourceGroup = await azureClient.ResourceGroups.GetByNameAsync(configuration.ResourceGroupName);

                    if (string.IsNullOrWhiteSpace(configuration.VmPassword))
                    {
                        throw new ValidationException($"--VmPassword is required for update.");
                    }

                    var existingVms = await azureClient.VirtualMachines.ListByResourceGroupAsync(configuration.ResourceGroupName);

                    if (!existingVms.Any())
                    {
                        throw new ValidationException($"Update was requested but resource group {configuration.ResourceGroupName} does not contain any virtual machines.");
                    }

                    if (existingVms.Count() > 1 && string.IsNullOrWhiteSpace(configuration.VmName))
                    {
                        throw new ValidationException($"Resource group {configuration.ResourceGroupName} contains multiple virtual machines. --VmName must be provided.");
                    }

                    if (!string.IsNullOrWhiteSpace(configuration.VmName))
                    {
                        linuxVm = existingVms.FirstOrDefault(vm => vm.Name.Equals(configuration.VmName, StringComparison.OrdinalIgnoreCase));

                        if (linuxVm == null)
                        {
                            throw new ValidationException($"Virtual machine {configuration.VmName} does not exist in resource group {configuration.ResourceGroupName}.");
                        }
                    }
                    else
                    {
                        linuxVm = existingVms.Single();
                    }

                    configuration.VmName = linuxVm.Name;

                    var existingUserManagedIdentityId = linuxVm.UserAssignedManagedServiceIdentityIds.FirstOrDefault();

                    if (existingUserManagedIdentityId == null)
                    {
                        managedIdentity = await ReplaceSystemManagedIdentityWithUserManagedIdentityAsync(resourceGroup, linuxVm);
                    }
                    else
                    {
                        managedIdentity = await azureClient.Identities.GetByIdAsync(existingUserManagedIdentityId);
                    }

                    networkSecurityGroup = (await azureClient.NetworkSecurityGroups.ListByResourceGroupAsync(configuration.ResourceGroupName)).FirstOrDefault();

                    if(networkSecurityGroup == null)
                    {
                        if (string.IsNullOrWhiteSpace(configuration.NetworkSecurityGroupName))
                        {
                            configuration.NetworkSecurityGroupName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}", 15);
                        }

                        networkSecurityGroup = await CreateNetworkSecurityGroupAsync(resourceGroup, configuration.NetworkSecurityGroupName);
                        await AssociateNicWithNetworkSecurityGroupAsync(linuxVm.GetPrimaryNetworkInterface(), networkSecurityGroup);
                    }

                    sshConnectionInfo = new ConnectionInfo(linuxVm.GetPrimaryPublicIPAddress().Fqdn, configuration.VmUsername, new PasswordAuthenticationMethod(configuration.VmUsername, configuration.VmPassword));

                    await WaitForSshConnectivityAsync(sshConnectionInfo);

                    var installedVersion = await GetInstalledCromwellOnAzureVersionAsync(sshConnectionInfo);

                    await ConfigureVmAsync(sshConnectionInfo);

                    var accountNames = DelimitedTextToDictionary((await ExecuteCommandOnVirtualMachineAsync(sshConnectionInfo, $"cat {CromwellAzureRootDir}/env-01-account-names.txt || echo ''")).Output);

                    if(!accountNames.Any())
                    {
                        throw new ValidationException($"Could not retrieve account names from virtual machine {configuration.VmName}.");
                    }

                    if (!accountNames.TryGetValue("BatchAccountName", out var batchAccountName))
                    {
                        throw new ValidationException($"Could not retrieve the Batch account name from virtual machine {configuration.VmName}.");
                    }

                    batchAccount = (await new BatchManagementClient(tokenCredentials) { SubscriptionId = configuration.SubscriptionId }.BatchAccount.ListByResourceGroupAsync(configuration.ResourceGroupName))
                        .FirstOrDefault(a => a.Name.Equals(batchAccountName, StringComparison.OrdinalIgnoreCase)) 
                            ?? throw new ValidationException($"Batch account {batchAccountName} does not exist in resource group {configuration.ResourceGroupName}.");

                    configuration.BatchAccountName = batchAccountName;

                    if (!accountNames.TryGetValue("DefaultStorageAccountName", out var storageAccountName))
                    {
                        throw new ValidationException($"Could not retrieve the default storage account name from virtual machine {configuration.VmName}.");
                    }

                    storageAccount = (await azureClient.StorageAccounts.ListByResourceGroupAsync(configuration.ResourceGroupName))
                        .FirstOrDefault(a => a.Name.Equals(storageAccountName, StringComparison.OrdinalIgnoreCase))
                            ?? throw new ValidationException($"Storage account {storageAccountName} does not exist in resource group {configuration.ResourceGroupName}.");

                    configuration.StorageAccountName = storageAccountName;

                    await WriteNonPersonalizedFilesToStorageAccountAsync(storageAccount);

                    if(installedVersion == null)
                    {
                        // If upgrading from pre-2.0 version, patch the installed cromwell-application.conf (disable call caching and default to preemptible)
                        await PatchCromwellConfigurationFileAsync(storageAccount);
                    }
                }

                if (!configuration.Update)
                {
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
                    await ValidateBatchQuotaAsync();

                    RefreshableConsole.WriteLine();
                    RefreshableConsole.WriteLine($"VM host: {configuration.VmName}.{configuration.RegionName}.cloudapp.azure.com");
                    RefreshableConsole.WriteLine($"VM username: {configuration.VmUsername}");
                    RefreshableConsole.WriteLine($"VM password: {configuration.VmPassword}");
                    RefreshableConsole.WriteLine();

                    if (string.IsNullOrWhiteSpace(configuration.ResourceGroupName))
                    {
                        configuration.ResourceGroupName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}-", 15);
                        resourceGroup = await CreateResourceGroupAsync();
                    }
                    else
                    {
                        resourceGroup = await azureClient.ResourceGroups.GetByNameAsync(configuration.ResourceGroupName);
                    }

                    managedIdentity = await CreateUserManagedIdentityAsync(resourceGroup);

                    await Task.WhenAll(new Task[]
                    {
                        Task.Run(async () => batchAccount = await CreateBatchAccountAsync()),
                        Task.Run(async () => appInsights = await CreateAppInsightsResourceAsync()),
                        Task.Run(async () => cosmosDb = await CreateCosmosDbAsync()),

                        Task.Run(() => CreateStorageAccountAsync()
                            .ContinueWith(async t => 
                                { 
                                    storageAccount = t.Result; 
                                    await WriteNonPersonalizedFilesToStorageAccountAsync(storageAccount);
                                    await WritePersonalizedFilesToStorageAccountAsync(storageAccount);
                                },
                                TaskContinuationOptions.OnlyOnRanToCompletion)
                            .Unwrap()),

                        Task.Run(() => CreateVirtualMachineAsync(managedIdentity)
                            .ContinueWith(async t =>
                                {
                                    linuxVm = t.Result;
                                    networkSecurityGroup = await CreateNetworkSecurityGroupAsync(resourceGroup, configuration.NetworkSecurityGroupName);
                                    await AssociateNicWithNetworkSecurityGroupAsync(linuxVm.GetPrimaryNetworkInterface(), networkSecurityGroup);

                                    sshConnectionInfo = new ConnectionInfo(linuxVm.GetPrimaryPublicIPAddress().Fqdn, configuration.VmUsername, new PasswordAuthenticationMethod(configuration.VmUsername, configuration.VmPassword));
                                    await WaitForSshConnectivityAsync(sshConnectionInfo);
                                    await ConfigureVmAsync(sshConnectionInfo);
                                },
                                TaskContinuationOptions.OnlyOnRanToCompletion)
                            .Unwrap())
                    });

                    if (!SkipBillingReaderRoleAssignment)
                    {
                        await AssignVmAsBillingReaderToSubscriptionAsync(managedIdentity);
                    }

                    await AssignVmAsContributorToAppInsightsAsync(managedIdentity, appInsights);
                    await AssignVmAsContributorToCosmosDb(managedIdentity, cosmosDb);
                    await AssignVmAsContributorToBatchAccountAsync(managedIdentity, batchAccount);
                    await AssignVmAsContributorToStorageAccountAsync(managedIdentity, storageAccount);
                    await AssignVmAsDataReaderToStorageAccountAsync(managedIdentity, storageAccount);
                }

                await RestartVmAsync(linuxVm);
                await WaitForSshConnectivityAsync(sshConnectionInfo);

                if (!await IsStartupSuccessfulAsync(sshConnectionInfo))
                {
                    RefreshableConsole.WriteLine($"Startup script on the VM failed. Check {CromwellAzureRootDir}/startup.log for details", ConsoleColor.Red);
                    return 1;
                }

                await WaitForDockerComposeAsync(sshConnectionInfo);
                await WaitForCromwellAsync(sshConnectionInfo);

                var isBatchQuotaAvailable = batchAccount.LowPriorityCoreQuota > 0 || batchAccount.DedicatedCoreQuota > 0;

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
                    if(!configuration.SkipTestWorkflow)
                    {
                        RefreshableConsole.WriteLine($"Could not run the test workflow.", ConsoleColor.Yellow);
                    }

                    RefreshableConsole.WriteLine($"Deployment was successful, but Batch account {configuration.BatchAccountName} does not have sufficient core quota to run workflows.", ConsoleColor.Yellow);
                    RefreshableConsole.WriteLine($"Request Batch core quota: https://docs.microsoft.com/en-us/azure/batch/batch-quota-limit", ConsoleColor.Yellow);
                    RefreshableConsole.WriteLine($"After receiving the quota, read the docs to run a test workflow and confirm successful deployment.", ConsoleColor.Yellow);
                    exitCode = 2;
                }

                RefreshableConsole.WriteLine($"Completed in {mainTimer.Elapsed.TotalMinutes:n1} minutes.");

                return exitCode;
            }
            catch (ValidationException validationException)
            {
                DisplayValidationExceptionAndExit(validationException);
                return 1;
            }
            catch (Microsoft.Rest.Azure.CloudException cloudException)
            {
                RefreshableConsole.WriteLine();
                RefreshableConsole.WriteLine(cloudException.Message, ConsoleColor.Red);
                RefreshableConsole.WriteLine();
                WriteGeneralRetryMessageToConsole();
                Debugger.Break();
                await DeleteResourceGroupIfUserConsentsAsync();
                return 1;
            }
            catch (Exception exc)
            {
                RefreshableConsole.WriteLine();
                RefreshableConsole.WriteLine(exc.Message, ConsoleColor.Red);
                RefreshableConsole.WriteLine();
                Debugger.Break();
                WriteGeneralRetryMessageToConsole();
                await DeleteResourceGroupIfUserConsentsAsync();
                return 1;
            }
        }

        private Task DelayAsync(string message, TimeSpan duration)
        {
            return Execute(message, () => Task.Delay(duration));
        }

        private Task WaitForSshConnectivityAsync(ConnectionInfo sshConnectionInfo)
        {
            var timeout = TimeSpan.FromMinutes(10);

            return Execute(
                "Waiting for VM to accept SSH connections...",
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
        {
            return Execute(
                "Waiting for docker containers to download and start...",
                async () =>
                {
                    while (!cts.IsCancellationRequested)
                    {
                        var (numberOfRunningContainers, _, _) = await ExecuteCommandOnVirtualMachineAsync(sshConnectionInfo, "sudo docker ps -a | grep -c 'Up ' || :");

                        if (numberOfRunningContainers == "4")
                        {
                            break;
                        }

                        await Task.Delay(5000, cts.Token);
                    }
                });
        }

        private Task WaitForCromwellAsync(ConnectionInfo sshConnectionInfo)
        {
            return Execute(
                "Waiting for Cromwell to perform one-time database preparation...",
                async () =>
                {
                    while (!cts.IsCancellationRequested)
                    {
                        var (isCromwellAvailable, _, _) = await ExecuteCommandOnVirtualMachineAsync(sshConnectionInfo, "[ $(sudo docker logs cromwellazure_triggerservice_1 | grep -c 'Cromwell is available.') -gt 0 ] && echo 1 || echo 0");

                        if (isCromwellAvailable == "1")
                        {
                            break;
                        }

                        await Task.Delay(5000, cts.Token);
                    }
                });
        }

        private Task<bool> IsStartupSuccessfulAsync(ConnectionInfo sshConnectionInfo)
        {
            return Execute(
                "Waiting for startup script completion...",
                async () =>
                {
                    while (!cts.IsCancellationRequested)
                    {
                        var (startupLogContent, _, _) = await ExecuteCommandOnVirtualMachineAsync(sshConnectionInfo, $"cat {CromwellAzureRootDir}/startup.log || echo ''");

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
        }

        private IAzure GetAzureClient(AzureCredentials azureCredentials)
        {
            return Azure
                .Configure()
                .WithLogLevel(HttpLoggingDelegatingHandler.Level.Basic)
                .Authenticate(azureCredentials)
                .WithSubscription(configuration.SubscriptionId);
        }

        private IResourceManager GetResourceManagerClient(AzureCredentials azureCredentials)
        {
            return ResourceManager
                .Configure()
                .WithLogLevel(HttpLoggingDelegatingHandler.Level.Basic)
                .Authenticate(azureCredentials)
                .WithSubscription(configuration.SubscriptionId);
        }

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
                RefreshableConsole.WriteLine();
                RefreshableConsole.WriteLine("Unable to programatically register the required resource providers.", ConsoleColor.Red);
                RefreshableConsole.WriteLine("This can happen if you don't have the Owner or Contributor role assignment for the subscription.", ConsoleColor.Red);
                RefreshableConsole.WriteLine();
                RefreshableConsole.WriteLine("Please contact the Owner or Contributor of your Azure subscription, and have them:", ConsoleColor.Yellow);
                RefreshableConsole.WriteLine();
                RefreshableConsole.WriteLine("1. Navigate to https://portal.azure.com", ConsoleColor.Yellow);
                RefreshableConsole.WriteLine("2. Select Subscription -> Resource Providers", ConsoleColor.Yellow);
                RefreshableConsole.WriteLine("3. Select each of the following and click Register:", ConsoleColor.Yellow);
                RefreshableConsole.WriteLine();
                unregisteredResourceProviders.ForEach(rp => RefreshableConsole.WriteLine($"- {rp}", ConsoleColor.Yellow));
                RefreshableConsole.WriteLine();
                RefreshableConsole.WriteLine("After completion, please re-attempt deployment.");

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

        private async Task ConfigureVmAsync(ConnectionInfo sshConnectionInfo)
        {
            await MountDataDiskOnTheVirtualMachineAsync(sshConnectionInfo);
            await ExecuteCommandOnVirtualMachineAsync(sshConnectionInfo, $"sudo mkdir -p {CromwellAzureRootDir} && sudo chown {configuration.VmUsername} {CromwellAzureRootDir} && sudo chmod ug=rwx,o= {CromwellAzureRootDir}");
            await WriteNonPersonalizedFilesToVmAsync(sshConnectionInfo);
            await RunInstallationScriptAsync(sshConnectionInfo);
            await HandleCustomImagesAsync(sshConnectionInfo);

            if (!configuration.Update)
            {
                await WritePersonalizedFilesToVmAsync(sshConnectionInfo);
            }
        }

        private async Task<Version> GetInstalledCromwellOnAzureVersionAsync(ConnectionInfo sshConnectionInfo)
        {
            var versionString = (await ExecuteCommandOnVirtualMachineAsync(sshConnectionInfo, @"grep -sPo 'CromwellOnAzureVersion=\K(.*)$' /cromwellazure/env-02-internal-images.txt || :")).Output;

            return !string.IsNullOrEmpty(versionString) && Version.TryParse(versionString, out var version) ? version : null;
        }

        private Task MountDataDiskOnTheVirtualMachineAsync(ConnectionInfo sshConnectionInfo)
        {
            return Execute(
                $"Mounting data disk to the VM...",
                async () =>
                {
                    await UploadFileToVirtualMachineAsync(sshConnectionInfo, ReadAllTextWithUnixLineEndings(GetPathFromAppRelativePath("scripts", "mount-data-disk.sh")), $"/tmp/mount-data-disk.sh", true);
                    await ExecuteCommandOnVirtualMachineAsync(sshConnectionInfo, $"/tmp/mount-data-disk.sh");
                });
        }

        private Task WriteNonPersonalizedFilesToVmAsync(ConnectionInfo sshConnectionInfo)
        {
            return Execute(
                $"Writing files to the VM...",
                async () =>
                {
                    await UploadFileToVirtualMachineAsync(sshConnectionInfo, ReadAllTextWithUnixLineEndings(GetPathFromAppRelativePath("scripts", "startup.sh")), $"{CromwellAzureRootDir}/startup.sh", true);
                    await UploadFileToVirtualMachineAsync(sshConnectionInfo, ReadAllTextWithUnixLineEndings(GetPathFromAppRelativePath("scripts", "wait-for-it.sh")), $"{CromwellAzureRootDir}/wait-for-it/wait-for-it.sh", true);
                    await UploadFileToVirtualMachineAsync(sshConnectionInfo, ReadAllTextWithUnixLineEndings(GetPathFromAppRelativePath("scripts", "install-cromwellazure.sh")), $"{CromwellAzureRootDir}/install-cromwellazure.sh", true);
                    await UploadFileToVirtualMachineAsync(sshConnectionInfo, ReadAllTextWithUnixLineEndings(GetPathFromAppRelativePath("scripts", "mount_containers.sh")), $"{CromwellAzureRootDir}/mount_containers.sh", true);
                    await UploadFileToVirtualMachineAsync(sshConnectionInfo, ReadAllTextWithUnixLineEndings(GetPathFromAppRelativePath("scripts", "env-02-internal-images.txt")), $"{CromwellAzureRootDir}/env-02-internal-images.txt", false);
                    await UploadFileToVirtualMachineAsync(sshConnectionInfo, ReadAllTextWithUnixLineEndings(GetPathFromAppRelativePath("scripts", "env-03-external-images.txt")), $"{CromwellAzureRootDir}/env-03-external-images.txt", false);
                    await UploadFileToVirtualMachineAsync(sshConnectionInfo, ReadAllTextWithUnixLineEndings(GetPathFromAppRelativePath("scripts", "docker-compose.yml")), $"{CromwellAzureRootDir}/docker-compose.yml", false);
                    await UploadFileToVirtualMachineAsync(sshConnectionInfo, ReadAllTextWithUnixLineEndings(GetPathFromAppRelativePath("scripts", "cromwellazure.service")), "/lib/systemd/system/cromwellazure.service", false);
                    await UploadFileToVirtualMachineAsync(sshConnectionInfo, ReadAllTextWithUnixLineEndings(GetPathFromAppRelativePath("scripts", "mount.blobfuse")), "/usr/sbin/mount.blobfuse", true);
                });
        }

        private Task RunInstallationScriptAsync(ConnectionInfo sshConnectionInfo)
        {
            return Execute(
                $"Running installation script on the VM...",
                async () => { 
                    await ExecuteCommandOnVirtualMachineAsync(sshConnectionInfo, $"{CromwellAzureRootDir}/install-cromwellazure.sh");
                    await ExecuteCommandOnVirtualMachineAsync(sshConnectionInfo, $"sudo usermod -aG docker {configuration.VmUsername}");
                });
        }

        private async Task WritePersonalizedFilesToVmAsync(ConnectionInfo sshConnectionInfo)
        {
            await UploadFileToVirtualMachineAsync(
                sshConnectionInfo, 
                ReadAllTextWithUnixLineEndings(GetPathFromAppRelativePath("scripts", "env-01-account-names.txt"))
                    .Replace("{DefaultStorageAccountName}", configuration.StorageAccountName)
                    .Replace("{CosmosDbAccountName}", configuration.CosmosDbAccountName)
                    .Replace("{BatchAccountName}", configuration.BatchAccountName)
                    .Replace("{ApplicationInsightsAccountName}", configuration.ApplicationInsightsAccountName), 
                $"{CromwellAzureRootDir}/env-01-account-names.txt", 
                false);

            await UploadFileToVirtualMachineAsync(sshConnectionInfo, ReadAllTextWithUnixLineEndings(GetPathFromAppRelativePath("scripts", "env-04-settings.txt")), $"{CromwellAzureRootDir}/env-04-settings.txt", false);
        }

        private async Task HandleCustomImagesAsync(ConnectionInfo sshConnectionInfo)
        {
            await HandleCustomImageAsync(sshConnectionInfo, configuration.CromwellVersion, configuration.CustomCromwellImagePath, "env-05-custom-cromwell-image-name.txt", "CromwellImageName", cromwellVersion => $"broadinstitute/cromwell:{cromwellVersion}");
            await HandleCustomImageAsync(sshConnectionInfo, configuration.TesImageName, configuration.CustomTesImagePath, "env-06-custom-tes-image-name.txt", "TesImageName");
            await HandleCustomImageAsync(sshConnectionInfo, configuration.TriggerServiceImageName, configuration.CustomTriggerServiceImagePath, "env-07-custom-trigger-service-image-name.txt", "TriggerServiceImageName");
        }

        private async Task HandleCustomImageAsync(ConnectionInfo sshConnectionInfo, string imageNameOrTag, string customImagePath, string envFileName, string envFileKey, Func<string, string> imageNameFactory = null)
        {
            async Task CopyCustomDockerImageAsync(string customImagePath)
            {
                var startTime = DateTime.UtcNow;
                var line = RefreshableConsole.WriteLine($"Copying custom image from {customImagePath} to the VM...");
                var remotePath = $"{CromwellAzureRootDir}/{Path.GetFileName(customImagePath)}";
                await UploadFileToVirtualMachineAsync(sshConnectionInfo, File.OpenRead(customImagePath), remotePath, false);
                WriteExecutionTime(line, startTime);
            }

            async Task<string> LoadCustomDockerImageAsync(string customImagePath)
            {
                var startTime = DateTime.UtcNow;
                var line = RefreshableConsole.WriteLine($"Loading custom image {customImagePath} on the VM...");
                var remotePath = $"{CromwellAzureRootDir}/{Path.GetFileName(customImagePath)}";
                var (loadedImageName, _, _) = await ExecuteCommandOnVirtualMachineAsync(sshConnectionInfo, $"imageName=$(sudo docker load -i {remotePath}) && rm {remotePath} && imageName=$(expr \"$imageName\" : 'Loaded.*: \\(.*\\)') && echo $imageName");
                WriteExecutionTime(line, startTime);

                return loadedImageName;
            }

            if (imageNameOrTag != null && imageNameOrTag.Equals(string.Empty))
            {
                await DeleteFileFromVirtualMachineAsync(sshConnectionInfo, $"{CromwellAzureRootDir}/{envFileName}");
            }
            else if (!string.IsNullOrEmpty(imageNameOrTag))
            {
                var actualImageName = imageNameFactory != null ? imageNameFactory(imageNameOrTag) : imageNameOrTag;
                await UploadFileToVirtualMachineAsync(sshConnectionInfo, $"{envFileKey}={actualImageName}", $"{CromwellAzureRootDir}/{envFileName}", false);
            }
            else if (!string.IsNullOrEmpty(customImagePath))
            {
                await CopyCustomDockerImageAsync(customImagePath);
                var loadedImageName = await LoadCustomDockerImageAsync(customImagePath);
                await UploadFileToVirtualMachineAsync(sshConnectionInfo, $"{envFileKey}={loadedImageName}", $"{CromwellAzureRootDir}/{envFileName}", false);
            }
        }

        private Task RestartVmAsync(IVirtualMachine linuxVm)
        {
            return Execute(
                "Restarting VM...",
                async () =>
                {
                    await linuxVm.RestartAsync(cts.Token);
                    return Task.CompletedTask;
                });
        }

        private Task AssignVmAsDataReaderToStorageAccountAsync(IIdentity managedIdentity, IStorageAccount storageAccount)
        {
            // https://docs.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#storage-blob-data-reader
            var roleDefinitionId = $"/subscriptions/{configuration.SubscriptionId}/providers/Microsoft.Authorization/roleDefinitions/2a2b9908-6ea1-4ae2-8e65-a410df84e7d1";

            return Execute(
                $"Assigning Storage Blob Data Reader role for VM to Storage Account resource scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    () => azureClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithRoleDefinition(roleDefinitionId)
                        .WithResourceScope(storageAccount)
                        .CreateAsync(cts.Token)));
        }

        private Task AssignVmAsContributorToStorageAccountAsync(IIdentity managedIdentity, IResource storageAccount)
        {
            return Execute(
                $"Assigning {BuiltInRole.Contributor} role for VM to Storage Account resource scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    () => azureClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithBuiltInRole(BuiltInRole.Contributor)
                        .WithResourceScope(storageAccount)
                        .CreateAsync(cts.Token)));
        }

        private Task<IStorageAccount> CreateStorageAccountAsync()
        {
            return Execute(
                $"Creating Storage Account: {configuration.StorageAccountName}...",
                async () =>
                {
                    var storageAccount = await azureClient.StorageAccounts
                        .Define(configuration.StorageAccountName)
                        .WithRegion(configuration.RegionName)
                        .WithExistingResourceGroup(configuration.ResourceGroupName)
                        .CreateAsync(cts.Token);

                    cts.Token.ThrowIfCancellationRequested();

                    var blobClient = await GetBlobClientAsync(storageAccount);
                    var defaultContainers = new List<string> { WorkflowsContainerName, InputsContainerName, "cromwell-executions", "cromwell-workflow-logs", "outputs", ConfigurationContainerName };

                    await Task.WhenAll(defaultContainers.Select(c => blobClient.GetContainerReference(c).CreateIfNotExistsAsync(cts.Token)));

                    return storageAccount;
                });
        }

        private Task WriteNonPersonalizedFilesToStorageAccountAsync(IStorageAccount storageAccount)
        {
            return Execute(
                $"Writing readme.txt files to '{WorkflowsContainerName}' storage container...",
                async () =>
                {
                    await UploadTextToStorageAccountAsync(storageAccount, WorkflowsContainerName, "new/readme.txt", "Upload a trigger file to this virtual directory to create a new workflow. Additional information here: https://github.com/microsoft/CromwellOnAzure");
                    await UploadTextToStorageAccountAsync(storageAccount, WorkflowsContainerName, "abort/readme.txt", "Upload an empty file to this virtual directory to abort an existing workflow. The empty file's name shall be the Cromwell workflow ID you wish to cancel.  Additional information here: https://github.com/microsoft/CromwellOnAzure");
                });
        }

        private Task WritePersonalizedFilesToStorageAccountAsync(IStorageAccount storageAccount)
        {
            return Execute(
                $"Writing containers-to-mount and cromwell-application.conf files to '{ConfigurationContainerName}' storage container...",
                async () =>
                {
                    var containersToMountConfigPath = GetPathFromAppRelativePath("scripts", "containers-to-mount");
                    var containersToMountConfigText = ReadAllTextWithUnixLineEndings(containersToMountConfigPath).Replace("{DefaultStorageAccountName}", configuration.StorageAccountName);
                    await UploadTextToStorageAccountAsync(storageAccount, ConfigurationContainerName, Path.GetFileName(containersToMountConfigPath), containersToMountConfigText);

                    var cromwellAppConfigPath = GetPathFromAppRelativePath("scripts", "cromwell-application.conf");
                    var cromwellAppConfigText = ReadAllTextWithUnixLineEndings(cromwellAppConfigPath);
                    await UploadTextToStorageAccountAsync(storageAccount, ConfigurationContainerName, Path.GetFileName(cromwellAppConfigPath), cromwellAppConfigText);

                });
        }

        private Task AssignVmAsContributorToBatchAccountAsync(IIdentity managedIdentity, BatchAccount batchAccount)
        {
            return Execute(
                $"Assigning {BuiltInRole.Contributor} role for VM to Batch Account resource scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    () => azureClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithBuiltInRole(BuiltInRole.Contributor)
                        .WithScope(batchAccount.Id)
                        .CreateAsync(cts.Token)));
        }

        private Task AssignVmAsContributorToCosmosDb(IIdentity managedIdentity, IResource cosmosDb)
        {
            return Execute(
                $"Assigning {BuiltInRole.Contributor} role for VM to Cosmos DB resource scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    () => azureClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithBuiltInRole(BuiltInRole.Contributor)
                        .WithResourceScope(cosmosDb)
                        .CreateAsync(cts.Token)));
        }

        private Task<ICosmosDBAccount> CreateCosmosDbAsync()
        {
            return Execute(
                $"Creating Cosmos DB: {configuration.CosmosDbAccountName}...",
                () => azureClient.CosmosDBAccounts
                    .Define(configuration.CosmosDbAccountName)
                    .WithRegion(configuration.RegionName)
                    .WithExistingResourceGroup(configuration.ResourceGroupName)
                    .WithDataModelSql()
                    .WithSessionConsistency()
                    .WithWriteReplication(Region.Create(configuration.RegionName))
                    .CreateAsync(cts.Token));
        }

        private Task AssignVmAsBillingReaderToSubscriptionAsync(IIdentity managedIdentity)
        {
            return Execute(
                $"Assigning {BuiltInRole.BillingReader} role for VM to Subscription scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    () => azureClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithBuiltInRole(BuiltInRole.BillingReader)
                        .WithSubscriptionScope(configuration.SubscriptionId)
                        .CreateAsync(cts.Token)));
        }

        private Task AssignVmAsContributorToAppInsightsAsync(IIdentity managedIdentity, IResource appInsights)
        {
            return Execute(
                $"Assigning {BuiltInRole.Contributor} role for VM to App Insights resource scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    () => azureClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithBuiltInRole(BuiltInRole.Contributor)
                        .WithResourceScope(appInsights)
                        .CreateAsync(cts.Token)));
        }

        private Task<IVirtualMachine> CreateVirtualMachineAsync(IIdentity managedIdentity)
        {
            const int dataDiskSizeGiB = 32;
            const int dataDiskLun = 0;

            return Execute(
                $"Creating Linux VM: {configuration.VmName}...",
                () => azureClient.VirtualMachines.Define(configuration.VmName)
                    .WithRegion(configuration.RegionName)
                    .WithExistingResourceGroup(configuration.ResourceGroupName)
                    .WithNewPrimaryNetwork(configuration.VnetAddressSpace)
                    .WithPrimaryPrivateIPAddressDynamic()
                    .WithNewPrimaryPublicIPAddress(configuration.VmName)
                    .WithLatestLinuxImage("Canonical", "UbuntuServer", configuration.VmOsVersion)
                    .WithRootUsername(configuration.VmUsername)
                    .WithRootPassword(configuration.VmPassword)
                    .WithNewDataDisk(dataDiskSizeGiB, dataDiskLun, CachingTypes.None)
                    .WithSize(configuration.VmSize)
                    .WithExistingUserAssignedManagedServiceIdentity(managedIdentity)
                    .CreateAsync(cts.Token));
        }

        private Task<INetworkSecurityGroup> CreateNetworkSecurityGroupAsync(IResourceGroup resourceGroup, string networkSecurityGroupName)
        {
            const string ruleName = "SSH";
            const int allowedPort = 22;
            const int defaultPriority = 300;

            return Execute(
                $"Creating Network Security Group: {networkSecurityGroupName}...",
                () => azureClient.NetworkSecurityGroups.Define(networkSecurityGroupName)
                    .WithRegion(resourceGroup.RegionName)
                    .WithExistingResourceGroup(resourceGroup)
                    .DefineRule(ruleName)
                    .AllowInbound()
                    .FromAnyAddress()
                    .FromAnyPort()
                    .ToAnyAddress()
                    .ToPort(allowedPort)
                    .WithProtocol(SecurityRuleProtocol.Tcp)
                    .WithPriority(defaultPriority)
                    .Attach()
                    .CreateAsync(cts.Token)
            );
        }

        private Task<INetworkInterface> AssociateNicWithNetworkSecurityGroupAsync(INetworkInterface networkInterface, INetworkSecurityGroup networkSecurityGroup)
        {
            return Execute(
                $"Associating VM NIC with Network Security Group {networkSecurityGroup.Name}...",
                () => networkInterface.Update().WithExistingNetworkSecurityGroup(networkSecurityGroup).ApplyAsync()
            );
        }

        private Task<IGenericResource> CreateAppInsightsResourceAsync()
        {
            return Execute(
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
                    .WithApiVersion("2015-05-01")
                    .WithParentResource(string.Empty)
                    .WithProperties(new Dictionary<string, string>() { { "Application_Type", "other" } })
                    .CreateAsync(cts.Token));
        }

        private Task<BatchAccount> CreateBatchAccountAsync()
        {
            return Execute(
                $"Creating Batch Account: {configuration.BatchAccountName}...",
                () => new BatchManagementClient(tokenCredentials) { SubscriptionId = configuration.SubscriptionId }
                    .BatchAccount
                    .CreateAsync(configuration.ResourceGroupName, configuration.BatchAccountName, new BatchAccountCreateParameters { Location = configuration.RegionName }, cts.Token)
                );
        }

        private Task<IResourceGroup> CreateResourceGroupAsync()
        {
            return Execute(
                $"Creating Resource Group: {configuration.ResourceGroupName}...",
                () => azureClient.ResourceGroups
                    .Define(configuration.ResourceGroupName)
                    .WithRegion(configuration.RegionName)
                    .CreateAsync(cts.Token));
        }

        private Task<IIdentity> CreateUserManagedIdentityAsync(IResourceGroup resourceGroup)
        {
            var managedIdentityName = $"{resourceGroup.Name}-identity";

            return Execute(
                $"Creating user-managed identity: {managedIdentityName}...",
                () => azureClient.Identities.Define(managedIdentityName)
                    .WithRegion(resourceGroup.RegionName)
                    .WithExistingResourceGroup(resourceGroup)
                    .CreateAsync());
        }

        private Task<IIdentity> ReplaceSystemManagedIdentityWithUserManagedIdentityAsync(IResourceGroup resourceGroup, IVirtualMachine linuxVm)
        {
            return Execute(
                "Replacing VM system-managed identity with user-managed identity for easier VM upgrades in the future...",
                async () =>
                {
                    var userManagedIdentity = await CreateUserManagedIdentityAsync(resourceGroup);

                    var existingVmRoles =
                        (await azureClient.AccessManagement.RoleAssignments.Inner.ListForScopeWithHttpMessagesAsync(
                            $"/subscriptions/{configuration.SubscriptionId}",
                            new ODataQuery<RoleAssignmentFilter>($"assignedTo('{linuxVm.SystemAssignedManagedServiceIdentityPrincipalId}')")))
                        .Body
                        .ToList();

                    foreach (var role in existingVmRoles)
                    {
                        await azureClient.AccessManagement.RoleAssignments
                            .Define(Guid.NewGuid().ToString())
                            .ForObjectId(userManagedIdentity.PrincipalId)
                            .WithRoleDefinition(role.RoleDefinitionId)
                            .WithScope(role.Scope)
                            .CreateAsync();
                    }

                    foreach (var role in existingVmRoles)
                    {
                        await azureClient.AccessManagement.RoleAssignments.DeleteByIdAsync(role.Id);
                    }

                    await Execute(
                        "Removing existing system-managed identity and assigning new user-managed identity to the VM...",
                        () => linuxVm.Update().WithoutSystemAssignedManagedServiceIdentity().WithExistingUserAssignedManagedServiceIdentity(userManagedIdentity).ApplyAsync());

                    return userManagedIdentity;
                });
        }

        private async Task DeleteResourceGroupAsync()
        {
            var startTime = DateTime.UtcNow;
            var line = RefreshableConsole.WriteLine("Deleting resource group...");
            await azureClient.ResourceGroups.DeleteByNameAsync(configuration.ResourceGroupName, CancellationToken.None);
            WriteExecutionTime(line, startTime);
        }

        private Task PatchCromwellConfigurationFileAsync(IStorageAccount storageAccount)
        {
            return Execute(
                "Patching cromwell-application.conf in 'configuration' storage container...",
                async () =>
                {
                    var cromwellConfigText = await DownloadTextFromStorageAccountAsync(storageAccount, ConfigurationContainerName, "cromwell-application.conf");

                    if (cromwellConfigText == null)
                    {
                        cromwellConfigText = ReadAllTextWithUnixLineEndings(GetPathFromAppRelativePath("scripts", "cromwell-application.conf"));
                    }
                    else
                    {
                        // Replace "enabled = true" with "enabled = false" in call-caching element
                        // Add "preemptible: true" to default-runtime-attributes element, if preemptible is not already present
                        var callCachingRegex = new Regex(@"^(\s*call-caching\s*{[^}]*enabled\s*[=:]{1}\s*)(true)$", RegexOptions.Multiline);
                        var preemptibleRegex = new Regex(@"(?![\s\S]*preemptible)^(\s*default-runtime-attributes\s*{)([^}]*$)(\s*})$", RegexOptions.Multiline);

                        cromwellConfigText = callCachingRegex.Replace(cromwellConfigText, "$1false");
                        cromwellConfigText = preemptibleRegex.Replace(cromwellConfigText, match => $"{match.Groups[1].Value}{match.Groups[2].Value}\n          preemptible: true{match.Groups[3].Value}");
                    }

                    await UploadTextToStorageAccountAsync(storageAccount, ConfigurationContainerName, "cromwell-application.conf", cromwellConfigText);
                });
        }

        private static string GetPathFromAppRelativePath(params string[] paths)
        {
            return Path.Combine(paths.Prepend(AppContext.BaseDirectory).ToArray());
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

        private static string ReadAllTextWithUnixLineEndings(string path)
        {
            return File.ReadAllText(path).Replace("\r\n", "\n");
        }

        private static void ValidateRegionName(string regionName)
        {
            if (string.IsNullOrWhiteSpace(regionName))
            {
                throw new ValidationException("RegionName is required.");
            }

            var invalidRegionsRegex = new Regex("^(germany|china|usgov|usdod)");
            var validRegionNames = Region.Values.Select(r => r.Name).Where(rn => !invalidRegionsRegex.IsMatch(rn));

            if (!validRegionNames.Contains(regionName, StringComparer.OrdinalIgnoreCase))
            {
                throw new ValidationException($"Invalid region name '{regionName}'. Valid names are: {string.Join(", ", validRegionNames)}");
            }
        }

        private static void ValidateSubscriptionId(string subscriptionId)
        {
            if (string.IsNullOrWhiteSpace(subscriptionId))
            {
                throw new ValidationException($"SubscriptionId is required.");
            }
        }

        private async Task ValidateSubscriptionAndResourceGroupAsync(string subscriptionId, string resourceGroupName, bool isUpdate)
        {
            const string ownerRoleId = "8e3af657-a8ff-443c-a75c-2fe8c4bcb635";
            const string contributorRoleId = "b24988ac-6180-42a0-ab88-20f7382dd24c";
            const string userAccessAdministratorRoleId = "18d7d88d-d35e-4fb5-a5c3-7773c20a72d9";

            var azure = Azure
                .Configure()
                .WithLogLevel(HttpLoggingDelegatingHandler.Level.Basic)
                .Authenticate(azureCredentials);

            var subscriptionExists = (await azure.Subscriptions.ListAsync()).Any(sub => sub.SubscriptionId.Equals(subscriptionId, StringComparison.OrdinalIgnoreCase));

            if (!subscriptionExists)
            {
                throw new ValidationException($"Invalid or inaccessible subcription id '{subscriptionId}'. Make sure that subscription exists and that you are either an Owner or have Contributor and User Access Administrator roles on the subscription.", displayExample: false);
            }

            if (isUpdate && string.IsNullOrEmpty(resourceGroupName))
            {
                throw new ValidationException($"ResourceGroupName is required for the update.", displayExample: false);
            }

            var rgExists = !string.IsNullOrEmpty(resourceGroupName) && await azureClient.ResourceGroups.ContainAsync(resourceGroupName);

            if (!string.IsNullOrEmpty(resourceGroupName) && !rgExists)
            {
                throw new ValidationException($"If ResourceGroupName is provided, the resource group must already exist.", displayExample: false);
            }

            var token = await new AzureServiceTokenProvider().GetAccessTokenAsync("https://management.azure.com/");
            var currentPrincipalObjectId = new JwtSecurityTokenHandler().ReadJwtToken(token).Claims.FirstOrDefault(c => c.Type == "oid").Value;

            var currentPrincipalSubscriptionRoleIds = (await azureClient.AccessManagement.RoleAssignments.Inner.ListForScopeWithHttpMessagesAsync($"/subscriptions/{subscriptionId}", new ODataQuery<RoleAssignmentFilter>($"atScope() and assignedTo('{currentPrincipalObjectId}')")))
                .Body
                .Select(b => b.RoleDefinitionId.Split(new[] { '/' }).Last());

            if (!currentPrincipalSubscriptionRoleIds.Contains(ownerRoleId) && !(currentPrincipalSubscriptionRoleIds.Contains(contributorRoleId) && currentPrincipalSubscriptionRoleIds.Contains(userAccessAdministratorRoleId)))
            {
                if (!rgExists)
                {
                    throw new ValidationException($"Insufficient access to deploy. You must be: 1) Owner of the subscription, or 2) Contributor and User Access Administrator of the subscription, or 3) Owner of the resource group", displayExample: false);
                }

                var currentPrincipalRgRoleIds = (await azureClient.AccessManagement.RoleAssignments.Inner.ListForScopeWithHttpMessagesAsync($"/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}", new ODataQuery<RoleAssignmentFilter>($"atScope() and assignedTo('{currentPrincipalObjectId}')")))
                    .Body
                    .Select(b => b.RoleDefinitionId.Split(new[] { '/' }).Last());

                if (!currentPrincipalRgRoleIds.Contains(ownerRoleId))
                {
                    throw new ValidationException($"Insufficient access to deploy. You must be: 1) Owner of the subscription, or 2) Contributor and User Access Administrator of the subscription, or 3) Owner of the resource group", displayExample: false);
                }

                SkipBillingReaderRoleAssignment = true;

                RefreshableConsole.WriteLine("Warning: insufficient subscription access level to assign the Billing Reader", ConsoleColor.Yellow);
                RefreshableConsole.WriteLine("role for the VM to your Azure Subscription.", ConsoleColor.Yellow);
                RefreshableConsole.WriteLine("Deployment will continue, but only default VM prices will be used for your workflows,", ConsoleColor.Yellow);
                RefreshableConsole.WriteLine("since the Billing Reader role is required to access RateCard API pricing data.", ConsoleColor.Yellow);
                RefreshableConsole.WriteLine("To resolve this in the future, have your Azure subscription Owner or Contributor", ConsoleColor.Yellow);
                RefreshableConsole.WriteLine("assign the Billing Reader role for the VM's managed identity to your Azure Subscription scope.", ConsoleColor.Yellow);
                RefreshableConsole.WriteLine("More info: https://github.com/microsoft/CromwellOnAzure/blob/master/docs/troubleshooting-guide.md#dynamic-cost-optimization-and-ratecard-api-access", ConsoleColor.Yellow);
            }
        }

        private async Task ValidateBatchQuotaAsync()
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
            var computeSkus = (await azureClient.ComputeSkus.ListByRegionAsync(configuration.RegionName))
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

        private static async Task<CloudBlobClient> GetBlobClientAsync(IStorageAccount storageAccount)
        {
            var accessKey = (await storageAccount.GetKeysAsync()).First().Value;
            var storageCredentials = new StorageCredentials(storageAccount.Name, accessKey);

            return new CloudStorageAccount(storageCredentials, true).CreateCloudBlobClient();
        }

        private static async Task ValidateTokenProviderAsync()
        {
            try
            {
                await new AzureServiceTokenProvider().GetAccessTokenAsync("https://management.azure.com/");
            }
            catch (AzureServiceTokenProviderException ex)
            {
                RefreshableConsole.WriteLine("No access token found.  Please install the Azure CLI and login with 'az login'", ConsoleColor.Red);
                RefreshableConsole.WriteLine("Link: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli");
                RefreshableConsole.WriteLine($"Error details: {ex.Message}");
                Environment.Exit(1);
            }
        }

        private void ValidateInitialCommandLineArgsAsync()
        {
            ValidateSubscriptionId(configuration.SubscriptionId);
            ValidateRegionName(configuration.RegionName);
            ValidateMainIdentifierPrefix(configuration.MainIdentifierPrefix);
        }

        private static void DisplayValidationExceptionAndExit(ValidationException validationException)
        {
            RefreshableConsole.WriteLine(validationException.Reason, ConsoleColor.Red);

            if (validationException.DisplayExample)
            {
                RefreshableConsole.WriteLine();
                RefreshableConsole.WriteLine($"Example: ", ConsoleColor.Green).Write($"deploy-cromwell-on-azure --subscriptionid {Guid.NewGuid()} --regionname westus2 --mainidentifierprefix coa", ConsoleColor.White);
            }

            Environment.Exit(1);
        }

        private async Task DeleteResourceGroupIfUserConsentsAsync()
        {
            if (configuration.Update)
            {
                return;
            }

            var userResponse = string.Empty;

            if (!configuration.Silent)
            {
                RefreshableConsole.WriteLine();
                RefreshableConsole.Write("Delete the resource group?  Type 'yes' and press enter, or, press any key to exit: ");
                userResponse = RefreshableConsole.ReadLine();
            }

            if (userResponse.Equals("yes", StringComparison.OrdinalIgnoreCase) || (configuration.Silent && configuration.DeleteResourceGroupOnFailure))
            {
                await DeleteResourceGroupAsync();
            }
        }

        private async Task<bool> RunTestWorkflow(IStorageAccount storageAccount, bool usePreemptibleVm = true)
        {
            var startTime = DateTime.UtcNow;
            var line = RefreshableConsole.WriteLine("Running a test workflow...");
            var isTestWorkflowSuccessful = await TestWorkflowAsync(storageAccount, usePreemptibleVm);
            WriteExecutionTime(line, startTime);

            if (isTestWorkflowSuccessful)
            {
                RefreshableConsole.WriteLine();
                RefreshableConsole.WriteLine($"Test workflow succeeded.", ConsoleColor.Green);
                RefreshableConsole.WriteLine();
                RefreshableConsole.WriteLine("Learn more about how to use Cromwell on Azure: https://github.com/microsoft/CromwellOnAzure");
                RefreshableConsole.WriteLine();
            }
            else
            {
                RefreshableConsole.WriteLine();
                RefreshableConsole.WriteLine($"Test workflow failed.", ConsoleColor.Red);
                RefreshableConsole.WriteLine();
                WriteGeneralRetryMessageToConsole();
                RefreshableConsole.WriteLine();
            }

            return isTestWorkflowSuccessful;
        }

        private void WriteGeneralRetryMessageToConsole()
        {
            RefreshableConsole.WriteLine("Please try deployment again, and create an issue if this continues to fail: https://github.com/microsoft/CromwellOnAzure/issues");
        }

        private async Task<bool> TestWorkflowAsync(IStorageAccount storageAccount, bool usePreemptibleVm = true)
        {
            const string testDirectoryName = "test";
            const string testWdlFilename = "test.wdl";
            const string testInputFilename = "test.json";

            var id = Guid.NewGuid();
            var wdlText = await File.ReadAllTextAsync(GetPathFromAppRelativePath(testWdlFilename));
            var triggerJson = await File.ReadAllTextAsync(GetPathFromAppRelativePath(testInputFilename));

            if (!usePreemptibleVm)
            {
                wdlText = wdlText.Replace("preemptible: true", "preemptible: false", StringComparison.OrdinalIgnoreCase);
            }

            await UploadTextToStorageAccountAsync(storageAccount, InputsContainerName, $"{testDirectoryName}/{testWdlFilename}", wdlText);
            await UploadTextToStorageAccountAsync(storageAccount, InputsContainerName, $"{testDirectoryName}/{testInputFilename}", triggerJson);

            var workflowTrigger = new Workflow
            {
                WorkflowUrl = $"/{storageAccount.Name}/{InputsContainerName}/{testDirectoryName}/{testWdlFilename}",
                WorkflowInputsUrl = $"/{storageAccount.Name}/{InputsContainerName}/{testDirectoryName}/{testInputFilename}"
            };

            await UploadTextToStorageAccountAsync(storageAccount, WorkflowsContainerName, $"new/{id}.json", JsonConvert.SerializeObject(workflowTrigger, Formatting.Indented));

            return await IsWorkflowSuccessfulAfterLongPollingAsync(storageAccount, WorkflowsContainerName, id);
        }

        private async Task<bool> IsWorkflowSuccessfulAfterLongPollingAsync(IStorageAccount storageAccount, string containerName, Guid id)
        {
            var container = (await GetBlobClientAsync(storageAccount)).GetContainerReference(containerName);

            while (true)
            {
                try
                {
                    var succeeded = container.ListBlobs($"succeeded/{id}", useFlatBlobListing: true).Count() == 1;
                    var failed = container.ListBlobs($"failed/{id}", useFlatBlobListing: true).Count() == 1;

                    if (succeeded || failed)
                    {
                        return succeeded && !failed;
                    }
                }
                catch (Exception exc)
                {
                    // "Server is busy" occasionally can be ignored
                    RefreshableConsole.WriteLine(exc.Message);
                }

                await Task.Delay(TimeSpan.FromSeconds(10));
            }
        }

        private Task Execute(string message, Func<Task> func)
        {
            return Execute(message, async () => { await func(); return false; });
        }

        private async Task<T> Execute<T>(string message, Func<Task<T>> func)
        {
            const int retryCount = 3;

            var startTime = DateTime.UtcNow;
            var line = RefreshableConsole.WriteLine(message);

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
                catch
                {
                    line.Write($" Failed", ConsoleColor.Red);
                    cts.Cancel();
                    throw;
                }
            }

            line.Write($" Failed", ConsoleColor.Red);
            cts.Cancel();
            throw new Exception($"Failed after {retryCount} attempts");
        }

        private void WriteExecutionTime(RefreshableConsole.Line line, DateTime startTime)
        {
            line.Write($" Completed in {DateTime.UtcNow.Subtract(startTime).TotalSeconds:n0}s", ConsoleColor.Green);
        }

        private async Task<(string Output, string Error, int ExitStatus)> ExecuteCommandOnVirtualMachineAsync(ConnectionInfo sshConnectionInfo, string command)
        {
            using var sshClient = new SshClient(sshConnectionInfo);
            sshClient.ConnectWithRetries();
            var (output, error, exitStatus) = await sshClient.ExecuteCommandAsync(command);
            sshClient.Disconnect();

            return (output, error, exitStatus);
        }

        private async Task UploadFileToVirtualMachineAsync(ConnectionInfo sshConnectionInfo, string fileContent, string remoteFilePath, bool makeExecutable)
        {
            using var input = new MemoryStream(Encoding.UTF8.GetBytes(fileContent));
            await UploadFileToVirtualMachineAsync(sshConnectionInfo, input, remoteFilePath, makeExecutable);
        }

        private async Task UploadFileToVirtualMachineAsync(ConnectionInfo sshConnectionInfo, Stream input, string remoteFilePath, bool makeExecutable)
        {
            var dir = GetLinuxParentPath(remoteFilePath);

            using var sshClient = new SshClient(sshConnectionInfo);
            using var sftpClient = new SftpClient(sshConnectionInfo);

            sshClient.ConnectWithRetries();

            // Create destination directory if needed and make it writable for the current user
            var (output, _, _) = await sshClient.ExecuteCommandAsync($"sudo mkdir -p {dir} && owner=$(stat -c '%U' {dir}) && mask=$(stat -c '%a' {dir}) && ownerCanWrite=$(( (16#$mask & 16#200) > 0 )) && othersCanWrite=$(( (16#$mask & 16#002) > 0 )) && ( [[ $owner == $(whoami) && $ownerCanWrite == 1 || $othersCanWrite == 1 ]] && echo 0 || ( sudo chmod o+w {dir} && echo 1 ))");
            var dirWasMadeWritableToOthers = output == "1";

            sftpClient.Connect();
            await sftpClient.UploadFileAsync(input, remoteFilePath, true);
            sftpClient.Disconnect();

            if (makeExecutable)
            {
                await sshClient.ExecuteCommandAsync($"sudo chmod +x {remoteFilePath}");
            }

            if (dirWasMadeWritableToOthers)
            {
                await sshClient.ExecuteCommandAsync($"sudo chmod o-w {dir}");
            }

            sshClient.Disconnect();
        }

        private async Task DeleteFileFromVirtualMachineAsync(ConnectionInfo sshConnectionInfo, string filePath)
        {
            using var sshClient = new SshClient(sshConnectionInfo);
            sshClient.ConnectWithRetries();
            await sshClient.ExecuteCommandAsync($"sudo rm -f {filePath}");
            sshClient.Disconnect();
        }

        private async Task<string> DownloadTextFromStorageAccountAsync(IStorageAccount storageAccount, string containerName, string blobName)
        {
            var blobClient = await GetBlobClientAsync(storageAccount);
            var container = blobClient.GetContainerReference(containerName);

            return await container.GetBlockBlobReference(blobName).DownloadTextAsync(cts.Token);
        }

        private async Task UploadTextToStorageAccountAsync(IStorageAccount storageAccount, string containerName, string blobName, string content)
        {
            var blobClient = await GetBlobClientAsync(storageAccount);
            var container = blobClient.GetContainerReference(containerName);

            await container.CreateIfNotExistsAsync();
            await container.GetBlockBlobReference(blobName).UploadTextAsync(content, cts.Token);
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

        private static Dictionary<string, string> DelimitedTextToDictionary(string text, string fieldDelimiter = "=", string rowDelimiter = "\n")
        {
            return text.Split(rowDelimiter)
                .Select(line => { var parts = line.Split(fieldDelimiter); return new KeyValuePair<string, string>(parts[0], parts[1]); })
                .ToDictionary(kv => kv.Key, kv => kv.Value);
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
