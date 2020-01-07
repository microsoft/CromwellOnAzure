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
using Renci.SshNet;

namespace CromwellOnAzureDeployer
{
    public class Deployer
    {
        private const string WorkflowsContainerName = "workflows";
        private const string ConfigurationContainerName = "configuration";
        private const string InputsContainerName = "inputs";

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

        public async Task<bool> DeployAsync()
        {
            ValidateInitialCommandLineArgsAsync();

            var isDeploymentSuccessful = false;
            var mainTimer = Stopwatch.StartNew();

            RefreshableConsole.WriteLine("Running...");

            await ValidateTokenProviderAsync();
            
            tokenCredentials = new TokenCredentials(new RefreshableAzureServiceTokenProvider("https://management.azure.com/"));
            azureCredentials = new AzureCredentials(tokenCredentials, null, null, AzureEnvironment.AzureGlobalCloud);
            azureClient = GetAzureClient(azureCredentials);
            resourceManagerClient = GetResourceManagerClient(azureCredentials);

            await ValidateSubscriptionAndResourceGroupAsync(configuration.SubscriptionId, configuration.ResourceGroupName);
            await RegisterResourceProvidersAsync();
            await ValidateBatchQuotaAsync();

            try
            {
                RefreshableConsole.WriteLine();
                RefreshableConsole.WriteLine($"VM host: {configuration.VmName}.{configuration.RegionName}.cloudapp.azure.com");
                RefreshableConsole.WriteLine($"VM username: {configuration.VmUsername}");
                RefreshableConsole.WriteLine($"VM password: {configuration.VmPassword}");
                RefreshableConsole.WriteLine();

                await CreateResourceGroupAsync();

                BatchAccount batchAccount = null;
                IGenericResource appInsights = null;
                ICosmosDBAccount cosmosDb = null;
                IStorageAccount storageAccount = null;
                IVirtualMachine linuxVm = null;
                ConnectionInfo sshConnectionInfo = null;

                await Task.WhenAll(new Task[]
                {
                    Task.Run(async () => batchAccount = await CreateBatchAccountAsync(), cts.Token),
                    Task.Run(async () => appInsights = await CreateAppInsightsResourceAsync(), cts.Token),
                    Task.Run(async () => cosmosDb = await CreateCosmosDbAsync(), cts.Token),
                    Task.Run(async () => storageAccount = await CreateStorageAccountAsync(), cts.Token),

                    Task.Run(async () => 
                        { 
                            linuxVm = await CreateVirtualMachineAsync();

                            var nic = linuxVm.GetPrimaryNetworkInterface();
                            var nsg = await CreateNetworkSecurityGroupAsync();
                            await AssociateNicWithNetworkSecurityGroupAsync(nic, nsg);

                            sshConnectionInfo = new ConnectionInfo(linuxVm.GetPrimaryPublicIPAddress().Fqdn, configuration.VmUsername, new PasswordAuthenticationMethod(configuration.VmUsername, configuration.VmPassword));
                            await WaitForSshConnectivityAsync(sshConnectionInfo);
                            await ConfigureVmAsync(sshConnectionInfo);
                        }, cts.Token)
                });

                var vmManagedIdentity = linuxVm.SystemAssignedManagedServiceIdentityPrincipalId;

                if (!SkipBillingReaderRoleAssignment)
                {
                    await AssignVmAsBillingReaderToSubscriptionAsync(vmManagedIdentity);
                }

                await AssignVmAsContributorToAppInsightsAsync(vmManagedIdentity, appInsights);
                await AssignVmAsContributorToCosmosDb(vmManagedIdentity, cosmosDb);
                await AssignVmAsContributorToBatchAccountAsync(vmManagedIdentity, batchAccount);
                await AssignVmAsContributorToStorageAccountAsync(vmManagedIdentity, storageAccount);
                await AssignVmAsDataReaderToStorageAccountAsync(vmManagedIdentity, storageAccount);

                await RestartVmAsync(linuxVm);
                await WaitForSshConnectivityAsync(sshConnectionInfo);
                await WaitForDockerComposeAsync(sshConnectionInfo);
                await WaitForCromwellAsync(sshConnectionInfo);

                isDeploymentSuccessful = await VerifyInstallationAsync(storageAccount);

                if (!isDeploymentSuccessful)
                {
                    await DeleteResourceGroupIfUserConsentsAsync();
                }
            }
            catch (Microsoft.Rest.Azure.CloudException cloudException)
            {
                var json = cloudException.Response.Content;
                RefreshableConsole.WriteLine(json);
                Debugger.Break();
                WriteGeneralRetryMessageToConsole();
                await DeleteResourceGroupIfUserConsentsAsync();
            }
            catch (Exception exc)
            {
                RefreshableConsole.WriteLine(exc.ToString());
                Debugger.Break();
                WriteGeneralRetryMessageToConsole();
                await DeleteResourceGroupIfUserConsentsAsync();
            }

            RefreshableConsole.WriteLine($"Completed in {mainTimer.Elapsed.TotalMinutes:n1} minutes.");

            return isDeploymentSuccessful;
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
                            sshClient.Connect();
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

                    return Task.FromResult(false);
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
                        var (numberOfRunningContainers, _, _) = await ExecuteCommandOnVirtualMachineAsync(sshConnectionInfo, "sudo docker ps -a | grep -c 'Up '", false);

                        if (numberOfRunningContainers == "4")
                        {
                            break;
                        }

                        await Task.Delay(5000, cts.Token);
                    }

                    return Task.FromResult(false);
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

                    return Task.FromResult(false);
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

                        return Task.FromResult(false);
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
            await ExecuteCommandOnVirtualMachineAsync(sshConnectionInfo, "sudo mkdir /cromwellazure && sudo chown vmadmin /cromwellazure && sudo chmod ug=rwx,o= /cromwellazure");
            await CopyInstallationFilesAsync(sshConnectionInfo);
            await Task.WhenAll(new[] { RunInstallationScriptAsync(sshConnectionInfo), CopyAnyCustomDockerImagesToTheVmAsync(sshConnectionInfo) });
            await LoadAnyCustomDockerImagesAndCopyDockerComposeFileAsync(sshConnectionInfo);
        }

        private async Task CopyInstallationFilesAsync(ConnectionInfo sshConnectionInfo)
        {
            var startTime = DateTime.UtcNow;
            var line = RefreshableConsole.WriteLine("Copying installation files to the VM...");

            var startupText = ReadAllTextWithUnixLineEndings(GetPathFromAppRelativePath("scripts", "startup.sh")).Replace("STORAGEACCOUNTNAME", configuration.StorageAccountName);
            await UploadFileToVirtualMachineAsync(sshConnectionInfo, startupText, "/cromwellazure/startup.sh", true);

            await UploadFileToVirtualMachineAsync(sshConnectionInfo, ReadAllTextWithUnixLineEndings(GetPathFromAppRelativePath("scripts", "wait-for-it.sh")), "/cromwellazure/wait-for-it/wait-for-it.sh", true);
            await UploadFileToVirtualMachineAsync(sshConnectionInfo, ReadAllTextWithUnixLineEndings(GetPathFromAppRelativePath("scripts", "install-cromwellazure.sh")), "/cromwellazure/install-cromwellazure.sh", true);
            await UploadFileToVirtualMachineAsync(sshConnectionInfo, ReadAllTextWithUnixLineEndings(GetPathFromAppRelativePath("scripts", "mount_containers.sh")), "/cromwellazure/mount_containers.sh", true);
            await UploadFileToVirtualMachineAsync(sshConnectionInfo, ReadAllTextWithUnixLineEndings(GetPathFromAppRelativePath("scripts", "cromwellazure.service")), "/lib/systemd/system/cromwellazure.service", false);
            await UploadFileToVirtualMachineAsync(sshConnectionInfo, ReadAllTextWithUnixLineEndings(GetPathFromAppRelativePath("scripts", "mount.blobfuse")), "/usr/sbin/mount.blobfuse", true);

            WriteExecutionTime(line, startTime);
        }

        private async Task RunInstallationScriptAsync(ConnectionInfo sshConnectionInfo)
        {
            var startTime = DateTime.UtcNow;
            var line = RefreshableConsole.WriteLine("Running installation script on the VM...");
            await ExecuteCommandOnVirtualMachineAsync(sshConnectionInfo, $"/cromwellazure/install-cromwellazure.sh");
            WriteExecutionTime(line, startTime);
        }

        private async Task CopyAnyCustomDockerImagesToTheVmAsync(ConnectionInfo sshConnectionInfo)
        {
            async Task CopyCustomDockerImageAsync(string customImagePath)
            {
                var startTime = DateTime.UtcNow;
                var line = RefreshableConsole.WriteLine($"Copying custom image from {customImagePath} to the VM...");
                var remotePath = $"/cromwellazure/{Path.GetFileName(customImagePath)}";
                await UploadFileToVirtualMachineAsync(sshConnectionInfo, File.OpenRead(customImagePath), remotePath, false);
                WriteExecutionTime(line, startTime);
            }

            if (!string.IsNullOrEmpty(configuration.CustomTesImagePath))
            {
                await CopyCustomDockerImageAsync(configuration.CustomTesImagePath);
            }

            if (!string.IsNullOrEmpty(configuration.CustomCromwellImagePath))
            {
                await CopyCustomDockerImageAsync(configuration.CustomCromwellImagePath);
            }

            if (!string.IsNullOrEmpty(configuration.CustomTriggerServiceImagePath))
            {
                await CopyCustomDockerImageAsync(configuration.CustomTriggerServiceImagePath);
            }
        }

        private async Task LoadAnyCustomDockerImagesAndCopyDockerComposeFileAsync(ConnectionInfo sshConnectionInfo)
        {
            async Task<string> LoadCustomDockerImageAsync(string customImagePath)
            {
                var startTime = DateTime.UtcNow;
                var line = RefreshableConsole.WriteLine($"Loading custom image on the VM...");
                var remotePath = $"/cromwellazure/{Path.GetFileName(customImagePath)}";
                var (loadedImageName, _, _) = await ExecuteCommandOnVirtualMachineAsync(sshConnectionInfo, $"imageName=$(sudo docker load -i {remotePath}) && rm {remotePath} && imageName=$(expr \"$imageName\" : 'Loaded.*: \\(.*\\)') && echo $imageName");
                WriteExecutionTime(line, startTime);

                return loadedImageName;
            }

            var tesImageName = configuration.TesImageName;
            var cromwellImageName = configuration.CromwellImageName;
            var triggerServiceImageName = configuration.TriggerServiceImageName;

            if (!string.IsNullOrEmpty(configuration.CustomTesImagePath))
            {
                tesImageName = await LoadCustomDockerImageAsync(configuration.CustomTesImagePath);
            }

            if (!string.IsNullOrEmpty(configuration.CustomCromwellImagePath))
            {
                cromwellImageName = await LoadCustomDockerImageAsync(configuration.CustomCromwellImagePath);
            }

            if (!string.IsNullOrEmpty(configuration.CustomTriggerServiceImagePath))
            {
                triggerServiceImageName = await LoadCustomDockerImageAsync(configuration.CustomTriggerServiceImagePath);
            }

            var dockerComposeConfigText = ReadAllTextWithUnixLineEndings(GetPathFromAppRelativePath("scripts", "docker-compose.yml"))
                .Replace("STORAGEACCOUNTNAME", configuration.StorageAccountName)
                .Replace("COSMOSDBNAME", configuration.CosmosDbAccountName)
                .Replace("VARBATCHACCOUNTNAME", configuration.BatchAccountName)
                .Replace("VARAPPLICATIONINSIGHTSACCOUNTNAME", configuration.ApplicationInsightsAccountName)
                .Replace("TESIMAGENAME", tesImageName)
                .Replace("CROMWELLIMAGENAME", cromwellImageName)
                .Replace("TRIGGERSERVICEIMAGENAME", triggerServiceImageName);

            await UploadFileToVirtualMachineAsync(sshConnectionInfo, dockerComposeConfigText, "/cromwellazure/docker-compose.yml", false);
        }

        private Task RestartVmAsync(IVirtualMachine linuxVm)
        {
            return Execute(
                "Restarting VM...",
                async () =>
                {
                    await linuxVm.RestartAsync(cts.Token);
                    return Task.FromResult(false);
                });
        }

        private Task AssignVmAsDataReaderToStorageAccountAsync(string vmManagedIdentity, IStorageAccount storageAccount)
        {
            // https://docs.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#storage-blob-data-reader
            var roleDefinitionId = $"/subscriptions/{configuration.SubscriptionId}/providers/Microsoft.Authorization/roleDefinitions/2a2b9908-6ea1-4ae2-8e65-a410df84e7d1";

            return Execute(
                $"Assigning Storage Blob Data Reader role for VM to Storage Account resource scope...",
                () => azureClient.AccessManagement.RoleAssignments
                    .Define(Guid.NewGuid().ToString())
                    .ForObjectId(vmManagedIdentity)
                    .WithRoleDefinition(roleDefinitionId)
                    .WithResourceScope(storageAccount)
                    .CreateAsync(cts.Token));
        }

        private Task AssignVmAsContributorToStorageAccountAsync(string vmManagedIdentity, IResource storageAccount)
        {
            return Execute(
                $"Assigning {BuiltInRole.Contributor} role for VM to Storage Account resource scope...",
                () => azureClient.AccessManagement.RoleAssignments
                    .Define(Guid.NewGuid().ToString())
                    .ForObjectId(vmManagedIdentity)
                    .WithBuiltInRole(BuiltInRole.Contributor)
                    .WithResourceScope(storageAccount)
                    .CreateAsync(cts.Token));
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

                    cts.Token.ThrowIfCancellationRequested();

                    // Upload config files
                    var configContainer = blobClient.GetContainerReference(ConfigurationContainerName);

                    var cromwellAppConfigPath = GetPathFromAppRelativePath("scripts", "cromwell-application.conf");
                    var cromwellAppConfigText = ReadAllTextWithUnixLineEndings(cromwellAppConfigPath);
                    await configContainer.GetBlockBlobReference(Path.GetFileName(cromwellAppConfigPath)).UploadTextAsync(cromwellAppConfigText, cts.Token);

                    var containersToMountConfigPath = GetPathFromAppRelativePath("scripts", "containers-to-mount");
                    var containersToMountConfigText = ReadAllTextWithUnixLineEndings(containersToMountConfigPath);
                    containersToMountConfigText = containersToMountConfigText.Replace("STORAGEACCOUNTNAME", configuration.StorageAccountName);
                    await configContainer.GetBlockBlobReference(Path.GetFileName(containersToMountConfigPath)).UploadTextAsync(containersToMountConfigText, cts.Token);

                    var workflowsContainer = blobClient.GetContainerReference(WorkflowsContainerName);
                    await workflowsContainer.GetBlockBlobReference("new/readme.txt").UploadTextAsync("Upload a trigger file to this virtual directory to create a new workflow. Additional information here: https://github.com/microsoft/CromwellOnAzure", cts.Token);
                    await workflowsContainer.GetBlockBlobReference("abort/readme.txt").UploadTextAsync("Upload an empty file to this virtual directory to abort an existing workflow. The empty file's name shall be the Cromwell workflow ID you wish to cancel.  Additional information here: https://github.com/microsoft/CromwellOnAzure", cts.Token);

                    return storageAccount;
                });
        }

        private Task AssignVmAsContributorToBatchAccountAsync(string vmManagedIdentity, BatchAccount batchAccount)
        {
            return Execute(
                $"Assigning {BuiltInRole.Contributor} role for VM to Batch Account resource scope...",
                () => azureClient.AccessManagement.RoleAssignments
                    .Define(Guid.NewGuid().ToString())
                    .ForObjectId(vmManagedIdentity)
                    .WithBuiltInRole(BuiltInRole.Contributor)
                    .WithScope(batchAccount.Id)
                    .CreateAsync(cts.Token));
        }

        private Task AssignVmAsContributorToCosmosDb(string vmManagedIdentity, IResource cosmosDb)
        {
            return Execute(
                $"Assigning {BuiltInRole.Contributor} role for VM to Cosmos DB resource scope...",
                () => azureClient.AccessManagement.RoleAssignments
                    .Define(Guid.NewGuid().ToString())
                    .ForObjectId(vmManagedIdentity)
                    .WithBuiltInRole(BuiltInRole.Contributor)
                    .WithResourceScope(cosmosDb)
                    .CreateAsync(cts.Token));
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

        private Task AssignVmAsBillingReaderToSubscriptionAsync(string vmManagedIdentity)
        {
            return Execute(
                $"Assigning {BuiltInRole.BillingReader} role for VM to Subscription scope...",
                () => azureClient.AccessManagement.RoleAssignments.Define(Guid.NewGuid().ToString())
                    .ForObjectId(vmManagedIdentity)
                    .WithBuiltInRole(BuiltInRole.BillingReader)
                    .WithSubscriptionScope(configuration.SubscriptionId)
                    .CreateAsync(cts.Token));
        }

        private Task AssignVmAsContributorToAppInsightsAsync(string vmManagedIdentity, IResource appInsights)
        {
            return Execute(
                $"Assigning {BuiltInRole.Contributor} role for VM to App Insights resource scope...",
                () => azureClient.AccessManagement.RoleAssignments
                    .Define(Guid.NewGuid().ToString())
                    .ForObjectId(vmManagedIdentity)
                    .WithBuiltInRole(BuiltInRole.Contributor)
                    .WithResourceScope(appInsights)
                    .CreateAsync(cts.Token));
        }

        private Task<IVirtualMachine> CreateVirtualMachineAsync()
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
                    .WithPopularLinuxImage(configuration.VmImage)
                    .WithRootUsername(configuration.VmUsername)
                    .WithRootPassword(configuration.VmPassword)
                    .WithNewDataDisk(dataDiskSizeGiB, dataDiskLun, CachingTypes.None)
                    .WithSize(configuration.VmSize)
                    .WithSystemAssignedManagedServiceIdentity()
                    .CreateAsync(cts.Token));
        }

        private Task<INetworkSecurityGroup> CreateNetworkSecurityGroupAsync()
        {
            const string ruleName = "SSH";
            const int allowedPort = 22;
            const int defaultPriority = 300;

            return Execute(
                $"Creating Network Security Group: {configuration.NetworkSecurityGroupName}...",
                () => azureClient.NetworkSecurityGroups.Define(configuration.NetworkSecurityGroupName)
                    .WithRegion(configuration.RegionName)
                    .WithExistingResourceGroup(configuration.ResourceGroupName)
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

        private Task<INetworkInterface> AssociateNicWithNetworkSecurityGroupAsync(INetworkInterface nic, INetworkSecurityGroup nsg)
        {
            return Execute(
                $"Associating NIC with Network Security Group: {configuration.NetworkSecurityGroupName}...",
                () => nic.Update().WithExistingNetworkSecurityGroup(nsg).ApplyAsync()
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
                () =>
                {
                    return new BatchManagementClient(tokenCredentials) { SubscriptionId = configuration.SubscriptionId }
                        .BatchAccount
                        .CreateAsync(configuration.ResourceGroupName, configuration.BatchAccountName, new BatchAccountCreateParameters { Location = configuration.RegionName }, cts.Token);
                });
        }

        private Task CreateResourceGroupAsync()
        {
            return Execute(
                $"Creating Resource Group: {configuration.ResourceGroupName}...",
                () => azureClient.ResourceGroups
                    .Define(configuration.ResourceGroupName)
                    .WithRegion(configuration.RegionName)
                    .CreateAsync(cts.Token));
        }

        private async Task DeleteResourceGroupAsync()
        {
            var startTime = DateTime.UtcNow;
            var line = RefreshableConsole.WriteLine("Deleting resource group...");
            await azureClient.ResourceGroups.DeleteByNameAsync(configuration.ResourceGroupName, CancellationToken.None);
            WriteExecutionTime(line, startTime);
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

        private async Task ValidateSubscriptionAndResourceGroupAsync(string subscriptionId, string resourceGroupName)
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

            var token = await new AzureServiceTokenProvider().GetAccessTokenAsync("https://management.azure.com/");
            var currentPrincipalObjectId = new JwtSecurityTokenHandler().ReadJwtToken(token).Claims.FirstOrDefault(c => c.Type == "oid").Value;

            var currentPrincipalSubscriptionRoleIds = (await azureClient.AccessManagement.RoleAssignments.Inner.ListForScopeWithHttpMessagesAsync($"/subscriptions/{subscriptionId}", new ODataQuery<RoleAssignmentFilter>($"atScope() and assignedTo('{currentPrincipalObjectId}')")))
                .Body
                .Select(b => b.RoleDefinitionId.Split(new[] { '/' }).Last());

            if (!currentPrincipalSubscriptionRoleIds.Contains(ownerRoleId) && !(currentPrincipalSubscriptionRoleIds.Contains(contributorRoleId) && currentPrincipalSubscriptionRoleIds.Contains(userAccessAdministratorRoleId)))
            {
                var rgExists = await azureClient.ResourceGroups.ContainAsync(resourceGroupName);

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
            try
            {
                var accountQuota = (await new BatchManagementClient(tokenCredentials) { SubscriptionId = configuration.SubscriptionId }.Location.GetQuotasAsync(configuration.RegionName)).AccountQuota;
                var existingBatchAccountCount = (await new BatchManagementClient(tokenCredentials) { SubscriptionId = configuration.SubscriptionId }.BatchAccount.ListAsync()).AsEnumerable().Count(b => b.Location.Equals(configuration.RegionName));

                if (existingBatchAccountCount >= accountQuota)
                {
                    throw new ValidationException($"The regional Batch account quota ({accountQuota} account(s) per region) for the specified subscription has been reached. Submit a support request to increase the quota or choose another region.", displayExample: false);
                }
            }
            catch (ValidationException validationException)
            {
                DisplayValidationExceptionAndExit(validationException);
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
            try
            {
                ValidateSubscriptionId(configuration.SubscriptionId);
                ValidateRegionName(configuration.RegionName);
                ValidateMainIdentifierPrefix(configuration.MainIdentifierPrefix);
            }
            catch (ValidationException validationException)
            {
                DisplayValidationExceptionAndExit(validationException);
            }
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
            var userResponse = string.Empty;

            if (!configuration.Silent)
            {
                RefreshableConsole.WriteLine();
                RefreshableConsole.Write("Delete the resource group?  Type 'yes' and press enter, or, press any key to exit: ");
                userResponse = RefreshableConsole.ReadLine();
            }

            if (userResponse.Equals("yes", StringComparison.OrdinalIgnoreCase) || configuration.DeleteResourceGroupOnFailure)
            {
                await DeleteResourceGroupAsync();
            }
        }

        private async Task<bool> VerifyInstallationAsync(IStorageAccount storageAccount)
        {
            var startTime = DateTime.UtcNow;
            var line = RefreshableConsole.WriteLine("Running a test workflow...");
            var isTestWorkflowSuccessful = await TestWorkflowAsync(storageAccount);
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

        private async Task<bool> TestWorkflowAsync(IStorageAccount storageAccount)
        {
            var id = Guid.NewGuid();
            var blobClient = await GetBlobClientAsync(storageAccount);
            await UploadFileTextAsync(blobClient, File.ReadAllText(GetPathFromAppRelativePath("test.wdl")), InputsContainerName, "test/test.wdl");
            await UploadFileTextAsync(blobClient, File.ReadAllText(GetPathFromAppRelativePath("test.json")), InputsContainerName, "test/test.json");

            var workflow = new Workflow
            {
                WorkflowUrl = $"/{configuration.StorageAccountName}/{InputsContainerName}/test/test.wdl",
                WorkflowInputsUrl = $"/{configuration.StorageAccountName}/{InputsContainerName}/test/test.json"
            };

            var json = JsonConvert.SerializeObject(workflow, Formatting.Indented);
            await UploadFileTextAsync(blobClient, json, WorkflowsContainerName, $"new/{id}.json");

            return await IsSuccessfulAfterLongPollingAsync(blobClient.GetContainerReference(WorkflowsContainerName), id);
        }

        private async Task<bool> IsSuccessfulAfterLongPollingAsync(CloudBlobContainer container, Guid id)
        {
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
                catch (OperationCanceledException ex) when (ex.CancellationToken == cts.Token)
                {
                    line.Write(" Cancelled", ConsoleColor.Red);
                    return await Task.FromResult(default(T));
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

        private async Task<(string Output, string Error, int ExitStatus)> ExecuteCommandOnVirtualMachineAsync(ConnectionInfo sshConnectionInfo, string command, bool throwOnNonZeroExitCode = true)
        {
            using var sshClient = new SshClient(sshConnectionInfo);
            sshClient.Connect();
            var (output, error, exitStatus) = await sshClient.ExecuteCommandAsync(command, throwOnNonZeroExitCode, cts.Token);
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

            sshClient.Connect();

            // Create destination directory if needed and make it writable for the current user
            var (output, _, _) = await sshClient.ExecuteCommandAsync($"sudo mkdir -p {dir} && owner=$(stat -c '%U' {dir}) && mask=$(stat -c '%a' {dir}) && ownerCanWrite=$(( (16#$mask & 16#200) > 0 )) && othersCanWrite=$(( (16#$mask & 16#002) > 0 )) && ( [[ $owner == $(whoami) && $ownerCanWrite == 1 || $othersCanWrite == 1 ]] && echo 0 || ( sudo chmod o+w {dir} && echo 1 ))", true, cts.Token);
            var dirWasMadeWritableToOthers = output == "1";

            sftpClient.Connect();
            await sftpClient.UploadFileAsync(input, remoteFilePath, true, cts.Token);
            sftpClient.Disconnect();

            if (makeExecutable)
            {
                await sshClient.ExecuteCommandAsync($"sudo chmod +x {remoteFilePath}", true, cts.Token);
            }

            if (dirWasMadeWritableToOthers)
            {
                await sshClient.ExecuteCommandAsync($"sudo chmod o-w {dir}", true, cts.Token);
            }

            sshClient.Disconnect();
        }

        public async Task<string> UploadFileTextAsync(CloudBlobClient blobClient, string content, string container, string blobName)
        {
            var containerReference = blobClient.GetContainerReference(container);
            await containerReference.CreateIfNotExistsAsync();
            var blob = containerReference.GetBlockBlobReference(blobName);
            await blob.UploadTextAsync(content, cts.Token);

            return blob.Uri.AbsoluteUri;
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
