// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IdentityModel.Tokens.Jwt;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Azure.Core;
using Azure.Identity;
using Azure.ResourceManager;
using Azure.ResourceManager.Network;
using Azure.ResourceManager.Network.Models;
using Azure.Security.KeyVault.Secrets;
using Azure.Storage;
using Azure.Storage.Blobs;
using Common;
using k8s;
using Microsoft.Azure.Management.Batch;
using Microsoft.Azure.Management.Batch.Models;
using Microsoft.Azure.Management.Compute.Fluent;
using Microsoft.Azure.Management.ContainerRegistry.Fluent;
using Microsoft.Azure.Management.ContainerService;
using Microsoft.Azure.Management.ContainerService.Fluent;
using Microsoft.Azure.Management.ContainerService.Models;
using Microsoft.Azure.Management.Fluent;
using Microsoft.Azure.Management.Graph.RBAC.Fluent;
using Microsoft.Azure.Management.Graph.RBAC.Fluent.Models;
using Microsoft.Azure.Management.KeyVault;
using Microsoft.Azure.Management.KeyVault.Fluent;
using Microsoft.Azure.Management.KeyVault.Models;
using Microsoft.Azure.Management.Msi.Fluent;
using Microsoft.Azure.Management.Network;
using Microsoft.Azure.Management.Network.Fluent;
using Microsoft.Azure.Management.Network.Fluent.Models;
using Microsoft.Azure.Management.Network.Models;
using Microsoft.Azure.Management.PostgreSQL;
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
using static Microsoft.Azure.Management.PostgreSQL.FlexibleServers.DatabasesOperationsExtensions;
using static Microsoft.Azure.Management.PostgreSQL.FlexibleServers.ServersOperationsExtensions;
using static Microsoft.Azure.Management.PostgreSQL.ServersOperationsExtensions;
using static Microsoft.Azure.Management.ResourceManager.Fluent.Core.RestClient;
using Extensions = Microsoft.Azure.Management.ResourceManager.Fluent.Core.Extensions;
using FlexibleServer = Microsoft.Azure.Management.PostgreSQL.FlexibleServers;
using FlexibleServerModel = Microsoft.Azure.Management.PostgreSQL.FlexibleServers.Models;
using IResource = Microsoft.Azure.Management.ResourceManager.Fluent.Core.IResource;
using KeyVaultManagementClient = Microsoft.Azure.Management.KeyVault.KeyVaultManagementClient;
using SingleServer = Microsoft.Azure.Management.PostgreSQL;
using SingleServerModel = Microsoft.Azure.Management.PostgreSQL.Models;
using Sku = Microsoft.Azure.Management.PostgreSQL.FlexibleServers.Models.Sku;

namespace CromwellOnAzureDeployer
{
    public class Deployer
    {
        private static readonly AsyncRetryPolicy roleAssignmentHashConflictRetryPolicy = Policy
            .Handle<Microsoft.Rest.Azure.CloudException>(cloudException => cloudException.Body.Code.Equals("HashConflictOnDifferentRoleAssignmentIds"))
            .RetryAsync();

        private static readonly AsyncRetryPolicy generalRetryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(3, retryAttempt => System.TimeSpan.FromSeconds(1));

        public const string WorkflowsContainerName = "workflows";
        public const string ConfigurationContainerName = "configuration";
        public const string CromwellConfigurationFileName = "cromwell-application.conf";
        public const string AllowedVmSizesFileName = "allowed-vm-sizes";
        public const string InputsContainerName = "inputs";
        public const string CromwellAzureRootDir = "/data/cromwellazure";
        public const string CromwellAzureRootDirSymLink = "/cromwellazure";    // This path is present in all CoA versions
        public const string StorageAccountKeySecretName = "CoAStorageKey";
        public const string PostgresqlSslMode = "VerifyFull";

        private readonly CancellationTokenSource cts = new();

        private readonly List<string> requiredResourceProviders = new()
        {
            "Microsoft.Authorization",
            "Microsoft.Batch",
            "Microsoft.Compute",
            "Microsoft.ContainerService",
            "Microsoft.DocumentDB",
            "Microsoft.OperationalInsights",
            "Microsoft.OperationsManagement",
            "Microsoft.insights",
            "Microsoft.Network",
            "Microsoft.Storage",
            "Microsoft.DBforPostgreSQL"
        };

        private Configuration configuration { get; set; }
        private ITokenProvider tokenProvider;
        private TokenCredentials tokenCredentials;
        private IAzure azureSubscriptionClient { get; set; }
        private Microsoft.Azure.Management.Fluent.Azure.IAuthenticated azureClient { get; set; }
        private IResourceManager resourceManagerClient { get; set; }
        private ArmClient armClient { get; set; }
        private Microsoft.Azure.Management.Network.INetworkManagementClient networkManagementClient { get; set; }
        private AzureCredentials azureCredentials { get; set; }
        private FlexibleServer.IPostgreSQLManagementClient postgreSqlFlexManagementClient { get; set; }
        private SingleServer.IPostgreSQLManagementClient postgreSqlSingleManagementClient { get; set; }
        private IEnumerable<string> subscriptionIds { get; set; }
        private bool isResourceGroupCreated { get; set; }
        private KubernetesManager kubernetesManager { get; set; }

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

                await Execute("Connecting to Azure Services...", async () =>
                {
                    tokenProvider = new RefreshableAzureServiceTokenProvider("https://management.azure.com/");
                    tokenCredentials = new(tokenProvider);
                    azureCredentials = new(tokenCredentials, null, null, AzureEnvironment.AzureGlobalCloud);
                    azureClient = GetAzureClient(azureCredentials);
                    armClient = new ArmClient(new DefaultAzureCredential());
                    azureSubscriptionClient = azureClient.WithSubscription(configuration.SubscriptionId);
                    subscriptionIds = (await azureClient.Subscriptions.ListAsync()).Select(s => s.SubscriptionId);
                    resourceManagerClient = GetResourceManagerClient(azureCredentials);
                    networkManagementClient = new Microsoft.Azure.Management.Network.NetworkManagementClient(azureCredentials) { SubscriptionId = configuration.SubscriptionId };
                    postgreSqlFlexManagementClient = new FlexibleServer.PostgreSQLManagementClient(azureCredentials) { SubscriptionId = configuration.SubscriptionId, LongRunningOperationRetryTimeout = 1200 };
                    postgreSqlSingleManagementClient = new SingleServer.PostgreSQLManagementClient(azureCredentials) { SubscriptionId = configuration.SubscriptionId, LongRunningOperationRetryTimeout = 1200 };
                });

                await ValidateSubscriptionAndResourceGroupAsync(configuration);
                kubernetesManager = new KubernetesManager(configuration, azureCredentials, cts);

                IResourceGroup resourceGroup = null;
                ManagedCluster aksCluster = null;
                BatchAccount batchAccount = null;
                IGenericResource logAnalyticsWorkspace = null;
                IGenericResource appInsights = null;
                FlexibleServerModel.Server postgreSqlFlexServer = null;
                SingleServerModel.Server postgreSqlSingleServer = null;
                IStorageAccount storageAccount = null;
                var keyVaultUri = string.Empty;
                IIdentity managedIdentity = null;
                IPrivateDnsZone postgreSqlDnsZone = null;
                IKubernetes kubernetesClient = null;

                var containersToMount = await GetContainersToMount(configuration.ContainersToMountPath);

                try
                {
                    if (configuration.Update)
                    {
                        resourceGroup = await azureSubscriptionClient.ResourceGroups.GetByNameAsync(configuration.ResourceGroupName);
                        configuration.RegionName = resourceGroup.RegionName;

                        var targetVersion = Utility.DelimitedTextToDictionary(Utility.GetFileContent("scripts", "env-00-coa-version.txt")).GetValueOrDefault("CromwellOnAzureVersion");

                        ConsoleEx.WriteLine($"Upgrading Cromwell on Azure instance in resource group '{resourceGroup.Name}' to version {targetVersion}...");

                        if (!string.IsNullOrEmpty(configuration.StorageAccountName))
                        {
                            storageAccount = await GetExistingStorageAccountAsync(configuration.StorageAccountName)
                                ?? throw new ValidationException($"Storage account {configuration.StorageAccountName}, does not exist in region {configuration.RegionName} or is not accessible to the current user.");
                        }
                        else
                        {
                            var storageAccounts = (await azureSubscriptionClient.StorageAccounts.ListByResourceGroupAsync(configuration.ResourceGroupName)).ToList();

                            if (!storageAccounts.Any())
                            {
                                throw new ValidationException($"Update was requested but resource group {configuration.ResourceGroupName} does not contain any storage accounts.");
                            }
                            if (storageAccounts.Count > 1)
                            {
                                throw new ValidationException($"Resource group {configuration.ResourceGroupName} contains multiple storage accounts. {nameof(configuration.StorageAccountName)} must be provided.");
                            }

                            storageAccount = storageAccounts.First();
                        }

                        // If the previous installation was pre 4.0, we probably will error out below this line but before we make any changes. If not, we will guard below anyway.

                        var aksValues = await kubernetesManager.GetAKSSettingsAsync(storageAccount);

                        if (!aksValues.Any())
                        {
                            throw new ValidationException($"Could not retrieve account names");
                        }

                        if (!aksValues.TryGetValue("BatchAccountName", out var batchAccountName))
                        {
                            throw new ValidationException($"Could not retrieve the Batch account name");
                        }

                        batchAccount = await GetExistingBatchAccountAsync(batchAccountName)
                            ?? throw new ValidationException($"Batch account {batchAccountName}, referenced by the VM configuration, does not exist in region {configuration.RegionName} or is not accessible to the current user.");

                        configuration.BatchAccountName = batchAccountName;

                        aksValues.TryGetValue("PostgreSqlServerName", out var postgreSqlServerName);

                        configuration.PostgreSqlServerName = postgreSqlServerName;
                        //postgreSqlFlexServer = GetExistingPostgresqlService(postgreSqlServerName); // TODO: Get existing postgresql server

                        ManagedCluster existingAksCluster = default;

                        if (!string.IsNullOrEmpty(configuration.AksClusterName))
                        {
                            existingAksCluster = await ValidateAndGetExistingAKSClusterAsync();
                        }
                        else
                        {
                            using var client = new ContainerServiceClient(tokenCredentials) { SubscriptionId = configuration.SubscriptionId };
                            var aksClusters = (await client.ManagedClusters.ListByResourceGroupAsync(configuration.ResourceGroupName)).ToList();

                            if (!aksClusters.Any())
                            {
                                throw new ValidationException($"Update was requested but resource group {configuration.ResourceGroupName} does not contain any AKS clusters.");
                            }
                            if (aksClusters.Count > 1)
                            {
                                throw new ValidationException($"Resource group {configuration.ResourceGroupName} contains multiple AKS clusters. {nameof(configuration.AksClusterName)} must be provided.");
                            }

                            existingAksCluster = aksClusters.First();
                            configuration.AksClusterName = existingAksCluster.Name;
                        }

                        if (existingAksCluster is not null)
                        {
                            if (aksValues.TryGetValue("CrossSubscriptionAKSDeployment", out var crossSubscriptionAKSDeployment))
                            {
                                _ = bool.TryParse(crossSubscriptionAKSDeployment, out var parsed);
                                configuration.CrossSubscriptionAKSDeployment = parsed;
                            }

                            if (aksValues.TryGetValue("KeyVaultName", out var keyVaultName))
                            {
                                var keyVault = await GetKeyVaultAsync(keyVaultName);
                                keyVaultUri = keyVault.Properties.VaultUri;
                            }

                            if (!aksValues.TryGetValue("ManagedIdentityClientId", out var managedIdentityClientId))
                            {
                                throw new ValidationException($"Could not retrieve ManagedIdentityClientId.");
                            }

                            managedIdentity = azureSubscriptionClient.Identities.ListByResourceGroup(configuration.ResourceGroupName).Where(id => id.ClientId == managedIdentityClientId).FirstOrDefault()
                                ?? throw new ValidationException($"Managed Identity {managedIdentityClientId} does not exist in region {configuration.RegionName} or is not accessible to the current user.");

                            // Override any configuration that is used by the update.
                            var versionString = aksValues["CromwellOnAzureVersion"];
                            var installedVersion = !string.IsNullOrEmpty(versionString) && Version.TryParse(versionString, out var version) ? version : null;

                            if (installedVersion is null || installedVersion < new Version(4, 0))
                            {
                                throw new ValidationException("Upgrading pre-4.0 versions of CromwellOnAzure is not supported. Please see https://github.com/microsoft/CromwellOnAzure/wiki/4.0-Migration-Guide.");
                            }

                            var settings = ConfigureSettings(managedIdentity.ClientId, aksValues, installedVersion);

                            //if (installedVersion is null || installedVersion < new Version(4, 1))
                            //{
                            //}

                            if (installedVersion is null || installedVersion < new Version(4, 4))
                            {
                                // Ensure all storage containers are created.
                                await CreateDefaultStorageContainersAsync(storageAccount);
                            }

                            if ((installedVersion is null || installedVersion < new Version(4, 5)) && string.IsNullOrWhiteSpace(settings["BatchNodesSubnetId"]))
                            {
                                await UpdateVnetWithBatchSubnet();
                            }

                            await kubernetesManager.UpgradeValuesYamlAsync(storageAccount, settings, containersToMount, installedVersion);
                            kubernetesClient = await PerformHelmDeploymentAsync(resourceGroup);
                        }
                        else
                        {
                            throw new ValidationException("Upgrading pre-4.0 versions of CromwellOnAzure is not supported. Please see https://github.com/microsoft/CromwellOnAzure/wiki/4.0-Migration-Guide.");
                        }

                        await WriteNonPersonalizedFilesToStorageAccountAsync(storageAccount);
                    }

                    if (!configuration.Update)
                    {
                        if (string.IsNullOrWhiteSpace(configuration.BatchPrefix))
                        {
                            var blob = new byte[5];
                            RandomNumberGenerator.Fill(blob);
                            configuration.BatchPrefix = CommonUtilities.Base32.ConvertToBase32(blob).TrimEnd('=');
                        }

                        ValidateRegionName(configuration.RegionName);
                        ValidateMainIdentifierPrefix(configuration.MainIdentifierPrefix);
                        storageAccount = await ValidateAndGetExistingStorageAccountAsync();
                        batchAccount = await ValidateAndGetExistingBatchAccountAsync();
                        aksCluster = await ValidateAndGetExistingAKSClusterAsync();
                        postgreSqlFlexServer = await ValidateAndGetExistingPostgresqlServer();
                        var keyVault = await ValidateAndGetExistingKeyVault();

                        if (string.IsNullOrWhiteSpace(configuration.PostgreSqlServerName))
                        {
                            configuration.PostgreSqlServerName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}-", 15);
                        }

                        configuration.PostgreSqlAdministratorPassword = Utility.GeneratePassword();
                        configuration.PostgreSqlCromwellUserPassword = Utility.GeneratePassword();
                        configuration.PostgreSqlTesUserPassword = Utility.GeneratePassword();

                        if (string.IsNullOrWhiteSpace(configuration.BatchAccountName))
                        {
                            configuration.BatchAccountName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}", 15);
                        }

                        if (string.IsNullOrWhiteSpace(configuration.StorageAccountName))
                        {
                            configuration.StorageAccountName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}", 24);
                        }

                        if (string.IsNullOrWhiteSpace(configuration.ApplicationInsightsAccountName))
                        {
                            configuration.ApplicationInsightsAccountName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}-", 15);
                        }

                        if (string.IsNullOrWhiteSpace(configuration.AksClusterName))
                        {
                            configuration.AksClusterName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}-", 25);
                        }

                        if (string.IsNullOrWhiteSpace(configuration.KeyVaultName))
                        {
                            configuration.KeyVaultName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}-", 15);
                        }

                        await RegisterResourceProvidersAsync();
                        await ValidateVmAsync();

                        if (batchAccount is null)
                        {
                            await ValidateBatchAccountQuotaAsync();
                        }

                        var vnetAndSubnet = await ValidateAndGetExistingVirtualNetworkAsync();

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
                            configuration.PostgreSqlSubnetName = string.IsNullOrEmpty(configuration.PostgreSqlSubnetName) ? configuration.DefaultPostgreSqlSubnetName : configuration.PostgreSqlSubnetName;
                            configuration.BatchSubnetName = string.IsNullOrEmpty(configuration.BatchSubnetName) ? configuration.DefaultBatchSubnetName : configuration.BatchSubnetName;
                            configuration.VmSubnetName = string.IsNullOrEmpty(configuration.VmSubnetName) ? configuration.DefaultVmSubnetName : configuration.VmSubnetName;
                            vnetAndSubnet = await CreateVnetAndSubnetsAsync(resourceGroup);
                            this.configuration.BatchNodesSubnetId = vnetAndSubnet.Value.batchSubnet.Inner.Id;
                        }

                        if (string.IsNullOrWhiteSpace(configuration.LogAnalyticsArmId))
                        {
                            var workspaceName = SdkContext.RandomResourceName(configuration.MainIdentifierPrefix, 15);
                            logAnalyticsWorkspace = await CreateLogAnalyticsWorkspaceResourceAsync(workspaceName);
                            configuration.LogAnalyticsArmId = logAnalyticsWorkspace.Id;
                        }

                        await Task.Run(async () =>
                        {
                            storageAccount ??= await CreateStorageAccountAsync();
                            await CreateDefaultStorageContainersAsync(storageAccount);
                            await WriteNonPersonalizedFilesToStorageAccountAsync(storageAccount);
                            await WritePersonalizedFilesToStorageAccountAsync(storageAccount, managedIdentity.Name);
                            await AssignVmAsContributorToStorageAccountAsync(managedIdentity, storageAccount);
                            await AssignVmAsDataReaderToStorageAccountAsync(managedIdentity, storageAccount);
                            await AssignManagedIdOperatorToResourceAsync(managedIdentity, resourceGroup);
                            await AssignMIAsNetworkContributorToResourceAsync(managedIdentity, resourceGroup);
                        });

                        if (configuration.CrossSubscriptionAKSDeployment.GetValueOrDefault())
                        {
                            await Task.Run(async () =>
                            {
                                keyVault ??= await CreateKeyVaultAsync(configuration.KeyVaultName, managedIdentity, vnetAndSubnet.Value.vmSubnet);
                                keyVaultUri = keyVault.Properties.VaultUri;
                                var keys = await storageAccount.GetKeysAsync();
                                await SetStorageKeySecret(keyVaultUri, StorageAccountKeySecretName, keys[0].Value);
                            });
                        }

                        if (postgreSqlFlexServer is null)
                        {
                            postgreSqlDnsZone = await CreatePrivateDnsZoneAsync(vnetAndSubnet.Value.virtualNetwork, $"privatelink.postgres.database.azure.com", "PostgreSQL Server");
                        }

                        await Task.WhenAll(new Task[]
                        {
                            Task.Run(async () =>
                            {
                                batchAccount ??= await CreateBatchAccountAsync(storageAccount.Id);
                                await AssignVmAsContributorToBatchAccountAsync(managedIdentity, batchAccount);
                            }),
                            Task.Run(async () =>
                            {
                                appInsights = await CreateAppInsightsResourceAsync(configuration.LogAnalyticsArmId);
                                await AssignVmAsContributorToAppInsightsAsync(managedIdentity, appInsights);
                            }),
                            Task.Run(async () => {
                                if (configuration.UsePostgreSqlSingleServer)
                                {
                                    postgreSqlSingleServer ??= await CreateSinglePostgreSqlServerAndDatabaseAsync(postgreSqlSingleManagementClient, vnetAndSubnet.Value.vmSubnet, postgreSqlDnsZone);
                                }
                                else
                                {
                                    postgreSqlFlexServer ??= await CreatePostgreSqlServerAndDatabaseAsync(postgreSqlFlexManagementClient, vnetAndSubnet.Value.postgreSqlSubnet, postgreSqlDnsZone);
                                }
                            })
                        });

                        var clientId = managedIdentity.ClientId;
                        var settings = ConfigureSettings(clientId);

                        if (aksCluster is null && !configuration.ManualHelmDeployment)
                        {
                            await ProvisionManagedCluster(resourceGroup, managedIdentity, logAnalyticsWorkspace, vnetAndSubnet?.virtualNetwork, vnetAndSubnet?.vmSubnet.Name, configuration.PrivateNetworking.GetValueOrDefault());
                        }

                        await kubernetesManager.UpdateHelmValuesAsync(storageAccount, keyVaultUri, resourceGroup.Name, settings, managedIdentity, containersToMount);
                        kubernetesClient = await PerformHelmDeploymentAsync(resourceGroup,
                            new[]
                            {
                                "Run the following postgresql command to setup the database.",
                                "\tPostgreSQL command: " + GetPostgreSQLCreateCromwellUserCommand(configuration.UsePostgreSqlSingleServer, configuration.PostgreSqlCromwellDatabaseName, GetCreateCromwellUserString()),
                                "\tPostgreSQL command: " + GetPostgreSQLCreateCromwellUserCommand(configuration.UsePostgreSqlSingleServer, configuration.PostgreSqlTesDatabaseName, GetCreateTesUserString()),
                            },
                            async kubernetesClient =>
                            {
                                await kubernetesManager.DeployCoADependenciesAsync();

                                // Deploy an ubuntu pod to run PSQL commands, then delete it
                                const string deploymentNamespace = "default";
                                var (deploymentName, ubuntuDeployment) = KubernetesManager.GetUbuntuDeploymentTemplate();
                                await kubernetesClient.AppsV1.CreateNamespacedDeploymentAsync(ubuntuDeployment, deploymentNamespace);
                                await ExecuteQueriesOnAzurePostgreSQLDbFromK8(kubernetesClient, deploymentName, deploymentNamespace);
                                await kubernetesClient.AppsV1.DeleteNamespacedDeploymentAsync(deploymentName, deploymentNamespace);
                            });
                    }
                    if (kubernetesClient is not null)
                    {
                        await kubernetesManager.WaitForCromwellAsync(kubernetesClient);
                    }
                }
                finally
                {
                    if (!configuration.ManualHelmDeployment)
                    {
                        kubernetesManager?.DeleteTempFiles();
                    }
                }

                batchAccount = await GetExistingBatchAccountAsync(configuration.BatchAccountName);
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

                    if (configuration.DebugLogging)
                    {
                        ConsoleEx.WriteLine(exc.StackTrace, ConsoleColor.Red);

                        if (exc is KubernetesException kExc)
                        {
                            ConsoleEx.WriteLine($"Kubenetes Status: {kExc.Status}");
                        }

                        if (exc is WebSocketException wExc)
                        {
                            ConsoleEx.WriteLine($"WebSocket ErrorCode: {wExc.WebSocketErrorCode}");
                        }

                        if (exc is HttpOperationException hExc)
                        {
                            ConsoleEx.WriteLine($"HTTP Response: {hExc.Response.Content}");
                        }
                    }
                }

                ConsoleEx.WriteLine();
                Debugger.Break();
                WriteGeneralRetryMessageToConsole();
                await DeleteResourceGroupIfUserConsentsAsync();
                return 1;
            }
        }

        private async Task<IKubernetes> PerformHelmDeploymentAsync(IResourceGroup resourceGroup, IEnumerable<string> manualPrecommands = default, Func<IKubernetes, Task> asyncTask = default)
        {
            if (configuration.ManualHelmDeployment)
            {
                ConsoleEx.WriteLine($"Helm chart written to disk at: {kubernetesManager.helmScriptsRootDirectory}");
                ConsoleEx.WriteLine($"Please update values file if needed here: {kubernetesManager.TempHelmValuesYamlPath}");

                foreach (var line in manualPrecommands ?? Enumerable.Empty<string>())
                {
                    ConsoleEx.WriteLine(line);
                }

                ConsoleEx.WriteLine($"Then, deploy the helm chart, and press Enter to continue.");
                ConsoleEx.ReadLine();
                return default;
            }
            else
            {
                var kubernetesClient = await kubernetesManager.GetKubernetesClientAsync(resourceGroup);
                await (asyncTask?.Invoke(kubernetesClient) ?? Task.CompletedTask);
                await kubernetesManager.DeployHelmChartToClusterAsync();
                return kubernetesClient;
            }
        }

        private async Task<Vault> ValidateAndGetExistingKeyVault()
        {
            if (string.IsNullOrWhiteSpace(configuration.KeyVaultName))
            {
                return null;
            }

            return (await GetKeyVaultAsync(configuration.KeyVaultName))
                ?? throw new ValidationException($"If key vault name is provided, it must already exist in region {configuration.RegionName}, and be accessible to the current user.", displayExample: false);
        }

        private async Task<FlexibleServerModel.Server> ValidateAndGetExistingPostgresqlServer()
        {
            if (string.IsNullOrWhiteSpace(configuration.PostgreSqlServerName))
            {
                return null;
            }

            return (await GetExistingPostgresqlService(configuration.PostgreSqlServerName))
                ?? throw new ValidationException($"If Postgresql server name is provided, the server must already exist in region {configuration.RegionName}, and be accessible to the current user.", displayExample: false);
        }

        private async Task<ManagedCluster> ValidateAndGetExistingAKSClusterAsync()
        {
            if (string.IsNullOrWhiteSpace(configuration.AksClusterName))
            {
                return null;
            }

            return (await GetExistingAKSClusterAsync(configuration.AksClusterName))
                ?? throw new ValidationException($"If AKS cluster name is provided, the cluster must already exist in region {configuration.RegionName}, and be accessible to the current user.", displayExample: false);
        }

        private async Task<FlexibleServerModel.Server> GetExistingPostgresqlService(string serverName)
        {
            return (await Task.WhenAll(subscriptionIds.Select(async s =>
            {
                try
                {
                    var client = new FlexibleServer.PostgreSQLManagementClient(tokenCredentials) { SubscriptionId = s };
                    return await client.Servers.ListAsync();
                }
                catch (Exception e)
                {
                    ConsoleEx.WriteLine(e.Message);
                    return null;
                }
            })))
                .Where(a => a is not null)
                .SelectMany(a => a)
                .SingleOrDefault(a => a.Name.Equals(serverName, StringComparison.OrdinalIgnoreCase) && Regex.Replace(a.Location, @"\s+", "").Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase));
        }

        private async Task<ManagedCluster> GetExistingAKSClusterAsync(string aksClusterName)
        {
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
        }

        private async Task<ManagedCluster> ProvisionManagedCluster(IResource resourceGroupObject, IIdentity managedIdentity, IGenericResource logAnalyticsWorkspace, INetwork virtualNetwork, string subnetName, bool privateNetworking)
        {
            var resourceGroup = resourceGroupObject.Name;
            var nodePoolName = "nodepool1";
            var containerServiceClient = new ContainerServiceClient(azureCredentials) { SubscriptionId = configuration.SubscriptionId };
            var cluster = new ManagedCluster
            {
                AddonProfiles = new Dictionary<string, ManagedClusterAddonProfile>
                {
                    { "omsagent", new ManagedClusterAddonProfile(true, new Dictionary<string, string>() { { "logAnalyticsWorkspaceResourceID", logAnalyticsWorkspace.Id } }) }
                },
                Location = configuration.RegionName,
                DnsPrefix = configuration.AksClusterName,
                NetworkProfile = new ContainerServiceNetworkProfile
                {
                    NetworkPlugin = NetworkPlugin.Azure,
                    ServiceCidr = configuration.KubernetesServiceCidr,
                    DnsServiceIP = configuration.KubernetesDnsServiceIP,
                    DockerBridgeCidr = configuration.KubernetesDockerBridgeCidr,
                    NetworkPolicy = NetworkPolicy.Azure
                },
                Identity = new ManagedClusterIdentity(managedIdentity.PrincipalId, managedIdentity.TenantId, Microsoft.Azure.Management.ContainerService.Models.ResourceIdentityType.UserAssigned)
                {
                    UserAssignedIdentities = new Dictionary<string, ManagedClusterIdentityUserAssignedIdentitiesValue>()
                }
            };
            cluster.Identity.UserAssignedIdentities.Add(managedIdentity.Id, new ManagedClusterIdentityUserAssignedIdentitiesValue(managedIdentity.PrincipalId, managedIdentity.ClientId));
            cluster.IdentityProfile = new Dictionary<string, ManagedClusterPropertiesIdentityProfileValue>
            {
                { "kubeletidentity", new ManagedClusterPropertiesIdentityProfileValue(managedIdentity.Id, managedIdentity.ClientId, managedIdentity.PrincipalId) }
            };
            cluster.AgentPoolProfiles = new List<ManagedClusterAgentPoolProfile>
            {
                new ManagedClusterAgentPoolProfile()
                {
                    Name = nodePoolName,
                    Count = configuration.AksPoolSize,
                    VmSize = configuration.VmSize,
                    OsDiskSizeGB = 128,
                    OsDiskType = OSDiskType.Managed,
                    Type = "VirtualMachineScaleSets",
                    EnableAutoScaling = false,
                    EnableNodePublicIP = false,
                    OsType = "Linux",
                    Mode = "System",
                    VnetSubnetID = virtualNetwork.Subnets[subnetName].Inner.Id,
                }
            };

            if (privateNetworking)
            {
                cluster.ApiServerAccessProfile = new ManagedClusterAPIServerAccessProfile()
                {
                    EnablePrivateCluster = true,
                    EnablePrivateClusterPublicFQDN = true
                };
            }

            return await Execute(
                $"Creating AKS Cluster: {configuration.AksClusterName}...",
                () => containerServiceClient.ManagedClusters.CreateOrUpdateAsync(resourceGroup, configuration.AksClusterName, cluster));
        }

        private static Dictionary<string, string> GetDefaultValues(string[] files)
        {
            var settings = new Dictionary<string, string>();

            foreach (var file in files)
            {
                settings = settings.Union(Utility.DelimitedTextToDictionary(Utility.GetFileContent("scripts", file))).ToDictionary(kv => kv.Key, kv => kv.Value);
            }

            return settings;
        }

        private Dictionary<string, string> ConfigureSettings(string managedIdentityClientId, Dictionary<string, string> settings = null, Version installedVersion = null)
        {
            settings ??= new();
            var defaults = GetDefaultValues(new[] { "env-00-coa-version.txt", "env-01-account-names.txt", "env-02-internal-images.txt", "env-03-external-images.txt", "env-04-settings.txt" });

            // We always overwrite the CoA version
            UpdateSetting(settings, defaults, "CromwellOnAzureVersion", default(string), ignoreDefaults: false);

            // Process images
            UpdateSetting(settings, defaults, "CromwellImageName", configuration.CromwellVersion, v => $"broadinstitute/cromwell:{v}",
                ignoreDefaults: ImageNameIgnoreDefaults(settings, defaults, "CromwellImageName", configuration.CromwellVersion is null, installedVersion,
                    tag => GetTag(settings["CromwellImageName"]) <= GetTag(defaults["CromwellImageName"]))); // There's not a good way to detect customization of this property, so default to forced upgrade to the new default.
            UpdateSetting(settings, defaults, "TesImageName", configuration.TesImageName, ignoreDefaults: ImageNameIgnoreDefaults(settings, defaults, "TesImageName", configuration.TesImageName is null, installedVersion));
            UpdateSetting(settings, defaults, "TriggerServiceImageName", configuration.TriggerServiceImageName, ignoreDefaults: ImageNameIgnoreDefaults(settings, defaults, "TriggerServiceImageName", configuration.TriggerServiceImageName is null, installedVersion));

            // Additional non-personalized settings
            UpdateSetting(settings, defaults, "BatchNodesSubnetId", configuration.BatchNodesSubnetId);
            UpdateSetting(settings, defaults, "DockerInDockerImageName", configuration.DockerInDockerImageName);
            UpdateSetting(settings, defaults, "BlobxferImageName", configuration.BlobxferImageName);
            UpdateSetting(settings, defaults, "DisableBatchNodesPublicIpAddress", configuration.DisableBatchNodesPublicIpAddress, b => b.GetValueOrDefault().ToString(), configuration.DisableBatchNodesPublicIpAddress.GetValueOrDefault().ToString());

            if (installedVersion is null)
            {
                UpdateSetting(settings, defaults, "BatchPrefix", configuration.BatchPrefix, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "DefaultStorageAccountName", configuration.StorageAccountName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "BatchAccountName", configuration.BatchAccountName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "ApplicationInsightsAccountName", configuration.ApplicationInsightsAccountName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "ManagedIdentityClientId", managedIdentityClientId, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "AzureServicesAuthConnectionString", managedIdentityClientId, s => $"RunAs=App;AppId={s}", ignoreDefaults: true);
                UpdateSetting(settings, defaults, "KeyVaultName", configuration.KeyVaultName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "AksCoANamespace", configuration.AksCoANamespace, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "CrossSubscriptionAKSDeployment", configuration.CrossSubscriptionAKSDeployment);
                UpdateSetting(settings, defaults, "PostgreSqlServerName", configuration.PostgreSqlServerName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "PostgreSqlServerNameSuffix", configuration.PostgreSqlServerNameSuffix, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "PostgreSqlServerPort", configuration.PostgreSqlServerPort.ToString(), ignoreDefaults: true);
                UpdateSetting(settings, defaults, "PostgreSqlServerSslMode", configuration.PostgreSqlServerSslMode, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "PostgreSqlTesDatabaseName", configuration.PostgreSqlTesDatabaseName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "PostgreSqlTesDatabaseUserLogin", GetFormattedPostgresqlUser(isCromwellPostgresUser: false), ignoreDefaults: true);
                UpdateSetting(settings, defaults, "PostgreSqlTesDatabaseUserPassword", configuration.PostgreSqlTesUserPassword, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "PostgreSqlCromwellDatabaseName", configuration.PostgreSqlCromwellDatabaseName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "PostgreSqlCromwellDatabaseUserLogin", GetFormattedPostgresqlUser(isCromwellPostgresUser: true), ignoreDefaults: true);
                UpdateSetting(settings, defaults, "PostgreSqlCromwellDatabaseUserPassword", configuration.PostgreSqlCromwellUserPassword, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "UsePostgreSqlSingleServer", configuration.UsePostgreSqlSingleServer.ToString(), ignoreDefaults: true);
            }

            BackFillSettings(settings, defaults);
            return settings;

            static int GetTag(string imageName)
                => int.TryParse(imageName?[(imageName.LastIndexOf(':') + 1)..] ?? string.Empty, System.Globalization.NumberStyles.Integer, System.Globalization.NumberFormatInfo.InvariantInfo, out var tag) ? tag : 0;
        }

        /// <summary>
        /// Determines if current setting should be ignored (used for product image names).
        /// </summary>
        /// <param name="settings">Property bag being updated.</param>
        /// <param name="defaults">Property bag containing default values.</param>
        /// <param name="key">Key of value in both <paramref name="settings"/> and <paramref name="defaults"/>.</param>
        /// <param name="valueIsNull">True if configuration value to set is null. See <see cref="UpdateSetting{T}(Dictionary{string, string}, Dictionary{string, string}, string, T, Func{T, string}, string, bool?)"/>'s "value" parameter.</param>
        /// <param name="installedVersion">A <see cref="Version"/> of the current configuration, or null if this is not an update.</param>
        /// <param name="IsInstalledNotCustomized">A <see cref="Predicate{T}"/> where the parameter is the image tag of the currently configured image. Only called if the rest of the image name is identical to the default. Return False if the value is considered customized, otherwise True.</param>
        /// <returns>false if current setting should be ignored, null otherwise.</returns>
        /// <remarks>This method provides a value for the "ignoreDefaults" parameter to <see cref="UpdateSetting{T}(Dictionary{string, string}, Dictionary{string, string}, string, T, Func{T, string}, string, bool?)"/> for use with container image names.</remarks>
        private static bool? ImageNameIgnoreDefaults(Dictionary<string, string> settings, Dictionary<string, string> defaults, string key, bool valueIsNull, Version installedVersion, Predicate<string> IsInstalledNotCustomized = default)
        {
            if (installedVersion is null || !valueIsNull)
            {
                return null;
            }

            var sameVersionUpgrade = installedVersion.Equals(new(defaults["CromwellOnAzureVersion"]));
            _ = settings.TryGetValue(key, out var installed);
            _ = defaults.TryGetValue(key, out var @default);
            var defaultPath = @default?[..@default.LastIndexOf(':')];
            var installedTag = installed?[(installed.LastIndexOf(':') + 1)..];
            bool? result;

            IsInstalledNotCustomized ??= new(tag =>
                // Is the tag a version (without decorations)?
                Version.TryParse(tag, out var version) &&
                // Is the image's version the same as the installed version?
                version.Equals(installedVersion));

            try
            {
                // Is this our official prepository/image?
                result = installed.StartsWith(defaultPath + ":")
                    // Is the installed tag customized?
                    && IsInstalledNotCustomized(installedTag)
                    // Upgrade image
                    ? false
                    // Preserve configured image
                    : null;
            }
            catch (ArgumentException)
            {
                result = null;
            }

            if (result is null && !sameVersionUpgrade)
            {
                ConsoleEx.WriteLine($"Warning: CromwellOnAzure is being upgraded, but {key} was customized, and is not being upgraded, which might not be what you want. (To remove the customization of {key}, set it to the empty string.)", ConsoleColor.Yellow);
            }

            return result;
        }

        /// <summary>
        /// Pupulates <paramref name="settings"/> with missing values.
        /// </summary>
        /// <param name="settings">Property bag being updated.</param>
        /// <param name="defaults">Property bag containing default values.</param>
        /// <remarks>Copy to settings any missing values found in defaults</remarks>
        private static void BackFillSettings(Dictionary<string, string> settings, Dictionary<string, string> defaults)
        {
            foreach (var key in defaults.Keys.Except(settings.Keys))
            {
                settings[key] = defaults[key];
            }
        }

        /// <summary>
        /// Updates <paramref name="settings"/>.
        /// </summary>
        /// <typeparam name="T">Type of <paramref name="value"/>.</typeparam>
        /// <param name="settings">Property bag being updated.</param>
        /// <param name="defaults">Property bag containing default values.</param>
        /// <param name="key">Key of value in both <paramref name="settings"/> and <paramref name="defaults"/>.</param>
        /// <param name="value">Configuration value to set. Nullable. See remarks.</param>
        /// <param name="ConvertValue">Function that converts <paramref name="value"/> to a string. Can be used for formatting. Defaults to returning the value's string.</param>
        /// <param name="defaultValue">Value to use if <paramref name="defaults"/> does not contain a record for <paramref name="key"/> when <paramref name="value"/> is null.</param>
        /// <param name="ignoreDefaults">True to never use value from <paramref name="defaults"/>, False to never keep the value from <paramref name="settings"/>, null to follow remarks.</param>
        /// <remarks>
        /// If value is null, keep the value already in settings. If the key is not in settings, set the corresponding value from defaults. If key is not found in defaults, use defaultValue.
        /// Otherwise, convert value to a string using convertValue().
        /// </remarks>
        private static void UpdateSetting<T>(Dictionary<string, string> settings, Dictionary<string, string> defaults, string key, T value, Func<T, string> ConvertValue = default, string defaultValue = "", bool? ignoreDefaults = null)
        {
            ConvertValue ??= new(v => v switch
            {
                string s => s,
                _ => v?.ToString(),
            });

            var valueIsNull = value is null;
            var valueIsEmpty = !valueIsNull && value switch
            {
                string s => string.IsNullOrWhiteSpace(s),
                _ => string.IsNullOrWhiteSpace(value?.ToString()),
            };

            if (valueIsNull && settings.ContainsKey(key) && ignoreDefaults != false)
            {
                return; // No changes to this setting, no need to rewrite it.
            }

            var GetDefault = new Func<string>(() => ignoreDefaults switch
            {
                true => defaultValue,
                _ => defaults.TryGetValue(key, out var @default) ? @default : defaultValue,
            });

            settings[key] = valueIsEmpty || valueIsNull ? GetDefault() : ConvertValue(value);
        }
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

                            await Task.Delay(System.TimeSpan.FromSeconds(15));
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

        private Task AssignManagedIdOperatorToResourceAsync(IIdentity managedIdentity, IResource resource)
        {
            // https://docs.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#managed-identity-operator
            var roleDefinitionId = $"/subscriptions/{configuration.SubscriptionId}/providers/Microsoft.Authorization/roleDefinitions/f1a07417-d97a-45cb-824c-7a7467783830";
            return Execute(
                $"Assigning Managed ID Operator role for the managed id to resource group scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    () => azureSubscriptionClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithRoleDefinition(roleDefinitionId)
                        .WithResourceScope(resource)
                        .CreateAsync(cts.Token)));
        }

        private Task AssignMIAsNetworkContributorToResourceAsync(IIdentity managedIdentity, IResource resource)
        {
            // https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#network-contributor
            var roleDefinitionId = $"/subscriptions/{configuration.SubscriptionId}/providers/Microsoft.Authorization/roleDefinitions/4d97b98b-1d4f-4787-a291-c67834d212e7";
            return Execute(
                $"Assigning Network Contributor role for the managed id to resource group scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    () => azureSubscriptionClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithRoleDefinition(roleDefinitionId)
                        .WithResourceScope(resource)
                        .CreateAsync(cts.Token)));
        }

        private Task AssignVmAsDataReaderToStorageAccountAsync(IIdentity managedIdentity, IStorageAccount storageAccount)
        {
            // https://docs.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#storage-blob-data-reader
            var roleDefinitionId = $"/subscriptions/{configuration.SubscriptionId}/providers/Microsoft.Authorization/roleDefinitions/2a2b9908-6ea1-4ae2-8e65-a410df84e7d1";

            return Execute(
                $"Assigning Storage Blob Data Reader role for user-managed identity to Storage Account resource scope...",
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
                $"Assigning {BuiltInRole.Contributor} role for user-managed identity to Storage Account resource scope...",
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
                    .WithSku(StorageAccountSkuType.Standard_LRS)
                    .CreateAsync(cts.Token));

        private async Task<IStorageAccount> GetExistingStorageAccountAsync(string storageAccountName)
            => await GetExistingStorageAccountAsync(storageAccountName, azureClient, subscriptionIds, configuration);

        public static async Task<IStorageAccount> GetExistingStorageAccountAsync(string storageAccountName, Microsoft.Azure.Management.Fluent.Azure.IAuthenticated azureClient, IEnumerable<string> subscriptionIds, Configuration configuration)
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

        private async Task<BatchAccount> GetExistingBatchAccountAsync(string batchAccountName)
            => (await Task.WhenAll(subscriptionIds.Select(async s =>
            {
                try
                {
                    var client = new BatchManagementClient(tokenCredentials) { SubscriptionId = s };
                    return await client.BatchAccount.ListAsync();
                }
                catch (Exception e)
                {
                    ConsoleEx.WriteLine(e.Message);
                    return null;
                }
            })))
                .Where(a => a is not null)
                .SelectMany(a => a)
                .SingleOrDefault(a => a.Name.Equals(batchAccountName, StringComparison.OrdinalIgnoreCase) && a.Location.Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase));

        private async Task CreateDefaultStorageContainersAsync(IStorageAccount storageAccount)
        {
            var blobClient = await GetBlobClientAsync(storageAccount);

            var defaultContainers = new List<string> { WorkflowsContainerName, InputsContainerName, "cromwell-executions", "cromwell-workflow-logs", "outputs", "tes-internal", ConfigurationContainerName };
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
                $"Writing {CromwellConfigurationFileName} & {AllowedVmSizesFileName} files to '{ConfigurationContainerName}' storage container...",
                async () =>
                {
                    // Configure Cromwell config file for PostgreSQL on Azure.
                    await UploadTextToStorageAccountAsync(storageAccount, ConfigurationContainerName, CromwellConfigurationFileName, Utility.PersonalizeContent(new[]
                    {
                        new Utility.ConfigReplaceTextItem("{DatabaseUrl}", $"\"jdbc:postgresql://{configuration.PostgreSqlServerName}.postgres.database.azure.com/{configuration.PostgreSqlCromwellDatabaseName}?sslmode=require\""),
                        new Utility.ConfigReplaceTextItem("{DatabaseUser}", configuration.UsePostgreSqlSingleServer ? $"\"{configuration.PostgreSqlCromwellUserLogin}@{configuration.PostgreSqlServerName}\"": $"\"{configuration.PostgreSqlCromwellUserLogin}\""),
                        new Utility.ConfigReplaceTextItem("{DatabasePassword}", $"\"{configuration.PostgreSqlCromwellUserPassword}\""),
                        new Utility.ConfigReplaceTextItem("{DatabaseDriver}", $"\"org.postgresql.Driver\""),
                        new Utility.ConfigReplaceTextItem("{DatabaseProfile}", "\"slick.jdbc.PostgresProfile$\""),
                    }, "scripts", CromwellConfigurationFileName));

                    await UploadTextToStorageAccountAsync(storageAccount, ConfigurationContainerName, AllowedVmSizesFileName, Utility.GetFileContent("scripts", AllowedVmSizesFileName));
                });

        private Task AssignVmAsContributorToBatchAccountAsync(IIdentity managedIdentity, BatchAccount batchAccount)
            => Execute(
                $"Assigning {BuiltInRole.Contributor} role for user-managed identity to Batch Account resource scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    () => azureSubscriptionClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithBuiltInRole(BuiltInRole.Contributor)
                        .WithScope(batchAccount.Id)
                        .CreateAsync(cts.Token)));

        private async Task<FlexibleServerModel.Server> CreatePostgreSqlServerAndDatabaseAsync(FlexibleServer.IPostgreSQLManagementClient postgresManagementClient, ISubnet subnet, IPrivateDnsZone postgreSqlDnsZone)
        {
            if (!subnet.Inner.Delegations.Any())
            {
                subnet.Parent.Update().UpdateSubnet(subnet.Name).WithDelegation("Microsoft.DBforPostgreSQL/flexibleServers");
                await subnet.Parent.Update().ApplyAsync();
            }

            FlexibleServerModel.Server server = null;

            await Execute(
                $"Creating Azure Flexible Server for PostgreSQL: {configuration.PostgreSqlServerName}...",
                async () =>
                {
                    server = await postgresManagementClient.Servers.CreateAsync(
                        configuration.ResourceGroupName, configuration.PostgreSqlServerName,
                        new FlexibleServerModel.Server(
                           location: configuration.RegionName,
                           version: configuration.PostgreSqlFlexibleVersion,
                           sku: new Sku(configuration.PostgreSqlSkuName, configuration.PostgreSqlTier),
                           storage: new FlexibleServerModel.Storage(configuration.PostgreSqlStorageSize),
                           administratorLogin: configuration.PostgreSqlAdministratorLogin,
                           administratorLoginPassword: configuration.PostgreSqlAdministratorPassword,
                           network: new FlexibleServerModel.Network(publicNetworkAccess: "Disabled", delegatedSubnetResourceId: subnet.Inner.Id, privateDnsZoneArmResourceId: postgreSqlDnsZone.Id),
                           highAvailability: new FlexibleServerModel.HighAvailability("Disabled")
                        ));
                });

            await Execute(
                $"Creating PostgreSQL Cromwell database: {configuration.PostgreSqlCromwellDatabaseName}...",
                () => postgresManagementClient.Databases.CreateAsync(
                    configuration.ResourceGroupName, configuration.PostgreSqlServerName, configuration.PostgreSqlCromwellDatabaseName,
                    new FlexibleServerModel.Database()));

            await Execute(
                $"Creating PostgreSQL TES database: {configuration.PostgreSqlTesDatabaseName}...",
                () => postgresManagementClient.Databases.CreateAsync(
                    configuration.ResourceGroupName, configuration.PostgreSqlServerName, configuration.PostgreSqlTesDatabaseName,
                    new FlexibleServerModel.Database()));

            return server;
        }

        private async Task<SingleServerModel.Server> CreateSinglePostgreSqlServerAndDatabaseAsync(SingleServer.IPostgreSQLManagementClient postgresManagementClient, ISubnet subnet, IPrivateDnsZone postgreSqlDnsZone)
        {
            SingleServerModel.Server server = null;

            await Execute(
                $"Creating Azure Single Server for PostgreSQL: {configuration.PostgreSqlServerName}...",
                async () =>
                {
                    server = await postgresManagementClient.Servers.CreateAsync(
                        configuration.ResourceGroupName, configuration.PostgreSqlServerName,
                        new SingleServerModel.ServerForCreate(
                           new SingleServerModel.ServerPropertiesForDefaultCreate(
                               administratorLogin: configuration.PostgreSqlAdministratorLogin,
                               administratorLoginPassword: configuration.PostgreSqlAdministratorPassword,
                               version: configuration.PostgreSqlSingleServerVersion,
                               publicNetworkAccess: "Disabled",
                               storageProfile: new Microsoft.Azure.Management.PostgreSQL.Models.StorageProfile(
                                   storageMB: configuration.PostgreSqlStorageSize * 1000)),
                           configuration.RegionName,
                           sku: new Microsoft.Azure.Management.PostgreSQL.Models.Sku("GP_Gen5_4")
                       ));

                    var privateEndpoint = await networkManagementClient.PrivateEndpoints.CreateOrUpdateAsync(configuration.ResourceGroupName, "pe-postgres1",
                        new Microsoft.Azure.Management.Network.Models.PrivateEndpoint(
                            name: "pe-coa-postgresql",
                            location: configuration.RegionName,
                            privateLinkServiceConnections: new List<Microsoft.Azure.Management.Network.Models.PrivateLinkServiceConnection>()
                            { new PrivateLinkServiceConnection(name: "pe-coa-postgresql", privateLinkServiceId: server.Id, groupIds: new List<string>(){"postgresqlServer"}) },
                            subnet: new Subnet(subnet.Inner.Id)));
                    var networkInterfaceName = privateEndpoint.NetworkInterfaces.First().Id.Split("/").Last();
                    var networkInterface = await networkManagementClient.NetworkInterfaces.GetAsync(configuration.ResourceGroupName, networkInterfaceName);

                    await postgreSqlDnsZone
                        .Update()
                        .DefineARecordSet(server.Name)
                        .WithIPv4Address(networkInterface.IpConfigurations.First().PrivateIPAddress)
                        .Attach()
                        .ApplyAsync();
                });

            await Execute(
                $"Creating PostgreSQL cromwell database: {configuration.PostgreSqlCromwellDatabaseName}...",
                async () => await postgresManagementClient.Databases.CreateOrUpdateAsync(
                    configuration.ResourceGroupName, configuration.PostgreSqlServerName, configuration.PostgreSqlCromwellDatabaseName,
                    new SingleServerModel.Database()));

            await Execute(
                $"Creating PostgreSQL tes database: {configuration.PostgreSqlTesDatabaseName}...",
                () => postgresManagementClient.Databases.CreateOrUpdateAsync(
                    configuration.ResourceGroupName, configuration.PostgreSqlServerName, configuration.PostgreSqlTesDatabaseName,
                    new SingleServerModel.Database()));
            return server;
        }

        private Task AssignVmAsContributorToAppInsightsAsync(IIdentity managedIdentity, IResource appInsights)
            => Execute(
                $"Assigning {BuiltInRole.Contributor} role for user-managed identity to App Insights resource scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    () => azureSubscriptionClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithBuiltInRole(BuiltInRole.Contributor)
                        .WithResourceScope(appInsights)
                        .CreateAsync(cts.Token)));

        private Task<(INetwork virtualNetwork, ISubnet vmSubnet, ISubnet postgreSqlSubnet, ISubnet batchSubnet)> CreateVnetAndSubnetsAsync(IResourceGroup resourceGroup)
          => Execute(
                $"Creating virtual network and subnets: {configuration.VnetName}...",
                async () =>
                {
                    var vnetDefinition = azureSubscriptionClient.Networks
                        .Define(configuration.VnetName)
                        .WithRegion(configuration.RegionName)
                        .WithExistingResourceGroup(resourceGroup)
                        .WithAddressSpace(configuration.VnetAddressSpace)
                        .DefineSubnet(configuration.VmSubnetName)
                        .WithAddressPrefix(configuration.VmSubnetAddressSpace).Attach();

                    vnetDefinition = vnetDefinition.DefineSubnet(configuration.PostgreSqlSubnetName)
                        .WithAddressPrefix(configuration.PostgreSqlSubnetAddressSpace)
                        .WithDelegation("Microsoft.DBforPostgreSQL/flexibleServers")
                        .Attach();

                    vnetDefinition = vnetDefinition.DefineSubnet(configuration.BatchSubnetName)
                        .WithAddressPrefix(configuration.BatchNodesSubnetAddressSpace)
                        .WithAccessFromService(ServiceEndpointType.MicrosoftStorage)
                        .WithAccessFromService(ServiceEndpointType.MicrosoftSql)
                        .Attach();

                    var vnet = await vnetDefinition.CreateAsync();
                    var batchSubnet = vnet.Subnets.FirstOrDefault(s => s.Key.Equals(configuration.BatchSubnetName, StringComparison.OrdinalIgnoreCase)).Value;

                    // Use the new ResourceManager sdk to add the ACR service endpoint since it is absent from the fluent sdk.
                    var armBatchSubnet = (await armClient.GetSubnetResource(new ResourceIdentifier(batchSubnet.Inner.Id)).GetAsync()).Value;

                    armBatchSubnet.Data.ServiceEndpoints.Add(new ServiceEndpointProperties()
                    {
                        Service = "Microsoft.ContainerRegistry",
                    });

                    await armBatchSubnet.UpdateAsync(Azure.WaitUntil.Completed, armBatchSubnet.Data);

                    return (vnet, 
                        vnet.Subnets.FirstOrDefault(s => s.Key.Equals(configuration.VmSubnetName, StringComparison.OrdinalIgnoreCase)).Value, 
                        vnet.Subnets.FirstOrDefault(s => s.Key.Equals(configuration.PostgreSqlSubnetName, StringComparison.OrdinalIgnoreCase)).Value,
                        batchSubnet);
                });

        private string GetFormattedPostgresqlUser(bool isCromwellPostgresUser)
        {
            var user = isCromwellPostgresUser ?
                configuration.PostgreSqlCromwellUserLogin :
                configuration.PostgreSqlTesUserLogin;

            if (configuration.UsePostgreSqlSingleServer)
            {
                return $"{user}@{configuration.PostgreSqlServerName}";
            }

            return user;
        }

        private string GetCreateCromwellUserString()
        {
            return $"CREATE USER {configuration.PostgreSqlCromwellUserLogin} WITH PASSWORD '{configuration.PostgreSqlCromwellUserPassword}'; GRANT ALL PRIVILEGES ON DATABASE {configuration.PostgreSqlCromwellDatabaseName} TO {configuration.PostgreSqlCromwellUserLogin};";
        }

        private string GetCreateTesUserString()
        {
            return $"CREATE USER {configuration.PostgreSqlTesUserLogin} WITH PASSWORD '{configuration.PostgreSqlTesUserPassword}'; GRANT ALL PRIVILEGES ON DATABASE {configuration.PostgreSqlTesDatabaseName} TO {configuration.PostgreSqlTesUserLogin};";
        }

        private string GetPostgreSQLCreateCromwellUserCommand(bool useSingleServer, string dbName, string sqlCommand)
        {
            if (useSingleServer)
            {
                return $"PGPASSWORD={configuration.PostgreSqlAdministratorPassword} psql -U {configuration.PostgreSqlAdministratorLogin}@{configuration.PostgreSqlServerName} -h {configuration.PostgreSqlServerName}.postgres.database.azure.com -d {dbName}  -v sslmode=true -c \"{sqlCommand}\"";
            }
            else
            {
                return $"psql postgresql://{configuration.PostgreSqlAdministratorLogin}:{configuration.PostgreSqlAdministratorPassword}@{configuration.PostgreSqlServerName}.postgres.database.azure.com/{dbName} -c \"{sqlCommand}\"";
            }
        }

        private Task<IPrivateDnsZone> CreatePrivateDnsZoneAsync(INetwork virtualNetwork, string name, string title)
            => Execute(
                $"Creating private DNS Zone for {title}...",
                async () =>
                {
                    // Note: for a potential future implementation of this method without Fluent,
                    // please see commit cbffa28 in #392
                    var dnsZone = await azureSubscriptionClient.PrivateDnsZones
                        .Define(name)
                        .WithExistingResourceGroup(configuration.ResourceGroupName)
                        .DefineVirtualNetworkLink($"{virtualNetwork.Name}-link")
                        .WithReferencedVirtualNetworkId(virtualNetwork.Id)
                        .DisableAutoRegistration()
                        .Attach()
                        .CreateAsync();
                    return dnsZone;
                });

        private Task ExecuteQueriesOnAzurePostgreSQLDbFromK8(IKubernetes kubernetesClient, string podName, string aksNamespace)
            => Execute(
                $"Executing script to create users in tes_db and cromwell_db...",
                async () =>
                {
                    var cromwellScript = GetCreateCromwellUserString();
                    var tesScript = GetCreateTesUserString();
                    var serverPath = $"{configuration.PostgreSqlServerName}.postgres.database.azure.com";
                    var adminUser = configuration.PostgreSqlAdministratorLogin;

                    if (configuration.UsePostgreSqlSingleServer)
                    {
                        adminUser = $"{configuration.PostgreSqlAdministratorLogin}@{configuration.PostgreSqlServerName}";
                    }

                    var commands = new List<string[]> {
                        new string[] { "apt", "-qq", "update" },
                        new string[] { "apt", "-qq", "install", "-y", "postgresql-client" },
                        new string[] { "bash", "-lic", $"echo {configuration.PostgreSqlServerName}.postgres.database.azure.com:{configuration.PostgreSqlServerPort}:{configuration.PostgreSqlCromwellDatabaseName}:{adminUser}:{configuration.PostgreSqlAdministratorPassword} > ~/.pgpass" },
                        new string[] { "bash", "-lic", $"echo {configuration.PostgreSqlServerName}.postgres.database.azure.com:{configuration.PostgreSqlServerPort}:{configuration.PostgreSqlTesDatabaseName}:{adminUser}:{configuration.PostgreSqlAdministratorPassword} >> ~/.pgpass" },
                        new string[] { "bash", "-lic", "chmod 0600 ~/.pgpass" },
                        new string[] { "/usr/bin/psql", "-h", serverPath, "-U", adminUser, "-d", configuration.PostgreSqlCromwellDatabaseName, "-c", cromwellScript },
                        new string[] { "/usr/bin/psql", "-h", serverPath, "-U", adminUser, "-d", configuration.PostgreSqlTesDatabaseName, "-c", tesScript }
                    };

                    await kubernetesManager.ExecuteCommandsOnPodAsync(kubernetesClient, podName, commands, aksNamespace);
                });

        private static async Task SetStorageKeySecret(string vaultUrl, string secretName, string secretValue)
        {
            var client = new SecretClient(new Uri(vaultUrl), new DefaultAzureCredential());
            await client.SetSecretAsync(secretName, secretValue);
        }

        private Task<Vault> GetKeyVaultAsync(string vaultName)
        {
            var keyVaultManagementClient = new KeyVaultManagementClient(azureCredentials) { SubscriptionId = configuration.SubscriptionId };
            return keyVaultManagementClient.Vaults.GetAsync(configuration.ResourceGroupName, vaultName);
        }

        private Task<Vault> CreateKeyVaultAsync(string vaultName, IIdentity managedIdentity, ISubnet subnet)
            => Execute(
                $"Creating Key Vault: {vaultName}...",
                async () =>
                {
                    var tenantId = managedIdentity.TenantId;
                    var secrets = new List<string>
                    {
                        "get",
                        "list",
                        "set",
                        "delete",
                        "backup",
                        "restore",
                        "recover",
                        "purge"
                    };

                    var keyVaultManagementClient = new KeyVaultManagementClient(azureCredentials) { SubscriptionId = configuration.SubscriptionId };
                    var properties = new VaultCreateOrUpdateParameters()
                    {
                        Location = configuration.RegionName,
                        Properties = new VaultProperties()
                        {
                            TenantId = new Guid(tenantId),
                            Sku = new Microsoft.Azure.Management.KeyVault.Models.Sku(SkuName.Standard),
                            NetworkAcls = new NetworkRuleSet()
                            {
                                DefaultAction = configuration.PrivateNetworking.GetValueOrDefault() ? "Deny" : "Allow"
                            },
                            AccessPolicies = new List<AccessPolicyEntry>()
                            {
                                new AccessPolicyEntry()
                                {
                                    TenantId = new Guid(tenantId),
                                    ObjectId = await GetUserObjectId(),
                                    Permissions = new Permissions()
                                    {
                                        Secrets = secrets
                                    }
                                },
                                new AccessPolicyEntry()
                                {
                                    TenantId = new Guid(tenantId),
                                    ObjectId = managedIdentity.PrincipalId,
                                    Permissions = new Permissions()
                                    {
                                        Secrets = secrets
                                    }
                                }
                            }
                        }
                    };

                    var vault = await keyVaultManagementClient.Vaults.CreateOrUpdateAsync(configuration.ResourceGroupName, vaultName, properties);

                    if (configuration.PrivateNetworking.GetValueOrDefault())
                    {
                        var privateEndpoint = await networkManagementClient.PrivateEndpoints.CreateOrUpdateAsync(configuration.ResourceGroupName, "pe-keyvault",
                            new Microsoft.Azure.Management.Network.Models.PrivateEndpoint(
                                name: "pe-coa-keyvault",
                                location: configuration.RegionName,
                                privateLinkServiceConnections: new List<Microsoft.Azure.Management.Network.Models.PrivateLinkServiceConnection>()
                                { new PrivateLinkServiceConnection(name: "pe-coa-keyvault", privateLinkServiceId: vault.Id, groupIds: new List<string>(){"vault"}) },
                                subnet: new Subnet(subnet.Inner.Id)));

                        var networkInterfaceName = privateEndpoint.NetworkInterfaces.First().Id.Split("/").Last();
                        var networkInterface = await networkManagementClient.NetworkInterfaces.GetAsync(configuration.ResourceGroupName, networkInterfaceName);

                        var dnsZone = await CreatePrivateDnsZoneAsync(subnet.Parent, "privatelink.vaultcore.azure.net", "KeyVault");
                        await dnsZone
                            .Update()
                            .DefineARecordSet(vault.Name)
                            .WithIPv4Address(networkInterface.IpConfigurations.First().PrivateIPAddress)
                            .Attach()
                            .ApplyAsync();
                    }

                    return vault;

                    async ValueTask<string> GetUserObjectId()
                    {
                        const string graphUri = "https://graph.windows.net";
                        var credentials = new AzureCredentials(default, new TokenCredentials(new RefreshableAzureServiceTokenProvider(graphUri)), tenantId, AzureEnvironment.AzureGlobalCloud);
                        using GraphRbacManagementClient rbacClient = new(Configure().WithEnvironment(AzureEnvironment.AzureGlobalCloud).WithCredentials(credentials).WithBaseUri(graphUri).Build()) { TenantID = tenantId };
                        credentials.InitializeServiceClient(rbacClient);
                        return (await rbacClient.SignedInUser.GetAsync()).ObjectId;
                    }
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
                $"Obtaining user-managed identity: {managedIdentityName}...",
                async () => await azureSubscriptionClient.Identities.GetByResourceGroupAsync(configuration.ResourceGroupName, managedIdentityName)
                    ?? await azureSubscriptionClient.Identities.Define(managedIdentityName)
                        .WithRegion(configuration.RegionName)
                        .WithExistingResourceGroup(resourceGroup)
                        .CreateAsync());
        }

        private async Task DeleteResourceGroupAsync()
        {
            var startTime = DateTime.UtcNow;
            var line = ConsoleEx.WriteLine("Deleting resource group...");
            await azureSubscriptionClient.ResourceGroups.DeleteByNameAsync(configuration.ResourceGroupName, CancellationToken.None);
            WriteExecutionTime(line, startTime);
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

            var token = (await tokenProvider.GetAuthenticationHeaderAsync(CancellationToken.None)).Parameter;
            var currentPrincipalObjectId = new JwtSecurityTokenHandler().ReadJwtToken(token).Claims.FirstOrDefault(c => c.Type == "oid").Value;

            var currentPrincipalSubscriptionRoleIds = (await azureSubscriptionClient.AccessManagement.RoleAssignments.Inner.ListForScopeWithHttpMessagesAsync($"/subscriptions/{configuration.SubscriptionId}", new ODataQuery<RoleAssignmentFilter>($"atScope() and assignedTo('{currentPrincipalObjectId}')")))
                .Body.AsContinuousCollection(link => Extensions.Synchronize(() => azureSubscriptionClient.AccessManagement.RoleAssignments.Inner.ListForScopeNextWithHttpMessagesAsync(link)).Body)
                .Select(b => b.RoleDefinitionId.Split(new[] { '/' }).Last());

            if (!currentPrincipalSubscriptionRoleIds.Contains(ownerRoleId) && !(currentPrincipalSubscriptionRoleIds.Contains(contributorRoleId)))
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
            }

            if (configuration.UsePostgreSqlSingleServer && int.Parse(configuration.PostgreSqlSingleServerVersion) > 11)
            {
                // https://learn.microsoft.com/en-us/azure/postgresql/single-server/concepts-version-policy
                ConsoleEx.WriteLine($"Warning: as of 2/7/2023, Azure Database for PostgreSQL Single Server only supports up to PostgreSQL version 11", ConsoleColor.Yellow);
                ConsoleEx.WriteLine($"{nameof(configuration.PostgreSqlSingleServerVersion)} is currently set to {configuration.PostgreSqlSingleServerVersion}", ConsoleColor.Yellow);
                ConsoleEx.WriteLine($"Deployment will continue but could fail; please consider including '--{nameof(configuration.PostgreSqlSingleServerVersion)} 11'", ConsoleColor.Yellow);
                ConsoleEx.WriteLine("More info: https://learn.microsoft.com/en-us/azure/postgresql/single-server/concepts-version-policy");
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

        private async Task<(INetwork virtualNetwork, ISubnet vmSubnet, ISubnet postgreSqlSubnet, ISubnet batchSubnet)?> ValidateAndGetExistingVirtualNetworkAsync()
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

            if (!AllOrNoneSet(configuration.VnetResourceGroupName, configuration.VnetName, configuration.VmSubnetName, configuration.PostgreSqlSubnetName))
            {
                throw new ValidationException($"{nameof(configuration.VnetResourceGroupName)}, {nameof(configuration.VnetName)}, {nameof(configuration.VmSubnetName)} and {nameof(configuration.PostgreSqlSubnetName)} are required when using an existing virtual network.");
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

            if (vmSubnet is null)
            {
                throw new ValidationException($"Virtual network '{configuration.VnetName}' does not contain subnet '{configuration.VmSubnetName}'");
            }

            var resourceGraphClient = new ResourceGraphClient(tokenCredentials);
            var postgreSqlSubnet = vnet.Subnets.FirstOrDefault(s => s.Key.Equals(configuration.PostgreSqlSubnetName, StringComparison.OrdinalIgnoreCase)).Value;

            if (postgreSqlSubnet is null)
            {
                throw new ValidationException($"Virtual network '{configuration.VnetName}' does not contain subnet '{configuration.PostgreSqlSubnetName}'");
            }

            var delegatedServices = postgreSqlSubnet.Inner.Delegations.Select(d => d.ServiceName);
            var hasOtherDelegations = delegatedServices.Any(s => s != "Microsoft.DBforPostgreSQL/flexibleServers");
            var hasNoDelegations = !delegatedServices.Any();

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

            var batchSubnet = vnet.Subnets.FirstOrDefault(s => s.Key.Equals(configuration.BatchSubnetName, StringComparison.OrdinalIgnoreCase)).Value;

            return (vnet, vmSubnet, postgreSqlSubnet, batchSubnet);
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

        private Task UpdateVnetWithBatchSubnet()
            => Execute(
                $"Creating batch subnet...",
                async () =>
                {
                    var resourceId = new ResourceIdentifier($"/subscriptions/{configuration.SubscriptionId}/resourceGroups/{configuration.ResourceGroupName}/");
                    var coaRg = armClient.GetResourceGroupResource(resourceId);

                    var vnetCollection = coaRg.GetVirtualNetworks();
                    var vnet = vnetCollection.FirstOrDefault();

                    if (vnetCollection.Count() != 1)
                    {
                        ConsoleEx.WriteLine("There are multiple vnets found in the resource group so the deployer cannot automatically create the subnet.", ConsoleColor.Red);
                        ConsoleEx.WriteLine("In order to avoid unnecessary load balancer charges we suggest manually configuring your deployment to use a subnet for batch pools with service endpoints.", ConsoleColor.Red);
                        ConsoleEx.WriteLine("See: https://github.com/microsoft/CromwellOnAzure/wiki/Using-a-batch-pool-subnet-with-service-endpoints-to-avoid-load-balancer-charges.", ConsoleColor.Red);

                        return;
                    }

                    var vnetData = vnet.Data;
                    var ipRange = vnetData.AddressPrefixes.Single();

                    var defaultSubnetNames = new List<string> { configuration.DefaultVmSubnetName, configuration.DefaultPostgreSqlSubnetName, configuration.DefaultBatchSubnetName };

                    if (!string.Equals(ipRange, configuration.VnetAddressSpace, StringComparison.OrdinalIgnoreCase) ||
                        vnetData.Subnets.Select(x => x.Name).Except(defaultSubnetNames).Any())
                    {
                        ConsoleEx.WriteLine("We detected a customized networking setup so the deployer will not automatically create the subnet.", ConsoleColor.Red);
                        ConsoleEx.WriteLine("In order to avoid unnecessary load balancer charges we suggest manually configuring your deployment to use a subnet for batch pools with service endpoints.", ConsoleColor.Red);
                        ConsoleEx.WriteLine("See: https://github.com/microsoft/CromwellOnAzure/wiki/Using-a-batch-pool-subnet-with-service-endpoints-to-avoid-load-balancer-charges.", ConsoleColor.Red);

                        return;
                    }

                    var batchSubnet = new SubnetData
                    {
                        Name = configuration.DefaultBatchSubnetName,
                        AddressPrefix = configuration.BatchNodesSubnetAddressSpace,
                    };

                    batchSubnet.ServiceEndpoints.Add(new ServiceEndpointProperties()
                    {
                        Service = "Microsoft.Storage",
                    });

                    batchSubnet.ServiceEndpoints.Add(new ServiceEndpointProperties()
                    {
                        Service = "Microsoft.Sql",
                    });

                    batchSubnet.ServiceEndpoints.Add(new ServiceEndpointProperties()
                    {
                        Service = "Microsoft.ContainerRegistry",
                    });

                    vnetData.Subnets.Add(batchSubnet);
                    await vnetCollection.CreateOrUpdateAsync(Azure.WaitUntil.Completed, vnetData.Name, vnetData);
                });

        private async Task ValidateVmAsync()
        {
            var computeSkus = (await generalRetryPolicy.ExecuteAsync(() =>
                azureSubscriptionClient.ComputeSkus.ListbyRegionAndResourceTypeAsync(
                    Region.Create(configuration.RegionName),
                    ComputeResourceType.VirtualMachines)))
                .Where(s => !s.Restrictions.Any())
                .Select(s => s.Name.Value)
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
                await Execute("Retrieving Azure management token...", () => new AzureServiceTokenProvider("RunAs=Developer; DeveloperTool=AzureCli").GetAccessTokenAsync("https://management.azure.com/"));
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

            void ValidateHelmInstall(string helmPath, string featureName)
            {
                if (!File.Exists(helmPath))
                {
                    throw new ValidationException($"Helm must be installed and set with the {featureName} flag. You can find instructions for install Helm here: https://helm.sh/docs/intro/install/");
                }
            }

            ThrowIfNotProvided(configuration.SubscriptionId, nameof(configuration.SubscriptionId));

            ThrowIfNotProvidedForInstall(configuration.RegionName, nameof(configuration.RegionName));

            ThrowIfNotProvidedForUpdate(configuration.ResourceGroupName, nameof(configuration.ResourceGroupName));


            ThrowIfProvidedForUpdate(configuration.BatchPrefix, nameof(configuration.BatchPrefix));
            ThrowIfProvidedForUpdate(configuration.RegionName, nameof(configuration.RegionName));
            ThrowIfProvidedForUpdate(configuration.BatchAccountName, nameof(configuration.BatchAccountName));
            ThrowIfProvidedForUpdate(configuration.CrossSubscriptionAKSDeployment, nameof(configuration.CrossSubscriptionAKSDeployment));
            //ThrowIfProvidedForUpdate(configuration.ProvisionPostgreSqlOnAzure, nameof(configuration.ProvisionPostgreSqlOnAzure));
            ThrowIfProvidedForUpdate(configuration.ApplicationInsightsAccountName, nameof(configuration.ApplicationInsightsAccountName));
            ThrowIfProvidedForUpdate(configuration.PrivateNetworking, nameof(configuration.PrivateNetworking));
            ThrowIfProvidedForUpdate(configuration.VnetName, nameof(configuration.VnetName));
            ThrowIfProvidedForUpdate(configuration.VnetResourceGroupName, nameof(configuration.VnetResourceGroupName));
            ThrowIfProvidedForUpdate(configuration.SubnetName, nameof(configuration.SubnetName));
            ThrowIfProvidedForUpdate(configuration.Tags, nameof(configuration.Tags));
            ThrowIfTagsFormatIsUnacceptable(configuration.Tags, nameof(configuration.Tags));

            if (!configuration.ManualHelmDeployment)
            {
                ValidateHelmInstall(configuration.HelmBinaryPath, nameof(configuration.HelmBinaryPath));
            }

            if (!configuration.Update)
            {
                if (configuration.BatchPrefix?.Length > 11 || (configuration.BatchPrefix?.Any(c => !char.IsAsciiLetterOrDigit(c)) ?? false))
                {
                    throw new ValidationException("BatchPrefix must not be longer than 11 chars and may contain only ASCII letters or digits", false);
                }
            }

            if (!string.IsNullOrWhiteSpace(configuration.BatchNodesSubnetId) && !string.IsNullOrWhiteSpace(configuration.BatchSubnetName))
            {
                throw new Exception("Invalid configuration options BatchNodesSubnetId and BatchSubnetName are mutually exclusive.");
            }
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

                await Task.Delay(System.TimeSpan.FromSeconds(10));
            }
        }

        public Task Execute(string message, Func<Task> func)
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

        public static async Task<string> DownloadTextFromStorageAccountAsync(IStorageAccount storageAccount, string containerName, string blobName, CancellationTokenSource cts)
        {
            var blobClient = await GetBlobClientAsync(storageAccount);
            var container = blobClient.GetBlobContainerClient(containerName);

            return (await container.GetBlobClient(blobName).DownloadContentAsync(cts.Token)).Value.Content.ToString();
        }

        private async Task UploadTextToStorageAccountAsync(IStorageAccount storageAccount, string containerName, string blobName, string content)
            => await UploadTextToStorageAccountAsync(storageAccount, containerName, blobName, content, cts.Token);

        public static async Task UploadTextToStorageAccountAsync(IStorageAccount storageAccount, string containerName, string blobName, string content, CancellationToken token)
        {
            var blobClient = await GetBlobClientAsync(storageAccount);
            var container = blobClient.GetBlobContainerClient(containerName);

            await container.CreateIfNotExistsAsync(cancellationToken: token);
            await container.GetBlobClient(blobName).UploadAsync(BinaryData.FromString(content), true, token);
        }

        public async Task<List<MountableContainer>> GetContainersToMount(string path)
        {
            if (string.IsNullOrWhiteSpace(path))
            {
                return null;
            }

            var containers = new HashSet<MountableContainer>();
            var exclusion = new HashSet<MountableContainer>();
            var wildCardAccounts = new List<string>();
            var contents = await File.ReadAllLinesAsync(path);

            foreach (var line in contents)
            {
                if (line.StartsWith("#"))
                {
                    continue;
                }

                if (line.StartsWith("-"))
                {
                    var parts = line.Trim('-').Split("/", StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length > 1)
                    {
                        var storageAccount = await GetExistingStorageAccountAsync(parts[0]);
                        exclusion.Add(new MountableContainer() { StorageAccount = parts[0], ContainerName = parts[1].Trim(), ResourceGroupName = storageAccount.ResourceGroupName });
                    }
                }

                if (line.StartsWith("/"))
                {
                    var parts = line.Split("/", StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length > 1)
                    {
                        if (string.Equals(parts[1].Trim(), "*"))
                        {
                            wildCardAccounts.Add(parts[0]);
                        }
                        else
                        {
                            var storageAccount = await GetExistingStorageAccountAsync(parts[0]);
                            containers.Add(new MountableContainer() { StorageAccount = parts[0], ContainerName = parts[1].Trim(), ResourceGroupName = storageAccount.ResourceGroupName });
                        }
                    }
                }

                if (line.StartsWith("http"))
                {
                    var blobUrl = new BlobUriBuilder(new Uri(line));
                    var blobHostStorageAccount = blobUrl.Host.Split(".").First();
                    containers.Add(new MountableContainer()
                    {
                        StorageAccount = blobHostStorageAccount,
                        ContainerName = blobUrl.BlobContainerName,
                        SasToken = blobUrl.Sas.ToString()
                    });
                }
            }

            foreach (var accountName in wildCardAccounts)
            {
                var storageAccount = await GetExistingStorageAccountAsync(accountName);
                var blobContainers = await storageAccount.Manager.BlobContainers.ListAsync(storageAccount.ResourceGroupName, storageAccount.Name);
                foreach (var page in blobContainers)
                {
                    foreach (var container in page.Value.ToList())
                    {
                        containers.Add(new MountableContainer() { StorageAccount = accountName, ContainerName = container.Name, ResourceGroupName = storageAccount.ResourceGroupName });
                    }
                }
            }

            containers.ExceptWith(exclusion);
            return containers.ToList();
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
