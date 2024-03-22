// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.ClientModel.Primitives;
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
using Azure.ResourceManager.ContainerService;
using Azure.ResourceManager.ContainerService.Models;
using Azure.ResourceManager.ManagedServiceIdentities;
using Azure.ResourceManager.Models;
using Azure.ResourceManager.Network;
using Azure.ResourceManager.Network.Models;
using Azure.ResourceManager.Resources;
using Azure.Security.KeyVault.Secrets;
using Azure.Storage;
using Azure.Storage.Blobs;
using Common;
using CommonUtilities;
using CommonUtilities.AzureCloud;
using k8s;
using Microsoft.Azure.Management.Batch;
using Microsoft.Azure.Management.Batch.Models;
using Microsoft.Azure.Management.Compute.Fluent;
using Microsoft.Azure.Management.ContainerRegistry.Fluent;
using Microsoft.Azure.Management.Fluent;
using Microsoft.Azure.Management.Graph.RBAC.Fluent;
using Microsoft.Azure.Management.Graph.RBAC.Fluent.Models;
using Microsoft.Azure.Management.KeyVault;
using Microsoft.Azure.Management.KeyVault.Fluent;
using Microsoft.Azure.Management.KeyVault.Models;
using Microsoft.Azure.Management.Msi.Fluent;
using Microsoft.Azure.Management.Network.Fluent;
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
using FlexibleServer = Microsoft.Azure.Management.PostgreSQL.FlexibleServers;
using FlexibleServerModel = Microsoft.Azure.Management.PostgreSQL.FlexibleServers.Models;
using IResource = Microsoft.Azure.Management.ResourceManager.Fluent.Core.IResource;
using KeyVaultManagementClient = Microsoft.Azure.Management.KeyVault.KeyVaultManagementClient;

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
        public const string TesInternalContainerName = "tes-internal";
        public const string CromwellConfigurationFileName = "cromwell-application.conf";
        public const string AllowedVmSizesFileName = "allowed-vm-sizes";
        public const string InputsContainerName = "inputs";
        public const string OutputsContainerName = "outputs";
        public const string LogsContainerName = "cromwell-workflow-logs";
        public const string ExecutionsContainerName = "cromwell-executions";
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

        private readonly Dictionary<string, List<string>> requiredResourceProviderFeatures = new()
        {
            { "Microsoft.Compute", new() { "EncryptionAtHost" } }
        };

        private Configuration configuration { get; set; }
        private ITokenProvider tokenProvider;
        private TokenCredentials tokenCredentials;
        private IAzure azureSubscriptionClient { get; set; }
        private Microsoft.Azure.Management.Fluent.Azure.IAuthenticated azureClient { get; set; }
        private IResourceManager resourceManagerClient { get; set; }
        private ArmClient armClient { get; set; }
        private AzureCredentials azureCredentials { get; set; }
        private FlexibleServer.IPostgreSQLManagementClient postgreSqlFlexManagementClient { get; set; }
        private IEnumerable<string> subscriptionIds { get; set; }
        private bool isResourceGroupCreated { get; set; }
        private KubernetesManager kubernetesManager { get; set; }
        internal static AzureCloudConfig azureCloudConfig { get; set; }

        public Deployer(Configuration configuration)
        {
            this.configuration = configuration;
        }

        public async Task<int> DeployAsync()
        {
            var mainTimer = Stopwatch.StartNew();

            try
            {
                ConsoleEx.WriteLine("Running...");

                await Execute($"Getting cloud configuration for {configuration.AzureCloudName}...", async () =>
                {
                    azureCloudConfig = await AzureCloudConfig.CreateAsync(configuration.AzureCloudName);
                });

                await Execute("Validating command line arguments...", () =>
                {
                    ValidateInitialCommandLineArgs();
                    return Task.CompletedTask;
                });

                await ValidateTokenProviderAsync();

                await Execute("Connecting to Azure Services...", async () =>
                {
                    tokenProvider = new RefreshableAzureServiceTokenProvider(azureCloudConfig.ResourceManagerUrl, null, azureCloudConfig.Authentication.LoginEndpointUrl);
                    tokenCredentials = new(tokenProvider);
                    azureCredentials = new(tokenCredentials, null, null, azureCloudConfig.AzureEnvironment);
                    azureClient = GetAzureClient(azureCredentials);
                    armClient = new ArmClient(new AzureCliCredential(), null, new ArmClientOptions { Environment = azureCloudConfig.ArmEnvironment });
                    azureSubscriptionClient = azureClient.WithSubscription(configuration.SubscriptionId);
                    subscriptionIds = await (await azureClient.Subscriptions.ListAsync(cancellationToken: cts.Token)).ToAsyncEnumerable().Select(s => s.SubscriptionId).ToListAsync(cts.Token);
                    resourceManagerClient = GetResourceManagerClient(azureCredentials);
                    postgreSqlFlexManagementClient = new FlexibleServer.PostgreSQLManagementClient(azureCredentials) { SubscriptionId = configuration.SubscriptionId, BaseUri = new Uri(azureCloudConfig.ResourceManagerUrl), LongRunningOperationRetryTimeout = 1200 };
                });

                await ValidateSubscriptionAndResourceGroupAsync(configuration);
                kubernetesManager = new KubernetesManager(configuration, azureCredentials, azureCloudConfig, cts.Token);

                IResourceGroup resourceGroup = null;
                ContainerServiceManagedClusterResource aksCluster = null;
                BatchAccount batchAccount = null;
                IGenericResource logAnalyticsWorkspace = null;
                IGenericResource appInsights = null;
                FlexibleServerModel.Server postgreSqlFlexServer = null;
                IStorageAccount storageAccount = null;
                var keyVaultUri = string.Empty;
                IIdentity managedIdentity = null;
                IIdentity aksNodepoolIdentity = null;
                IPrivateDnsZone postgreSqlDnsZone = null;
                IKubernetes kubernetesClient = null;

                var containersToMount = await GetContainersToMount(configuration.ContainersToMountPath);

                try
                {
                    var targetVersion = Utility.DelimitedTextToDictionary(Utility.GetFileContent("scripts", "env-00-coa-version.txt")).GetValueOrDefault("CromwellOnAzureVersion");

                    if (configuration.Update)
                    {
                        resourceGroup = await azureSubscriptionClient.ResourceGroups.GetByNameAsync(configuration.ResourceGroupName, cts.Token);
                        configuration.RegionName = resourceGroup.RegionName;

                        ConsoleEx.WriteLine($"Upgrading Cromwell on Azure instance in resource group '{resourceGroup.Name}' to version {targetVersion}...");

                        if (string.IsNullOrEmpty(configuration.StorageAccountName))
                        {
                            var storageAccounts = await (await azureSubscriptionClient.StorageAccounts.ListByResourceGroupAsync(configuration.ResourceGroupName, cancellationToken: cts.Token))
                                .ToAsyncEnumerable().ToListAsync(cts.Token);

                            storageAccount = storageAccounts.Count switch
                            {
                                0 => throw new ValidationException($"Update was requested but resource group {configuration.ResourceGroupName} does not contain any storage accounts.", displayExample: false),
                                1 => storageAccounts.Single(),
                                _ => throw new ValidationException($"Resource group {configuration.ResourceGroupName} contains multiple storage accounts. {nameof(configuration.StorageAccountName)} must be provided.", displayExample: false),
                            };
                        }
                        else
                        {
                            storageAccount = await GetExistingStorageAccountAsync(configuration.StorageAccountName)
                                ?? throw new ValidationException($"Storage account {configuration.StorageAccountName}, does not exist in region {configuration.RegionName} or is not accessible to the current user.", displayExample: false);
                        }

                        var aksValues = await kubernetesManager.GetAKSSettingsAsync(storageAccount);

                        if (!aksValues.Any())
                        {
                            throw new ValidationException("Upgrading pre-4.0 versions of CromwellOnAzure is not supported. Please see https://github.com/microsoft/CromwellOnAzure/wiki/4.0-Migration-Guide.", displayExample: false);
                        }

                        if (!aksValues.TryGetValue("BatchAccountName", out var batchAccountName))
                        {
                            throw new ValidationException($"Could not retrieve the Batch account name", displayExample: false);
                        }

                        batchAccount = await GetExistingBatchAccountAsync(batchAccountName)
                            ?? throw new ValidationException($"Batch account {batchAccountName}, referenced by the stored configuration, does not exist in region {configuration.RegionName} or is not accessible to the current user.", displayExample: false);

                        configuration.BatchAccountName = batchAccountName;

                        if (!aksValues.TryGetValue("PostgreSqlServerName", out var postgreSqlServerName))
                        {
                            throw new ValidationException($"Could not retrieve the PostgreSqlServer account name from stored configuration in {storageAccount.Name}.", displayExample: false);
                        }

                        configuration.PostgreSqlServerName = postgreSqlServerName;

                        if (string.IsNullOrEmpty(configuration.AksClusterName))
                        {
                            var client = armClient.GetResourceGroupResource(new(resourceGroup.Id));
                            var aksClusters = await client.GetContainerServiceManagedClusters().GetAllAsync(cts.Token).ToListAsync(cts.Token);

                            aksCluster = aksClusters.Count switch
                            {
                                0 => throw new ValidationException($"Update was requested but resource group {configuration.ResourceGroupName} does not contain any AKS clusters.", displayExample: false),
                                1 => (await aksClusters.Single().GetAsync()).Value,
                                _ => throw new ValidationException($"Resource group {configuration.ResourceGroupName} contains multiple AKS clusters. {nameof(configuration.AksClusterName)} must be provided.", displayExample: false),
                            };

                            configuration.AksClusterName = aksCluster.Data.Name;
                        }
                        else
                        {
                            aksCluster = await ValidateAndGetExistingAKSClusterAsync()
                                ?? throw new ValidationException($"AKS cluster {configuration.AksClusterName} does not exist in region {configuration.RegionName} or is not accessible to the current user.", displayExample: false);
                        }

                        if (aksValues.TryGetValue("CrossSubscriptionAKSDeployment", out var crossSubscriptionAKSDeployment))
                        {
                            configuration.CrossSubscriptionAKSDeployment = bool.TryParse(crossSubscriptionAKSDeployment, out var parsed) ? parsed : null;
                        }

                        if (aksValues.TryGetValue("KeyVaultName", out var keyVaultName))
                        {
                            var keyVault = await GetKeyVaultAsync(keyVaultName);
                            keyVaultUri = keyVault.Properties.VaultUri;
                        }

                        if (!aksValues.TryGetValue("ManagedIdentityClientId", out var managedIdentityClientId))
                        {
                            throw new ValidationException($"Could not retrieve ManagedIdentityClientId.", displayExample: false);
                        }

                        managedIdentity = await (await azureSubscriptionClient.Identities.ListByResourceGroupAsync(configuration.ResourceGroupName))
                                .ToAsyncEnumerable().FirstOrDefaultAsync(id => id.ClientId == managedIdentityClientId, cts.Token)
                            ?? throw new ValidationException($"Managed Identity {managedIdentityClientId} does not exist in region {configuration.RegionName} or is not accessible to the current user.", displayExample: false);

                        // Override any configuration that is used by the update.
                        var versionString = aksValues["CromwellOnAzureVersion"];
                        var installedVersion = !string.IsNullOrEmpty(versionString) && Version.TryParse(versionString, out var version) ? version : null;

                        if (installedVersion is null || installedVersion < new Version(4, 0))
                        {
                            throw new ValidationException("Upgrading pre-4.0 versions of CromwellOnAzure is not supported. Please see https://github.com/microsoft/CromwellOnAzure/wiki/4.0-Migration-Guide.");
                        }

                        var settings = ConfigureSettings(managedIdentity.ClientId, aksValues, installedVersion);
                        var waitForRoleAssignmentPropagation = false;

                        if (installedVersion is null || installedVersion < new Version(4, 4))
                        {
                            // Ensure all storage containers are created.
                            await CreateDefaultStorageContainersAsync(storageAccount);

                            if (string.IsNullOrWhiteSpace(settings["BatchNodesSubnetId"]))
                            {
                                settings["BatchNodesSubnetId"] = await UpdateVnetWithBatchSubnet(resourceGroup.Inner.Id);
                            }
                        }

                        if (installedVersion is null || installedVersion < new Version(4, 7))
                        {
                            await AssignMIAsNetworkContributorToResourceAsync(managedIdentity, resourceGroup);
                            await AssignMIAsDataOwnerToStorageAccountAsync(managedIdentity, storageAccount);

                            await Execute($"Moving {AllowedVmSizesFileName} file to new location: {TesInternalContainerName}/{ConfigurationContainerName}/{AllowedVmSizesFileName}",
                                () => MoveAllowedVmSizesFileAsync(storageAccount));

                            waitForRoleAssignmentPropagation = true;
                        }

                        if (installedVersion is null || installedVersion < new Version(5, 0, 1))
                        {
                            if (!settings.ContainsKey("ExecutionsContainerName"))
                            {
                                settings["ExecutionsContainerName"] = ExecutionsContainerName;
                            }
                        }

                        if (installedVersion is null || installedVersion < new Version(5, 2, 2))
                        {
                            await EnableWorkloadIdentity(aksCluster, managedIdentity, resourceGroup);
                            await kubernetesManager.RemovePodAadChart();
                        }

                        if (installedVersion < new Version(5, 3, 0))
                        {
                            settings["AzureCloudName"] = configuration.AzureCloudName;
                        }

                        if (waitForRoleAssignmentPropagation)
                        {
                            await Execute("Waiting 5 minutes for role assignment propagation...",
                                () => Task.Delay(System.TimeSpan.FromMinutes(5), cts.Token));
                        }

                        await kubernetesManager.UpgradeValuesYamlAsync(storageAccount, settings, containersToMount, installedVersion);
                        kubernetesClient = await PerformHelmDeploymentAsync(aksCluster);

                        await WriteNonPersonalizedFilesToStorageAccountAsync(storageAccount);
                    }

                    if (!configuration.Update)
                    {
                        if (string.IsNullOrWhiteSpace(configuration.BatchPrefix))
                        {
                            var blob = new byte[5];
                            RandomNumberGenerator.Fill(blob);
                            configuration.BatchPrefix = blob.ConvertToBase32().TrimEnd('=');
                        }

                        ValidateRegionName(configuration.RegionName);
                        ValidateMainIdentifierPrefix(configuration.MainIdentifierPrefix);
                        storageAccount = await ValidateAndGetExistingStorageAccountAsync();
                        batchAccount = await ValidateAndGetExistingBatchAccountAsync();
                        postgreSqlFlexServer = await ValidateAndGetExistingPostgresqlServer();
                        var keyVault = await ValidateAndGetExistingKeyVault();

                        if (aksCluster is null && !configuration.ManualHelmDeployment)
                        {
                            //await ValidateVmAsync();
                        }

                        ConsoleEx.WriteLine($"Deploying Cromwell on Azure version {targetVersion}...");

                        if (string.IsNullOrWhiteSpace(configuration.PostgreSqlServerNameSuffix))
                        {
                            configuration.PostgreSqlServerNameSuffix = $".{azureCloudConfig.Suffixes.PostgresqlServerEndpointSuffix}";
                        }

                        if (string.IsNullOrWhiteSpace(configuration.PostgreSqlServerName))
                        {
                            configuration.PostgreSqlServerName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}-", 15);
                        }

                        configuration.PostgreSqlAdministratorPassword = PasswordGenerator.GeneratePassword();
                        configuration.PostgreSqlCromwellUserPassword = PasswordGenerator.GeneratePassword();
                        configuration.PostgreSqlTesUserPassword = PasswordGenerator.GeneratePassword();

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
                        await RegisterResourceProviderFeaturesAsync();

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
                            resourceGroup = await azureSubscriptionClient.ResourceGroups.GetByNameAsync(configuration.ResourceGroupName, cts.Token);
                        }

                        if (!string.IsNullOrWhiteSpace(configuration.IdentityResourceId))
                        {
                            ConsoleEx.WriteLine($"Using existing user-assigned managed identity: {configuration.IdentityResourceId}");
                            managedIdentity = await GetUserManagedIdentityAsync(configuration.IdentityResourceId);
                        }
                        else
                        {
                            managedIdentity = await CreateUserManagedIdentityAsync(resourceGroup);
                        }

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

                        await Task.WhenAll(new Task[]
                        {
                            Task.Run(async () =>
                            {
                                if (vnetAndSubnet is null)
                                {
                                    configuration.VnetName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}-", 15);
                                    configuration.PostgreSqlSubnetName = string.IsNullOrEmpty(configuration.PostgreSqlSubnetName) ? configuration.DefaultPostgreSqlSubnetName : configuration.PostgreSqlSubnetName;
                                    configuration.BatchSubnetName = string.IsNullOrEmpty(configuration.BatchSubnetName) ? configuration.DefaultBatchSubnetName : configuration.BatchSubnetName;
                                    configuration.VmSubnetName = string.IsNullOrEmpty(configuration.VmSubnetName) ? configuration.DefaultVmSubnetName : configuration.VmSubnetName;
                                    vnetAndSubnet = await CreateVnetAndSubnetsAsync(resourceGroup);

                                    if (string.IsNullOrEmpty(this.configuration.BatchNodesSubnetId))
                                    {
                                        this.configuration.BatchNodesSubnetId = vnetAndSubnet.Value.batchSubnet.Inner.Id;
                                    }
                                }
                            }),
                            Task.Run(async () =>
                            {
                                logAnalyticsWorkspace = await GetLogAnalyticsWorkspaceAsync(configuration.LogAnalyticsArmId);

                                if (logAnalyticsWorkspace == null)
                                {
                                    var workspaceName = SdkContext.RandomResourceName(configuration.MainIdentifierPrefix, 15);
                                    logAnalyticsWorkspace = await CreateLogAnalyticsWorkspaceAsync(workspaceName);
                                    configuration.LogAnalyticsArmId = logAnalyticsWorkspace.Id;
                                }
                            }),
                            Task.Run(async () =>
                            {
                                storageAccount ??= await CreateStorageAccountAsync();
                                await CreateDefaultStorageContainersAsync(storageAccount);
                                await WriteNonPersonalizedFilesToStorageAccountAsync(storageAccount);
                                await WritePersonalizedFilesToStorageAccountAsync(storageAccount);

                                await AssignVmAsContributorToStorageAccountAsync(managedIdentity, storageAccount);
                                await AssignMIAsDataOwnerToStorageAccountAsync(managedIdentity, storageAccount, true);
                                await AssignManagedIdOperatorToResourceAsync(managedIdentity, resourceGroup);
                                await AssignMIAsNetworkContributorToResourceAsync(managedIdentity, resourceGroup, true);

                                if (aksNodepoolIdentity is not null)
                                {
                                    await AssignVmAsContributorToStorageAccountAsync(aksNodepoolIdentity, storageAccount);
                                    await AssignMIAsDataOwnerToStorageAccountAsync(aksNodepoolIdentity, storageAccount, true);
                                    await AssignManagedIdOperatorToResourceAsync(aksNodepoolIdentity, resourceGroup);
                                }
                            }),
                        });

                        if (configuration.CrossSubscriptionAKSDeployment.GetValueOrDefault())
                        {
                            await Task.Run(async () =>
                            {
                                keyVault ??= await CreateKeyVaultAsync(configuration.KeyVaultName, managedIdentity, vnetAndSubnet.Value.vmSubnet);
                                keyVaultUri = keyVault.Properties.VaultUri;
                                var keys = await storageAccount.GetKeysAsync(cts.Token);
                                await SetStorageKeySecret(keyVaultUri, StorageAccountKeySecretName, keys[0].Value);
                            });
                        }

                        if (postgreSqlFlexServer is null)
                        {
                            postgreSqlDnsZone = await GetExistingPrivateDnsZoneAsync(vnetAndSubnet.Value.virtualNetwork, $"privatelink.{azureCloudConfig.Suffixes.PostgresqlServerEndpointSuffix}");

                            postgreSqlDnsZone ??= await CreatePrivateDnsZoneAsync(vnetAndSubnet.Value.virtualNetwork, $"privatelink.{azureCloudConfig.Suffixes.PostgresqlServerEndpointSuffix}", "PostgreSQL Server");
                        }

                        await Task.WhenAll(new Task[]
                        {
                            Task.Run(async () =>
                            {
                                if (aksCluster is null && !configuration.ManualHelmDeployment)
                                {
                                    aksCluster = await ProvisionManagedClusterAsync(resourceGroup, managedIdentity, logAnalyticsWorkspace, vnetAndSubnet?.virtualNetwork, vnetAndSubnet?.vmSubnet.Name, configuration.PrivateNetworking.GetValueOrDefault(), configuration.AksNodeResourceGroupName);
                                    await EnableWorkloadIdentity(aksCluster, managedIdentity, resourceGroup);
                                }
                            }),
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
                                postgreSqlFlexServer ??= await CreatePostgreSqlServerAndDatabaseAsync(postgreSqlFlexManagementClient, vnetAndSubnet.Value.postgreSqlSubnet, postgreSqlDnsZone);
                            })
                        });

                        var clientId = managedIdentity.ClientId;
                        var settings = ConfigureSettings(clientId);

                        await kubernetesManager.UpdateHelmValuesAsync(storageAccount, keyVaultUri, resourceGroup.Name, settings, managedIdentity, containersToMount);
                        kubernetesClient = await PerformHelmDeploymentAsync(aksCluster,
                            new[]
                            {
                                "Run the following postgresql command to setup the database.",
                                "\tPostgreSQL command: " + GetPostgreSQLCreateCromwellUserCommand(configuration.PostgreSqlCromwellDatabaseName, GetCreateCromwellUserString()),
                                "\tPostgreSQL command: " + GetPostgreSQLCreateCromwellUserCommand(configuration.PostgreSqlTesDatabaseName, GetCreateTesUserString()),
                            },
                            async kubernetesClient =>
                            {
                                await kubernetesManager.DeployCoADependenciesAsync();

                                // Deploy an ubuntu pod to run PSQL commands, then delete it
                                const string deploymentNamespace = "default";
                                var (deploymentName, ubuntuDeployment) = KubernetesManager.GetUbuntuDeploymentTemplate();
                                await kubernetesClient.AppsV1.CreateNamespacedDeploymentAsync(ubuntuDeployment, deploymentNamespace, cancellationToken: cts.Token);
                                await ExecuteQueriesOnAzurePostgreSQLDbFromK8(kubernetesClient, deploymentName, deploymentNamespace);
                                await kubernetesClient.AppsV1.DeleteNamespacedDeploymentAsync(deploymentName, deploymentNamespace, cancellationToken: cts.Token);
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
                var isBatchPoolQuotaAvailable = batchAccount.PoolQuota > 0;
                var isBatchJobQuotaAvailable = batchAccount.ActiveJobAndJobScheduleQuota > 0;
                var insufficientQuotas = new List<string>();
                int exitCode;

                if (!isBatchQuotaAvailable) insufficientQuotas.Add("core");
                if (!isBatchPoolQuotaAvailable) insufficientQuotas.Add("pool");
                if (!isBatchJobQuotaAvailable) insufficientQuotas.Add("job");

                if (insufficientQuotas.Any())
                {
                    if (!configuration.SkipTestWorkflow)
                    {
                        ConsoleEx.WriteLine("Could not run the test workflow.", ConsoleColor.Yellow);
                    }

                    var quotaMessage = string.Join(" and ", insufficientQuotas);
                    ConsoleEx.WriteLine($"Deployment was successful, but Batch account {configuration.BatchAccountName} does not have sufficient {quotaMessage} quota to run workflows.", ConsoleColor.Yellow);
                    ConsoleEx.WriteLine($"Request Batch {quotaMessage} quota: https://docs.microsoft.com/en-us/azure/batch/batch-quota-limit", ConsoleColor.Yellow);
                    ConsoleEx.WriteLine("After receiving the quota, read the docs to run a test workflow and confirm successful deployment.", ConsoleColor.Yellow);

                    exitCode = 2;
                }
                else
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

        private async Task MoveAllowedVmSizesFileAsync(IStorageAccount storageAccount)
        {
            var allowedVmSizesFileContent = Utility.GetFileContent("scripts", AllowedVmSizesFileName);
            var existingAllowedVmSizesBlobClient = (await GetBlobClientAsync(storageAccount, cts.Token))
                .GetBlobContainerClient(ConfigurationContainerName)
                .GetBlobClient(AllowedVmSizesFileName);

            var isExistingFile = false;

            // Get existing content if it exists
            if (await existingAllowedVmSizesBlobClient.ExistsAsync(cts.Token))
            {
                isExistingFile = true;

                var existingAllowedVmSizesContent = (await existingAllowedVmSizesBlobClient.DownloadContentAsync(cts.Token)).Value.Content.ToString();

                if (!string.IsNullOrWhiteSpace(existingAllowedVmSizesContent))
                {
                    // Use existing content
                    allowedVmSizesFileContent = existingAllowedVmSizesContent;
                }
            }

            // Upload to new location
            await UploadTextToStorageAccountAsync(storageAccount, TesInternalContainerName, $"{ConfigurationContainerName}/{AllowedVmSizesFileName}", allowedVmSizesFileContent);

            if (isExistingFile)
            {
                // Delete old file to prevent user confusion about source of truth
                await existingAllowedVmSizesBlobClient.DeleteAsync(cancellationToken: cts.Token);
            }
        }

        private async Task<IKubernetes> PerformHelmDeploymentAsync(ContainerServiceManagedClusterResource aksCluster, IEnumerable<string> manualPrecommands = default, Func<IKubernetes, Task> asyncTask = default)
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
                var kubernetesClient = await kubernetesManager.GetKubernetesClientAsync(aksCluster);
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

        private async Task<ContainerServiceManagedClusterResource> ValidateAndGetExistingAKSClusterAsync()
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
            var regex = new Regex(@"\s+");
            return await subscriptionIds.ToAsyncEnumerable().SelectAwait(async s =>
            {
                try
                {
                    var client = new FlexibleServer.PostgreSQLManagementClient(tokenCredentials) { SubscriptionId = s };
                    return (await client.Servers.ListAsync(cts.Token)).ToAsyncEnumerable(client.Servers.ListNextAsync);
                }
                catch (Exception e)
                {
                    ConsoleEx.WriteLine(e.Message);
                    return null;
                }
            })
            .Where(x => x != null)
            .SelectMany(a => a)
            .SingleOrDefaultAsync(a =>
                    a.Name.Equals(serverName, StringComparison.OrdinalIgnoreCase) &&
                    regex.Replace(a.Location, string.Empty).Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase),
                cts.Token);
        }

        private async Task<ContainerServiceManagedClusterResource> GetExistingAKSClusterAsync(string aksClusterName)
        {
            return await subscriptionIds.ToAsyncEnumerable().Select(s =>
            {
                try
                {
                    var client = armClient.GetSubscriptionResource(new(s));
                    return client.GetContainerServiceManagedClustersAsync(cts.Token);
                }
                catch (Exception e)
                {
                    ConsoleEx.WriteLine(e.Message);
                    return null;
                }
            })
            .Where(a => a is not null)
            .SelectMany(a => a)
            .SelectAwaitWithCancellation(async (a, ct) => (await a.GetAsync(ct)).Value)
            .SingleOrDefaultAsync(a =>
                    a.Data.Name.Equals(aksClusterName, StringComparison.OrdinalIgnoreCase) &&
                    a.Data.Location.Name.Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase),
                cts.Token);
        }

        private async Task<ContainerServiceManagedClusterResource> ProvisionManagedClusterAsync(IResource resourceGroupObject, IIdentity managedIdentity, IGenericResource logAnalyticsWorkspace, INetwork virtualNetwork, string subnetName, bool privateNetworking, string nodeResourceGroupName)
        {
            var uami = (await armClient.GetUserAssignedIdentityResource(new(managedIdentity.Id)).GetAsync(cts.Token)).Value;
            var resourceGroup = armClient.GetResourceGroupResource(new(resourceGroupObject.Id));
            var nodePoolName = "nodepool1";
            ContainerServiceManagedClusterData cluster = new(new(configuration.RegionName))
            {
                ClusterIdentity = new() { ResourceIdentityType = ManagedServiceIdentityType.UserAssigned },
                DnsPrefix = configuration.AksClusterName,
                NetworkProfile = new()
                {
                    NetworkPlugin = ContainerServiceNetworkPlugin.Azure,
                    ServiceCidr = configuration.KubernetesServiceCidr,
                    DnsServiceIP = configuration.KubernetesDnsServiceIP,
                    DockerBridgeCidr = configuration.KubernetesDockerBridgeCidr,
                    NetworkPolicy = ContainerServiceNetworkPolicy.Azure
                },
                NodeResourceGroup = nodeResourceGroupName
            };

            ManagedClusterAddonProfile clusterAddonProfile = new(isEnabled: true);
            clusterAddonProfile.Config.Add("logAnalyticsWorkspaceResourceID", logAnalyticsWorkspace.Id);
            cluster.AddonProfiles.Add("omsagent", clusterAddonProfile);
            cluster.ClusterIdentity.UserAssignedIdentities.Add(new(uami.Id, PopulateUserAssignedIdentity(uami.Data)));
            cluster.IdentityProfile.Add("kubeletidentity", new() { ResourceId = uami.Id, ClientId = uami.Data.ClientId, ObjectId = uami.Data.PrincipalId });

            if (!string.IsNullOrWhiteSpace(configuration.AadGroupIds))
            {
                cluster.EnableRbac = true;
                cluster.AadProfile = new()
                {
                    IsAzureRbacEnabled = false,
                    IsManagedAadEnabled = true
                };

                configuration.AadGroupIds.Split(",", StringSplitOptions.RemoveEmptyEntries).Select(Guid.Parse).ForEach(cluster.AadProfile.AdminGroupObjectIds.Add);
            }

            cluster.AgentPoolProfiles.Add(new(nodePoolName)
            {
                Count = configuration.AksPoolSize,
                VmSize = configuration.VmSize,
                OSDiskSizeInGB = 128,
                OSDiskType = ContainerServiceOSDiskType.Managed,
                EnableEncryptionAtHost = true,
                AgentPoolType = AgentPoolType.VirtualMachineScaleSets,
                EnableAutoScaling = false,
                EnableNodePublicIP = false,
                OSType = ContainerServiceOSType.Linux,
                OSSku = ContainerServiceOSSku.AzureLinux,
                Mode = AgentPoolMode.System,
                VnetSubnetId = new(virtualNetwork.Subnets[subnetName].Inner.Id),
            });

            if (privateNetworking)
            {
                cluster.ApiServerAccessProfile = new()
                {
                    EnablePrivateCluster = true,
                    EnablePrivateClusterPublicFqdn = true
                };

                cluster.PublicNetworkAccess = ContainerServicePublicNetworkAccess.Disabled;
            }

            return await Execute(
                $"Creating AKS Cluster: {configuration.AksClusterName}...",
                async () => (await resourceGroup.GetContainerServiceManagedClusters().CreateOrUpdateAsync(Azure.WaitUntil.Completed, configuration.AksClusterName, cluster, cts.Token)).Value);

            static UserAssignedIdentity PopulateUserAssignedIdentity(UserAssignedIdentityData data)
            {
                UserAssignedIdentityJson json = new(data.PrincipalId.Value, data.ClientId.Value);
                System.Text.Json.Utf8JsonReader reader = new(System.Text.Encoding.UTF8.GetBytes(System.Text.Json.JsonSerializer.Serialize(json, jsonSerializerWebOptions)));
                return ((IJsonModel<UserAssignedIdentity>)new UserAssignedIdentity()).Create(ref reader, ModelReaderWriterOptions.Json);
            }
        }

        private static readonly System.Text.Json.JsonSerializerOptions jsonSerializerWebOptions = new(System.Text.Json.JsonSerializerDefaults.Web);
        private record struct UserAssignedIdentityJson(Guid PrincipalId, Guid ClientId);

        private async Task EnableWorkloadIdentity(ContainerServiceManagedClusterResource aksCluster, IIdentity managedIdentity, IResourceGroup resourceGroup)
        {
            var armCluster = aksCluster.HasData ? aksCluster : (await aksCluster.GetAsync(cancellationToken: cts.Token)).Value;
            armCluster.Data.SecurityProfile.IsWorkloadIdentityEnabled = true;
            armCluster.Data.OidcIssuerProfile.IsEnabled = true;
            var coaRg = armClient.GetResourceGroupResource(new ResourceIdentifier(resourceGroup.Id));
            var aksClusterCollection = coaRg.GetContainerServiceManagedClusters();
            var cluster = await aksClusterCollection.CreateOrUpdateAsync(Azure.WaitUntil.Completed, armCluster.Data.Name, armCluster.Data, cts.Token);
            var aksOidcIssuer = cluster.Value.Data.OidcIssuerProfile.IssuerUriInfo;
            var uami = armClient.GetUserAssignedIdentityResource(new ResourceIdentifier(managedIdentity.Id));

            var federatedCredentialsCollection = uami.GetFederatedIdentityCredentials();
            var data = new FederatedIdentityCredentialData()
            {
                IssuerUri = new Uri(aksOidcIssuer),
                Subject = $"system:serviceaccount:{configuration.AksCoANamespace}:{managedIdentity.Name}-sa"
            };
            data.Audiences.Add("api://AzureADTokenExchange");

            await federatedCredentialsCollection.CreateOrUpdateAsync(Azure.WaitUntil.Completed, "coaFederatedIdentity", data, cts.Token);
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
            UpdateSetting(settings, defaults, "DisableBatchNodesPublicIpAddress", configuration.DisableBatchNodesPublicIpAddress, b => b.GetValueOrDefault().ToString(), configuration.DisableBatchNodesPublicIpAddress.GetValueOrDefault().ToString());

            if (installedVersion is null)
            {
                UpdateSetting(settings, defaults, "BatchPrefix", configuration.BatchPrefix, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "DefaultStorageAccountName", configuration.StorageAccountName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "ExecutionsContainerName", ExecutionsContainerName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "BatchAccountName", configuration.BatchAccountName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "ApplicationInsightsAccountName", configuration.ApplicationInsightsAccountName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "AzureCloudName", configuration.AzureCloudName, ignoreDefaults: true);
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

            // Check if the installed image is not customized and matches a version tag
            IsInstalledNotCustomized ??= new(tag =>
            {
                // Attempt to parse the tag as a version (ignoring any decorations)
                return Version.TryParse(tag, out var version) &&
                       // Check if the parsed version matches the installed version
                       version.Equals(installedVersion);
            });

            try
            {
                // Determine if the installed image is from our official repository
                result = installed.StartsWith(defaultPath + ":")
                            // Check if the installed tag has not been customized
                            && IsInstalledNotCustomized(installedTag)
                            // If not customized, consider it as not requiring an upgrade
                            ? false
                            // If customized, preserve the configured image without upgrading
                            : null;
            }
            catch (ArgumentException)
            {
                // In case of an argument exception, default to preserving the image
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
                                resourceManagerClient.Providers.RegisterAsync(rp, cts.Token))
                        );

                        // RP registration takes a few minutes; poll until done registering

                        while (!cts.IsCancellationRequested)
                        {
                            unregisteredResourceProviders = await GetRequiredResourceProvidersNotRegisteredAsync();

                            if (unregisteredResourceProviders.Count == 0)
                            {
                                break;
                            }

                            await Task.Delay(System.TimeSpan.FromSeconds(15), cts.Token);
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
            var cloudResourceProviders = (await resourceManagerClient.Providers.ListAsync(cancellationToken: cts.Token)).ToAsyncEnumerable();

            var notRegisteredResourceProviders = await requiredResourceProviders.ToAsyncEnumerable()
                .Intersect(cloudResourceProviders
                    .Where(rp => !rp.RegistrationState.Equals("Registered", StringComparison.OrdinalIgnoreCase))
                    .Select(rp => rp.Namespace), StringComparer.OrdinalIgnoreCase)
                .ToListAsync(cts.Token);

            return notRegisteredResourceProviders;
        }

        private async Task RegisterResourceProviderFeaturesAsync()
        {
            var unregisteredFeatures = new List<FeatureResource>();
            try
            {
                await Execute(
                    $"Registering resource provider features...",
                    async () =>
                    {
                        var subscription = armClient.GetSubscriptionResource(new($"/subscriptions/{configuration.SubscriptionId}"));

                        foreach (var rpName in requiredResourceProviderFeatures.Keys)
                        {
                            var rp = await subscription.GetResourceProviderAsync(rpName, cancellationToken: cts.Token);

                            foreach (var featureName in requiredResourceProviderFeatures[rpName])
                            {
                                var feature = await rp.Value.GetFeatureAsync(featureName, cts.Token);

                                if (!string.Equals(feature.Value.Data.FeatureState, "Registered", StringComparison.OrdinalIgnoreCase))
                                {
                                    unregisteredFeatures.Add(feature);
                                    _ = await feature.Value.RegisterAsync(cts.Token);
                                }
                            }
                        }

                        while (!cts.IsCancellationRequested)
                        {
                            if (unregisteredFeatures.Count == 0)
                            {
                                break;
                            }

                            await Task.Delay(System.TimeSpan.FromSeconds(30), cts.Token);
                            var finished = new List<FeatureResource>();

                            foreach (var feature in unregisteredFeatures)
                            {
                                var update = await feature.GetAsync(cts.Token);

                                if (string.Equals(update.Value.Data.FeatureState, "Registered", StringComparison.OrdinalIgnoreCase))
                                {
                                    finished.Add(feature);
                                }
                            }
                            unregisteredFeatures.RemoveAll(x => finished.Contains(x));
                        }
                    });
            }
            catch (Microsoft.Rest.Azure.CloudException ex) when (ex.ToCloudErrorType() == CloudErrorType.AuthorizationFailed)
            {
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine("Unable to programatically register the required features.", ConsoleColor.Red);
                ConsoleEx.WriteLine("This can happen if you don't have the Owner or Contributor role assignment for the subscription.", ConsoleColor.Red);
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine("Please contact the Owner or Contributor of your Azure subscription, and have them:", ConsoleColor.Yellow);
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine("1. For each of the following, execute 'az feature register --namespace {RESOURCE_PROVIDER_NAME} --name {FEATURE_NAME}'", ConsoleColor.Yellow);
                ConsoleEx.WriteLine();
                unregisteredFeatures.ForEach(f => ConsoleEx.WriteLine($"- {f.Data.Name}", ConsoleColor.Yellow));
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine("After completion, please re-attempt deployment.");

                Environment.Exit(1);
            }
        }

        private Task AssignManagedIdOperatorToResourceAsync(IIdentity managedIdentity, IResource resource)
        {
            // https://docs.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#managed-identity-operator
            var roleDefinitionId = $"/subscriptions/{configuration.SubscriptionId}/providers/Microsoft.Authorization/roleDefinitions/f1a07417-d97a-45cb-824c-7a7467783830";
            return Execute(
                $"Assigning Managed ID Operator role for the managed id to resource group scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    ct => azureSubscriptionClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithRoleDefinition(roleDefinitionId)
                        .WithResourceScope(resource)
                        .CreateAsync(ct),
                    cts.Token));
        }

        private Task AssignMIAsNetworkContributorToResourceAsync(IIdentity managedIdentity, IResource resource, bool cancelOnException = true)
        {
            // https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#network-contributor
            var roleDefinitionId = $"/subscriptions/{configuration.SubscriptionId}/providers/Microsoft.Authorization/roleDefinitions/4d97b98b-1d4f-4787-a291-c67834d212e7";
            return Execute(
                $"Assigning Network Contributor role for the managed id to resource group scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    ct => azureSubscriptionClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithRoleDefinition(roleDefinitionId)
                        .WithResourceScope(resource)
                        .CreateAsync(ct),
                    cts.Token),
                cancelOnException: cancelOnException);
        }

        private Task AssignMIAsDataOwnerToStorageAccountAsync(IIdentity managedIdentity, IResource storageAccount, bool cancelOnException = true)
        {
            //https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#storage-blob-data-owner
            var roleDefinitionId = $"/subscriptions/{configuration.SubscriptionId}/providers/Microsoft.Authorization/roleDefinitions/b7e6dc6d-f1e8-4753-8033-0f276bb0955b";

            return Execute(
                $"Assigning Storage Blob Data Owner role for user-managed identity to Storage Account resource scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    ct => azureSubscriptionClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithRoleDefinition(roleDefinitionId)
                        .WithResourceScope(storageAccount)
                        .CreateAsync(ct),
                    cts.Token),
                cancelOnException: cancelOnException);
        }

        private Task AssignVmAsContributorToStorageAccountAsync(IIdentity managedIdentity, IResource storageAccount)
            => Execute(
                $"Assigning {BuiltInRole.Contributor} role for user-managed identity to Storage Account resource scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    ct => azureSubscriptionClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithBuiltInRole(BuiltInRole.Contributor)
                        .WithResourceScope(storageAccount)
                        .CreateAsync(ct),
                    cts.Token));

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

        private Task<IStorageAccount> GetExistingStorageAccountAsync(string storageAccountName)
            => GetExistingStorageAccountAsync(storageAccountName, azureClient, subscriptionIds, configuration);

        public async Task<IStorageAccount> GetExistingStorageAccountAsync(string storageAccountName, Microsoft.Azure.Management.Fluent.Azure.IAuthenticated azureClient, IEnumerable<string> subscriptionIds, Configuration configuration)
            => await subscriptionIds.ToAsyncEnumerable().SelectAwait(async s =>
            {
                try
                {
                    return (await azureClient.WithSubscription(s).StorageAccounts.ListAsync(cancellationToken: cts.Token)).ToAsyncEnumerable();
                }
                catch (Exception)
                {
                    // Ignore exception if a user does not have the required role to list storage accounts in a subscription
                    return null;
                }
            })
            .Where(a => a is not null)
            .SelectMany(a => a)
            .SingleOrDefaultAsync(a =>
                    a.Name.Equals(storageAccountName, StringComparison.OrdinalIgnoreCase) &&
                    a.RegionName.Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase),
                cts.Token);

        private async Task<BatchAccount> GetExistingBatchAccountAsync(string batchAccountName)
            => await subscriptionIds.ToAsyncEnumerable().SelectAwait(async s =>
            {
                try
                {
                    var client = new BatchManagementClient(tokenCredentials) { SubscriptionId = s, BaseUri = new Uri(azureCloudConfig.ResourceManagerUrl) };
                    return (await client.BatchAccount.ListAsync(cts.Token))
                        .ToAsyncEnumerable(client.BatchAccount.ListNextAsync);
                }
                catch (Exception e)
                {
                    ConsoleEx.WriteLine(e.Message);
                    return null;
                }
            })
            .Where(a => a is not null)
            .SelectMany(a => a)
            .SingleOrDefaultAsync(a =>
                    a.Name.Equals(batchAccountName, StringComparison.OrdinalIgnoreCase) &&
                    a.Location.Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase),
                cts.Token);

        private async Task CreateDefaultStorageContainersAsync(IStorageAccount storageAccount)
        {
            var blobClient = await GetBlobClientAsync(storageAccount, cts.Token);

            var defaultContainers = new List<string> { WorkflowsContainerName, InputsContainerName, ExecutionsContainerName, LogsContainerName, OutputsContainerName, TesInternalContainerName, ConfigurationContainerName };
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

        private Task WritePersonalizedFilesToStorageAccountAsync(IStorageAccount storageAccount)
            => Execute(
                $"Writing {CromwellConfigurationFileName} & {AllowedVmSizesFileName} files to '{ConfigurationContainerName}' storage container...",
                async () =>
                {
                    // Configure Cromwell config file for PostgreSQL on Azure.
                    await UploadTextToStorageAccountAsync(storageAccount, ConfigurationContainerName, CromwellConfigurationFileName, Utility.PersonalizeContent(new[]
                    {
                        new Utility.ConfigReplaceTextItem("{DatabaseUrl}", $"\"jdbc:postgresql://{configuration.PostgreSqlServerName}.{azureCloudConfig.Suffixes.PostgresqlServerEndpointSuffix}/{configuration.PostgreSqlCromwellDatabaseName}?sslmode=require\""),
                        new Utility.ConfigReplaceTextItem("{DatabaseUser}", $"\"{configuration.PostgreSqlCromwellUserLogin}\""),
                        new Utility.ConfigReplaceTextItem("{DatabasePassword}", $"\"{configuration.PostgreSqlCromwellUserPassword}\""),
                        new Utility.ConfigReplaceTextItem("{DatabaseDriver}", $"\"org.postgresql.Driver\""),
                        new Utility.ConfigReplaceTextItem("{DatabaseProfile}", "\"slick.jdbc.PostgresProfile$\""),
                    }, "scripts", CromwellConfigurationFileName));

                    await UploadTextToStorageAccountAsync(storageAccount, TesInternalContainerName, $"{ConfigurationContainerName}/{AllowedVmSizesFileName}", Utility.GetFileContent("scripts", AllowedVmSizesFileName));
                });

        private Task AssignVmAsContributorToBatchAccountAsync(IIdentity managedIdentity, BatchAccount batchAccount)
            => Execute(
                $"Assigning {BuiltInRole.Contributor} role for user-managed identity to Batch Account resource scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    ct => azureSubscriptionClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithBuiltInRole(BuiltInRole.Contributor)
                        .WithScope(batchAccount.Id)
                        .CreateAsync(ct),
                    cts.Token));

        private async Task<FlexibleServerModel.Server> CreatePostgreSqlServerAndDatabaseAsync(FlexibleServer.IPostgreSQLManagementClient postgresManagementClient, ISubnet subnet, IPrivateDnsZone postgreSqlDnsZone)
        {
            if (!subnet.Inner.Delegations.Any())
            {
                subnet.Parent.Update().UpdateSubnet(subnet.Name).WithDelegation("Microsoft.DBforPostgreSQL/flexibleServers");
                await subnet.Parent.Update().ApplyAsync(cts.Token);
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
                           sku: new(configuration.PostgreSqlSkuName, configuration.PostgreSqlTier),
                           storage: new FlexibleServerModel.Storage(configuration.PostgreSqlStorageSize),
                           administratorLogin: configuration.PostgreSqlAdministratorLogin,
                           administratorLoginPassword: configuration.PostgreSqlAdministratorPassword,
                           network: new FlexibleServerModel.Network(publicNetworkAccess: "Disabled", delegatedSubnetResourceId: subnet.Inner.Id, privateDnsZoneArmResourceId: postgreSqlDnsZone.Id),
                           highAvailability: new FlexibleServerModel.HighAvailability("Disabled")
                        ), cts.Token);
                });

            await Execute(
                $"Creating PostgreSQL Cromwell database: {configuration.PostgreSqlCromwellDatabaseName}...",
                () => postgresManagementClient.Databases.CreateAsync(
                    configuration.ResourceGroupName, configuration.PostgreSqlServerName, configuration.PostgreSqlCromwellDatabaseName,
                    new FlexibleServerModel.Database(), cts.Token));

            await Execute(
                $"Creating PostgreSQL TES database: {configuration.PostgreSqlTesDatabaseName}...",
                () => postgresManagementClient.Databases.CreateAsync(
                    configuration.ResourceGroupName, configuration.PostgreSqlServerName, configuration.PostgreSqlTesDatabaseName,
                    new FlexibleServerModel.Database(), cts.Token));

            return server;
        }

        private Task AssignVmAsContributorToAppInsightsAsync(IIdentity managedIdentity, IResource appInsights)
            => Execute(
                $"Assigning {BuiltInRole.Contributor} role for user-managed identity to App Insights resource scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    ct => azureSubscriptionClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithBuiltInRole(BuiltInRole.Contributor)
                        .WithResourceScope(appInsights)
                        .CreateAsync(ct),
                    cts.Token));

        private Task<(INetwork virtualNetwork, ISubnet vmSubnet, ISubnet postgreSqlSubnet, ISubnet batchSubnet)> CreateVnetAndSubnetsAsync(IResourceGroup resourceGroup)
          => Execute(
                $"Creating virtual network and subnets: {configuration.VnetName}...",
                async () =>
                {
                    var defaultNsg = await CreateNetworkSecurityGroupAsync(resourceGroup, $"{configuration.VnetName}-default-nsg");

                    var vnetDefinition = azureSubscriptionClient.Networks
                        .Define(configuration.VnetName)
                        .WithRegion(configuration.RegionName)
                        .WithExistingResourceGroup(resourceGroup)
                        .WithAddressSpace(configuration.VnetAddressSpace)
                        .DefineSubnet(configuration.VmSubnetName)
                        .WithAddressPrefix(configuration.VmSubnetAddressSpace)
                        .WithExistingNetworkSecurityGroup(defaultNsg)
                        .Attach();

                    vnetDefinition = vnetDefinition.DefineSubnet(configuration.PostgreSqlSubnetName)
                        .WithAddressPrefix(configuration.PostgreSqlSubnetAddressSpace)
                        .WithExistingNetworkSecurityGroup(defaultNsg)
                        .WithDelegation("Microsoft.DBforPostgreSQL/flexibleServers")
                        .Attach();

                    vnetDefinition = vnetDefinition.DefineSubnet(configuration.BatchSubnetName)
                        .WithAddressPrefix(configuration.BatchNodesSubnetAddressSpace)
                        .WithExistingNetworkSecurityGroup(defaultNsg)
                        .Attach();

                    var vnet = await vnetDefinition.CreateAsync(cts.Token);
                    var batchSubnet = vnet.Subnets.FirstOrDefault(s => s.Key.Equals(configuration.BatchSubnetName, StringComparison.OrdinalIgnoreCase)).Value;

                    // Use the new ResourceManager sdk to add the ACR service endpoint since it is absent from the fluent sdk.
                    var armBatchSubnet = (await armClient.GetSubnetResource(new ResourceIdentifier(batchSubnet.Inner.Id)).GetAsync(cancellationToken: cts.Token)).Value;

                    AddServiceEndpointsToSubnet(armBatchSubnet.Data);

                    await armBatchSubnet.UpdateAsync(Azure.WaitUntil.Completed, armBatchSubnet.Data, cts.Token);

                    return (vnet,
                        vnet.Subnets.FirstOrDefault(s => s.Key.Equals(configuration.VmSubnetName, StringComparison.OrdinalIgnoreCase)).Value,
                        vnet.Subnets.FirstOrDefault(s => s.Key.Equals(configuration.PostgreSqlSubnetName, StringComparison.OrdinalIgnoreCase)).Value,
                        batchSubnet);
                });

        private Task<INetworkSecurityGroup> CreateNetworkSecurityGroupAsync(IResourceGroup resourceGroup, string networkSecurityGroupName)
        {
            return azureSubscriptionClient.NetworkSecurityGroups.Define(networkSecurityGroupName)
                    .WithRegion(configuration.RegionName)
                    .WithExistingResourceGroup(resourceGroup)
                    .CreateAsync(cts.Token);
        }

        private string GetFormattedPostgresqlUser(bool isCromwellPostgresUser)
        {
            return isCromwellPostgresUser ?
                configuration.PostgreSqlCromwellUserLogin :
                configuration.PostgreSqlTesUserLogin;
        }

        private string GetCreateCromwellUserString()
        {
            return $"CREATE USER {configuration.PostgreSqlCromwellUserLogin} WITH PASSWORD '{configuration.PostgreSqlCromwellUserPassword}'; GRANT ALL PRIVILEGES ON DATABASE {configuration.PostgreSqlCromwellDatabaseName} TO {configuration.PostgreSqlCromwellUserLogin};";
        }

        private string GetCreateTesUserString()
        {
            return $"CREATE USER {configuration.PostgreSqlTesUserLogin} WITH PASSWORD '{configuration.PostgreSqlTesUserPassword}'; GRANT ALL PRIVILEGES ON DATABASE {configuration.PostgreSqlTesDatabaseName} TO {configuration.PostgreSqlTesUserLogin};";
        }

        private string GetPostgreSQLCreateCromwellUserCommand(string dbName, string sqlCommand)
        {
            return $"psql postgresql://{configuration.PostgreSqlAdministratorLogin}:{configuration.PostgreSqlAdministratorPassword}@{configuration.PostgreSqlServerName}.{azureCloudConfig.Suffixes.PostgresqlServerEndpointSuffix}/{dbName} -c \"{sqlCommand}\"";
        }

        private async Task<IPrivateDnsZone> GetExistingPrivateDnsZoneAsync(INetwork virtualNetwork, string name)
        {
            var dnsZones = (await azureSubscriptionClient.PrivateDnsZones.ListAsync(cancellationToken: cts.Token)).ToAsyncEnumerable()
                .Where(x => x.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
            var dnsZonesMap = new Dictionary<string, IPrivateDnsZone>();

            await foreach (var zone in dnsZones.WithCancellation(cts.Token))
            {
                var pairs = (await zone.VirtualNetworkLinks.ListAsync()).ToAsyncEnumerable()
                    .Select(x => new KeyValuePair<string, IPrivateDnsZone>(x.ReferencedVirtualNetworkId, zone));

                await foreach (var pair in pairs.WithCancellation(cts.Token))
                {
                    dnsZonesMap.Add(pair.Key, pair.Value);
                }
            }

            dnsZonesMap.TryGetValue(virtualNetwork.Id, out var privateDnsZone);
            return privateDnsZone;
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
                        .CreateAsync(cts.Token);
                    return dnsZone;
                });

        private Task ExecuteQueriesOnAzurePostgreSQLDbFromK8(IKubernetes kubernetesClient, string podName, string aksNamespace)
            => Execute(
                $"Executing script to create users in tes_db and cromwell_db...",
                async () =>
                {
                    var cromwellScript = GetCreateCromwellUserString();
                    var tesScript = GetCreateTesUserString();
                    var serverPath = $"{configuration.PostgreSqlServerName}.{azureCloudConfig.Suffixes.PostgresqlServerEndpointSuffix}";
                    var adminUser = configuration.PostgreSqlAdministratorLogin;

                    var commands = new List<string[]> {
                        new string[] { "apt", "-qq", "update" },
                        new string[] { "apt", "-qq", "install", "-y", "postgresql-client" },
                        new string[] { "bash", "-lic", $"echo {configuration.PostgreSqlServerName}.{azureCloudConfig.Suffixes.PostgresqlServerEndpointSuffix}:{configuration.PostgreSqlServerPort}:{configuration.PostgreSqlCromwellDatabaseName}:{adminUser}:{configuration.PostgreSqlAdministratorPassword} > ~/.pgpass" },
                        new string[] { "bash", "-lic", $"echo {configuration.PostgreSqlServerName}.{azureCloudConfig.Suffixes.PostgresqlServerEndpointSuffix}:{configuration.PostgreSqlServerPort}:{configuration.PostgreSqlTesDatabaseName}:{adminUser}:{configuration.PostgreSqlAdministratorPassword} >> ~/.pgpass" },
                        new string[] { "bash", "-lic", "chmod 0600 ~/.pgpass" },
                        new string[] { "/usr/bin/psql", "-h", serverPath, "-U", adminUser, "-d", configuration.PostgreSqlCromwellDatabaseName, "-c", cromwellScript },
                        new string[] { "/usr/bin/psql", "-h", serverPath, "-U", adminUser, "-d", configuration.PostgreSqlTesDatabaseName, "-c", tesScript }
                    };

                    await kubernetesManager.ExecuteCommandsOnPodAsync(kubernetesClient, podName, commands, aksNamespace);
                });

        private async Task SetStorageKeySecret(string vaultUrl, string secretName, string secretValue)
        {
            var client = new SecretClient(new Uri(vaultUrl), new DefaultAzureCredential(new DefaultAzureCredentialOptions { AuthorityHost = new Uri(azureCloudConfig.Authentication.LoginEndpointUrl) }));
            await client.SetSecretAsync(secretName, secretValue, cts.Token);
        }

        private Task<Vault> GetKeyVaultAsync(string vaultName)
        {
            var keyVaultManagementClient = new KeyVaultManagementClient(azureCredentials) { SubscriptionId = configuration.SubscriptionId };
            return keyVaultManagementClient.Vaults.GetAsync(configuration.ResourceGroupName, vaultName, cts.Token);
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
                        Properties = new()
                        {
                            TenantId = new(tenantId),
                            Sku = new(SkuName.Standard),
                            NetworkAcls = new()
                            {
                                DefaultAction = configuration.PrivateNetworking.GetValueOrDefault() ? "Deny" : "Allow"
                            },
                            AccessPolicies = new List<AccessPolicyEntry>()
                            {
                                new()
                                {
                                    TenantId = new(tenantId),
                                    ObjectId = await GetUserObjectId(),
                                    Permissions = new()
                                    {
                                        Secrets = secrets
                                    }
                                },
                                new()
                                {
                                    TenantId = new(tenantId),
                                    ObjectId = managedIdentity.PrincipalId,
                                    Permissions = new()
                                    {
                                        Secrets = secrets
                                    }
                                }
                            }
                        }
                    };

                    var vault = await keyVaultManagementClient.Vaults.CreateOrUpdateAsync(configuration.ResourceGroupName, vaultName, properties, cts.Token);

                    if (configuration.PrivateNetworking.GetValueOrDefault())
                    {
                        var connection = new NetworkPrivateLinkServiceConnection
                        {
                            Name = "pe-coa-keyvault",
                            PrivateLinkServiceId = new(vault.Id),
                        };
                        connection.GroupIds.Add("vault");

                        var endpointData = new PrivateEndpointData
                        {
                            CustomNetworkInterfaceName = "pe-coa-keyvault",
                            ExtendedLocation = new() { Name = configuration.RegionName },
                            Subnet = new() { Id = new(subnet.Inner.Id), Name = subnet.Name },
                        };
                        endpointData.PrivateLinkServiceConnections.Add(connection);

                        var privateEndpoint = (await armClient
                                .GetResourceGroupResource(new ResourceIdentifier(subnet.Parent.Inner.Id).Parent)
                                .GetPrivateEndpoints()
                                .CreateOrUpdateAsync(Azure.WaitUntil.Completed, "pe-keyvault", endpointData, cts.Token))
                            .Value.Data;
                        var networkInterface = privateEndpoint.NetworkInterfaces[0];

                        var dnsZone = await CreatePrivateDnsZoneAsync(subnet.Parent, "privatelink.vaultcore.azure.net", "KeyVault");
                        await dnsZone
                            .Update()
                            .DefineARecordSet(vault.Name)
                            .WithIPv4Address(networkInterface.IPConfigurations.First().PrivateIPAddress)
                            .Attach()
                            .ApplyAsync(cts.Token);
                    }

                    return vault;

                    async ValueTask<string> GetUserObjectId()
                    {
                        const string graphUri = "https://graph.windows.net";
                        var credentials = new AzureCredentials(default, new TokenCredentials(new RefreshableAzureServiceTokenProvider(graphUri)), tenantId, AzureEnvironment.AzureGlobalCloud);
                        using GraphRbacManagementClient rbacClient = new(Configure().WithEnvironment(AzureEnvironment.AzureGlobalCloud).WithCredentials(credentials).WithBaseUri(graphUri).Build()) { TenantID = tenantId };
                        credentials.InitializeServiceClient(rbacClient);
                        return (await rbacClient.SignedInUser.GetAsync(cts.Token)).ObjectId;
                    }
                });

        private async Task<IGenericResource> GetLogAnalyticsWorkspaceAsync(string resourceId)
        {
            if (string.IsNullOrWhiteSpace(resourceId))
            {
                return null;
            }

            try
            {
                var resourceManager = ResourceManager
                    .Configure()
                    .Authenticate(azureCredentials)
                    .WithSubscription(configuration.SubscriptionId);

                return await resourceManager.GenericResources.GetByIdAsync(resourceId, "2020-08-01", cts.Token);
            }
            catch
            {
                ConsoleEx.WriteLine($"{resourceId} not found.");
                return null;
            }
        }

        private async Task<IGenericResource> CreateLogAnalyticsWorkspaceAsync(string workspaceName)
        {
            return await Execute($"Creating Log Analytics workspace {workspaceName}...", async () =>
            {
                return await ResourceManager
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
                    .CreateAsync(cts.Token);
            });
        }


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
                () => new BatchManagementClient(tokenCredentials) { SubscriptionId = configuration.SubscriptionId, BaseUri = new Uri(azureCloudConfig.ResourceManagerUrl) }
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
                () => resourceGroupDefinition.CreateAsync(cts.Token));
        }

        private Task<IIdentity> CreateUserManagedIdentityAsync(IResourceGroup resourceGroup)
        {
            // Resource group name supports periods and parenthesis but identity doesn't. Replacing them with hyphens.
            var managedIdentityName = $"{resourceGroup.Name.Replace(".", "-").Replace("(", "-").Replace(")", "-")}-identity";

            return Execute(
                $"Obtaining user-assigned managed identity: {managedIdentityName}...",
                async () => await azureSubscriptionClient.Identities.GetByResourceGroupAsync(configuration.ResourceGroupName, managedIdentityName, cts.Token)
                    ?? await azureSubscriptionClient.Identities.Define(managedIdentityName)
                        .WithRegion(configuration.RegionName)
                        .WithExistingResourceGroup(resourceGroup)
                        .CreateAsync(cts.Token));
        }

        private async Task<IIdentity> GetUserManagedIdentityAsync(string resourceId)
        {
            return await (await azureSubscriptionClient.Identities.ListAsync(cancellationToken: cts.Token)).ToAsyncEnumerable()
                .SingleOrDefaultAsync(x => string.Equals(x.Id, resourceId, StringComparison.OrdinalIgnoreCase), cts.Token);
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
            var validRegionNames = azureSubscriptionClient.GetCurrentSubscription().ListLocations().Select(loc => loc.Region.Name).Distinct();

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

            var subscriptionExists = await (await azure.Subscriptions.ListAsync(cancellationToken: cts.Token)).ToAsyncEnumerable()
                .AnyAsync(sub => sub.SubscriptionId.Equals(configuration.SubscriptionId, StringComparison.OrdinalIgnoreCase), cts.Token);

            if (!subscriptionExists)
            {
                throw new ValidationException($"Invalid or inaccessible subcription id '{configuration.SubscriptionId}'. Make sure that subscription exists and that you are either an Owner or have Contributor and User Access Administrator roles on the subscription.", displayExample: false);
            }

            var rgExists = !string.IsNullOrEmpty(configuration.ResourceGroupName) && await azureSubscriptionClient.ResourceGroups.ContainAsync(configuration.ResourceGroupName, cts.Token);

            if (!string.IsNullOrEmpty(configuration.ResourceGroupName) && !rgExists)
            {
                throw new ValidationException($"If ResourceGroupName is provided, the resource group must already exist.", displayExample: false);
            }

            var token = (await tokenProvider.GetAuthenticationHeaderAsync(cts.Token)).Parameter;
            var currentPrincipalObjectId = new JwtSecurityTokenHandler().ReadJwtToken(token).Claims.FirstOrDefault(c => c.Type == "oid").Value;

            var currentPrincipalSubscriptionRoleIds = (await azureSubscriptionClient.AccessManagement.RoleAssignments.Inner.ListForScopeWithHttpMessagesAsync(
                    $"/subscriptions/{configuration.SubscriptionId}", new ODataQuery<RoleAssignmentFilter>($"atScope() and assignedTo('{currentPrincipalObjectId}')"), cancellationToken: cts.Token)).Body
                .ToAsyncEnumerable(async (link, ct) => (await azureSubscriptionClient.AccessManagement.RoleAssignments.Inner.ListForScopeNextWithHttpMessagesAsync(link, cancellationToken: ct)).Body)
                .Select(b => b.RoleDefinitionId.Split(new[] { '/' }).Last());

            if (!await currentPrincipalSubscriptionRoleIds.AnyAsync(role => ownerRoleId.Equals(role, StringComparison.OrdinalIgnoreCase) || contributorRoleId.Equals(role, StringComparison.OrdinalIgnoreCase), cts.Token))
            {
                if (!rgExists)
                {
                    throw new ValidationException($"Insufficient access to deploy. You must be: 1) Owner of the subscription, or 2) Contributor and User Access Administrator of the subscription, or 3) Owner of the resource group", displayExample: false);
                }

                var currentPrincipalRgRoleIds = (await azureSubscriptionClient.AccessManagement.RoleAssignments.Inner.ListForScopeWithHttpMessagesAsync(
                        $"/subscriptions/{configuration.SubscriptionId}/resourceGroups/{configuration.ResourceGroupName}", new ODataQuery<RoleAssignmentFilter>($"atScope() and assignedTo('{currentPrincipalObjectId}')"), cancellationToken: cts.Token)).Body
                    .ToAsyncEnumerable(async (link, ct) => (await azureSubscriptionClient.AccessManagement.RoleAssignments.Inner.ListForScopeNextWithHttpMessagesAsync(link, cancellationToken: ct)).Body)
                    .Select(b => b.RoleDefinitionId.Split(new[] { '/' }).Last());

                if (!await currentPrincipalRgRoleIds.AnyAsync(role => ownerRoleId.Equals(role, StringComparison.OrdinalIgnoreCase), cts.Token))
                {
                    throw new ValidationException($"Insufficient access to deploy. You must be: 1) Owner of the subscription, or 2) Contributor and User Access Administrator of the subscription, or 3) Owner of the resource group", displayExample: false);
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

            if (!await (await azureSubscriptionClient.ResourceGroups.ListAsync(true, cts.Token)).ToAsyncEnumerable().AnyAsync(rg => rg.Name.Equals(configuration.VnetResourceGroupName, StringComparison.OrdinalIgnoreCase), cts.Token))
            {
                throw new ValidationException($"Resource group '{configuration.VnetResourceGroupName}' does not exist.");
            }

            var vnet = await azureSubscriptionClient.Networks.GetByResourceGroupAsync(configuration.VnetResourceGroupName, configuration.VnetName, cts.Token);

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

            var resourceGraphClient = new ResourceGraphClient(new Uri(azureCloudConfig.ResourceManagerUrl), tokenCredentials);
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
            var resourcesExist = (await resourceGraphClient.ResourcesAsync(new QueryRequest(new[] { configuration.SubscriptionId }, resourcesInPostgreSqlSubnetQuery), cts.Token)).TotalRecords > 0;

            if (hasNoDelegations && resourcesExist)
            {
                throw new ValidationException($"Subnet '{configuration.PostgreSqlSubnetName}' must be either empty or have 'Microsoft.DBforPostgreSQL/flexibleServers' delegation.");
            }

            var batchSubnet = vnet.Subnets.FirstOrDefault(s => s.Key.Equals(configuration.BatchSubnetName, StringComparison.OrdinalIgnoreCase)).Value;

            return (vnet, vmSubnet, postgreSqlSubnet, batchSubnet);
        }

        private async Task ValidateBatchAccountQuotaAsync()
        {
            var batchManagementClient = new BatchManagementClient(tokenCredentials) { SubscriptionId = configuration.SubscriptionId, BaseUri = new Uri(azureCloudConfig.ResourceManagerUrl) };
            var accountQuota = (await batchManagementClient.Location.GetQuotasAsync(configuration.RegionName, cts.Token)).AccountQuota;
            var existingBatchAccountCount = await (await batchManagementClient.BatchAccount.ListAsync(cts.Token)).ToAsyncEnumerable(batchManagementClient.BatchAccount.ListNextAsync)
                .CountAsync(b => b.Location.Equals(configuration.RegionName), cts.Token);

            if (existingBatchAccountCount >= accountQuota)
            {
                throw new ValidationException($"The regional Batch account quota ({accountQuota} account(s) per region) for the specified subscription has been reached. Submit a support request to increase the quota or choose another region.", displayExample: false);
            }
        }

        private Task<string> UpdateVnetWithBatchSubnet(string resourceGroupId)
            => Execute(
                $"Creating batch subnet...",
                async () =>
                {
                    var coaRg = armClient.GetResourceGroupResource(new(resourceGroupId));

                    var vnetCollection = coaRg.GetVirtualNetworks();
                    var vnet = vnetCollection.FirstOrDefault();

                    if (vnetCollection.Count() != 1)
                    {
                        ConsoleEx.WriteLine("There are multiple vnets found in the resource group so the deployer cannot automatically create the subnet.", ConsoleColor.Red);
                        ConsoleEx.WriteLine("In order to avoid unnecessary load balancer charges we suggest manually configuring your deployment to use a subnet for batch pools with service endpoints.", ConsoleColor.Red);
                        ConsoleEx.WriteLine("See: https://github.com/microsoft/CromwellOnAzure/wiki/Using-a-batch-pool-subnet-with-service-endpoints-to-avoid-load-balancer-charges.", ConsoleColor.Red);

                        return null;
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

                        return null;
                    }

                    var batchSubnet = new SubnetData
                    {
                        Name = configuration.DefaultBatchSubnetName,
                        AddressPrefix = configuration.BatchNodesSubnetAddressSpace,
                    };

                    AddServiceEndpointsToSubnet(batchSubnet);

                    vnetData.Subnets.Add(batchSubnet);
                    var updatedVnet = (await vnetCollection.CreateOrUpdateAsync(Azure.WaitUntil.Completed, vnetData.Name, vnetData, cts.Token)).Value;

                    return (await updatedVnet.GetSubnetAsync(configuration.DefaultBatchSubnetName, cancellationToken: cts.Token)).Value.Id.ToString();
                });

        private static void AddServiceEndpointsToSubnet(SubnetData subnet)
        {
            subnet.ServiceEndpoints.Add(new ServiceEndpointProperties()
            {
                Service = "Microsoft.Storage.Global",
            });

            subnet.ServiceEndpoints.Add(new ServiceEndpointProperties()
            {
                Service = "Microsoft.Sql",
            });

            subnet.ServiceEndpoints.Add(new ServiceEndpointProperties()
            {
                Service = "Microsoft.ContainerRegistry",
            });

            subnet.ServiceEndpoints.Add(new ServiceEndpointProperties()
            {
                Service = "Microsoft.KeyVault",
            });
        }

        private async Task ValidateVmAsync()
        {
            var computeSkus = await generalRetryPolicy.ExecuteAsync(async ct =>
                await (await azureSubscriptionClient.ComputeSkus.ListbyRegionAndResourceTypeAsync(
                    Region.Create(configuration.RegionName),
                    ComputeResourceType.VirtualMachines,
                    ct))
                    .ToAsyncEnumerable()
                    .Where(s => !s.Restrictions.Any())
                    .Select(s => s.Name.Value)
                    .ToListAsync(ct),
                cts.Token);

            if (!computeSkus.Any())
            {
                throw new ValidationException($"Your subscription doesn't support virtual machine creation in {configuration.RegionName}.  Please create an Azure Support case: https://docs.microsoft.com/en-us/azure/azure-portal/supportability/how-to-create-azure-support-request", displayExample: false);
            }
            else if (!computeSkus.Any(s => s.Equals(configuration.VmSize, StringComparison.OrdinalIgnoreCase)))
            {
                throw new ValidationException($"The VmSize {configuration.VmSize} is not available or does not exist in {configuration.RegionName}.  You can use 'az vm list-skus --location {configuration.RegionName} --output table' to find an available VM.", displayExample: false);
            }
        }

        private static async Task<BlobServiceClient> GetBlobClientAsync(IStorageAccount storageAccount, CancellationToken cancellationToken)
            => new(
                new($"https://{storageAccount.Name}.blob.{azureCloudConfig.Suffixes.StorageSuffix}"),
                new StorageSharedKeyCredential(
                    storageAccount.Name,
                    (await storageAccount.GetKeysAsync(cancellationToken))[0].Value));

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

        private void ValidateInitialCommandLineArgs()
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

            void ThrowIfProvidedForInstall(object attributeValue, string attributeName)
            {
                if (configuration.Update && attributeValue is not null)
                {
                    throw new ValidationException($"{attributeName} must not be provided when installing.", false);
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

            ThrowIfProvidedForInstall(configuration.AksClusterName, nameof(configuration.AksClusterName));

            ThrowIfProvidedForUpdate(configuration.BatchPrefix, nameof(configuration.BatchPrefix));
            ThrowIfProvidedForUpdate(configuration.RegionName, nameof(configuration.RegionName));
            ThrowIfProvidedForUpdate(configuration.BatchAccountName, nameof(configuration.BatchAccountName));
            ThrowIfProvidedForUpdate(configuration.CrossSubscriptionAKSDeployment, nameof(configuration.CrossSubscriptionAKSDeployment));
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

        private async Task<bool> IsWorkflowSuccessfulAfterLongPollingAsync(IStorageAccount storageAccount, string containerName, Guid id)
        {
            var container = (await GetBlobClientAsync(storageAccount, cts.Token)).GetBlobContainerClient(containerName);

            while (true)
            {
                try
                {
                    var hasSucceeded = container.GetBlobs(prefix: $"succeeded/{id}").Count() == 1;
                    var failedWorkflowTriggerFileBlobs = container.GetBlobs(prefix: $"failed/{id}").ToList();
                    var hasFailed = failedWorkflowTriggerFileBlobs.Count == 1;

                    if (hasSucceeded || hasFailed)
                    {
                        if (hasFailed)
                        {
                            var failedContent = (await container.GetBlobClient(failedWorkflowTriggerFileBlobs.First().Name).DownloadContentAsync(cts.Token)).Value.Content.ToString();
                            ConsoleEx.WriteLine($"Failed workflow trigger JSON: {failedContent}");
                        }

                        return hasSucceeded && !hasFailed;
                    }
                }
                catch (Exception exc)
                {
                    // "Server is busy" occasionally can be ignored
                    ConsoleEx.WriteLine(exc.Message);
                }

                await Task.Delay(System.TimeSpan.FromSeconds(10), cts.Token);
            }
        }

        public Task Execute(string message, Func<Task> func)
            => Execute(message, async () => { await func(); return false; });

        private async Task<T> Execute<T>(string message, Func<Task<T>> func, bool cancelOnException = true)
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
                catch (Microsoft.Rest.Azure.CloudException cloudException) when (cloudException.ToCloudErrorType() == CloudErrorType.RoleAssignmentExists)
                {
                    line.Write($" skipped. Role assignment already exists.", ConsoleColor.Yellow);
                    return default;
                }
                catch (OperationCanceledException) when (cts.Token.IsCancellationRequested)
                {
                    line.Write(" Cancelled", ConsoleColor.Red);
                    return await Task.FromCanceled<T>(cts.Token);
                }
                catch (Exception ex)
                {
                    line.Write($" Failed. {ex.GetType().Name}: {ex.Message}", ConsoleColor.Red);

                    if (cancelOnException)
                    {
                        cts.Cancel();
                    }

                    throw;
                }
            }

            line.Write($" Failed", ConsoleColor.Red);

            if (cancelOnException)
            {
                cts.Cancel();
            }

            throw new Exception($"Failed after {retryCount} attempts");
        }

        private static void WriteExecutionTime(ConsoleEx.Line line, DateTime startTime)
            => line.Write($" Completed in {DateTime.UtcNow.Subtract(startTime).TotalSeconds:n0}s", ConsoleColor.Green);

        public static async Task<string> DownloadTextFromStorageAccountAsync(IStorageAccount storageAccount, string containerName, string blobName, CancellationToken cancellationToken)
        {
            var blobClient = await GetBlobClientAsync(storageAccount, cancellationToken);
            var container = blobClient.GetBlobContainerClient(containerName);

            return (await container.GetBlobClient(blobName).DownloadContentAsync(cancellationToken)).Value.Content.ToString();
        }

        private async Task UploadTextToStorageAccountAsync(IStorageAccount storageAccount, string containerName, string blobName, string content)
            => await UploadTextToStorageAccountAsync(storageAccount, containerName, blobName, content, cts.Token);

        public static async Task UploadTextToStorageAccountAsync(IStorageAccount storageAccount, string containerName, string blobName, string content, CancellationToken token)
        {
            var blobClient = await GetBlobClientAsync(storageAccount, token);
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
            var contents = await File.ReadAllLinesAsync(path, cts.Token);

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
                var blobContainers = (await storageAccount.Manager.BlobContainers.ListAsync(storageAccount.ResourceGroupName, storageAccount.Name, cts.Token)).ToAsyncEnumerable();
                await foreach (var page in blobContainers.WithCancellation(cts.Token))
                {
                    foreach (var container in page.Value)
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
