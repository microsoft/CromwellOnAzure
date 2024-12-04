// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.ResourceManager;
using Azure.ResourceManager.ContainerService;
using Azure.ResourceManager.ManagedServiceIdentities;
using Azure.Storage.Blobs;
using CommonUtilities.AzureCloud;
using k8s;
using k8s.Models;
using Newtonsoft.Json;
using Polly;
using Polly.Retry;

namespace CromwellOnAzureDeployer
{
    /// <summary>
    /// Class to hold all the kubernetes specific deployer logic.
    /// </summary>
    internal class KubernetesManager
    {
        private static readonly AsyncRetryPolicy WorkloadReadyRetryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(80, retryAttempt => TimeSpan.FromSeconds(15));

        private static readonly AsyncRetryPolicy KubeExecRetryPolicy = Policy
            .Handle<WebSocketException>(ex => ex.WebSocketErrorCode == WebSocketError.NotAWebSocket)
            .WaitAndRetryAsync(200, retryAttempt => TimeSpan.FromSeconds(5));

        // "master" is used despite not being a best practice: https://github.com/kubernetes-sigs/blob-csi-driver/issues/783
        private const string BlobCsiDriverGithubReleaseBranch = "master";
        private const string BlobCsiDriverGithubReleaseVersion = "v1.24.0";
        private const string BlobCsiRepo = $"https://raw.githubusercontent.com/kubernetes-sigs/blob-csi-driver/{BlobCsiDriverGithubReleaseBranch}/charts";

        private Configuration configuration { get; }
        private AzureCloudConfig azureCloudConfig { get; }
        private CancellationToken cancellationToken { get; }
        private string workingDirectoryTemp { get; set; }
        private string kubeConfigPath { get; set; }
        private string valuesTemplatePath { get; set; }
        public string helmScriptsRootDirectory { get; private set; }
        public string TempHelmValuesYamlPath { get; private set; }

        public delegate BlobClient GetBlobClient(Azure.ResourceManager.Storage.StorageAccountData storageAccount, string containerName, string blobName);
        private readonly GetBlobClient _GetBlobClient;

        public KubernetesManager(Configuration config, AzureCloudConfig azureCloudConfig, GetBlobClient getBlobClient, CancellationToken cancellationToken)
        {
            this.azureCloudConfig = azureCloudConfig;
            this.cancellationToken = cancellationToken;
            this.configuration = config;
            this._GetBlobClient = getBlobClient;

            CreateAndInitializeWorkingDirectoriesAsync().Wait(cancellationToken);
        }

        public async Task<IKubernetes> GetKubernetesClientAsync(ContainerServiceManagedClusterResource aksCluster)
        {
            using MemoryStream kubeconfig = new((await aksCluster.GetClusterAdminCredentialsAsync(cancellationToken: cancellationToken)).Value.Kubeconfigs[0].Value, writable: false);

            // Write kubeconfig in the working directory as helm & kubctl need it.
            FileInfo kubeConfigFile = new(kubeConfigPath);
            await File.WriteAllTextAsync(kubeConfigFile.FullName, Encoding.Default.GetString(kubeconfig.ToArray()), cancellationToken);
            kubeConfigFile.Refresh();

            if (!OperatingSystem.IsWindows())
            {
                kubeConfigFile.UnixFileMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
            }

            var k8sClientConfiguration = KubernetesClientConfiguration.BuildConfigFromConfigFile(kubeconfig);
            return new Kubernetes(k8sClientConfiguration);
        }

        public static (string, V1Deployment) GetUbuntuDeploymentTemplate(string ubuntuImage)
        {
            return ("ubuntu", KubernetesYaml.Deserialize<V1Deployment>(
                $"""
                apiVersion: apps/v1
                kind: Deployment
                metadata:
                  labels:
                    io.kompose.service: ubuntu
                  name: ubuntu
                spec:
                  replicas: 1
                  selector:
                    matchLabels:
                      io.kompose.service: ubuntu
                  template:
                    metadata:
                      labels:
                        io.kompose.service: ubuntu
                    spec:
                      containers:
                        - name: ubuntu
                          image: {ubuntuImage}
                          command: ["/bin/bash", "-c", "--"]
                          args: ["while true; do sleep 30; done;"]
                """));
        }

        public async Task DeployCoADependenciesAsync()
        {
            var helmRepoList = await ExecHelmProcessAsync($"repo list", workingDirectory: null, throwOnNonZeroExitCode: false);

            foreach (var (helmCmd, throwOnNonZeroExitCode) in EnsureUpdateHelmRepo(helmRepoList, "blob-csi-driver", BlobCsiRepo))
            {
                await ExecHelmProcessAsync(helmCmd, throwOnNonZeroExitCode: throwOnNonZeroExitCode);
            }

            await ExecHelmProcessAsync($"repo update");
            await ExecHelmProcessAsync($"upgrade --install blob-csi-driver blob-csi-driver/blob-csi-driver --set node.enableBlobfuseProxy=true --namespace kube-system --version {BlobCsiDriverGithubReleaseVersion} --kubeconfig \"{kubeConfigPath}\"");

            static IEnumerable<(string HelmCmd, bool ThrowOnNonZeroExitCode)> EnsureUpdateHelmRepo(string helmRepoList, string helmRepo, string helmRepoUri)
            {
                if (string.IsNullOrWhiteSpace(helmRepoList) || !helmRepoList.Contains(helmRepo, StringComparison.OrdinalIgnoreCase))
                {
                    return [($"repo add {helmRepo} {helmRepoUri}", true)];
                }
                else if (!helmRepoList.Contains(helmRepoUri, StringComparison.OrdinalIgnoreCase))
                {
                    return
                    [
                        ($"repo remove {helmRepo}", false),
                        ($"repo add {helmRepo} {helmRepoUri}", true)
                    ];
                }
                else
                {
                    return [];
                }
            }
        }

        public async Task DeployHelmChartToClusterAsync()
        {
            // https://helm.sh/docs/helm/helm_upgrade/
            // The chart argument can be either: a chart reference('example/mariadb'), a path to a chart directory, a packaged chart, or a fully qualified URL
            await ExecHelmProcessAsync($"upgrade --install cromwellonazure ./helm --kubeconfig \"{kubeConfigPath}\" --namespace {configuration.AksCoANamespace} --create-namespace",
                workingDirectory: workingDirectoryTemp);
        }

        public async Task UpdateHelmValuesAsync(Azure.ResourceManager.Storage.StorageAccountData storageAccount, Uri keyVaultUrl, string resourceGroupName, Dictionary<string, string> settings, UserAssignedIdentityData managedId, List<MountableContainer> containersToMount)
        {
            var values = KubernetesYaml.Deserialize<HelmValues>(await File.ReadAllTextAsync(valuesTemplatePath, cancellationToken));
            UpdateValuesFromSettings(values, settings);
            values.Config["resourceGroup"] = resourceGroupName;
            values.Identity["name"] = managedId.Name;
            values.Identity["resourceId"] = managedId.Id;
            values.Identity["clientId"] = managedId.ClientId?.ToString("D");

            if (!Deployer.IsStorageInPublicCloud)
            {
                values.DefaultContainers.Add(Deployer.ExecutionsContainerName);
            }

            if (configuration.CrossSubscriptionAKSDeployment.GetValueOrDefault())
            {
                values.InternalContainersKeyVaultAuth = [];

                foreach (var container in values.DefaultContainers.Union(values.CromwellContainers, StringComparer.OrdinalIgnoreCase))
                {
                    var containerConfig = new Dictionary<string, string>()
                    {
                        { "accountName",  storageAccount.Name },
                        { "containerName", container },
                        { "keyVaultURL", keyVaultUrl.AbsoluteUri },
                        { "keyVaultSecretName", Deployer.StorageAccountKeySecretName}
                    };

                    values.InternalContainersKeyVaultAuth.Add(containerConfig);
                }
            }
            else
            {
                values.InternalContainersMIAuth = [];

                foreach (var container in values.DefaultContainers.Union(values.CromwellContainers, StringComparer.OrdinalIgnoreCase))
                {
                    var containerConfig = new Dictionary<string, string>()
                    {
                        { "accountName",  storageAccount.Name },
                        { "containerName", container },
                        { "resourceGroup", resourceGroupName },
                    };

                    values.InternalContainersMIAuth.Add(containerConfig);
                }
            }

            MergeContainers(containersToMount, values);

            // TODO: Does this make sense? If so, do we need this in TES?
            if (azureCloudConfig.ArmEnvironment != ArmEnvironment.AzurePublicCloud)
            {
                values.ExternalSasContainers = null;
            }

            var valuesString = KubernetesYaml.Serialize(values);
            await File.WriteAllTextAsync(TempHelmValuesYamlPath, valuesString, cancellationToken);
            await Deployer.UploadTextToStorageAccountAsync(_GetBlobClient(storageAccount, Deployer.ConfigurationContainerName, "aksValues.yaml"), valuesString, cancellationToken);
        }

        public async Task UpgradeValuesYamlAsync(Azure.ResourceManager.Storage.StorageAccountData storageAccount, Dictionary<string, string> settings, List<MountableContainer> containersToMount, Version previousVersion)
        {
            var blobClient = _GetBlobClient(storageAccount, Deployer.ConfigurationContainerName, "aksValues.yaml");
            var values = KubernetesYaml.Deserialize<HelmValues>(await Deployer.DownloadTextFromStorageAccountAsync(blobClient, cancellationToken));
            UpdateValuesFromSettings(values, settings);
            MergeContainers(containersToMount, values);
            ProcessHelmValuesUpdates(values, previousVersion);
            var valuesString = KubernetesYaml.Serialize(values);
            await File.WriteAllTextAsync(TempHelmValuesYamlPath, valuesString, cancellationToken);
            await Deployer.UploadTextToStorageAccountAsync(blobClient, valuesString, cancellationToken);
        }

        private static void ProcessHelmValuesUpdates(HelmValues values, Version previousVersion)
        {
            if (previousVersion is null || previousVersion < new Version(4, 3))
            {
                values.CromwellContainers = [Deployer.ConfigurationContainerName, Deployer.LogsContainerName, Deployer.OutputsContainerName];
                values.DefaultContainers = [Deployer.InputsContainerName];
            }

            if (previousVersion is null || previousVersion < new Version(5, 5, 0))
            {
                var datasettestinputs = values.ExternalSasContainers?.SingleOrDefault(container => container.TryGetValue("accountName", out var name) && "datasettestinputs".Equals(name, StringComparison.OrdinalIgnoreCase));

                if (datasettestinputs is not null)
                {
                    _ = values.ExternalSasContainers.Remove(datasettestinputs);

                    if (values.ExternalSasContainers.Count == 0)
                    {
                        values.ExternalSasContainers = null;
                    }
                }
            }

            if (Deployer.IsStorageInPublicCloud && previousVersion < new Version(5, 5, 1))
            {
                _ = values.CromwellContainers.Remove(Deployer.ExecutionsContainerName);
            }

            if (!Deployer.IsStorageInPublicCloud && previousVersion == new Version(5, 5, 1)) // special case: revert 5.5.1 changes
            {
                values.CromwellContainers.Add(Deployer.ExecutionsContainerName);
            }

            if (Deployer.IsStorageInPublicCloud && previousVersion < new Version(5, 5, 2))
            {
                if (values.InternalContainersMIAuth?.Any(values => values.ContainsKey("containerName") && values["containerName"] == Deployer.ExecutionsContainerName) ?? false)
                {
                    values.InternalContainersMIAuth.Remove(values.InternalContainersMIAuth.First(values => values.ContainsKey("containerName") && values["containerName"] == Deployer.ExecutionsContainerName));
                }

                if (values.InternalContainersKeyVaultAuth?.Any(values => values.ContainsKey("containerName") && values["containerName"] == Deployer.ExecutionsContainerName) ?? false)
                {
                    values.InternalContainersKeyVaultAuth.Remove(values.InternalContainersMIAuth.First(values => values.ContainsKey("containerName") && values["containerName"] == Deployer.ExecutionsContainerName));
                }
            }
        }

        public async Task<Dictionary<string, string>> GetAKSSettingsAsync(Azure.ResourceManager.Storage.StorageAccountData storageAccount)
        {
            var values = KubernetesYaml.Deserialize<HelmValues>(await Deployer.DownloadTextFromStorageAccountAsync(_GetBlobClient(storageAccount, Deployer.ConfigurationContainerName, "aksValues.yaml"), cancellationToken));
            return ValuesToSettings(values);
        }

        public async Task RemovePodAadChart()
        {
            await ExecHelmProcessAsync($"uninstall aad-pod-identity --namespace kube-system --kubeconfig \"{kubeConfigPath}\"", throwOnNonZeroExitCode: false);
        }

        public async Task ExecuteCommandsOnPodAsync(IKubernetes client, string podName, IEnumerable<string[]> commands, string aksNamespace)
        {
            async Task StreamHandler(Stream stream)
            {
                using var reader = new StreamReader(stream);
                var line = await reader.ReadLineAsync(CancellationToken.None);

                while (line is not null)
                {
                    if (configuration.DebugLogging)
                    {
                        ConsoleEx.WriteLine(podName + ": " + line);
                    }
                    line = await reader.ReadLineAsync(CancellationToken.None);
                }
            }

            if (!await WaitForWorkloadAsync(client, podName, aksNamespace, cancellationToken))
            {
                if (configuration.DebugLogging)
                {
                    await WritePodLogsAndEventsToDisk(client, podName, aksNamespace);
                }

                throw new Exception($"Timed out waiting for {podName} to start.");
            }

            var pods = await client.CoreV1.ListNamespacedPodAsync(aksNamespace, cancellationToken: cancellationToken);
            var workloadPod = pods.Items.Where(x => x.Metadata.Name.Contains(podName)).FirstOrDefault();

            // Pod Exec can fail even after the pod is marked ready.
            // Retry on WebSocketExceptions for up to 40 secs.
            var result = await KubeExecRetryPolicy.ExecuteAndCaptureAsync(async token =>
            {
                foreach (var command in commands)
                {
                    _ = await client.NamespacedPodExecAsync(workloadPod.Metadata.Name, aksNamespace, podName, command, true,
                        (stdIn, stdOut, stdError) => Task.WhenAll(StreamHandler(stdOut), StreamHandler(stdError)), CancellationToken.None);
                }
            }, cancellationToken);

            if (result.Outcome != OutcomeType.Successful && result.FinalException is not null)
            {
                if (configuration.DebugLogging)
                {
                    await WritePodLogsAndEventsToDisk(client, podName, aksNamespace);
                }

                throw new Exception($"Pod failed to run commands after being marked ready.", result.FinalException);
            }
        }

        /// <summary>
        /// Writes pod logs and events to disk, as well as all events for the AKS cluster. 
        /// </summary>
        /// <param name="client">IKubernetes client</param>
        /// <param name="podName">Name of pod to get the logs.</param>
        /// <param name="aksNamespace">Namespace where the pod is running.</param>
        /// <returns></returns>
        public async Task WritePodLogsAndEventsToDisk(IKubernetes client, string podName, string aksNamespace)
        {
            try
            {
                var pods = await client.CoreV1.ListNamespacedPodAsync(aksNamespace, cancellationToken: cancellationToken);
                var workloadPod = pods.Items.Where(x => x.Metadata.Name.Contains(podName)).FirstOrDefault();
                ConsoleEx.WriteLine($"Pod {podName} Status:\n\t{JsonConvert.SerializeObject(workloadPod.Status)}");
            }
            catch (Exception e)
            {
                ConsoleEx.WriteLine($"Exception thrown retrieving {podName} pod status.");
                ConsoleEx.WriteLine(e.Message);
            }

            try
            {
                var logStream = await client.CoreV1.ReadNamespacedPodLogAsync(podName, aksNamespace, cancellationToken: cancellationToken);
                var podTempFile = Path.GetTempFileName();

                var reader = new StreamReader(logStream);
                var logs = await reader.ReadToEndAsync(cancellationToken);
                await File.WriteAllTextAsync(podTempFile, logs, cancellationToken);
                ConsoleEx.WriteLine($"Pod {podName} Logs: {podTempFile}");
            }
            catch (Exception e)
            {
                ConsoleEx.WriteLine($"Exception thrown retrieving {podName} pod log.");
                ConsoleEx.WriteLine(e.Message);
            }

            try
            {
                var events = await client.CoreV1.ListEventForAllNamespacesAsync(cancellationToken: cancellationToken);
                var podEventsFile = Path.GetTempFileName();
                var allEventsFile = Path.GetTempFileName();

                await File.WriteAllTextAsync(podEventsFile, string.Join("\n", events.Items.Where(x => x.InvolvedObject.Name.Contains(podName)).Select(JsonConvert.SerializeObject)), cancellationToken);
                ConsoleEx.WriteLine($"Pod {podName} Events: {podEventsFile}");

                await File.WriteAllTextAsync(allEventsFile, string.Join("\n", events.Items.Select(JsonConvert.SerializeObject)), cancellationToken);
                ConsoleEx.WriteLine($"All Events: {allEventsFile}");
            }
            catch (Exception e)
            {
                ConsoleEx.WriteLine($"Exception thrown retrieving AKS events.");
                ConsoleEx.WriteLine(e.Message);
            }
        }

        public async Task WaitForCromwellAsync(IKubernetes client)
        {
            if (!await WaitForWorkloadAsync(client, "cromwell", configuration.AksCoANamespace, cancellationToken))
            {
                throw new Exception("Timed out waiting for Cromwell to start.");
            }
        }

        public void DeleteTempFiles()
        {
            if (Directory.Exists(workingDirectoryTemp))
            {
                Directory.Delete(workingDirectoryTemp, true);
            }
        }

        private async Task CreateAndInitializeWorkingDirectoriesAsync()
        {
            try
            {
                var workingDirectory = Directory.GetCurrentDirectory();
                workingDirectoryTemp = Path.Join(workingDirectory, "cromwell-on-azure");
                helmScriptsRootDirectory = Path.Join(workingDirectoryTemp, "helm");
                kubeConfigPath = Path.Join(workingDirectoryTemp, "aks", "kubeconfig.txt");
                TempHelmValuesYamlPath = Path.Join(helmScriptsRootDirectory, "values.yaml");
                valuesTemplatePath = Path.Join(helmScriptsRootDirectory, "values-template.yaml");
                Directory.CreateDirectory(helmScriptsRootDirectory);
                Directory.CreateDirectory(Path.GetDirectoryName(kubeConfigPath));
                await Utility.WriteEmbeddedFilesAsync(helmScriptsRootDirectory, cancellationToken, "scripts", "helm");
            }
            catch (Exception exc)
            {
                ConsoleEx.WriteLine(exc.ToString());
                throw;
            }
        }

        private static void MergeContainers(List<MountableContainer> containersToMount, HelmValues values)
        {
            if (containersToMount is null)
            {
                return;
            }

            var internalContainersMIAuth = values.InternalContainersMIAuth?.Select(x => new MountableContainer(x)).ToHashSet() ?? [];
            var sasContainers = values.ExternalSasContainers?.Select(x => new MountableContainer(x)).ToHashSet() ?? [];

            foreach (var container in containersToMount)
            {
                if (string.IsNullOrWhiteSpace(container.SasToken))
                {
                    internalContainersMIAuth.Add(container);
                }
                else
                {
                    sasContainers.Add(container);
                }
            }

            values.InternalContainersMIAuth = internalContainersMIAuth.Select(x => x.ToDictionary()).ToList();
            values.ExternalSasContainers = sasContainers.Select(x => x.ToDictionary()).ToList();
        }

        private void UpdateValuesFromSettings(HelmValues values, Dictionary<string, string> settings)
        {
            var batchAccount = GetObjectFromConfig(values, "batchAccount") ?? new Dictionary<string, string>();
            var batchNodes = GetObjectFromConfig(values, "batchNodes") ?? new Dictionary<string, string>();
            var batchScheduling = GetObjectFromConfig(values, "batchScheduling") ?? new Dictionary<string, string>();
            var batchImageGen2 = GetObjectFromConfig(values, "batchImageGen2") ?? new Dictionary<string, string>();
            var batchImageGen1 = GetObjectFromConfig(values, "batchImageGen1") ?? new Dictionary<string, string>();
            var martha = GetObjectFromConfig(values, "martha") ?? new Dictionary<string, string>();
            var deployment = GetObjectFromConfig(values, "deployment") ?? new Dictionary<string, string>();

            values.Config["cromwellOnAzureVersion"] = GetValueOrDefault(settings, "CromwellOnAzureVersion");
            values.Config["azureServicesAuthConnectionString"] = GetValueOrDefault(settings, "AzureServicesAuthConnectionString");
            values.Config["applicationInsightsAccountName"] = GetValueOrDefault(settings, "ApplicationInsightsAccountName");
            values.Config["azureCloudName"] = GetValueOrDefault(settings, "AzureCloudName");
            batchAccount["accountName"] = GetValueOrDefault(settings, "BatchAccountName");
            batchNodes["subnetId"] = GetValueOrDefault(settings, "BatchNodesSubnetId");
            values.Config["coaNamespace"] = GetValueOrDefault(settings, "AksCoANamespace");
            batchNodes["disablePublicIpAddress"] = GetValueOrDefault(settings, "DisableBatchNodesPublicIpAddress");
            batchScheduling["usePreemptibleVmsOnly"] = GetValueOrDefault(settings, "UsePreemptibleVmsOnly");
            batchImageGen2["offer"] = GetValueOrDefault(settings, "Gen2BatchImageOffer");
            batchImageGen2["publisher"] = GetValueOrDefault(settings, "Gen2BatchImagePublisher");
            batchImageGen2["sku"] = GetValueOrDefault(settings, "Gen2BatchImageSku");
            batchImageGen2["version"] = GetValueOrDefault(settings, "Gen2BatchImageVersion");
            batchImageGen2["nodeAgentSkuId"] = GetValueOrDefault(settings, "Gen2BatchNodeAgentSkuId");
            batchImageGen1["offer"] = GetValueOrDefault(settings, "Gen1BatchImageOffer");
            batchImageGen1["publisher"] = GetValueOrDefault(settings, "Gen1BatchImagePublisher");
            batchImageGen1["sku"] = GetValueOrDefault(settings, "Gen1BatchImageSku");
            batchImageGen1["version"] = GetValueOrDefault(settings, "Gen1BatchImageVersion");
            batchImageGen1["nodeAgentSkuId"] = GetValueOrDefault(settings, "Gen1BatchNodeAgentSkuId");
            martha["url"] = GetValueOrDefault(settings, "MarthaUrl");
            martha["keyVaultName"] = GetValueOrDefault(settings, "MarthaKeyVaultName");
            martha["secretName"] = GetValueOrDefault(settings, "MarthaSecretName");
            batchScheduling["prefix"] = GetValueOrDefault(settings, "BatchPrefix");
            values.Config["crossSubscriptionAKSDeployment"] = GetValueOrDefault(settings, "CrossSubscriptionAKSDeployment");
            values.Config["usePostgreSqlSingleServer"] = GetValueOrDefault(settings, "UsePostgreSqlSingleServer");

            values.Images["tes"] = GetValueOrDefault(settings, "TesImageName");
            values.Images["triggerservice"] = GetValueOrDefault(settings, "TriggerServiceImageName");
            values.Images["cromwell"] = GetValueOrDefault(settings, "CromwellImageName");

            values.Persistence["storageAccount"] = GetValueOrDefault(settings, "DefaultStorageAccountName");
            values.Persistence["executionsContainerName"] = GetValueOrDefault(settings, "ExecutionsContainerName");
            values.Persistence["storageEndpointSuffix"] = azureCloudConfig.Suffixes.StorageSuffix;

            values.TesDatabase["serverName"] = GetValueOrDefault(settings, "PostgreSqlServerName");
            values.TesDatabase["serverNameSuffix"] = GetValueOrDefault(settings, "PostgreSqlServerNameSuffix");
            values.TesDatabase["serverPort"] = GetValueOrDefault(settings, "PostgreSqlServerPort");
            values.TesDatabase["serverSslMode"] = GetValueOrDefault(settings, "PostgreSqlServerSslMode");
            // Note: Notice "Tes" is omitted from the property name since it's now in the TesDatabase section
            values.TesDatabase["databaseName"] = GetValueOrDefault(settings, "PostgreSqlTesDatabaseName");
            values.TesDatabase["databaseUserLogin"] = GetValueOrDefault(settings, "PostgreSqlTesDatabaseUserLogin");
            values.TesDatabase["databaseUserPassword"] = GetValueOrDefault(settings, "PostgreSqlTesDatabaseUserPassword");

            values.CromwellDatabase["serverName"] = GetValueOrDefault(settings, "PostgreSqlServerName");
            values.CromwellDatabase["serverNameSuffix"] = GetValueOrDefault(settings, "PostgreSqlServerNameSuffix");
            values.CromwellDatabase["serverPort"] = GetValueOrDefault(settings, "PostgreSqlServerPort");
            values.CromwellDatabase["serverSslMode"] = GetValueOrDefault(settings, "PostgreSqlServerSslMode");
            // Note: Notice "Cromwell" is omitted from the property name since it's now in the CromwellDatabase section
            values.CromwellDatabase["databaseName"] = GetValueOrDefault(settings, "PostgreSqlCromwellDatabaseName");
            values.CromwellDatabase["databaseUserLogin"] = GetValueOrDefault(settings, "PostgreSqlCromwellDatabaseUserLogin");
            values.CromwellDatabase["databaseUserPassword"] = GetValueOrDefault(settings, "PostgreSqlCromwellDatabaseUserPassword");
            deployment["organizationName"] = GetValueOrDefault(settings, "DeploymentOrganizationName");
            deployment["organizationUrl"] = GetValueOrDefault(settings, "DeploymentOrganizationUrl");
            deployment["contactUri"] = GetValueOrDefault(settings, "DeploymentContactUri");
            deployment["environment"] = GetValueOrDefault(settings, "DeploymentEnvironment");
            deployment["created"] = GetValueOrDefault(settings, "DeploymentCreated");
            deployment["updated"] = GetValueOrDefault(settings, "DeploymentUpdated");

            // ensure entries have values
            _ = batchNodes.TryAdd("contentMD5", "false");
            _ = batchScheduling.TryAdd("poolRotationForcedDays", "7");
            _ = batchScheduling.TryAdd("taskMaxWallClockTimeDays", "7");

            values.Config["batchAccount"] = batchAccount;
            values.Config["batchNodes"] = batchNodes;
            values.Config["batchScheduling"] = batchScheduling;
            values.Config["batchImageGen2"] = batchImageGen2;
            values.Config["batchImageGen1"] = batchImageGen1;
            values.Config["martha"] = martha;
            values.Config["deployment"] = deployment;
        }

        private static IDictionary<string, string> GetObjectFromConfig(HelmValues values, string key)
            => (values?.Config.TryGetValue(key, out var config) ?? false ? (config as IDictionary<object, object>) : null)?.ToDictionary(p => p.Key as string, p => p.Value as string);

        private static T GetValueOrDefault<T>(IDictionary<string, T> propertyBag, string key)
            => propertyBag.TryGetValue(key, out var value) ? value : default;

        private static Dictionary<string, string> ValuesToSettings(HelmValues values)
        {
            var batchAccount = GetObjectFromConfig(values, "batchAccount") ?? new Dictionary<string, string>();
            var batchNodes = GetObjectFromConfig(values, "batchNodes") ?? new Dictionary<string, string>();
            var batchScheduling = GetObjectFromConfig(values, "batchScheduling") ?? new Dictionary<string, string>();
            var batchImageGen2 = GetObjectFromConfig(values, "batchImageGen2") ?? new Dictionary<string, string>();
            var batchImageGen1 = GetObjectFromConfig(values, "batchImageGen1") ?? new Dictionary<string, string>();
            var martha = GetObjectFromConfig(values, "martha") ?? new Dictionary<string, string>();
            var deployment = GetObjectFromConfig(values, "deployment") ?? new Dictionary<string, string>();

            return new()
            {
                ["CromwellOnAzureVersion"] = GetValueOrDefault(values.Config, "cromwellOnAzureVersion") as string,
                ["AzureServicesAuthConnectionString"] = GetValueOrDefault(values.Config, "azureServicesAuthConnectionString") as string,
                ["ApplicationInsightsAccountName"] = GetValueOrDefault(values.Config, "applicationInsightsAccountName") as string,
                ["AzureCloudName"] = GetValueOrDefault(values.Config, "azureCloudName") as string,
                ["BatchAccountName"] = GetValueOrDefault(batchAccount, "accountName"),
                ["BatchNodesSubnetId"] = GetValueOrDefault(batchNodes, "subnetId"),
                ["AksCoANamespace"] = GetValueOrDefault(values.Config, "coaNamespace") as string,
                ["DisableBatchNodesPublicIpAddress"] = GetValueOrDefault(batchNodes, "disablePublicIpAddress"),
                ["UsePreemptibleVmsOnly"] = GetValueOrDefault(batchScheduling, "usePreemptibleVmsOnly"),
                ["Gen2BatchImageOffer"] = GetValueOrDefault(batchImageGen2, "offer"),
                ["Gen2BatchImagePublisher"] = GetValueOrDefault(batchImageGen2, "publisher"),
                ["Gen2BatchImageSku"] = GetValueOrDefault(batchImageGen2, "sku"),
                ["Gen2BatchImageVersion"] = GetValueOrDefault(batchImageGen2, "version"),
                ["Gen2BatchNodeAgentSkuId"] = GetValueOrDefault(batchImageGen2, "nodeAgentSkuId"),
                ["Gen1BatchImageOffer"] = GetValueOrDefault(batchImageGen1, "offer"),
                ["Gen1BatchImagePublisher"] = GetValueOrDefault(batchImageGen1, "publisher"),
                ["Gen1BatchImageSku"] = GetValueOrDefault(batchImageGen1, "sku"),
                ["Gen1BatchImageVersion"] = GetValueOrDefault(batchImageGen1, "version"),
                ["Gen1BatchNodeAgentSkuId"] = GetValueOrDefault(batchImageGen1, "nodeAgentSkuId"),
                ["MarthaUrl"] = GetValueOrDefault(martha, "url"),
                ["MarthaKeyVaultName"] = GetValueOrDefault(martha, "keyVaultName"),
                ["MarthaSecretName"] = GetValueOrDefault(martha, "secretName"),
                ["BatchPrefix"] = GetValueOrDefault(batchScheduling, "prefix"),
                ["CrossSubscriptionAKSDeployment"] = GetValueOrDefault(values.Config, "crossSubscriptionAKSDeployment") as string,
                ["UsePostgreSqlSingleServer"] = GetValueOrDefault(values.Config, "usePostgreSqlSingleServer") as string,
                ["ManagedIdentityClientId"] = GetValueOrDefault(values.Identity, "clientId"),
                ["TesImageName"] = GetValueOrDefault(values.Images, "tes"),
                ["TriggerServiceImageName"] = GetValueOrDefault(values.Images, "triggerservice"),
                ["CromwellImageName"] = GetValueOrDefault(values.Images, "cromwell"),
                ["DefaultStorageAccountName"] = GetValueOrDefault(values.Persistence, "storageAccount"),
                ["ExecutionsContainerName"] = GetValueOrDefault(values.Persistence, "executionsContainerName"),
                ["StorageEndpointSuffix"] = GetValueOrDefault(values.Persistence, "storageEndpointSuffix"),

                // This is only defined once, so use the TesDatabase values
                ["PostgreSqlServerName"] = GetValueOrDefault(values.TesDatabase, "serverName"),
                ["PostgreSqlServerNameSuffix"] = GetValueOrDefault(values.TesDatabase, "serverNameSuffix"),
                ["PostgreSqlServerPort"] = GetValueOrDefault(values.TesDatabase, "serverPort"),
                ["PostgreSqlServerSslMode"] = GetValueOrDefault(values.TesDatabase, "serverSslMode"),

                // Note: Notice "Tes" is added to the property name since it's coming from the TesDatabase section
                ["PostgreSqlTesDatabaseName"] = GetValueOrDefault(values.TesDatabase, "databaseName"),
                ["PostgreSqlTesDatabaseUserLogin"] = GetValueOrDefault(values.TesDatabase, "databaseUserLogin"),
                ["PostgreSqlTesDatabaseUserPassword"] = GetValueOrDefault(values.TesDatabase, "databaseUserPassword"),

                // Note: Notice "Cromwell" is added to the property name since it's coming from the TesDatabase section
                ["PostgreSqlCromwellDatabaseName"] = GetValueOrDefault(values.CromwellDatabase, "databaseName"),
                ["PostgreSqlCromwellDatabaseUserLogin"] = GetValueOrDefault(values.CromwellDatabase, "databaseUserLogin"),
                ["PostgreSqlCromwellDatabaseUserPassword"] = GetValueOrDefault(values.CromwellDatabase, "databaseUserPassword"),

                ["DeploymentOrganizationName"] = GetValueOrDefault(deployment, "organizationName"),
                ["DeploymentOrganizationUrl"] = GetValueOrDefault(deployment, "organizationUrl"),
                ["DeploymentContactUri"] = GetValueOrDefault(deployment, "contactUri"),
                ["DeploymentEnvironment"] = GetValueOrDefault(deployment, "environment"),
                ["DeploymentCreated"] = GetValueOrDefault(deployment, "created"),
                ["DeploymentUpdated"] = GetValueOrDefault(deployment, "updated"),
            };
        }

        private async Task<string> ExecHelmProcessAsync(string command, string workingDirectory = null, bool throwOnNonZeroExitCode = true)
        {
            var outputStringBuilder = new StringBuilder();

            void OutputHandler(object sendingProcess, DataReceivedEventArgs outLine)
            {
                if (configuration.DebugLogging)
                {
                    ConsoleEx.WriteLine($"HELM: {outLine.Data}");
                }

                outputStringBuilder.AppendLine(outLine.Data);
            }

            var process = new Process();

            try
            {
                process.StartInfo.UseShellExecute = false;
                process.StartInfo.RedirectStandardOutput = true;
                process.StartInfo.RedirectStandardError = true;
                process.StartInfo.FileName = configuration.HelmBinaryPath;
                process.StartInfo.Arguments = command;
                process.OutputDataReceived += OutputHandler;
                process.ErrorDataReceived += OutputHandler;

                if (!string.IsNullOrWhiteSpace(workingDirectory))
                {
                    process.StartInfo.WorkingDirectory = workingDirectory;
                }

                process.Start();
                process.BeginOutputReadLine();
                process.BeginErrorReadLine();
                await process.WaitForExitAsync(cancellationToken);
            }
            finally
            {
                if (cancellationToken.IsCancellationRequested && !process.HasExited)
                {
                    process.Kill();
                }
            }

            var output = outputStringBuilder.ToString();

            if (throwOnNonZeroExitCode && process.ExitCode != 0)
            {
                if (!configuration.DebugLogging) // already written to console
                {
                    foreach (var line in output.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries))
                    {
                        ConsoleEx.WriteLine($"HELM: {line}");
                    }
                }

                Debugger.Break();
                throw new Exception($"HELM ExitCode = {process.ExitCode}");
            }

            return output;
        }

        private static async Task<bool> WaitForWorkloadAsync(IKubernetes client, string deploymentName, string aksNamespace, CancellationToken cancellationToken)
        {
            var result = await WorkloadReadyRetryPolicy.ExecuteAndCaptureAsync(async token =>
            {
                var deployments = await client.AppsV1.ListNamespacedDeploymentAsync(aksNamespace, cancellationToken: token);
                var deployment = deployments.Items.Where(x => x.Metadata.Name.Equals(deploymentName, StringComparison.OrdinalIgnoreCase)).FirstOrDefault();

                if ((deployment?.Status?.ReadyReplicas ?? 0) < 1)
                {
                    throw new Exception("Workload not ready.");
                }
            }, cancellationToken);

            return result.Outcome == OutcomeType.Successful;
        }

        private class HelmValues
        {
            public Dictionary<string, string> Service { get; set; }
            public Dictionary<string, object> Config { get; set; }
            public Dictionary<string, string> TesDatabase { get; set; }
            public Dictionary<string, string> CromwellDatabase { get; set; }
            public Dictionary<string, string> Images { get; set; }
            public List<string> CromwellContainers { get; set; }
            public List<string> DefaultContainers { get; set; }
            public List<Dictionary<string, string>> InternalContainersMIAuth { get; set; }
            public List<Dictionary<string, string>> InternalContainersKeyVaultAuth { get; set; }
            public List<Dictionary<string, string>> ExternalContainers { get; set; }
            public List<Dictionary<string, string>> ExternalSasContainers { get; set; }
            public Dictionary<string, string> Persistence { get; set; }
            public Dictionary<string, string> Identity { get; set; }
            public Dictionary<string, string> Db { get; set; }
        }
    }
}
