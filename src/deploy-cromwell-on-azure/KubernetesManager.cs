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
using k8s;
using k8s.Models;
using Microsoft.Azure.Management.ContainerService;
using Microsoft.Azure.Management.ContainerService.Fluent;
using Microsoft.Azure.Management.Msi.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Microsoft.Azure.Management.Storage.Fluent;
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
            .WaitAndRetryAsync(40, retryAttempt => TimeSpan.FromSeconds(15));

        private static readonly AsyncRetryPolicy KubeExecRetryPolicy = Policy
            .Handle<WebSocketException>(ex => ex.WebSocketErrorCode == WebSocketError.NotAWebSocket)
            .WaitAndRetryAsync(8, retryAttempt => TimeSpan.FromSeconds(5));

        // "master" is used despite not being a best practice: https://github.com/kubernetes-sigs/blob-csi-driver/issues/783
        private const string BlobCsiDriverGithubReleaseBranch = "master";
        private const string BlobCsiDriverGithubReleaseVersion = "v1.18.0";
        private const string BlobCsiRepo = $"https://raw.githubusercontent.com/kubernetes-sigs/blob-csi-driver/{BlobCsiDriverGithubReleaseBranch}/charts";
        private const string AadPluginGithubReleaseVersion = "v1.8.13";
        private const string AadPluginRepo = $"https://raw.githubusercontent.com/Azure/aad-pod-identity/{AadPluginGithubReleaseVersion}/charts";
        private const string AadPluginVersion = "4.1.14";

        private Configuration configuration { get; set; }
        private AzureCredentials azureCredentials { get; set; }
        private CancellationTokenSource cts { get; set; }
        private string workingDirectoryTemp { get; set; }
        private string kubeConfigPath { get; set; }
        private string valuesTemplatePath { get; set; }
        public string helmScriptsRootDirectory { get; set; }
        public string TempHelmValuesYamlPath { get; set; }

        public KubernetesManager(Configuration config, AzureCredentials credentials, CancellationTokenSource cts)
        {
            this.cts = cts;
            configuration = config;
            azureCredentials = credentials;

            CreateAndInitializeWorkingDirectoriesAsync().Wait();
        }

        public async Task<IKubernetes> GetKubernetesClientAsync(IResource resourceGroupObject)
        {
            var resourceGroup = resourceGroupObject.Name;
            var containerServiceClient = new ContainerServiceClient(azureCredentials) { SubscriptionId = configuration.SubscriptionId };

            // Write kubeconfig in the working directory, because KubernetesClientConfiguration needs to read from a file, TODO figure out how to pass this directly. 
            var creds = await containerServiceClient.ManagedClusters.ListClusterAdminCredentialsAsync(resourceGroup, configuration.AksClusterName);
            var kubeConfigFile = new FileInfo(kubeConfigPath);
            await File.WriteAllTextAsync(kubeConfigFile.FullName, Encoding.Default.GetString(creds.Kubeconfigs.First().Value));

            var k8sConfiguration = KubernetesClientConfiguration.LoadKubeConfig(kubeConfigFile, false);
            var k8sClientConfiguration = KubernetesClientConfiguration.BuildConfigFromConfigObject(k8sConfiguration);
            return new Kubernetes(k8sClientConfiguration);
        }

        public static V1Deployment GetUbuntuDeploymentTemplate()
        {
            return KubernetesYaml.Deserialize<V1Deployment>(
                """
                apiVersion: apps/v1
                kind: Deployment
                metadata:
                  creationTimestamp: null
                  labels:
                    io.kompose.service: ubuntu
                  name: ubuntu
                spec:
                  replicas: 1
                  selector:
                    matchLabels:
                      io.kompose.service: ubuntu
                  strategy: {}
                  template:
                    metadata:
                      creationTimestamp: null
                      labels:
                        io.kompose.service: ubuntu
                    spec:
                      containers:
                        - name: ubuntu
                          image: mcr.microsoft.com/mirror/docker/library/ubuntu:22.04
                          command: [ "/bin/bash", "-c", "--" ]
                          args: [ "while true; do sleep 30; done;" ]
                          resources: {}
                      restartPolicy: Always
                status: {}
                """);
        }

        public async Task DeployCoADependenciesAsync()
        {
            var helmRepoList = await ExecHelmProcessAsync($"repo list", workingDirectory: null, throwOnNonZeroExitCode: false);

            if (string.IsNullOrWhiteSpace(helmRepoList) || !helmRepoList.Contains("aad-pod-identity", StringComparison.OrdinalIgnoreCase))
            {
                await ExecHelmProcessAsync($"repo add aad-pod-identity {AadPluginRepo}");
            }

            if (string.IsNullOrWhiteSpace(helmRepoList) || !helmRepoList.Contains("blob-csi-driver", StringComparison.OrdinalIgnoreCase))
            {
                await ExecHelmProcessAsync($"repo add blob-csi-driver {BlobCsiRepo}");
            }

            await ExecHelmProcessAsync($"repo update");
            await ExecHelmProcessAsync($"install aad-pod-identity aad-pod-identity/aad-pod-identity --namespace kube-system --version {AadPluginVersion} --kubeconfig {kubeConfigPath}");
            await ExecHelmProcessAsync($"install blob-csi-driver blob-csi-driver/blob-csi-driver --set node.enableBlobfuseProxy=true --namespace kube-system --version {BlobCsiDriverGithubReleaseVersion} --kubeconfig {kubeConfigPath}");
        }

        public async Task DeployHelmChartToClusterAsync()
            // https://helm.sh/docs/helm/helm_upgrade/
            // The chart argument can be either: a chart reference('example/mariadb'), a path to a chart directory, a packaged chart, or a fully qualified URL
            => await ExecHelmProcessAsync($"upgrade --install cromwellonazure ./helm --kubeconfig {kubeConfigPath} --namespace {configuration.AksCoANamespace} --create-namespace",
                workingDirectory: workingDirectoryTemp);

        public async Task UpdateHelmValuesAsync(IStorageAccount storageAccount, string keyVaultUrl, string resourceGroupName, Dictionary<string, string> settings, IIdentity managedId, List<MountableContainer> containersToMount)
        {
            var values = KubernetesYaml.Deserialize<HelmValues>(await File.ReadAllTextAsync(valuesTemplatePath));
            UpdateValuesFromSettings(values, settings);
            values.Config["resourceGroup"] = resourceGroupName;
            values.Identity["name"] = managedId.Name;
            values.Identity["resourceId"] = managedId.Id;
            values.Identity["clientId"] = managedId.ClientId;

            if (configuration.CrossSubscriptionAKSDeployment.GetValueOrDefault())
            {
                values.InternalContainersKeyVaultAuth = new List<Dictionary<string, string>>();

                foreach (var container in values.DefaultContainers)
                {
                    var containerConfig = new Dictionary<string, string>()
                    {
                        { "accountName",  storageAccount.Name },
                        { "containerName", container },
                        { "keyVaultURL", keyVaultUrl },
                        { "keyVaultSecretName", Deployer.StorageAccountKeySecretName}
                    };

                    values.InternalContainersKeyVaultAuth.Add(containerConfig);
                }
            }
            else
            {
                values.InternalContainersMIAuth = new List<Dictionary<string, string>>();

                foreach (var container in values.DefaultContainers)
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
            var valuesString = KubernetesYaml.Serialize(values);
            await File.WriteAllTextAsync(TempHelmValuesYamlPath, valuesString);
            await Deployer.UploadTextToStorageAccountAsync(storageAccount, Deployer.ConfigurationContainerName, "aksValues.yaml", valuesString, cts.Token);
        }

        

        public async Task UpgradeValuesYamlAsync(IStorageAccount storageAccount, Dictionary<string, string> settings, List<MountableContainer> containersToMount)
        {
            var values = KubernetesYaml.Deserialize<HelmValues>(await Deployer.DownloadTextFromStorageAccountAsync(storageAccount, Deployer.ConfigurationContainerName, "aksValues.yaml", cts));
            UpdateValuesFromSettings(values, settings);
            MergeContainers(containersToMount, values);
            var valuesString = KubernetesYaml.Serialize(values);
            await File.WriteAllTextAsync(TempHelmValuesYamlPath, valuesString);
            await Deployer.UploadTextToStorageAccountAsync(storageAccount, Deployer.ConfigurationContainerName, "aksValues.yaml", valuesString, cts.Token);
        }

        public async Task<Dictionary<string, string>> GetAKSSettingsAsync(IStorageAccount storageAccount)
        {
            var values = KubernetesYaml.Deserialize<HelmValues>(await Deployer.DownloadTextFromStorageAccountAsync(storageAccount, Deployer.ConfigurationContainerName, "aksValues.yaml", cts));
            return ValuesToSettings(values);
        }

        public async Task ExecuteCommandsOnPodAsync(IKubernetes client, string podName, IEnumerable<string[]> commands, string aksNamespace)
        {
            var printHandler = new ExecAsyncCallback(async (stdIn, stdOut, stdError) =>
            {
                using (var reader = new StreamReader(stdOut))
                {
                    var line = await reader.ReadLineAsync();

                    while (line is not null)
                    {
                        if (configuration.DebugLogging)
                        {
                            ConsoleEx.WriteLine(podName + ": " + line);
                        }
                        line = await reader.ReadLineAsync();
                    }
                }

                using (var reader = new StreamReader(stdError))
                {
                    var line = await reader.ReadLineAsync();

                    while (line is not null)
                    {
                        if (configuration.DebugLogging)
                        {
                            ConsoleEx.WriteLine(podName + ": " + line);
                        }
                        line = await reader.ReadLineAsync();
                    }
                }
            });

            var pods = await client.CoreV1.ListNamespacedPodAsync(aksNamespace);
            var workloadPod = pods.Items.Where(x => x.Metadata.Name.Contains(podName)).FirstOrDefault();

            if (!await WaitForWorkloadAsync(client, podName, aksNamespace, cts.Token))
            {
                throw new Exception($"Timed out waiting for {podName} to start.");
            }

            // Pod Exec can fail even after the pod is marked ready.
            // Retry on WebSocketExceptions for up to 40 secs.
            var result = await KubeExecRetryPolicy.ExecuteAndCaptureAsync(async () =>
            {
                foreach (var command in commands)
                {
                    await client.NamespacedPodExecAsync(workloadPod.Metadata.Name, aksNamespace, podName, command, true, printHandler, CancellationToken.None);
                }
            });

            if (result.Outcome != OutcomeType.Successful && result.FinalException is not null)
            {
                throw result.FinalException;
            }
        }

        public async Task WaitForCromwellAsync(IKubernetes client)
        {
            if (!await WaitForWorkloadAsync(client, "cromwell", configuration.AksCoANamespace, cts.Token))
            {
                throw new Exception("Timed out waiting for Cromwell to start.");
            }
        }

        public async Task UpgradeAKSDeploymentAsync(Dictionary<string, string> settings, IStorageAccount storageAccount, List<MountableContainer> containersToMount)
        {
            await UpgradeValuesYamlAsync(storageAccount, settings, containersToMount);
            await DeployHelmChartToClusterAsync();
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
                await Utility.WriteEmbeddedFilesAsync(helmScriptsRootDirectory, "scripts", "helm");
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

            var internalContainersMIAuth = values.InternalContainersMIAuth.Select(x => new MountableContainer(x)).ToHashSet();
            var sasContainers = values.ExternalSasContainers.Select(x => new MountableContainer(x)).ToHashSet();

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

        private static void UpdateValuesFromSettings(HelmValues values, Dictionary<string, string> settings)
        {
            var batchAccount = values.Config["batchAccount"] as IDictionary<string, string> ?? new Dictionary<string, string>();
            var batchNodes = values.Config["batchNodes"] as IDictionary<string, string> ?? new Dictionary<string, string>();
            var batchScheduling = values.Config["batchScheduling"] as IDictionary<string, string> ?? new Dictionary<string, string>();
            var nodeImages = values.Config["nodeImages"] as IDictionary<string, string> ?? new Dictionary<string, string>();
            var batchImageGen2 = values.Config["batchImageGen2"] as IDictionary<string, string> ?? new Dictionary<string, string>();
            var batchImageGen1 = values.Config["batchImageGen1"] as IDictionary<string, string> ?? new Dictionary<string, string>();
            var martha = values.Config["martha"] as IDictionary<string, string> ?? new Dictionary<string, string>();

            values.Config["cromwellOnAzureVersion"] = settings["CromwellOnAzureVersion"];
            values.Config["azureServicesAuthConnectionString"] = settings["AzureServicesAuthConnectionString"];
            values.Config["applicationInsightsAccountName"] = settings["ApplicationInsightsAccountName"];
            batchAccount["accountName"] = settings["BatchAccountName"];
            batchNodes["subnetId"] = settings["BatchNodesSubnetId"];
            values.Config["coaNamespace"] = settings["AksCoANamespace"];
            batchNodes["disablePublicIpAddress"] = settings["DisableBatchNodesPublicIpAddress"];
            batchScheduling["disable"] = settings["DisableBatchScheduling"];
            batchScheduling["usePreemptibleVmsOnly"] = settings["UsePreemptibleVmsOnly"];
            nodeImages["blobxfer"] = settings["BlobxferImageName"];
            nodeImages["docker"] = settings["DockerInDockerImageName"];
            batchImageGen2["offer"] = settings["Gen2BatchImageOffer"];
            batchImageGen2["publisher"] = settings["Gen2BatchImagePublisher"];
            batchImageGen2["sku"] = settings["Gen2BatchImageSku"];
            batchImageGen2["version"] = settings["Gen2BatchImageVersion"];
            batchImageGen2["nodeAgentSkuId"] = settings["Gen2BatchNodeAgentSkuId"];
            batchImageGen1["offer"] = settings["Gen1BatchImageOffer"];
            batchImageGen1["publisher"] = settings["Gen1BatchImagePublisher"];
            batchImageGen1["sku"] = settings["Gen1BatchImageSku"];
            batchImageGen1["version"] = settings["Gen1BatchImageVersion"];
            batchImageGen1["nodeAgentSkuId"] = settings["BatchNodeAgentSkuId"];
            martha["url"] = settings["MarthaUrl"];
            martha["keyVaultName"] = settings["MarthaKeyVaultName"];
            martha["secretName"] = settings["MarthaSecretName"];
            batchScheduling["prefix"] = settings["BatchPrefix"];
            values.Config["crossSubscriptionAKSDeployment"] = settings["CrossSubscriptionAKSDeployment"];
            values.Config["usePostgreSqlSingleServer"] = settings["UsePostgreSqlSingleServer"];

            values.Images["tes"] = settings["TesImageName"];
            values.Images["triggerservice"] = settings["TriggerServiceImageName"];
            values.Images["cromwell"] = settings["CromwellImageName"];

            values.Persistence["storageAccount"] = settings["DefaultStorageAccountName"];

            values.TesDatabase["serverName"] = settings["PostgreSqlServerName"];
            values.TesDatabase["serverNameSuffix"] = settings["PostgreSqlServerNameSuffix"];
            values.TesDatabase["serverPort"] = settings["PostgreSqlServerPort"];
            values.TesDatabase["serverSslMode"] = settings["PostgreSqlServerSslMode"];
            // Note: Notice "Tes" is omitted from the property name since it's now in the TesDatabase section
            values.TesDatabase["databaseName"] = settings["PostgreSqlTesDatabaseName"];
            values.TesDatabase["databaseUserLogin"] = settings["PostgreSqlTesDatabaseUserLogin"];
            values.TesDatabase["databaseUserPassword"] = settings["PostgreSqlTesDatabaseUserPassword"];

            values.CromwellDatabase["serverName"] = settings["PostgreSqlServerName"];
            values.CromwellDatabase["serverNameSuffix"] = settings["PostgreSqlServerNameSuffix"];
            values.CromwellDatabase["serverPort"] = settings["PostgreSqlServerPort"];
            values.CromwellDatabase["serverSslMode"] = settings["PostgreSqlServerSslMode"];
            // Note: Notice "Cromwell" is omitted from the property name since it's now in the CromwellDatabase section
            values.CromwellDatabase["databaseName"] = settings["PostgreSqlCromwellDatabaseName"];
            values.CromwellDatabase["databaseUserLogin"] = settings["PostgreSqlCromwellDatabaseUserLogin"];
            values.CromwellDatabase["databaseUserPassword"] = settings["PostgreSqlCromwellDatabaseUserPassword"];

            values.Config["batchAccount"] = batchAccount;
            values.Config["batchNodes"] = batchNodes;
            values.Config["batchScheduling"] = batchScheduling;
            values.Config["nodeImages"] = nodeImages;
            values.Config["batchImageGen2"] = batchImageGen2;
            values.Config["batchImageGen1"] = batchImageGen1;
            values.Config["martha"] = martha;
        }

        private static Dictionary<string, string> ValuesToSettings(HelmValues values)
        {
            var batchAccount = values.Config["batchAccount"] as IDictionary<string, string> ?? new Dictionary<string, string>();
            var batchNodes = values.Config["batchNodes"] as IDictionary<string, string> ?? new Dictionary<string, string>();
            var batchScheduling = values.Config["batchScheduling"] as IDictionary<string, string> ?? new Dictionary<string, string>();
            var nodeImages = values.Config["nodeImages"] as IDictionary<string, string> ?? new Dictionary<string, string>();
            var batchImageGen2 = values.Config["batchImageGen2"] as IDictionary<string, string> ?? new Dictionary<string, string>();
            var batchImageGen1 = values.Config["batchImageGen1"] as IDictionary<string, string> ?? new Dictionary<string, string>();
            var martha = values.Config["martha"] as IDictionary<string, string> ?? new Dictionary<string, string>();

            return new()
            {
                ["CromwellOnAzureVersion"] = values.Config["cromwellOnAzureVersion"] as string,
                ["AzureServicesAuthConnectionString"] = values.Config["azureServicesAuthConnectionString"] as string,
                ["ApplicationInsightsAccountName"] = values.Config["applicationInsightsAccountName"] as string,
                ["BatchAccountName"] = batchAccount["accountName"],
                ["BatchNodesSubnetId"] = batchNodes["subnetId"],
                ["AksCoANamespace"] = values.Config["coaNamespace"] as string,
                ["DisableBatchNodesPublicIpAddress"] = batchNodes["disablePublicIpAddress"],
                ["DisableBatchScheduling"] = batchScheduling["disable"],
                ["UsePreemptibleVmsOnly"] = batchScheduling["usePreemptibleVmsOnly"],
                ["BlobxferImageName"] = nodeImages["blobxfer"],
                ["DockerInDockerImageName"] = nodeImages["docker"],
                ["Gen2BatchImageOffer"] = batchImageGen2["offer"],
                ["Gen2BatchImagePublisher"] = batchImageGen2["publisher"],
                ["Gen2BatchImageSku"] = batchImageGen2["sku"],
                ["Gen2BatchImageVersion"] = batchImageGen2["version"],
                ["Gen2BatchNodeAgentSkuId"] = batchImageGen2["nodeAgentSkuId"],
                ["Gen1BatchImageOffer"] = batchImageGen1["offer"],
                ["Gen1BatchImagePublisher"] = batchImageGen1["publisher"],
                ["Gen1BatchImageSku"] = batchImageGen1["sku"],
                ["Gen1BatchImageVersion"] = batchImageGen1["version"],
                ["BatchNodeAgentSkuId"] = batchImageGen1["nodeAgentSkuId"],
                ["MarthaUrl"] = martha["url"],
                ["MarthaKeyVaultName"] = martha["keyVaultName"],
                ["MarthaSecretName"] = martha["secretName"],
                ["BatchPrefix"] = batchScheduling["prefix"],
                ["CrossSubscriptionAKSDeployment"] = values.Config["crossSubscriptionAKSDeployment"] as string,
                ["UsePostgreSqlSingleServer"] = values.Config["usePostgreSqlSingleServer"] as string,
                ["ManagedIdentityClientId"] = values.Identity["clientId"],
                ["TesImageName"] = values.Images["tes"],
                ["TriggerServiceImageName"] = values.Images["triggerservice"],
                ["CromwellImageName"] = values.Images["cromwell"],
                ["DefaultStorageAccountName"] = values.Persistence["storageAccount"],

                // This is only defined once, so use the TesDatabase values
                ["PostgreSqlServerName"] = values.TesDatabase["serverName"],
                ["PostgreSqlServerNameSuffix"] = values.TesDatabase["serverNameSuffix"],
                ["PostgreSqlServerPort"] = values.TesDatabase["serverPort"],
                ["PostgreSqlServerSslMode"] = values.TesDatabase["serverSslMode"],

                // Note: Notice "Tes" is added to the property name since it's coming from the TesDatabase section
                ["PostgreSqlTesDatabaseName"] = values.TesDatabase["databaseName"],
                ["PostgreSqlTesDatabaseUserLogin"] = values.TesDatabase["databaseUserLogin"],
                ["PostgreSqlTesDatabaseUserPassword"] = values.TesDatabase["databaseUserPassword"],

                // Note: Notice "Cromwell" is added to the property name since it's coming from the TesDatabase section
                ["PostgreSqlCromwellDatabaseName"] = values.CromwellDatabase["databaseName"],
                ["PostgreSqlCromwellDatabaseUserLogin"] = values.CromwellDatabase["databaseUserLogin"],
                ["PostgreSqlCromwellDatabaseUserPassword"] = values.CromwellDatabase["databaseUserPassword"],
            };
        }

        private async Task<string> ExecHelmProcessAsync(string command, string workingDirectory = null, bool throwOnNonZeroExitCode = true)
        {
            var process = new Process();
            process.StartInfo.UseShellExecute = false;
            process.StartInfo.RedirectStandardOutput = true;
            process.StartInfo.RedirectStandardError = true;
            process.StartInfo.FileName = configuration.HelmBinaryPath;
            process.StartInfo.Arguments = command;

            if (!string.IsNullOrWhiteSpace(workingDirectory))
            {
                process.StartInfo.WorkingDirectory = workingDirectory;
            }

            process.Start();

            var outputStringBuilder = new StringBuilder();

            _ = Task.Run(async () =>
            {
                var line = (await process.StandardOutput.ReadLineAsync())?.Trim();

                while (line is not null)
                {
                    if (configuration.DebugLogging)
                    {
                        ConsoleEx.WriteLine($"HELM: {line}");
                    }

                    outputStringBuilder.AppendLine(line);
                    line = await process.StandardOutput.ReadLineAsync().WaitAsync(cts.Token);
                }
            });

            _ = Task.Run(async () =>
            {
                var line = (await process.StandardError.ReadLineAsync())?.Trim();

                while (line is not null)
                {
                    if (configuration.DebugLogging)
                    {
                        ConsoleEx.WriteLine($"HELM: {line}");
                    }

                    outputStringBuilder.AppendLine(line);
                    line = await process.StandardError.ReadLineAsync().WaitAsync(cts.Token);
                }
            });

            await process.WaitForExitAsync();
            var output = outputStringBuilder.ToString();

            if (throwOnNonZeroExitCode && process.ExitCode != 0)
            {
                foreach (var line in output.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries))
                {
                    ConsoleEx.WriteLine($"HELM: {line}");
                }

                Debugger.Break();
                throw new Exception($"HELM ExitCode = {process.ExitCode}");
            }

            return output;
        }

        private static async Task<bool> WaitForWorkloadAsync(IKubernetes client, string deploymentName, string aksNamespace, CancellationToken cancellationToken)
        {
            var deployments = await client.AppsV1.ListNamespacedDeploymentAsync(aksNamespace, cancellationToken: cancellationToken);
            var deployment = deployments.Items.Where(x => x.Metadata.Name.Equals(deploymentName, StringComparison.OrdinalIgnoreCase)).FirstOrDefault();

            var result = await WorkloadReadyRetryPolicy.ExecuteAndCaptureAsync(async () =>
            {
                deployments = await client.AppsV1.ListNamespacedDeploymentAsync(aksNamespace, cancellationToken: cancellationToken);
                deployment = deployments.Items.Where(x => x.Metadata.Name.Equals(deploymentName, StringComparison.OrdinalIgnoreCase)).FirstOrDefault();

                if ((deployment?.Status?.ReadyReplicas ?? 0) < 1)
                {
                    throw new Exception("Workload not ready.");
                }
            });

            return result.Outcome == OutcomeType.Successful;
        }

        private class HelmValues
        {
            public Dictionary<string, string> Service { get; set; }
            public Dictionary<string, object> Config { get; set; }
            public Dictionary<string, string> TesDatabase { get; set; }
            public Dictionary<string, string> CromwellDatabase { get; set; }
            public Dictionary<string, string> Images { get; set; }
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
