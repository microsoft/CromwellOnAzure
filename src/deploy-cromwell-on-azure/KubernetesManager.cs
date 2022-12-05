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
            .WaitAndRetryAsync(12, retryAttempt => TimeSpan.FromSeconds(15));

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
        private string helmScriptsRootDirectory { get; set; }
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
        {
            // https://helm.sh/docs/helm/helm_upgrade/
            // The chart argument can be either: a chart reference('example/mariadb'), a path to a chart directory, a packaged chart, or a fully qualified URL
            await ExecHelmProcessAsync($"upgrade --install cromwellonazure ./helm --kubeconfig {kubeConfigPath} --namespace {configuration.AksCoANamespace} --create-namespace",
                workingDirectory: workingDirectoryTemp);
        }

        public async Task UpdateHelmValuesAsync(IStorageAccount storageAccount, string keyVaultUrl, string resourceGroupName, Dictionary<string, string> settings, IIdentity managedId)
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

            var valuesString = KubernetesYaml.Serialize(values);
            await File.WriteAllTextAsync(TempHelmValuesYamlPath, valuesString);
            await Deployer.UploadTextToStorageAccountAsync(storageAccount, Deployer.ConfigurationContainerName, "aksValues.yaml", valuesString, cts.Token);
        }

        public async Task UpgradeValuesYamlAsync(IStorageAccount storageAccount, Dictionary<string, string> settings)
        {
            var values = KubernetesYaml.Deserialize<HelmValues>(await Deployer.DownloadTextFromStorageAccountAsync(storageAccount, Deployer.ConfigurationContainerName, "aksValues.yaml", cts));
            UpdateValuesFromSettings(values, settings);
            var valuesString = KubernetesYaml.Serialize(values);
            await File.WriteAllTextAsync(TempHelmValuesYamlPath, valuesString);
            await Deployer.UploadTextToStorageAccountAsync(storageAccount, Deployer.ConfigurationContainerName, "aksValues.yaml", valuesString, cts.Token);
        }

        public async Task<Dictionary<string, string>> GetAKSSettingsAsync(IStorageAccount storageAccount)
        {
            var values = KubernetesYaml.Deserialize<HelmValues>(await Deployer.DownloadTextFromStorageAccountAsync(storageAccount, Deployer.ConfigurationContainerName, "aksValues.yaml", cts));
            return ValuesToSettings(values);
        }

        public async Task ExecuteCommandsOnPodAsync(IKubernetes client, string podName, IEnumerable<string[]> commands)
        {
            var printHandler = new ExecAsyncCallback(async (stdIn, stdOut, stdError) =>
            {
                using (var reader = new StreamReader(stdOut))
                {
                    var line = await reader.ReadLineAsync();

                    while (line != null)
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

                    while (line != null)
                    {
                        if (configuration.DebugLogging)
                        {
                            ConsoleEx.WriteLine(podName + ": " + line);
                        }
                        line = await reader.ReadLineAsync();
                    }
                }
            });

            var pods = await client.CoreV1.ListNamespacedPodAsync(configuration.AksCoANamespace);
            var workloadPod = pods.Items.Where(x => x.Metadata.Name.Contains(podName)).FirstOrDefault();

            if (!await WaitForWorkloadAsync(client, podName, cts.Token))
            {
                throw new Exception($"Timed out waiting for {podName} to start.");
            }

            // Pod Exec can fail even after the pod is marked ready.
            // Retry on WebSocketExceptions for up to 40 secs. 
            var result = await KubeExecRetryPolicy.ExecuteAndCaptureAsync(async () =>
            {
                foreach (var command in commands)
                {
                    await client.NamespacedPodExecAsync(workloadPod.Metadata.Name, configuration.AksCoANamespace, podName, command, true, printHandler, CancellationToken.None);
                }
            });

            if (result.Outcome != OutcomeType.Successful && result.FinalException != null)
            {
                throw result.FinalException;
            }
        }

        public async Task WaitForCromwellAsync(IKubernetes client)
        {
            if (!await WaitForWorkloadAsync(client, "cromwell", cts.Token))
            {
                throw new Exception("Timed out waiting for Cromwell to start.");
            }
        }

        public async Task UpgradeAKSDeploymentAsync(Dictionary<string, string> settings, IStorageAccount storageAccount)
        {
            await UpgradeValuesYamlAsync(storageAccount, settings);
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

        private static void UpdateValuesFromSettings(HelmValues values, Dictionary<string, string> settings)
        {
            values.Config["cromwellOnAzureVersion"] = settings["CromwellOnAzureVersion"];
            values.Config["azureServicesAuthConnectionString"] = settings["AzureServicesAuthConnectionString"];
            values.Config["applicationInsightsAccountName"] = settings["ApplicationInsightsAccountName"];
            values.Config["cosmosDbAccountName"] = settings["CosmosDbAccountName"];
            values.Config["batchAccountName"] = settings["BatchAccountName"];
            values.Config["batchNodesSubnetId"] = settings["BatchNodesSubnetId"];
            values.Config["coaNamespace"] = settings["AksCoANamespace"];
            values.Config["disableBatchNodesPublicIpAddress"] = settings["DisableBatchNodesPublicIpAddress"];
            values.Config["disableBatchScheduling"] = settings["DisableBatchScheduling"];
            values.Config["usePreemptibleVmsOnly"] = settings["UsePreemptibleVmsOnly"];
            values.Config["blobxferImageName"] = settings["BlobxferImageName"];
            values.Config["dockerInDockerImageName"] = settings["DockerInDockerImageName"];
            values.Config["batchImageOffer"] = settings["BatchImageOffer"];
            values.Config["batchImagePublisher"] = settings["BatchImagePublisher"];
            values.Config["batchImageSku"] = settings["BatchImageSku"];
            values.Config["batchImageVersion"] = settings["BatchImageVersion"];
            values.Config["batchNodeAgentSkuId"] = settings["BatchNodeAgentSkuId"];
            values.Config["marthaUrl"] = settings["MarthaUrl"];
            values.Config["marthaKeyVaultName"] = settings["MarthaKeyVaultName"];
            values.Config["marthaSecretName"] = settings["MarthaSecretName"];
            values.Config["crossSubscriptionAKSDeployment"] = settings["CrossSubscriptionAKSDeployment"];
            values.Config["postgreSqlServerName"] = settings["PostgreSqlServerName"];
            values.Config["postgreSqlDatabaseName"] = settings["PostgreSqlDatabaseName"];
            values.Config["postgreSqlUserLogin"] = settings["PostgreSqlUserLogin"];
            values.Config["postgreSqlUserPassword"] = settings["PostgreSqlUserPassword"];
            values.Config["usePostgreSqlSingleServer"] = settings["UsePostgreSqlSingleServer"];
            values.Images["tes"] = settings["TesImageName"];
            values.Images["triggerservice"] = settings["TriggerServiceImageName"];
            values.Images["cromwell"] = settings["CromwellImageName"];
            values.Persistence["storageAccount"] = settings["DefaultStorageAccountName"];
        }

        private static Dictionary<string, string> ValuesToSettings(HelmValues values)
        {
            var settings = new Dictionary<string, string>();
            settings["CromwellOnAzureVersion"] = values.Config["cromwellOnAzureVersion"];
            settings["AzureServicesAuthConnectionString"] = values.Config["azureServicesAuthConnectionString"];
            settings["ApplicationInsightsAccountName"] = values.Config["applicationInsightsAccountName"];
            settings["CosmosDbAccountName"] = values.Config["cosmosDbAccountName"];
            settings["BatchAccountName"] = values.Config["batchAccountName"];
            settings["BatchNodesSubnetId"] = values.Config["batchNodesSubnetId"];
            settings["AksCoANamespace"] = values.Config["coaNamespace"];
            settings["DisableBatchNodesPublicIpAddress"] = values.Config["disableBatchNodesPublicIpAddress"];
            settings["DisableBatchScheduling"] = values.Config["disableBatchScheduling"];
            settings["UsePreemptibleVmsOnly"] = values.Config["usePreemptibleVmsOnly"];
            settings["BlobxferImageName"] = values.Config["blobxferImageName"];
            settings["DockerInDockerImageName"] = values.Config["dockerInDockerImageName"];
            settings["BatchImageOffer"] = values.Config["batchImageOffer"];
            settings["BatchImagePublisher"] = values.Config["batchImagePublisher"];
            settings["BatchImageSku"] = values.Config["batchImageSku"];
            settings["BatchImageVersion"] = values.Config["batchImageVersion"];
            settings["BatchNodeAgentSkuId"] = values.Config["batchNodeAgentSkuId"];
            settings["MarthaUrl"] = values.Config["marthaUrl"];
            settings["MarthaKeyVaultName"] = values.Config["marthaKeyVaultName"];
            settings["MarthaSecretName"] = values.Config["marthaSecretName"];
            settings["CrossSubscriptionAKSDeployment"] = values.Config["crossSubscriptionAKSDeployment"];
            settings["PostgreSqlServerName"] = values.Config["postgreSqlServerName"];
            settings["PostgreSqlDatabaseName"] = values.Config["postgreSqlDatabaseName"];
            settings["PostgreSqlUserLogin"] = values.Config["postgreSqlUserLogin"];
            settings["PostgreSqlUserPassword"] = values.Config["postgreSqlUserPassword"];
            settings["UsePostgreSqlSingleServer"] = values.Config["usePostgreSqlSingleServer"];
            settings["ManagedIdentityClientId"] = values.Identity["clientId"];
            settings["TesImageName"] = values.Images["tes"];
            settings["TriggerServiceImageName"] = values.Images["triggerservice"];
            settings["CromwellImageName"] = values.Images["cromwell"];
            settings["DefaultStorageAccountName"] = values.Persistence["storageAccount"];
            return settings;
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

                while (line != null)
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

                while (line != null)
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

        private async Task<bool> WaitForWorkloadAsync(IKubernetes client, string deploymentName, CancellationToken cancellationToken)
        {
            var deployments = await client.AppsV1.ListNamespacedDeploymentAsync(configuration.AksCoANamespace, cancellationToken: cancellationToken);
            var deployment = deployments.Items.Where(x => x.Metadata.Name.Equals(deploymentName, StringComparison.OrdinalIgnoreCase)).FirstOrDefault();

            var result = await WorkloadReadyRetryPolicy.ExecuteAndCaptureAsync(async () => 
            {
                deployments = await client.AppsV1.ListNamespacedDeploymentAsync(configuration.AksCoANamespace, cancellationToken: cancellationToken);
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
            public Dictionary<string, string> Config { get; set; }
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
