// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using k8s;
using Microsoft.Azure.Management.ContainerService;
using Microsoft.Azure.Management.ContainerService.Fluent;
using Microsoft.Azure.Management.Msi.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Microsoft.Azure.Management.Storage.Fluent;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CromwellOnAzureDeployer
{
    /// <summary>
    ///     Class to hold all the kubernetes specific deployer logic. 
    /// </summary>
    internal class KubernetesManager
    {
        private readonly string BlobCsiRepo = "https://raw.githubusercontent.com/kubernetes-sigs/blob-csi-driver/master/charts";
        private readonly string BlobCsiDriverVersion = "v1.15.0";
        private readonly string AadPluginRepo = "https://raw.githubusercontent.com/Azure/aad-pod-identity/master/charts";
        private readonly string AadPluginVersion = "4.1.12";

        private Configuration configuration { get; set; }
        private AzureCredentials azureCredentials { get; set; }
        private CancellationTokenSource cts { get; set; }

        public KubernetesManager(Configuration config, AzureCredentials credentials, CancellationTokenSource cts)
        {
            this.configuration = config;
            this.azureCredentials = credentials;
            this.cts = cts;
        }

        public async Task<IKubernetes> GetKubernetesClient(IResource resourceGroupObject)
        {
            var resourceGroup = resourceGroupObject.Name;
            var containerServiceClient = new ContainerServiceClient(azureCredentials) { SubscriptionId = configuration.SubscriptionId };

            // Write kubeconfig in the working directory, because KubernetesClientConfiguration needs to read from a file, TODO figure out how to pass this directly. 
            var creds = await containerServiceClient.ManagedClusters.ListClusterAdminCredentialsAsync(resourceGroup, configuration.AksClusterName);
            var kubeConfigFile = new FileInfo("kubeconfig.txt");
            var contents = Encoding.Default.GetString(creds.Kubeconfigs.First().Value);
            var writer = kubeConfigFile.CreateText();
            writer.Write(contents);
            writer.Close();

            var k8sConfiguration = KubernetesClientConfiguration.LoadKubeConfig(kubeConfigFile, false);
            var k8sConfig = KubernetesClientConfiguration.BuildConfigFromConfigObject(k8sConfiguration);
            return new Kubernetes(k8sConfig);
        }

        public async Task DeployCoADependencies()
        {
            await ExecHelmProcess($"repo add aad-pod-identity {AadPluginRepo}");
            await ExecHelmProcess($"install aad-pod-identity aad-pod-identity/aad-pod-identity --namespace kube-system --version {AadPluginVersion} --kubeconfig kubeconfig.txt");
            await ExecHelmProcess($"repo add blob-csi-driver {BlobCsiRepo}");
            await ExecHelmProcess($"install blob-csi-driver blob-csi-driver/blob-csi-driver --set node.enableBlobfuseProxy=true --namespace kube-system --version {BlobCsiDriverVersion} --kubeconfig kubeconfig.txt");
        }

        public async Task DeployHelmChartToCluster()
        {
           await ExecHelmProcess($"upgrade --install cromwellonazure ./scripts/helm --kubeconfig kubeconfig.txt --namespace {configuration.AksCoANamespace} --create-namespace");
        }

        public void UpdateHelmValues(string storageAccountName, string keyVaultUrl, string resourceGroupName, Dictionary<string, string> settings, IIdentity managedId)
        {
            var values = Yaml.LoadFromString<HelmValues>(Utility.GetFileContent("scripts", "helm", "values-template.yaml"));
            values.Persistence["storageAccount"] = storageAccountName;
            values.Persistence["keyVaultUrl"] = keyVaultUrl;
            values.Persistence["keyVaultSecretName"] = Deployer.StorageAccountKeySecretName;
            values.Config["resourceGroup"] = resourceGroupName;
            values.Config["azureServicesAuthConnectionString"] = settings["AzureServicesAuthConnectionString"];
            values.Config["applicationInsightsAccountName"] = settings["ApplicationInsightsAccountName"];
            values.Config["cosmosDbAccountName"] = settings["CosmosDbAccountName"];
            values.Config["batchAccountName"] = settings["BatchAccountName"];
            values.Config["batchNodesSubnetId"] = settings["BatchNodesSubnetId"];
            values.Config["coaNamespace"] = configuration.AksCoANamespace;
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
            values.Identity["name"] = managedId.Name;
            values.Identity["resourceId"] = managedId.Id;
            values.Identity["clientId"] = managedId.ClientId;

            if (!string.IsNullOrWhiteSpace(configuration.CustomTesImagePath))
            {
                values.Images["tes"] = configuration.CustomTesImagePath;
            }

            if (!string.IsNullOrWhiteSpace(configuration.CustomTriggerServiceImagePath))
            {
                values.Images["triggerservice"] = configuration.CustomTriggerServiceImagePath;
            }

            if (!string.IsNullOrWhiteSpace(configuration.CustomCromwellImagePath))
            {
                values.Images["cromwell"] = configuration.CustomCromwellImagePath;
            }

            var writer = new StreamWriter(Path.Join("scripts", "helm", "values.yaml"));
            writer.Write(Yaml.SaveToString(values));
            writer.Close();
        }

        private async Task ExecHelmProcess(string command)
        {
            var p = new Process();
            p.StartInfo.UseShellExecute = false;
            p.StartInfo.RedirectStandardOutput = true;
            p.StartInfo.FileName = configuration.HelmExePath;
            p.StartInfo.Arguments = command;
            p.Start();

            if (configuration.DebugLogging)
            {
                var line = p.StandardOutput.ReadLine();
                while (line != null)
                {
                    ConsoleEx.WriteLine("HELM: " + line);
                    line = p.StandardOutput.ReadLine();
                }
            }

            await p.WaitForExitAsync();
        }

        public async Task ExecuteCommandsOnPod(IKubernetes client, string podName, IEnumerable<string[]> commands, TimeSpan timeout)
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

            var pods = await client.ListNamespacedPodAsync(configuration.AksCoANamespace);
            var workloadPod = pods.Items.Where(x => x.Metadata.Name.Contains(podName)).FirstOrDefault();

            if (!await WaitForWorkloadWithTimeout(client, podName, timeout, cts.Token))
            {
                throw new Exception($"Timed out waiting for {podName} to start.");
            }
            // For some reason even if a pod says it ready, calls made immediately will fail. Wait for 40 seconds for safety. 
            await Task.Delay(TimeSpan.FromSeconds(40));

            foreach (var command in commands)
            {
                await client.NamespacedPodExecAsync(workloadPod.Metadata.Name, configuration.AksCoANamespace, podName, command, true, printHandler, CancellationToken.None);
            }
        }

        public async Task WaitForCromwell(IKubernetes client)
        {
            if (!await WaitForWorkloadWithTimeout(client, "cromwell", TimeSpan.FromMinutes(3), cts.Token))
            {
                throw new Exception("Timed out waiting for Cromwell to start.");
            }
        }

        private async Task<bool> WaitForWorkloadWithTimeout(IKubernetes client, string deploymentName, System.TimeSpan timeout, CancellationToken cancellationToken)
        {
            var timer = Stopwatch.StartNew();
            var deployments = await client.ListNamespacedDeploymentAsync(configuration.AksCoANamespace, cancellationToken: cancellationToken);
            var deployment = deployments.Items.Where(x => x.Metadata.Name.Equals(deploymentName, StringComparison.OrdinalIgnoreCase)).FirstOrDefault();
            
            while (deployment is null ||
                deployment.Status is null ||
                deployment.Status.ReadyReplicas is null ||
                deployment.Status.ReadyReplicas < 1)
            {
                if (timer.Elapsed > timeout)
                {
                    return false;
                }
                await Task.Delay(TimeSpan.FromSeconds(15), cancellationToken);
                deployments = await client.ListNamespacedDeploymentAsync(configuration.AksCoANamespace, cancellationToken: cancellationToken);
                deployment = deployments.Items.Where(x => x.Metadata.Name.Equals(deploymentName, StringComparison.OrdinalIgnoreCase)).FirstOrDefault();
            }

            return true;
        }

        public async Task UpgradeAKSDeployment(Dictionary<string, string> settings, IResourceGroup resourceGroup, IStorageAccount storageAccount, IIdentity managedId, string keyVaultUrl)
        {
            // Override any configuration that is used by the update.
            Deployer.UpdateImageVersions(settings, configuration);

            var containerServiceClient = new ContainerServiceClient(azureCredentials) { SubscriptionId = configuration.SubscriptionId };
            var creds = await containerServiceClient.ManagedClusters.ListClusterAdminCredentialsAsync(resourceGroup.Name, configuration.AksClusterName);
            var kubeConfigFile = new FileInfo("kubeconfig.txt");
            var contents = Encoding.Default.GetString(creds.Kubeconfigs.First().Value);
            var writer = kubeConfigFile.CreateText();
            writer.Write(contents);
            writer.Close();

            var k8sConfiguration = KubernetesClientConfiguration.LoadKubeConfig(kubeConfigFile, false);
            var k8sConfig = KubernetesClientConfiguration.BuildConfigFromConfigObject(k8sConfiguration);
            IKubernetes client = new Kubernetes(k8sConfig);

            UpdateHelmValues(storageAccount.Name, keyVaultUrl, resourceGroup.Name, settings, managedId);
            await DeployHelmChartToCluster();
        }

        private class HelmValues
        {
            public Dictionary<string, string> Service { get; set; }
            public Dictionary<string, string> Config { get; set; }
            public Dictionary<string, string> Images { get; set; }
            public List<string> DefaultContainers { get; set; }
            public List<Dictionary<string, string>> ExternalContainers { get; set; }
            public List<Dictionary<string, string>> ExternalSasContainers { get; set; }
            public Dictionary<string, string> Persistence { get; set; }
            public Dictionary<string, string> Identity { get; set; }
            public Dictionary<string, string> Db { get; set; }
        }
    }
}
