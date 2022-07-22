// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Storage.Blobs;
using k8s;
using k8s.Models;
using Microsoft.Azure.Management.ContainerService;
using Microsoft.Azure.Management.ContainerService.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using static Microsoft.Azure.Management.Fluent.Azure;
using Microsoft.Azure.Management.Storage.Fluent;
using Microsoft.Rest;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.ComponentModel;

namespace CromwellOnAzureDeployer
{
    /// <summary>
    ///     Class to hold all the kubernetes specific deployer logic. 
    /// </summary>
    internal class KubernetesManager
    {
        private readonly string BlobCsiRepo = "https://raw.githubusercontent.com/kubernetes-sigs/blob-csi-driver/master/charts";
        private readonly string BlobCsiDriverVersion = "v1.15.0";

        private Configuration configuration { get; set; }
        private AzureCredentials azureCredentials { get; set; }
        private IAuthenticated azureClient { get; set; }
        private CancellationTokenSource cts { get; set; }
        private IEnumerable<string> subscriptionIds { get; set; }
        private ExecAsyncCallback printHandler = new ExecAsyncCallback(async (stdIn, stdOut, stdError) =>
        {
            using (var reader = new StreamReader(stdOut))
            {
                ConsoleEx.Write(reader.ReadToEnd());
            }

            using (var reader = new StreamReader(stdError))
            {
                ConsoleEx.Write(reader.ReadToEnd());
            }
        });

        private ExecAsyncCallback silentHandler = new ExecAsyncCallback(async (stdIn, stdOut, stdError) => {
            using (var reader = new StreamReader(stdOut))
            {
                reader.ReadToEnd();
            }

            using (var reader = new StreamReader(stdError))
            {
                reader.ReadToEnd();
            }
        });


        public KubernetesManager(Configuration config, AzureCredentials credentials, IAuthenticated azureClient, IEnumerable<string> subscriptionIds, CancellationTokenSource cts)
        {
            this.configuration = config;
            this.azureCredentials = credentials;
            this.azureClient = azureClient;
            this.subscriptionIds = subscriptionIds;
            this.cts = cts;
        }

        public async Task<IKubernetes> GetKubernetesClient(IResource resourceGroupObject)
        {
            var resourceGroup = resourceGroupObject.Name;
            var containerServiceClient = new ContainerServiceClient(azureCredentials);
            containerServiceClient.SubscriptionId = configuration.SubscriptionId;

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

        public async Task DeployHelmChartToCluster(IKubernetes client, IResource resourceGroupObject, Dictionary<string, string> settings, IStorageAccount storageAccount)
        {
            UpdateHelmValues(storageAccount.Name, resourceGroupObject.Name, settings["AzureServicesAuthConnectionString"], settings["ApplicationInsightsAccountName"], settings["CosmosDbAccountName"], settings["BatchAccountName"], settings["BatchNodesSubnetId"]);
            await DeployBlobCSIDriverHelmChart();

            Process p = new Process();
            p.StartInfo.UseShellExecute = false;
            p.StartInfo.RedirectStandardOutput = true;
            p.StartInfo.FileName = configuration.HelmExePath;
            p.StartInfo.Arguments = $"install --generate-name ./scripts/helm --kubeconfig kubeconfig.txt --namespace {configuration.AksCoANamespace} --create-namespace";
            p.Start();
            string output = p.StandardOutput.ReadToEnd();
            ConsoleEx.WriteLine(output);
            await p.WaitForExitAsync();

            if (!configuration.ProvisionPostgreSqlOnAzure.GetValueOrDefault())
            {
                var commands = new List<string[]>
                {
                    new string[] { "bash", "-lic", "mysql -pcromwell < /configuration/init-user.sql" },
                };
                await ExecuteCommandsOnPod(client, "mysqldb", commands, TimeSpan.FromMinutes(6));
            }
        }

        private void UpdateHelmValues(string storageAccountName, string resourceGroupName, string azureServiceAuthString, string appInsightsName, string cosmosDbName, string batchAccountName, string batchNodesSubnetId)
        {
            var values = Yaml.LoadFromString<HelmValues>(Utility.GetFileContent("scripts", "helm", "values-template.yaml"));
            values.Persistence["storageAccount"] = storageAccountName;
            values.Config["resourceGroup"] = resourceGroupName;
            values.Config["azureServicesAuthConnectionString"] = azureServiceAuthString;
            values.Config["applicationInsightsAccountName"] = appInsightsName;
            values.Config["cosmosDbAccountName"] = cosmosDbName;
            values.Config["batchAccountName"] = batchAccountName;
            values.Config["batchNodesSubnetId"] = batchNodesSubnetId;

            var writer = new StreamWriter(Path.Join("scripts", "helm", "values.yaml"));
            writer.Write(Yaml.SaveToString(values));
            writer.Close();
        }

        private async Task DeployBlobCSIDriverHelmChart()
        {
            Process p = new Process();
            p.StartInfo.UseShellExecute = false;
            p.StartInfo.RedirectStandardOutput = true;
            p.StartInfo.FileName = configuration.HelmExePath;
            p.StartInfo.Arguments = $"repo add blob-csi-driver {BlobCsiRepo}";
            p.Start();
            string output = p.StandardOutput.ReadToEnd();
            ConsoleEx.WriteLine(output);
            await p.WaitForExitAsync();

            p = new Process();
            p.StartInfo.UseShellExecute = false;
            p.StartInfo.RedirectStandardOutput = true;
            p.StartInfo.FileName = configuration.HelmExePath;
            p.StartInfo.Arguments = $"install blob-csi-driver blob-csi-driver/blob-csi-driver --set node.enableBlobfuseProxy=true --namespace kube-system --version {BlobCsiDriverVersion} --kubeconfig kubeconfig.txt";
            p.Start();
            output = p.StandardOutput.ReadToEnd();
            ConsoleEx.WriteLine(output);
            await p.WaitForExitAsync();
        }


        public async Task UnlockCromwellChangeLog(IKubernetes client)
        {
            var commands = new List<string[]>
            {
                new string[] { "bash", "-lic", "mysql -pcromwell < /configuration/unlock-change-log.sql" }
            };
            await ExecuteCommandsOnPod(client, "mysqldb", commands, TimeSpan.FromMinutes(3));
        }

        public async Task ExecuteCommandsOnPod(IKubernetes client, string podName, IEnumerable<string[]> commands, TimeSpan timeout)
        {
            var pods = await client.ListNamespacedPodAsync(configuration.AksCoANamespace);
            var workloadPod = pods.Items.Where(x => x.Metadata.Name.Contains(podName)).FirstOrDefault();

            if (!await WaitForWorkloadWithTimeout(client, podName, timeout, cts.Token))
            {
                throw new Exception($"Timed out waiting for {podName} to start.");
            }
            // For some reason even if a pod says it ready, calls made immediately will fail. Wait for 20 seconds for safety. 
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
            var deployments = await client.ListNamespacedDeploymentAsync(configuration.AksCoANamespace);
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
                await Task.Delay(TimeSpan.FromSeconds(15));
                deployments = await client.ListNamespacedDeploymentAsync(configuration.AksCoANamespace);
                deployment = deployments.Items.Where(x => x.Metadata.Name.Equals(deploymentName, StringComparison.OrdinalIgnoreCase)).FirstOrDefault();
            }

            return true;
        }

        public async Task UpgradeAKSDeployment(Dictionary<string, string> settings, IResourceGroup resourceGroup, IStorageAccount storageAccount)
        {
            // Override any configuration that is used by the update.
            Deployer.OverrideSettingsFromConfiguration(settings, configuration);

            var containerServiceClient = new ContainerServiceClient(azureCredentials);
            containerServiceClient.SubscriptionId = configuration.SubscriptionId;
            var creds = await containerServiceClient.ManagedClusters.ListClusterAdminCredentialsAsync(resourceGroup.Name, configuration.AksClusterName);
            var kubeConfigFile = new FileInfo("kubeconfig.txt");
            var contents = Encoding.Default.GetString(creds.Kubeconfigs.First().Value);
            var writer = kubeConfigFile.CreateText();
            writer.Write(contents);
            writer.Close();

            var k8sConfiguration = KubernetesClientConfiguration.LoadKubeConfig(kubeConfigFile, false);
            var k8sConfig = KubernetesClientConfiguration.BuildConfigFromConfigObject(k8sConfiguration);
            IKubernetes client = new Kubernetes(k8sConfig);

            await UnlockCromwellChangeLog(client);
            await DeployHelmChartToCluster(client, resourceGroup, settings, storageAccount);
        }

        private class MountableContainer
        {
            public string StorageAccount { get; set; }
            public string ContainerName { get; set; }
            public string SasToken { get; set; }


            public override bool Equals(object other)
            {
                return string.Equals(StorageAccount, ((MountableContainer)other).StorageAccount, StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(ContainerName, ((MountableContainer)other).ContainerName, StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(SasToken, ((MountableContainer)other).SasToken, StringComparison.OrdinalIgnoreCase);
            }

            public override int GetHashCode()
            {
                if (SasToken is null)
                {
                    return HashCode.Combine(StorageAccount.GetHashCode(), ContainerName.GetHashCode());
                }
                else
                {
                    return HashCode.Combine(StorageAccount.GetHashCode(), ContainerName.GetHashCode(), SasToken.GetHashCode());
                }
            }
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
            public Dictionary<string, string> Db { get; set; }
        }
    }
}
