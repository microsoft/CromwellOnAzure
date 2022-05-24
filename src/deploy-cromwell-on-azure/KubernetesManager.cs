// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Management.ContainerService;
using Microsoft.Azure.Management.Network.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Microsoft.Azure.Management.Storage.Fluent;
using Microsoft.Rest;
using k8s;
using k8s.Models;
using Azure.Storage.Blobs;
using Microsoft.Azure.Management.ContainerService.Fluent;
using static Microsoft.Azure.Management.Fluent.Azure;

namespace CromwellOnAzureDeployer
{
    /// <summary>
    ///     Class to hold all the kubernetes specific deployer logic. 
    /// </summary>
    internal class KubernetesManager
    {
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

        private ExecAsyncCallback silentHandler = new ExecAsyncCallback(async (stdIn, stdOut, stdError) => {});

        public KubernetesManager(Configuration config, AzureCredentials credentials, IAuthenticated azureClient, IEnumerable<string> subscriptionIds, CancellationTokenSource cts)
        {
            this.configuration = config;
            this.azureCredentials = credentials;
            this.azureClient = azureClient;
            this.subscriptionIds = subscriptionIds;
            this.cts = cts;
        }

        private async Task<string> CreateContainerMounts(IStorageAccount storageAccount, string resourceGroup, IKubernetes client, List<V1Deployment> deployments)
        {
            var externalStorageContainers = new StringBuilder();
            var existingContainers = new HashSet<MountableContainer>(await GetExistingContainers(client));
            var containers = new HashSet<MountableContainer>(await GetContainersToMount(storageAccount));

            var containersToDelete = existingContainers.Except(containers).ToList();
            var containersToAdd = containers.Except(existingContainers).ToList();

            foreach (var container in containersToDelete)
            {
                var pvcName = $"{container.StorageAccount}-{container.ContainerName}-blob-claim1";
                await client.DeleteNamespacedPersistentVolumeClaimAsync(pvcName, configuration.AksCoANamespace);
                if (!string.IsNullOrEmpty(container.SasToken))
                {
                    var pvName = $"pv-blob-{container.StorageAccount}-{container.ContainerName}";
                    await client.DeletePersistentVolumeAsync(pvName);
                }
                else
                {
                    var scName = $"sc-blob-{container.StorageAccount}-{container.ContainerName}";
                    await client.DeleteStorageClassAsync(scName);
                }
            }

            foreach (var container in containersToAdd)
            {
                if (!string.IsNullOrEmpty(container.SasToken))
                {
                    await CreateContainerMountWithSas(container, client, deployments);
                    externalStorageContainers.Append($"https://{container.StorageAccount}.blob.core.windows.net/{container.ContainerName}?{container.SasToken};");
                }
                else
                {
                    await CreateContainerMountWithManagedId(container, resourceGroup, client, deployments, storageAccount.Name);
                }
            }

            return externalStorageContainers.ToString();
        }

        private async Task<List<MountableContainer>> GetExistingContainers(IKubernetes client)
        {
            var list = new List<MountableContainer>();
            var claims = await client.ListPersistentVolumeClaimForAllNamespacesAsync();

            foreach (var claim in claims.Items)
            {
                if (string.Equals(claim.Namespace(), configuration.AksCoANamespace, StringComparison.OrdinalIgnoreCase) &&
                    claim.Metadata.Name.EndsWith("-blob-claim1"))
                {
                    var prefix = claim.Metadata.Name.Replace("-blob-claim1", "");
                    var parts = prefix.Split("-");
                    var containerName = string.Join("-", parts.Skip(1));
                    var container = new MountableContainer()
                    {
                        StorageAccount = parts[0],
                        ContainerName = containerName
                    };

                    var secretName = $"sas-secret-{container.StorageAccount}-{container.ContainerName}";
                    V1Secret secret = null;
                    try
                    {
                        secret = await client.ReadNamespacedSecretAsync(secretName, configuration.AksCoANamespace);
                    }
                    catch (HttpOperationException) { }

                    if (secret != null)
                    {
                        container.SasToken = Encoding.UTF8.GetString(secret.Data["azurestorageaccountsastoken"]);
                    }
                    list.Add(container);
                }
            }

            return list;
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

        public async Task DeployCoAServicesToCluster(IKubernetes client, IResource resourceGroupObject, Dictionary<string, string> settings, IGenericResource logAnalyticsWorkspace, INetwork virtualNetwork, string subnetName, IStorageAccount storageAccount)
        {
            var resourceGroup = resourceGroupObject.Name;
            await InstallCSIBlobDriver(client);

            if (!string.Equals(configuration.AksCoANamespace, "default", StringComparison.OrdinalIgnoreCase))
            {
                V1Namespace existing = null;

                try
                {
                    existing = await client.ReadNamespaceStatusAsync(configuration.AksCoANamespace);
                }
                catch (HttpOperationException) { }

                if (existing is null)
                {
                    await client.CreateNamespaceAsync(new V1Namespace
                    {
                        Metadata = new V1ObjectMeta
                        {
                            Name = configuration.AksCoANamespace
                        }
                    });
                }
            }

            var cromwellTempClaim = Yaml.LoadFromString<V1PersistentVolumeClaim>(Utility.GetFileContent("scripts", "k8s", "cromwell-tmp-claim.yaml"));
            var mysqlDataClaim = Yaml.LoadFromString<V1PersistentVolumeClaim>(Utility.GetFileContent("scripts", "k8s", "mysqldb-data-claim.yaml"));

            var tesServiceBody = Yaml.LoadFromString<V1Service>(Utility.GetFileContent("scripts", "k8s", "tes-service.yaml"));
            var mysqlServiceBody = Yaml.LoadFromString<V1Service>(Utility.GetFileContent("scripts", "k8s", "mysqldb-service.yaml"));
            var cromwellServiceBody = Yaml.LoadFromString<V1Service>(Utility.GetFileContent("scripts", "k8s", "cromwell-service.yaml"));

            await client.CreateNamespacedPersistentVolumeClaimAsync(cromwellTempClaim, configuration.AksCoANamespace);
            await client.CreateNamespacedPersistentVolumeClaimAsync(mysqlDataClaim, configuration.AksCoANamespace);

            var tesDeploymentBody = BuildTesDeployment(settings);
            var triggerDeploymentBody = BuildTriggerServiceDeployment(settings);
            var cromwellDeploymentBody = BuildCromwellDeployment(settings, storageAccount.Name);

            var deploymentsWithStorageMounts = new List<V1Deployment> { tesDeploymentBody, cromwellDeploymentBody };
            var externalStorageContainers = await CreateContainerMounts(storageAccount, resourceGroup, client, deploymentsWithStorageMounts);
            tesDeploymentBody.Spec.Template.Spec.Containers.First().Env.Add(new V1EnvVar("ExternalStorageContainers", externalStorageContainers.ToString()));

            await client.CreateNamespacedDeploymentAsync(tesDeploymentBody, configuration.AksCoANamespace);
            await client.CreateNamespacedServiceAsync(tesServiceBody, configuration.AksCoANamespace);
            await client.CreateNamespacedDeploymentAsync(triggerDeploymentBody, configuration.AksCoANamespace);

            await client.CreateNamespacedDeploymentAsync(cromwellDeploymentBody, configuration.AksCoANamespace);
            await client.CreateNamespacedServiceAsync(cromwellServiceBody, configuration.AksCoANamespace);

            if (!configuration.ProvisionMySqlOnAzure.GetValueOrDefault())
            {
                var mysqlDeploymentBody = BuildMysqlDeployment(settings, storageAccount.Name);
                await client.CreateNamespacedDeploymentAsync(mysqlDeploymentBody, configuration.AksCoANamespace);
                await client.CreateNamespacedServiceAsync(mysqlServiceBody, configuration.AksCoANamespace);
                var commands = new List<string[]>
                {
                    new string[] { "bash", "-lic", "mysql -pcromwell < /configuration/init-user.sql" },
                    new string[] { "bash", "-lic", "mysql -pcromwell < /configuration/unlock-change-log.sql" }
                };
                await ExecuteCommandsOnPod(client, "mysqldb", commands, TimeSpan.FromMinutes(3));
            }
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
            await Task.Delay(TimeSpan.FromSeconds(20));

            foreach (var command in commands)
            {
                await client.NamespacedPodExecAsync(workloadPod.Metadata.Name, configuration.AksCoANamespace, podName, command, true, silentHandler, CancellationToken.None);
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

        private V1Deployment BuildTesDeployment(Dictionary<string, string> settings)
        {
            var tesDeploymentBody = Yaml.LoadFromString<V1Deployment>(Utility.GetFileContent("scripts", "k8s", "tes-deployment.yaml"));
            if (tesDeploymentBody.Spec.Template.Spec.Containers.First().VolumeMounts is null)
            {
                tesDeploymentBody.Spec.Template.Spec.Containers.First().VolumeMounts = new List<V1VolumeMount>();
            }

            if (tesDeploymentBody.Spec.Template.Spec.Volumes is null)
            {
                tesDeploymentBody.Spec.Template.Spec.Volumes = new List<V1Volume>();
            }

            if (!string.IsNullOrWhiteSpace(settings["TesImageName"]))
            {
                tesDeploymentBody.Spec.Template.Spec.Containers.First().Image = settings["TesImageName"];
            }

            var tesEnv = new List<V1EnvVar>();
            foreach (var setting in settings)
            {
                tesEnv.Add(new V1EnvVar(setting.Key, setting.Value));
            }
            tesDeploymentBody.Spec.Template.Spec.Containers.First().Env = tesEnv;

            return tesDeploymentBody;
        }

        private V1Deployment BuildMysqlDeployment(Dictionary<string, string> settings, string storageAccountName)
        {
            var configurationMountName = $"{storageAccountName}-configuration-blob-claim1";
            var mysqlDeploymentBody = Yaml.LoadFromString<V1Deployment>(Utility.GetFileContent("scripts", "k8s", "mysqldb-deployment.yaml"));
            AddStaticVolumeClaimToDeploymentBody(mysqlDeploymentBody, configurationMountName, "/configuration");

            if (!string.IsNullOrWhiteSpace(settings["MySqlImageName"]))
            {
                mysqlDeploymentBody.Spec.Template.Spec.Containers.First().Image = settings["MySqlImageName"];
            }
            return mysqlDeploymentBody;
        }

        private V1Deployment BuildCromwellDeployment(Dictionary<string, string> settings, string storageAccountName)
        {
            var cromwellDeploymentBody = Yaml.LoadFromString<V1Deployment>(Utility.GetFileContent("scripts", "k8s", "cromwell-deployment.yaml"));

            if (!string.IsNullOrWhiteSpace(settings["CromwellImageName"]))
            {
                cromwellDeploymentBody.Spec.Template.Spec.Containers.First().Image = settings["CromwellImageName"];
            }
            return cromwellDeploymentBody;
        }

        private V1Deployment BuildTriggerServiceDeployment(Dictionary<string, string> settings)
        {
            var triggerDeploymentBody = Yaml.LoadFromString<V1Deployment>(Utility.GetFileContent("scripts", "k8s", "triggerservice-deployment.yaml"));

            var triggerEnv = new List<V1EnvVar>();
            triggerEnv.Add(new V1EnvVar("DefaultStorageAccountName", settings["DefaultStorageAccountName"]));
            triggerEnv.Add(new V1EnvVar("AzureServicesAuthConnectionString", settings["AzureServicesAuthConnectionString"]));
            triggerEnv.Add(new V1EnvVar("ApplicationInsightsAccountName", settings["ApplicationInsightsAccountName"]));
            triggerEnv.Add(new V1EnvVar("CosmosDbAccountName", settings["CosmosDbAccountName"]));
            triggerDeploymentBody.Spec.Template.Spec.Containers.First().Env = triggerEnv;

            if (!string.IsNullOrWhiteSpace(settings["TriggerServiceImageName"]))
            {
                triggerDeploymentBody.Spec.Template.Spec.Containers.First().Image = settings["TriggerServiceImageName"];
            }
            return triggerDeploymentBody;
        }

        private static void AddStaticVolumeClaimToDeploymentBody(V1Deployment deploymentBody, string configurationMountName, string path)
        {
            deploymentBody.Spec.Template.Spec.Volumes.Add(new V1Volume
            {
                Name = configurationMountName,
                PersistentVolumeClaim = new V1PersistentVolumeClaimVolumeSource
                {
                    ClaimName = configurationMountName
                }
            });
            deploymentBody.Spec.Template.Spec.Containers.First().VolumeMounts.Add(new V1VolumeMount
            {
                MountPath = path,
                Name = configurationMountName
            });
        }

        private async Task CreateContainerMountWithSas(MountableContainer container, IKubernetes client, List<V1Deployment> deployments)
        {
            var secretName = $"sas-secret-{container.StorageAccount}-{container.ContainerName}";
            var volumeName = $"pv-blob-{container.StorageAccount}-{container.ContainerName}";
            var claimName = $"{container.StorageAccount}-{container.ContainerName}-blob-claim1";

            if (!(await KubernetesSecretExists(client, secretName)))
            {
                await client.CreateNamespacedSecretAsync(new V1Secret
                {
                    Metadata = new V1ObjectMeta
                    {
                        Name = secretName
                    },
                    StringData = new Dictionary<string, string> 
                    {
                        {"azurestorageaccountname", container.StorageAccount},
                        {"azurestorageaccountsastoken", container.SasToken}
                    },
                    Type = "Opaque",
                    Kind = "Secret",
                }, configuration.AksCoANamespace);
            }

            await client.CreatePersistentVolumeAsync(new V1PersistentVolume
            {
                Metadata = new V1ObjectMeta
                {
                    Name = volumeName
                },
                Spec = new V1PersistentVolumeSpec
                {
                    AccessModes = new List<string>() { "ReadWriteMany" },
                    PersistentVolumeReclaimPolicy = "Retain",
                    Capacity = new Dictionary<string, ResourceQuantity>
                    {
                        { "storage", new ResourceQuantity("10Gi") }
                    },
                    Csi = new V1CSIPersistentVolumeSource
                    {
                        Driver = "blob.csi.azure.com",
                        ReadOnlyProperty = false,
                        VolumeHandle = $"volume-{container.StorageAccount}-{container.ContainerName}-{new Random().Next(10000)}",
                        VolumeAttributes = new Dictionary<string, string>
                        {
                            { "containerName", container.ContainerName }
                        },
                        NodeStageSecretRef = new V1SecretReference()
                        {
                            Name = $"sas-secret-{container.StorageAccount}-{container.ContainerName}",
                            NamespaceProperty = configuration.AksCoANamespace
                        }
                    }
                }
            });

            await client.CreateNamespacedPersistentVolumeClaimAsync(new V1PersistentVolumeClaim
            {
                Metadata = new V1ObjectMeta
                {
                    Name = claimName
                },
                Spec = new V1PersistentVolumeClaimSpec
                {
                    AccessModes = new List<string>() { "ReadWriteMany" },
                    VolumeName = volumeName,
                    StorageClassName = "", // This is required, default value is not empty string, and empty string is required for PVC to work.
                    Resources = new V1ResourceRequirements
                    {
                        Requests = new Dictionary<string, ResourceQuantity>
                        {
                            { "storage", new ResourceQuantity("10Gi") }
                        }
                    }
                }
            }, configuration.AksCoANamespace);

            foreach(var deployment in deployments)
            {
                AddStaticVolumeClaimToDeploymentBody(deployment, claimName, $"/{container.StorageAccount}/{container.ContainerName}");
            }
        }

        private async Task CreateContainerMountWithManagedId(MountableContainer container, string resouceGroup, IKubernetes client, List<V1Deployment> deployments, string coaStorageAccount)
        {
            var sasSecretName = $"sa-secret-{container.StorageAccount}-{container.ContainerName}";
            var pvcName = $"{container.StorageAccount}-{container.ContainerName}-blob-claim1";
            var storageClassName = $"sc-blob-{container.StorageAccount}-{container.ContainerName}";
            var path = string.Empty;

            if (container.StorageAccount.Equals(coaStorageAccount, StringComparison.OrdinalIgnoreCase))
            {
                path = $"/{container.ContainerName}";
            }
            else
            {
                path = $"/{container.StorageAccount}/{container.ContainerName}";
            }

            var storageAccount = await Deployer.GetExistingStorageAccountAsync(container.StorageAccount, azureClient, subscriptionIds, configuration);
            
            if (!(await KubernetesSecretExists(client, sasSecretName)))
            {
                await client.CreateNamespacedSecretAsync(new V1Secret
                {
                    Metadata = new V1ObjectMeta()
                    {
                        Name = sasSecretName,
                    },
                    StringData = new Dictionary<string, string> {
                        {"azurestorageaccountname", storageAccount.Name},
                        {"azurestorageaccountkey", storageAccount.Key}
                    },
                    Type = "Opaque",
                    Kind = "Secret",
                }, configuration.AksCoANamespace);
            }

            var storageClassBody = new V1StorageClass
            {
                Metadata = new V1ObjectMeta
                {
                    Name = storageClassName
                },
                Provisioner = "blob.csi.azure.com",
                VolumeBindingMode = "Immediate",
                ReclaimPolicy = "Retain",
                Parameters = new Dictionary<string, string>()
                {
                    {"resourceGroup", resouceGroup},
                    {"storageAccount", container.StorageAccount},
                    {"containerName", container.ContainerName}
                }
            };
            await client.CreateStorageClassAsync(storageClassBody);

            var pvc = new V1PersistentVolumeClaim
            {
                Metadata = new V1ObjectMeta
                {
                    Name = pvcName
                },
                Spec = new V1PersistentVolumeClaimSpec
                {
                    AccessModes = new List<string> { "ReadWriteMany" },
                    StorageClassName = storageClassBody.Metadata.Name,
                    Resources = new V1ResourceRequirements
                    {
                        Requests = new Dictionary<string, ResourceQuantity>
                        {
                            { "storage", new ResourceQuantity("10Gi") }
                        }
                    }
                }
            };

            foreach (var deployment in deployments)
            {
                AddStaticVolumeClaimToDeploymentBody(deployment, pvcName, path);
            }
            await client.CreateNamespacedPersistentVolumeClaimAsync(pvc, configuration.AksCoANamespace);
        }

        private async Task<bool> KubernetesSecretExists(IKubernetes client, string sasSecretName)
        {
            V1Secret existing = null;

            try
            {
                existing = await client.ReadNamespacedSecretAsync(sasSecretName, configuration.AksCoANamespace);
            }
            catch (HttpOperationException) { }

            return existing != null;
        }

        private async Task<List<MountableContainer>> GetContainersToMount(IStorageAccount storageAccount)
        {
            var containers = new HashSet<MountableContainer>();
            var exclusion = new HashSet<MountableContainer>();
            var wildCardAccounts = new List<string>();
            var contents = await Deployer.DownloadTextFromStorageAccountAsync(storageAccount, Deployer.ConfigurationContainerName, Deployer.ContainersToMountFileName, cts);
            
            foreach (var line in contents.Split("\n", StringSplitOptions.RemoveEmptyEntries))
            {
                if (line.StartsWith("#"))
                {
                    continue;
                }

                if (line.StartsWith("-"))
                {
                    var parts = line.Split("/", StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length > 1)
                    {
                        exclusion.Add(new MountableContainer() { StorageAccount = parts[0], ContainerName = parts[1] });
                    }
                }

                if (line.StartsWith("/"))
                {
                    var parts = line.Split("/", StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length > 1)
                    {
                        if (string.Equals(parts[1], "*"))
                        {
                            wildCardAccounts.Add(parts[0]);
                        }
                        else
                        {
                            containers.Add(new MountableContainer() { StorageAccount = parts[0], ContainerName = parts[1] });
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

            containers.ExceptWith(exclusion);
            return containers.ToList();
        }

        /// <summary>
        /// Installs the Yaml files for CSI blob driver. 
        /// Current Version: 1.8.0 from https://github.com/kubernetes-sigs/blob-csi-driver/tree/master/deploy/v1.8.0
        /// </summary>
        /// <param name="client"></param>
        /// <returns></returns>
        private async Task InstallCSIBlobDriver(IKubernetes client)
        {
            // TODO check if CSI driver is already installed?

            var typeMap = new Dictionary<string, Type>();
            typeMap.Add("v1/Pod", typeof(V1Pod));
            typeMap.Add("v1/Service", typeof(V1Service));
            typeMap.Add("v1/ServiceAccount", typeof(V1ServiceAccount));
            typeMap.Add("v1/ClusterRole", typeof(V1ClusterRole));
            typeMap.Add("v1/ClusterRoleBinding", typeof(V1ClusterRoleBinding));
            typeMap.Add("apps/v1/DaemonSet", typeof(V1DaemonSet));
            typeMap.Add("storage.k8s.io/v1/CSIDriver", typeof(V1CSIDriver));
            typeMap.Add("apps/v1/Deployment", typeof(V1Deployment));

            var objects = Yaml.LoadAllFromString(Utility.GetFileContent("scripts", "k8s", "csi", "rbac-csi-blob-controller.yaml"), typeMap);
            objects.AddRange(Yaml.LoadAllFromString(Utility.GetFileContent("scripts", "k8s", "csi", "rbac-csi-blob-node.yaml"), typeMap));
            objects.AddRange(Yaml.LoadAllFromString(Utility.GetFileContent("scripts", "k8s", "csi", "csi-blob-driver.yaml"), typeMap));
            objects.AddRange(Yaml.LoadAllFromString(Utility.GetFileContent("scripts", "k8s", "csi", "csi-blob-controller.yaml"), typeMap));
            objects.AddRange(Yaml.LoadAllFromString(Utility.GetFileContent("scripts", "k8s", "csi", "blobfuse-proxy.yaml"), typeMap));

            var csiBlobNode = Yaml.LoadFromString<V1DaemonSet>(Utility.GetFileContent("scripts", "k8s", "csi", "csi-blob-node.yaml"));
            objects.Add(csiBlobNode);

            var @switch = new Dictionary<Type, Func<object, Task>> {
                { typeof(V1Service), (value) => client.CreateNamespacedServiceAsync((V1Service) value, "kube-system") },
                { typeof(V1ServiceAccount), (value) => client.CreateNamespacedServiceAccountAsync((V1ServiceAccount) value, "kube-system") },
                { typeof(V1ClusterRole), (value) => client.CreateClusterRoleAsync((V1ClusterRole) value) },
                { typeof(V1ClusterRoleBinding), (value) => client.CreateClusterRoleBindingAsync((V1ClusterRoleBinding) value) },
                { typeof(V1DaemonSet), (value) => client.CreateNamespacedDaemonSetAsync((V1DaemonSet) value, "kube-system") },
                { typeof(V1CSIDriver), (value) => client.CreateCSIDriverAsync((V1CSIDriver) value) },
                { typeof(V1Deployment), (value) => client.CreateNamespacedDeploymentAsync((V1Deployment) value, "kube-system") },
            };

            var tasks = new List<Task>();
            foreach (var value in objects)
            {
                tasks.Add(@switch[value.GetType()](value));
            }
            await Task.WhenAll(tasks);
        }

        public async Task UpgradeAKSDeployment(Dictionary<string, string> settings, IResourceGroup resourceGroup, IStorageAccount storageAccount)
        {
            // Override any configuration that is used by the update.
            Deployer.UpdateSettingsFromConfiguration(settings, configuration);

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

            var tesDeploymentBody = BuildTesDeployment(settings);
            var triggerServiceDeploymentBody = BuildTriggerServiceDeployment(settings);
            var cromwellDeploymentBody = BuildCromwellDeployment(settings, storageAccount.Name);
            var mysqlDeploymentBody = BuildMysqlDeployment(settings, storageAccount.Name);

            var deploymentsWithStorageMounts = new List<V1Deployment> { tesDeploymentBody, cromwellDeploymentBody };
            var externalStorageContainers = await CreateContainerMounts(storageAccount, resourceGroup.Name, client, deploymentsWithStorageMounts);
            tesDeploymentBody.Spec.Template.Spec.Containers.First().Env.Add(new V1EnvVar("ExternalStorageContainers", externalStorageContainers.ToString()));

            await client.ReplaceNamespacedDeploymentAsync(tesDeploymentBody, "tes", configuration.AksCoANamespace);
            await client.ReplaceNamespacedDeploymentAsync(triggerServiceDeploymentBody, "triggerservice", configuration.AksCoANamespace);
            await client.ReplaceNamespacedDeploymentAsync(cromwellDeploymentBody, "cromwell", configuration.AksCoANamespace);
            
            if (string.IsNullOrEmpty(configuration.MySqlServerName))
            {
                await client.ReplaceNamespacedDeploymentAsync(mysqlDeploymentBody, "mysqldb", configuration.AksCoANamespace);
            }
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
    }
}
