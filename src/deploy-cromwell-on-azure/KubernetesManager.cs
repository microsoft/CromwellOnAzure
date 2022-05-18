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
using ICSharpCode.SharpZipLib.GZip;
using ICSharpCode.SharpZipLib.Tar;
using System.Security.Cryptography;
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

        public KubernetesManager(Configuration config, AzureCredentials credentials, IAuthenticated azureClient, IEnumerable<string> subscriptionIds, CancellationTokenSource cts)
        {
            this.configuration = config;
            this.azureCredentials = credentials;
            this.azureClient = azureClient;
            this.subscriptionIds = subscriptionIds;
            this.cts = cts;
        }

        private async Task<V1Deployment> BuildTesDeployment(Dictionary<string, string> settings, IStorageAccount storageAccount, string resourceGroup, IKubernetes client)
        {
            var tesDeploymentBody = Yaml.LoadFromString<V1Deployment>(Utility.GetFileContent("scripts", "k8s", "tes-deployment.yaml"));
            if (tesDeploymentBody.Spec.Template.Spec.Containers.First().VolumeMounts == null)
            {
                tesDeploymentBody.Spec.Template.Spec.Containers.First().VolumeMounts = new List<V1VolumeMount>();
            }

            if (tesDeploymentBody.Spec.Template.Spec.Volumes == null)
            {
                tesDeploymentBody.Spec.Template.Spec.Volumes = new List<V1Volume>();
            }

            if (!string.IsNullOrWhiteSpace(settings["TesImageName"]))
            {
                tesDeploymentBody.Spec.Template.Spec.Containers.First().Image = settings["TesImageName"];
            }

            var externalStorageContainers = new StringBuilder();
            var tesEnv = new List<V1EnvVar>();
            foreach (var setting in settings)
            {
                tesEnv.Add(new V1EnvVar(setting.Key, setting.Value));
            }

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
                    await CreateContainerMountWithSas(container, resourceGroup, client, tesDeploymentBody);
                    externalStorageContainers.Append($"https://{container.StorageAccount}.blob.core.windows.net/{container.ContainerName}?{container.SasToken};");
                }
                else
                {
                    await CreateContainerMountWithManagedId(container, resourceGroup, client, tesDeploymentBody);
                }
            }
            tesEnv.Add(new V1EnvVar("ExternalStorageContainers", externalStorageContainers.ToString()));
            tesDeploymentBody.Spec.Template.Spec.Containers.First().Env = tesEnv;

            return tesDeploymentBody;
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
                    catch (HttpOperationException e) { }

                    if (secret != null)
                    {
                        container.SasToken = Encoding.UTF8.GetString(secret.Data["azurestorageaccountsastoken"]);
                    }
                    list.Add(container);
                }
            }

            return list;
        }

        public async Task DeployCoAServicesToCluster(IResource resourceGroupObject, Dictionary<string, string> settings, IGenericResource logAnalyticsWorkspace, INetwork virtualNetwork, string subnetName, IStorageAccount storageAccount)
        {
            string resourceGroup = resourceGroupObject.Name;
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
            IKubernetes client = new Kubernetes(k8sConfig);

            // Install CSI driver. 
            await InstallCSIBlobDriver(client);

            var cromwellTempClaim = Yaml.LoadFromString<V1PersistentVolumeClaim>(Utility.GetFileContent("scripts", "k8s", "cromwell-tmp-claim2.yaml"));
            var mysqlDataClaim = Yaml.LoadFromString<V1PersistentVolumeClaim>(Utility.GetFileContent("scripts", "k8s", "mysqldb-data-claim3.yaml"));

            var tesServiceBody = Yaml.LoadFromString<V1Service>(Utility.GetFileContent("scripts", "k8s", "tes-service.yaml"));
            var mysqlServiceBody = Yaml.LoadFromString<V1Service>(Utility.GetFileContent("scripts", "k8s", "mysqldb-service.yaml"));
            var cromwellServiceBody = Yaml.LoadFromString<V1Service>(Utility.GetFileContent("scripts", "k8s", "cromwell-service.yaml"));

            if (!string.Equals(configuration.AksCoANamespace, "default", StringComparison.OrdinalIgnoreCase))
            {
                V1Namespace existing = null;
                try
                {
                    existing = await client.ReadNamespaceStatusAsync(configuration.AksCoANamespace);
                }
                catch (HttpOperationException e) { }

                if (existing == null)
                {
                    await client.CreateNamespaceAsync(new V1Namespace()
                    {
                        Metadata = new V1ObjectMeta()
                        {
                            Name = configuration.AksCoANamespace
                        }
                    });
                }
            }


            await client.CreateNamespacedPersistentVolumeClaimAsync(cromwellTempClaim, configuration.AksCoANamespace);
            await client.CreateNamespacedPersistentVolumeClaimAsync(mysqlDataClaim, configuration.AksCoANamespace);

            var tesDeploymentBody = await BuildTesDeployment(settings, storageAccount, resourceGroup, client);
            var triggerDeploymentBody = BuildTriggerServiceDeployment(settings);
            var cromwellDeploymentBody = BuildCromwellDeployment(settings, storageAccount.Name);
            var mysqlDeploymentBody = BuildMysqlDeployment(settings, storageAccount.Name);

            var tesDeployment = await client.CreateNamespacedDeploymentAsync(tesDeploymentBody, configuration.AksCoANamespace);
            var tesService = await client.CreateNamespacedServiceAsync(tesServiceBody, configuration.AksCoANamespace);
            var triggerDeployment = await client.CreateNamespacedDeploymentAsync(triggerDeploymentBody, configuration.AksCoANamespace);
            var mysqlDeployment = await client.CreateNamespacedDeploymentAsync(mysqlDeploymentBody, configuration.AksCoANamespace);
            var mysqlService = await client.CreateNamespacedServiceAsync(mysqlServiceBody, configuration.AksCoANamespace);
            var cromwellDeployment = await client.CreateNamespacedDeploymentAsync(cromwellDeploymentBody, configuration.AksCoANamespace);
            var cromwellService = await client.CreateNamespacedServiceAsync(cromwellServiceBody, configuration.AksCoANamespace);


            var pods = await client.ListNamespacedPodAsync(configuration.AksCoANamespace);
            var mysqlPod = pods.Items.Where(x => x.Metadata.Name.Contains("mysql")).FirstOrDefault(); //.Metadata.Name;

            // Wait for mysql pod to transition to running to run setup sql scripts.
            while (mysqlPod == null ||
                mysqlPod.Status == null ||
                mysqlPod.Status.ContainerStatuses == null ||
                mysqlPod.Status.ContainerStatuses.FirstOrDefault() == null ||
                !(mysqlPod.Status.ContainerStatuses.FirstOrDefault().Started ?? false))
            {
                await Task.Delay(System.TimeSpan.FromSeconds(15));
                pods = await client.ListNamespacedPodAsync(configuration.AksCoANamespace);
                mysqlPod = pods.Items.Where(x => x.Metadata.Name.Contains("mysql")).First();
            }
            await Task.Delay(System.TimeSpan.FromSeconds(30));

            var printHandler = new ExecAsyncCallback(async (stdIn, stdOut, stdError) =>
            {
                using (var reader = new StreamReader(stdOut))
                {
                    RefreshableConsole.Write(reader.ReadToEnd());
                }

                using (var reader = new StreamReader(stdError))
                {
                    RefreshableConsole.Write(reader.ReadToEnd());
                }
            });

            await client.NamespacedPodExecAsync(mysqlPod.Metadata.Name, configuration.AksCoANamespace, "mysqldb", new string[] { "bash", "-lic", "mysql -pcromwell < /configuration/init-user.sql" }, true, printHandler, CancellationToken.None);
            await client.NamespacedPodExecAsync(mysqlPod.Metadata.Name, configuration.AksCoANamespace, "mysqldb", new string[] { "bash", "-lic", "mysql -pcromwell < /configuration/unlock-change-log.sql" }, true, printHandler, CancellationToken.None);
        }

        private async Task<bool> WaitForWorkloadWithTimeout(IKubernetes client, string deploymentName, System.TimeSpan timeout, CancellationToken cancellationToken)
        {
            var timer = Stopwatch.StartNew();
            var deployments = await client.ListNamespacedDeploymentAsync(configuration.AksCoANamespace);
            var deployment = deployments.Items.Where(x => x.Metadata.Name.Equals(deploymentName, StringComparison.OrdinalIgnoreCase)).FirstOrDefault();
            while (deployment == null ||
                deployment.Status == null ||
                deployment.Status.ReadyReplicas == null ||
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

        private V1Deployment BuildMysqlDeployment(Dictionary<string, string> settings, string storageAccountName)
        {
            var configurationMountName = $"{storageAccountName}-configuration-blob-claim1";
            var mysqlDeploymentBody = Yaml.LoadFromString<V1Deployment>(Utility.GetFileContent("scripts", "k8s", "mysqldb-deployment.yaml"));
            AddStaticVolumeClaim(mysqlDeploymentBody, configurationMountName, "/configuration");

            if (!string.IsNullOrWhiteSpace(settings["MySqlImageName"]))
            {
                mysqlDeploymentBody.Spec.Template.Spec.Containers.First().Image = settings["MySqlImageName"];
            }
            return mysqlDeploymentBody;
        }

        private V1Deployment BuildCromwellDeployment(Dictionary<string, string> settings, string storageAccountName)
        {
            var configurationMountName = $"{storageAccountName}-configuration-blob-claim1";
            var executionsMountName = $"{storageAccountName}-cromwell-executions-blob-claim1";
            var workflowLogsMountName = $"{storageAccountName}-cromwell-workflow-logs-blob-claim1";
            var cromwellDeploymentBody = Yaml.LoadFromString<V1Deployment>(Utility.GetFileContent("scripts", "k8s", "cromwell-deployment.yaml"));
            AddStaticVolumeClaim(cromwellDeploymentBody, configurationMountName, "/configuration");
            AddStaticVolumeClaim(cromwellDeploymentBody, executionsMountName, "/cromwell-executions");
            AddStaticVolumeClaim(cromwellDeploymentBody, workflowLogsMountName, "/cromwell-workflow-logs");

            if (!string.IsNullOrWhiteSpace(settings["CromwellImageName"]))
            {
                cromwellDeploymentBody.Spec.Template.Spec.Containers.First().Image = settings["CromwellImageName"];
            }
            return cromwellDeploymentBody;
        }

        private V1Deployment BuildTriggerServiceDeployment(Dictionary<string, string> settings)
        {
            var triggerDeploymentBody = Yaml.LoadFromString<V1Deployment>(Utility.GetFileContent("scripts", "k8s", "triggerservice-deployment.yaml"));
            // Example to set Environment Variables for trigger service.
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

        private static void AddStaticVolumeClaim(V1Deployment deploymentBody, string configurationMountName, string path)
        {
            deploymentBody.Spec.Template.Spec.Volumes.Add(new V1Volume()
            {
                Name = configurationMountName,
                PersistentVolumeClaim = new V1PersistentVolumeClaimVolumeSource()
                {
                    ClaimName = configurationMountName
                }
            });
            deploymentBody.Spec.Template.Spec.Containers.First().VolumeMounts.Add(new V1VolumeMount()
            {
                MountPath = path,
                Name = configurationMountName
            });
        }

        private async Task CreateContainerMountWithSas(MountableContainer container, string resourceGroup, IKubernetes client, V1Deployment tesDeployment)
        {
            var secretName = $"sas-secret-{container.StorageAccount}-{container.ContainerName}";
            var volumeName = $"pv-blob-{container.StorageAccount}-{container.ContainerName}";
            var claimName = $"{container.StorageAccount}-{container.ContainerName}-blob-claim1";

            if (!(await KubernetesSecretExists(client, secretName)))
            {
                await client.CreateNamespacedSecretAsync(new V1Secret()
                {
                    Metadata = new V1ObjectMeta()
                    {
                        Name = secretName
                    },
                    StringData = new Dictionary<string, string> {
                    {"azurestorageaccountname", container.StorageAccount},
                    {"azurestorageaccountsastoken", container.SasToken}
                },
                    Type = "Opaque",
                    Kind = "Secret",
                }, configuration.AksCoANamespace);
            }

            await client.CreatePersistentVolumeAsync(new V1PersistentVolume()
            {
                Metadata = new V1ObjectMeta()
                {
                    Name = volumeName
                },
                Spec = new V1PersistentVolumeSpec()
                {
                    AccessModes = new List<string>() { "ReadWriteMany" },
                    PersistentVolumeReclaimPolicy = "Retain",
                    Capacity = new Dictionary<string, ResourceQuantity>()
                    {
                        { "storage", new ResourceQuantity("10Gi") }
                    },
                    Csi = new V1CSIPersistentVolumeSource()
                    {
                        Driver = "blob.csi.azure.com",
                        ReadOnlyProperty = false,
                        VolumeHandle = $"volume-{container.StorageAccount}-{container.ContainerName}-{new Random().Next(10000)}",
                        VolumeAttributes = new Dictionary<string, string>()
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

            await client.CreateNamespacedPersistentVolumeClaimAsync(new V1PersistentVolumeClaim()
            {
                Metadata = new V1ObjectMeta()
                {
                    Name = claimName
                },
                Spec = new V1PersistentVolumeClaimSpec()
                {
                    AccessModes = new List<string>() { "ReadWriteMany" },
                    VolumeName = volumeName,
                    StorageClassName = "",
                    Resources = new V1ResourceRequirements()
                    {
                        Requests = new Dictionary<string, ResourceQuantity>()
                            {
                                { "storage", new ResourceQuantity("10Gi") }
                            }
                    }
                }
            }, configuration.AksCoANamespace);

            AddStaticVolumeClaim(tesDeployment, claimName, $"/{container.StorageAccount}/{container.ContainerName}");
        }

        private async Task CreateContainerMountWithManagedId(MountableContainer container, string resouceGroup, IKubernetes client, V1Deployment tesDeployment)
        {
            var sasSecretName = $"sa-secret-{container.StorageAccount}-{container.ContainerName}";
            var pvcName = $"{container.StorageAccount}-{container.ContainerName}-blob-claim1";
            var storageClassName = $"sc-blob-{container.StorageAccount}-{container.ContainerName}";

            var storageAccount = await Deployer.GetExistingStorageAccountAsync(container.StorageAccount, azureClient, subscriptionIds, configuration);
            if (!(await KubernetesSecretExists(client, sasSecretName)))
            {
                await client.CreateNamespacedSecretAsync(new V1Secret()
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

            var storageClassBody = new V1StorageClass()
            {
                Metadata = new V1ObjectMeta()
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

            var pvc = new V1PersistentVolumeClaim()
            {
                Metadata = new V1ObjectMeta()
                {
                    Name = pvcName
                },
                Spec = new V1PersistentVolumeClaimSpec()
                {
                    AccessModes = new List<string>() { "ReadWriteMany" },
                    StorageClassName = storageClassBody.Metadata.Name,
                    Resources = new V1ResourceRequirements()
                    {
                        Requests = new Dictionary<string, ResourceQuantity>()
                            {
                                { "storage", new ResourceQuantity("10Gi") }
                            }
                    }
                }
            };

            AddStaticVolumeClaim(tesDeployment, pvcName, $"/{container.StorageAccount}/{container.ContainerName}");
            await client.CreateNamespacedPersistentVolumeClaimAsync(pvc, configuration.AksCoANamespace);
        }

        private async Task<bool> KubernetesSecretExists(IKubernetes client, string sasSecretName)
        {
            V1Secret existing = null;
            try
            {
                existing = await client.ReadNamespacedSecretAsync(sasSecretName, configuration.AksCoANamespace);
            }
            catch (HttpOperationException e) { }
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

            var @switch = new Dictionary<Type, Action<object>> {
                { typeof(V1Service), (value) => client.CreateNamespacedService((V1Service)value, "kube-system") },
                { typeof(V1ServiceAccount), (value) => client.CreateNamespacedServiceAccount((V1ServiceAccount)value, "kube-system") },
                { typeof(V1ClusterRole), (value) => client.CreateClusterRole((V1ClusterRole)value) },
                { typeof(V1ClusterRoleBinding), (value) => client.CreateClusterRoleBinding((V1ClusterRoleBinding)value) },
                { typeof(V1DaemonSet), (value) => client.CreateNamespacedDaemonSet((V1DaemonSet)value, "kube-system") },
                { typeof(V1CSIDriver), (value) => client.CreateCSIDriver((V1CSIDriver)value) },
                { typeof(V1Deployment), (value) => client.CreateNamespacedDeployment((V1Deployment)value, "kube-system") },
            };

            foreach (var value in objects)
            {
                @switch[value.GetType()](value);
            }
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

            var tesDeploymentBody = await BuildTesDeployment(settings, storageAccount, resourceGroup.Name, client);
            var tesDeployment = await client.ReplaceNamespacedDeploymentAsync(tesDeploymentBody, "tes", configuration.AksCoANamespace);

            var triggerServiceDeploymentBody = BuildTriggerServiceDeployment(settings);
            var triggerServiceDeployment = await client.ReplaceNamespacedDeploymentAsync(triggerServiceDeploymentBody, "triggerservice", configuration.AksCoANamespace);

            var cromwellDeploymentBody = BuildCromwellDeployment(settings, storageAccount.Name);
            var cromwellDeployment = await client.ReplaceNamespacedDeploymentAsync(cromwellDeploymentBody, "cromwell", configuration.AksCoANamespace);

            var mysqlDeploymentBody = BuildMysqlDeployment(settings, storageAccount.Name);
            var mysqlDeployment = await client.ReplaceNamespacedDeploymentAsync(mysqlDeploymentBody, "mysqldb", configuration.AksCoANamespace);
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
                if (SasToken == null)
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
