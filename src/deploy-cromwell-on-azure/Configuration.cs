// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.Extensions.Configuration;

namespace CromwellOnAzureDeployer
{
    public class Configuration : UserAccessibleConfiguration
    {
        public string MySqlServerName { get; set; }
        public string MySqlDatabaseName { get; set; } = "cromwell_db";
        public string MySqlAdministratorLogin { get; set; } = "coa_admin";
        public string MySqlAdministratorPassword { get; set; }
        public string MySqlUserLogin { get; set; } = "cromwell";
        public string MySqlUserPassword { get; set; }
        public string MySqlSkuName { get; set; } = "Standard_B1s";
        public string MySqlTier { get; set; } = "Burstable";
        public string DefaultVmSubnetName { get; set; } = "vmsubnet";
        public string DefaultMySqlSubnetName { get; set; } = "mysqlsubnet";
        public string MySqlVersion { get; set; } = "8.0.21";
    }
    public abstract class UserAccessibleConfiguration
    {
        public string SubscriptionId { get; set; }
        public string RegionName { get; set; }
        public string MainIdentifierPrefix { get; set; } = "coa";
        public string VmOsProvider { get; set; } = "Canonical";
        public string VmOsName { get; set; } = "UbuntuServer";
        public string VmOsVersion { get; set; } = "18.04-LTS";
        public string VmSize { get; set; } = "Standard_D3_v2";
        public string VnetAddressSpace { get; set; } = "10.1.0.0/16";
        public string VmSubnetAddressSpace { get; set; } = "10.1.0.0/24";
        public string MySqlSubnetAddressSpace { get; set; } = "10.1.1.0/24";
        public string VmUsername { get; set; } = "vmadmin";
        public string VmPassword { get; set; }
        public string ResourceGroupName { get; set; }
        public string BatchAccountName { get; set; }
        public string StorageAccountName { get; set; }
        public string NetworkSecurityGroupName { get; set; }
        public string CosmosDbAccountName { get; set; }
        public string LogAnalyticsArmId { get; set; }
        public string ApplicationInsightsAccountName { get; set; }
        public string VmName { get; set; }
        public bool Silent { get; set; }
        public bool DeleteResourceGroupOnFailure { get; set; }
        public string CromwellVersion { get; set; }
        public string TesImageName { get; set; }
        public string TriggerServiceImageName { get; set; }
        public string CustomCromwellImagePath { get; set; }
        public string CustomTesImagePath { get; set; }
        public string CustomTriggerServiceImagePath { get; set; }
        public bool SkipTestWorkflow { get; set; } = false;
        public bool Update { get; set; } = false;
        public string VnetResourceGroupName { get; set; }
        public string VnetName { get; set; }
        public string SubnetName { get; set; }
        public string VmSubnetName { get; set; }
        public string MySqlSubnetName { get; set; }
        public bool? PrivateNetworking { get; set; } = null;
        public string Tags { get; set; } = null;
        public string BatchNodesSubnetId { get; set; } = null;
        public string DockerInDockerImageName { get; set; } = null;
        public string BlobxferImageName { get; set; } = null;
        public bool? DisableBatchNodesPublicIpAddress { get; set; } = null;
        public bool? KeepSshPortOpen { get; set; } = null;
        public bool? ProvisionMySqlOnAzure { get; set; } = null;

        public static Configuration BuildConfiguration(string[] args)
        {
            const string configFilename = "config.json";

            var configBuilder = new ConfigurationBuilder();

            if (System.IO.File.Exists(configFilename))
            {
                configBuilder.AddJsonFile(configFilename);
            }

            var configurationSource = configBuilder.AddCommandLine(args).Build();
            var configurationProperties = typeof(UserAccessibleConfiguration).GetTypeInfo().DeclaredProperties.Select(p => p.Name).ToList();

            var invalidArguments = configurationSource.Providers
                .SelectMany(p => p.GetChildKeys(new List<string>(), null))
                .Where(k => !configurationProperties.Contains(k, StringComparer.OrdinalIgnoreCase));

            if (invalidArguments.Any())
            {
                throw new ArgumentException($"Invalid argument(s): {string.Join(", ", invalidArguments)}");
            }

            var configuration = new Configuration();
            configurationSource.Bind(configuration);

            return configuration;
        }
    }
}
