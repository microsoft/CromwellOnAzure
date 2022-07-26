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
        public string PostgreSqlServerName { get; set; }
        public string PostgreSqlCromwellDatabaseName { get; set; } = "cromwell_db";
        public string PostgreSqlTesDatabaseName { get; set; } = "tes_db";
        public string PostgreSqlAdministratorLogin { get; set; } = "coa_admin";
        public string PostgreSqlAdministratorPassword { get; set; }
        public string PostgreSqlCromwellUserLogin { get; set; } = "cromwell";
        public string PostgreSqlCromwellUserPassword { get; set; }
        public string PostgreSqlTesUserLogin { get; set; } = "tes";
        public string PostgreSqlTesUserPassword { get; set; }
        public string PostgreSqlSkuName { get; set; } = "Standard_B2s";
        public string PostgreSqlTier { get; set; } = "Burstable";
        public string DefaultVmSubnetName { get; set; } = "vmsubnet";
        public string DefaultPostgreSqlSubnetName { get; set; } = "mysqlsubnet";
        public string PostgreSqlVersion { get; set; } = "11";
        public int PostgreSqlStorageSize { get; set; } = 128;  // GiB
        public bool? ProvisionPostgreSqlOnAzure { get; set; } // Will be accessible in 4.0 release
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
        public string PostgreSqlSubnetAddressSpace { get; set; } = "10.1.1.0/24";
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
        public string PostgreSqlSubnetName { get; set; }
        public bool? PrivateNetworking { get; set; } = null;
        public string Tags { get; set; } = null;
        public string BatchNodesSubnetId { get; set; } = null;
        public string DockerInDockerImageName { get; set; } = null;
        public string BlobxferImageName { get; set; } = null;
        public bool? DisableBatchNodesPublicIpAddress { get; set; } = null;
        public bool? KeepSshPortOpen { get; set; } = null;

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
