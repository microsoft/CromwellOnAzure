// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Azure.Management.Compute.Fluent;
using Microsoft.Azure.Management.Compute.Fluent.Models;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Extensions.Configuration;

namespace CromwellOnAzureDeployer
{
    public class Configuration
    {
        public string SubscriptionId { get; set; }
        public string RegionName { get; set; } = "westus";
        public string MainIdentifierPrefix { get; set; } = "coa";
        public KnownLinuxVirtualMachineImage VmImage { get; set; } = KnownLinuxVirtualMachineImage.UbuntuServer16_04_Lts;
        public string VmSize { get; set; } = "Standard_D2_v2";
        public string VnetAddressSpace { get; set; } = "10.0.0.0/24";
        public string VmUsername { get; set; } = "vmadmin";
        public string VmPassword { get; set; } = Utility.GeneratePassword();
        public string ResourceGroupName { get; set; }
        public string BatchAccountName { get; set; }
        public string StorageAccountName { get; set; }
        public string NetworkSecurityGroupName { get; set; }
        public string CosmosDbAccountName { get; set; }
        public string ApplicationInsightsAccountName { get; set; }
        public string VmName { get; set; }
        public bool Silent { get; set; }
        public bool DeleteResourceGroupOnFailure { get; set; }
        public string CromwellImageName { get; set; } = "broadinstitute/cromwell:prod";
        public string TesImageName { get; set; } = "mcr.microsoft.com/cromwellonazure/tes:1";
        public string TriggerServiceImageName { get; set; } = "mcr.microsoft.com/cromwellonazure/triggerservice:1";
        public string CustomCromwellImagePath { get; set; }
        public string CustomTesImagePath { get; set; }
        public string CustomTriggerServiceImagePath { get; set; }

        public static Configuration BuildConfiguration(string[] args)
        {
            const string configFilename = "config.json";

            var configBuilder = new ConfigurationBuilder();

            if (System.IO.File.Exists(configFilename))
            {
                configBuilder.AddJsonFile(configFilename);
            }

            var configuration = new Configuration();

            configBuilder.AddCommandLine(args)
                .Build()
                .Bind(configuration);

            configuration.SetDefaultsAndOverrides();

            return configuration;
        }

        public void SetDefaultsAndOverrides()
        {
            if (string.IsNullOrWhiteSpace(ResourceGroupName))
            {
                ResourceGroupName = SdkContext.RandomResourceName($"{MainIdentifierPrefix}-", 15);
            }

            if (string.IsNullOrWhiteSpace(BatchAccountName))
            {
                BatchAccountName = SdkContext.RandomResourceName($"{MainIdentifierPrefix}", 15);
            }

            if (string.IsNullOrWhiteSpace(StorageAccountName))
            {
                StorageAccountName = SdkContext.RandomResourceName($"{MainIdentifierPrefix}", 24);
            }

            if (string.IsNullOrWhiteSpace(NetworkSecurityGroupName))
            {
                NetworkSecurityGroupName = SdkContext.RandomResourceName($"{MainIdentifierPrefix}", 15);
            }

            if (string.IsNullOrWhiteSpace(CosmosDbAccountName))
            {
                CosmosDbAccountName = SdkContext.RandomResourceName($"{MainIdentifierPrefix}-", 15);
            }

            if (string.IsNullOrWhiteSpace(ApplicationInsightsAccountName))
            {
                ApplicationInsightsAccountName = SdkContext.RandomResourceName($"{MainIdentifierPrefix}-", 15);
            }

            if (string.IsNullOrWhiteSpace(VmName))
            {
                VmName = SdkContext.RandomResourceName($"{MainIdentifierPrefix}-", 25);
            }
        }
    }
}
