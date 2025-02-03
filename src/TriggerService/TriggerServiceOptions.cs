// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TriggerService
{
    public class TriggerServiceOptions
    {
        public const string TriggerServiceOptionsSectionName = "TriggerService";
        /// <summary>
        /// Azure cloud name.  Defined here: https://github.com/Azure/azure-sdk-for-net/blob/bc9f38eca0d8abbf0697dd3e3e75220553eeeafa/sdk/identity/Azure.Identity/src/AzureAuthorityHosts.cs#L11
        /// </summary>
        public string AzureCloudName { get; set; } = "AzureCloud"; // or "AzureUSGovernment"

        /// <summary>
        /// Version of the API to use to get the Azure cloud metadata
        /// </summary>
        public string AzureCloudMetadataUrlApiVersion { get; set; } = "2023-11-01";
        public string DefaultStorageAccountName { get; set; }
        public string ApplicationInsightsAccountName { get; set; }
        public int MainRunIntervalMilliseconds { get; set; } = 20000;
        public int AvailabilityCheckIntervalMilliseconds { get; set; } = 30000;
    }
}
