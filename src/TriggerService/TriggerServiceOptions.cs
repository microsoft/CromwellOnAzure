// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TriggerService
{
    public class TriggerServiceOptions
    {
        public const string TriggerServiceOptionsSectionName = "TriggerService";
        public string AzureCloudName { get; set; } = "AzureCloud";
        public string DefaultStorageAccountName { get; set; }
        public string ApplicationInsightsAccountName { get; set; }
        public int MainRunIntervalMilliseconds { get; set; } = 20000;
        public int AvailabilityCheckIntervalMilliseconds { get; set; } = 30000;
    }
}
