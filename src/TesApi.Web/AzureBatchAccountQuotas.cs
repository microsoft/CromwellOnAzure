// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web
{
    public struct AzureBatchAccountQuotas
    {
        public int ActiveJobAndJobScheduleQuota { get; set; }
        public int DedicatedCoreQuota { get; set; }
        public int LowPriorityCoreQuota { get; set; }
        public int PoolQuota { get; set; }
    }
}
