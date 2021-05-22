// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web
{
    /// <summary>
    /// AzureBatchAccountQuotas
    /// </summary>
    public struct AzureBatchAccountQuotas
    {
        /// <summary>
        /// ActiveJobAndJobScheduleQuota
        /// </summary>
        public int ActiveJobAndJobScheduleQuota { get; set; }

        /// <summary>
        /// DedicatedCoreQuota
        /// </summary>
        public int DedicatedCoreQuota { get; set; }

        /// <summary>
        /// LowPriorityCoreQuota
        /// </summary>
        public int LowPriorityCoreQuota { get; set; }

        /// <summary>
        /// PoolQuota
        /// </summary>
        public int PoolQuota { get; set; }
    }
}
