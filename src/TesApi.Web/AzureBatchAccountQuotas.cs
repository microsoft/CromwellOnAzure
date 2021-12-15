// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;
using Microsoft.Azure.Management.Batch.Models;

namespace TesApi.Web
{
    /// <summary>
    ///  Contains information about an Azure Batch account's quotas.
    /// </summary>
    public struct AzureBatchAccountQuotas
    {
        /// <summary>
        /// Gets the active job and job schedule quota for the Batch account.
        /// </summary>
        public int ActiveJobAndJobScheduleQuota { get; set; }
        /// <summary>
        /// Gets the dedicated core quota for the Batch account.
        /// </summary>
        /// <remarks>
        /// For accounts with PoolAllocationMode set to UserSubscription, quota is managed
        ///     on the subscription so this value is not returned.
        /// </remarks>
        public int DedicatedCoreQuota { get; set; }
        /// <summary>
        /// Gets a list of the dedicated core quota per Virtual Machine family for the Batch
        ///     account. For accounts with PoolAllocationMode set to UserSubscription, quota
        ///     is managed on the subscription so this value is not returned.
        /// </summary>
        public IList<VirtualMachineFamilyCoreQuota> DedicatedCoreQuotaPerVMFamily { get; set; }
        /// <summary>
        /// Gets a value indicating whether core quotas per Virtual Machine family are enforced
        ///     for this account
        /// </summary>
        /// <remarks>
        /// Batch is transitioning its core quota system for dedicated cores to be enforced
        ///     per Virtual Machine family. During this transitional phase, the dedicated core
        ///     quota per Virtual Machine family may not yet be enforced. If this flag is false,
        ///     dedicated core quota is enforced via the old dedicatedCoreQuota property on the
        ///     account and does not consider Virtual Machine family. If this flag is true, dedicated
        ///     core quota is enforced via the dedicatedCoreQuotaPerVMFamily property on the
        ///     account, and the old dedicatedCoreQuota does not apply.
        /// </remarks>
        public bool DedicatedCoreQuotaPerVMFamilyEnforced { get; set; }
        /// <summary>
        /// Gets the low priority core quota for the Batch account.
        /// </summary>
        /// <remarks>
        /// For accounts with PoolAllocationMode set to UserSubscription, quota is managed
        ///     on the subscription so this value is not returned.
        /// </remarks>
        public int LowPriorityCoreQuota { get; set; }
        /// <summary>
        /// Gets the pool quota for the Batch account.
        /// </summary>
        public int PoolQuota { get; set; }
    }
}
