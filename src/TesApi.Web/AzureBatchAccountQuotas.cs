// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;
using Microsoft.Azure.Management.Batch.Models;

namespace TesApi.Web
{
    public struct AzureBatchAccountQuotas
    {
        public int ActiveJobAndJobScheduleQuota { get; set; }
        public int DedicatedCoreQuota { get; set; }
        public IList<VirtualMachineFamilyCoreQuota> DedicatedCoreQuotaPerVMFamily { get; set; }
        public bool DedicatedCoreQuotaPerVMFamilyEnforced { get; set; }
        public int LowPriorityCoreQuota { get; set; }
        public int PoolQuota { get; set; }
    }
}
