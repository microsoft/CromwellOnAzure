namespace TesApi.Web.Management
{
    /// <summary>
    /// Record representing the batch account quotas for a vm family. 
    /// </summary>
    /// <param name="TotalCoreQuota">Total core quota</param>
    /// <param name="VmFamilyQuota">Vm Family quota</param>
    /// <param name="PoolQuota">Pool quota</param>
    /// <param name="ActiveJobAndJobScheduleQuota">Job and job schedule quota</param>
    /// <param name="DedicatedCoreQuotaPerVMFamilyEnforced">if quota per vm family is enforced for a vm family</param>
    public record BatchAccountQuotas(int TotalCoreQuota, int VmFamilyQuota, int PoolQuota,
        int ActiveJobAndJobScheduleQuota, bool DedicatedCoreQuotaPerVMFamilyEnforced);

}
