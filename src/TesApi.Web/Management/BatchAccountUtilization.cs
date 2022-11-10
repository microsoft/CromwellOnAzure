namespace TesApi.Web.Management
{
    /// <summary>
    /// A record containing utilization information for a batch account
    /// </summary>
    /// <param name="ActiveJobsCount">Active job counts</param>
    /// <param name="ActivePoolsCount">Active pool count</param>
    /// <param name="TotalCoresInUse">Total cores in use</param>
    /// <param name="DedicatedCoresInUseInRequestedVmFamily">Number of dedicated cores in requested Vm family</param>
    public record BatchAccountUtilization(
        int ActiveJobsCount,
        int ActivePoolsCount,
        int TotalCoresInUse,
        int DedicatedCoresInUseInRequestedVmFamily);

}
