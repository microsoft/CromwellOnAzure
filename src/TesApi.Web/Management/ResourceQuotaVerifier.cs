// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Tes.Models;

namespace TesApi.Web.Management;


/// <summary>
/// Contains logic that verifies if the batch account can fulfill the compute requirements using quota and sizing information.
/// </summary>
public class ResourceQuotaVerifier : IResourceQuotaVerifier
{
    private const string AzureSupportUrl = $"https://portal.azure.com/#blade/Microsoft_Azure_Support/HelpAndSupportBlade/newsupportrequest";
    private readonly IAzureProxy azureProxy;
    private readonly ILogger logger;
    private readonly IResourceQuotaProvider resourceQuotaProvider;
    private readonly IBatchSkuInformationProvider batchSkuInformationProvider;
    private readonly string region;


    /// <summary>
    /// Constructor of BaseResourceQuotaVerifier
    /// </summary>
    /// <param name="azureProxy"></param>
    /// <param name="resourceQuotaProvider"></param>
    /// <param name="batchSkuInformationProvider"></param>
    /// <param name="logger"></param>
    /// <param name="region"></param>
    public ResourceQuotaVerifier(IAzureProxy azureProxy, IResourceQuotaProvider resourceQuotaProvider,
        IBatchSkuInformationProvider batchSkuInformationProvider, string region, ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(azureProxy);
        ArgumentNullException.ThrowIfNull(resourceQuotaProvider);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(batchSkuInformationProvider);
        if (String.IsNullOrEmpty(region))
        {
            throw new ArgumentException("Invalid region. The value is null or empty.", nameof(region));
        }

        this.region = region;
        this.azureProxy = azureProxy;
        this.logger = logger;
        this.batchSkuInformationProvider = batchSkuInformationProvider;
        this.resourceQuotaProvider = resourceQuotaProvider;
    }

    /// <summary>
    /// Verifies if the batch account can fulfill the compute requirements
    /// </summary>
    /// <param name="virtualMachineInformation"></param>
    /// <exception cref="AzureBatchLowQuotaException"></exception>
    /// <exception cref="AzureBatchQuotaMaxedOutException"></exception>
    public async Task CheckBatchAccountQuotasAsync(VirtualMachineInformation virtualMachineInformation)
    {
        var workflowCoresRequirement = virtualMachineInformation.NumberOfCores ?? 0;
        var isDedicated = !virtualMachineInformation.LowPriority;
        var vmFamily = virtualMachineInformation.VmFamily;
        BatchAccountQuotas batchQuotas;

        try
        {
            batchQuotas = await resourceQuotaProvider.GetBatchAccountQuotaInformationAsync(virtualMachineInformation);

            if (batchQuotas == null)
            {
                throw new InvalidOperationException(
                    "Could not obtain quota information from the management service. The return value is null");
            }
        }
        catch (Exception e)
        {
            logger.LogError("Failed to retrieve quota information for the management provider", e);
            throw;
        }

        var isDedicatedAndPerVmFamilyCoreQuotaEnforced = isDedicated && batchQuotas.DedicatedCoreQuotaPerVMFamilyEnforced;
        var batchUtilization = await GetBatchAccountUtilizationAsync(virtualMachineInformation);


        if (workflowCoresRequirement > batchQuotas.TotalCoreQuota)
        {
            // The workflow task requires more cores than the total Batch account's cores quota - FAIL
            throw new AzureBatchLowQuotaException($"Azure Batch Account does not have enough {(isDedicated ? "dedicated" : "low priority")} cores quota to run a workflow with cpu core requirement of {workflowCoresRequirement}. Please submit an Azure Support request to increase your quota: {AzureSupportUrl}");
        }

        if (isDedicatedAndPerVmFamilyCoreQuotaEnforced && workflowCoresRequirement > batchQuotas.VmFamilyQuota)
        {
            // The workflow task requires more cores than the total Batch account's dedicated family quota - FAIL
            throw new AzureBatchLowQuotaException($"Azure Batch Account does not have enough dedicated {vmFamily} cores quota to run a workflow with cpu core requirement of {workflowCoresRequirement}. Please submit an Azure Support request to increase your quota: {AzureSupportUrl}");
        }

        if (batchUtilization.ActiveJobsCount + 1 > batchQuotas.ActiveJobAndJobScheduleQuota)
        {
            throw new AzureBatchQuotaMaxedOutException($"No remaining active jobs quota available. There are {batchUtilization.ActivePoolsCount} active jobs out of {batchQuotas.ActiveJobAndJobScheduleQuota}.");
        }

        if (batchUtilization.ActivePoolsCount + 1 > batchQuotas.PoolQuota)
        {
            throw new AzureBatchQuotaMaxedOutException($"No remaining pool quota available. There are {batchUtilization.ActivePoolsCount} pools in use out of {batchQuotas.PoolQuota}.");
        }

        if ((batchUtilization.TotalCoresInUse + workflowCoresRequirement) > batchQuotas.TotalCoreQuota)
        {
            throw new AzureBatchQuotaMaxedOutException($"Not enough core quota remaining to schedule task requiring {workflowCoresRequirement} {(isDedicated ? "dedicated" : "low priority")} cores. There are {batchUtilization.TotalCoresInUse} cores in use out of {batchQuotas.TotalCoreQuota}.");
        }

        if (isDedicatedAndPerVmFamilyCoreQuotaEnforced && batchUtilization.DedicatedCoresInUseInRequestedVmFamily + workflowCoresRequirement > batchQuotas.VmFamilyQuota)
        {

            throw new AzureBatchQuotaMaxedOutException($"Not enough core quota remaining to schedule task requiring {workflowCoresRequirement} dedicated {vmFamily} cores. There are {batchUtilization.DedicatedCoresInUseInRequestedVmFamily} cores in use out of {batchQuotas.VmFamilyQuota}.");
        }
    }

    private async Task<BatchAccountUtilization> GetBatchAccountUtilizationAsync(VirtualMachineInformation vmInfo)
    {
        var isDedicated = !vmInfo.LowPriority;
        var activeJobsCount = azureProxy.GetBatchActiveJobCount();
        var activePoolsCount = azureProxy.GetBatchActivePoolCount();
        var activeNodeCountByVmSize = azureProxy.GetBatchActiveNodeCountByVmSize().ToList();
        var virtualMachineInfoList = await batchSkuInformationProvider.GetVmSizesAndPricesAsync(region);

        var totalCoresInUse = activeNodeCountByVmSize
            .Sum(x =>
                virtualMachineInfoList
                    .FirstOrDefault(vm => vm.VmSize.Equals(x.VirtualMachineSize, StringComparison.OrdinalIgnoreCase))?
                    .NumberOfCores * (isDedicated ? x.DedicatedNodeCount : x.LowPriorityNodeCount)) ?? 0;

        var vmSizesInRequestedFamily = virtualMachineInfoList.Where(vm => String.Equals(vm.VmFamily, vmInfo.VmFamily, StringComparison.OrdinalIgnoreCase)).Select(vm => vm.VmSize).ToList();

        var activeNodeCountByVmSizeInRequestedFamily = activeNodeCountByVmSize.Where(x => vmSizesInRequestedFamily.Contains(x.VirtualMachineSize, StringComparer.OrdinalIgnoreCase));

        var dedicatedCoresInUseInRequestedVmFamily = activeNodeCountByVmSizeInRequestedFamily
            .Sum(x => virtualMachineInfoList.FirstOrDefault(vm => vm.VmSize.Equals(x.VirtualMachineSize, StringComparison.OrdinalIgnoreCase))?.NumberOfCores * x.DedicatedNodeCount) ?? 0;


        return new BatchAccountUtilization(activeJobsCount, activePoolsCount, totalCoresInUse, dedicatedCoresInUseInRequestedVmFamily);

    }

}
