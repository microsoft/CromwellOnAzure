// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Tes.Models;

namespace TesApi.Web.Management;


/// <summary>
/// Base class containing checks that verify that the batch account can fulfill the compute requirements.
/// </summary>
public abstract class BaseResourceQuotaVerifier : IResourceQuotaVerifier
{
    private const string AzureSupportUrl = $"https://portal.azure.com/#blade/Microsoft_Azure_Support/HelpAndSupportBlade/newsupportrequest";

    /// <summary>
    /// Azure proxy instance
    /// </summary>
    protected readonly IAzureProxy azureProxy;
    /// <summary>
    /// Logger instance.
    /// </summary>
    protected readonly ILogger logger;

    /// <summary>
    /// Constructor of BaseResourceQuotaVerifier
    /// </summary>
    /// <param name="azureProxy"></param>
    /// <param name="logger"></param>
    protected BaseResourceQuotaVerifier(IAzureProxy azureProxy, ILogger logger)
    {
        this.azureProxy = azureProxy;
        this.logger = logger;
    }

    /// <summary>
    /// Returns the quota information for the requested vm family 
    /// </summary>
    /// <param name="virtualMachineInformation">Requested vm family information</param>
    /// <returns></returns>
    protected abstract Task<BatchAccountQuotas> GetBatchAccountQuotasFromManagementServiceAsync(VirtualMachineInformation virtualMachineInformation);

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

        var batchQuotas = await GetBatchAccountQuotasFromManagementServiceAsync(virtualMachineInformation);
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
        var virtualMachineInfoList = await azureProxy.GetVmSizesAndPricesAsync();

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
