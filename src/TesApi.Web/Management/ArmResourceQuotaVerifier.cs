// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Tes.Models;

namespace TesApi.Web.Management;

/// <summary>
/// Verifies that batch account can fulfill the compute requirements using the ARM API. 
/// </summary>
public class ArmResourceQuotaVerifier : BaseResourceQuotaVerifier
{


    /// <summary>
    /// Constructor of ArmResourceQuotaVerifier
    /// </summary>
    /// <param name="azureProxy"></param>
    /// <param name="logger"></param>
    public ArmResourceQuotaVerifier(IAzureProxy azureProxy, ILogger logger) : base(azureProxy, logger)
    {
    }

    /// <summary>
    /// Retrieves batch account quota information using the ARM API. 
    /// </summary>
    /// <returns>Batch account quota information</returns>
    /// <exception cref="NotImplementedException"></exception>
    protected override async Task<BatchAccountQuotas> GetBatchAccountQuotasFromManagementServiceAsync(VirtualMachineInformation vmInfo)
    {
        return ToBatchAccountQuotas(await azureProxy.GetBatchAccountQuotasAsync(), vmInfo);
    }

    private BatchAccountQuotas ToBatchAccountQuotas(AzureBatchAccountQuotas batchAccountQuotas, VirtualMachineInformation vmInfo)
    {

        var isDedicated = !vmInfo.LowPriority;
        var vmCoresRequirement = vmInfo.NumberOfCores ?? 0;
        var vmFamily = vmInfo.VmFamily;
        var totalCoreQuota = isDedicated ? batchAccountQuotas.DedicatedCoreQuota : batchAccountQuotas.LowPriorityCoreQuota;
        var isDedicatedAndPerVmFamilyCoreQuotaEnforced =
            isDedicated && batchAccountQuotas.DedicatedCoreQuotaPerVMFamilyEnforced;

        var vmFamilyCoreQuota = isDedicatedAndPerVmFamilyCoreQuotaEnforced
            ? batchAccountQuotas.DedicatedCoreQuotaPerVMFamily.FirstOrDefault(q => q.Name.Equals(vmFamily,
                      StringComparison.OrdinalIgnoreCase))
                  ?.CoreQuota ??
              0
            : vmCoresRequirement;

        return new BatchAccountQuotas(totalCoreQuota, vmFamilyCoreQuota, batchAccountQuotas.PoolQuota,
            batchAccountQuotas.ActiveJobAndJobScheduleQuota, batchAccountQuotas.DedicatedCoreQuotaPerVMFamilyEnforced);
    }

}
