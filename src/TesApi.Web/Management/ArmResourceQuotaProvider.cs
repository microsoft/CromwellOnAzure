// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Tes.Models;

namespace TesApi.Web.Management;

/// <summary>
/// Quota provider that uses the ARM API. 
/// </summary>
public class ArmResourceQuotaProvider : IResourceQuotaProvider
{
    /// <summary>
    /// Azure proxy instance
    /// </summary>
    private readonly IAzureProxy azureProxy;
    /// <summary>
    /// Logger instance.
    /// </summary>
    private readonly ILogger logger;


    /// <summary>
    /// Constructor of ArmResourceQuotaVerifier
    /// </summary>
    /// <param name="azureProxy"></param>
    /// <param name="logger"></param>
    public ArmResourceQuotaProvider(IAzureProxy azureProxy, ILogger logger)
    {
        this.azureProxy = azureProxy;
        this.logger = logger;
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

    /// <summary>
    /// Retrieves batch account quota information using the ARM API. 
    /// </summary>
    public async Task<BatchAccountQuotas> GetBatchAccountQuotaInformationAsync(VirtualMachineInformation virtualMachineInformation)
    {
        return ToBatchAccountQuotas(await azureProxy.GetBatchAccountQuotasAsync(), virtualMachineInformation);
    }
}
