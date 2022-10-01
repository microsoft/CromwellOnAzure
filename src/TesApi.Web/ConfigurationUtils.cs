// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Tes.Models;

namespace TesApi.Web
{
    /// <summary>
    /// Provides methods for handling the configuration files in the configuration container
    /// </summary>
    public class ConfigurationUtils
    {
        private readonly IConfiguration configuration;
        private readonly IAzureProxy azureProxy;
        private readonly IStorageAccessProvider storageAccessProvider;
        private readonly ILogger logger;

        /// <summary>
        /// The constructor
        /// </summary>
        /// <param name="configuration"><see cref="IConfiguration"/></param>
        /// <param name="azureProxy"><see cref="IAzureProxy"/></param>
        /// <param name="storageAccessProvider"><see cref="IStorageAccessProvider"/></param>
        /// <param name="logger"><see cref="ILogger"/></param>
        public ConfigurationUtils(IConfiguration configuration, IAzureProxy azureProxy, IStorageAccessProvider storageAccessProvider, ILogger<ConfigurationUtils> logger)
        {
            this.configuration = configuration;
            this.azureProxy = azureProxy;
            this.storageAccessProvider = storageAccessProvider;
            this.logger = logger;
        }

        /// <summary>
        /// Combines the allowed-vm-sizes configuration file and list of supported+available VMs to produce the supported-vm-sizes file and tag incorrect 
        /// entries in the allowed-vm-sizes file with a warning. Sets the AllowedVmSizes configuration key.
        /// </summary>
        /// <returns></returns>
        public async Task ProcessAllowedVmSizesConfigurationFileAsync()
        {
            var defaultStorageAccountName = configuration["DefaultStorageAccountName"];
            var supportedVmSizesFilePath = $"/{defaultStorageAccountName}/configuration/supported-vm-sizes";
            var allowedVmSizesFilePath = $"/{defaultStorageAccountName}/configuration/allowed-vm-sizes";

            var supportedVmSizes = (await azureProxy.GetVmSizesAndPricesAsync()).ToList();
            var batchAccountQuotas = await azureProxy.GetBatchAccountQuotasAsync();
            var supportedVmSizesFileContent = VirtualMachineInfoToFixedWidthColumns(supportedVmSizes.OrderBy(v => v.VmFamily).ThenBy(v => v.VmSize), batchAccountQuotas);

            try
            {
                await storageAccessProvider.UploadBlobAsync(supportedVmSizesFilePath, supportedVmSizesFileContent);
            }
            catch
            {
                logger.LogWarning($"Failed to write {supportedVmSizesFilePath}. Updated VM size information will not be available in the configuration directory. This will not impact the workflow execution.");
            }

            var allowedVmSizesFileContent = await storageAccessProvider.DownloadBlobAsync(allowedVmSizesFilePath);

            if (allowedVmSizesFileContent is null)
            {
                logger.LogWarning($"Unable to read from {allowedVmSizesFilePath}. All supported VM sizes will be eligible for Azure Batch task scheduling.");
                return;
            }

            // Read the allowed-vm-sizes configuration file and remove any previous warnings (those start with "<" following the VM size or family name)
            var allowedVmSizesLines = allowedVmSizesFileContent
                .Split(new[] { '\r', '\n' })
                .Select(line => line.Split('<', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).FirstOrDefault() ?? string.Empty)
                .ToList();

            var allowedVmSizesWithoutComments = allowedVmSizesLines
                .Select(line => line.Trim())
                .Where(line => !string.IsNullOrWhiteSpace(line) && !line.StartsWith("#"))
                .ToList();

            var allowedAndSupportedVmSizes = allowedVmSizesWithoutComments.Intersect(supportedVmSizes.Select(v => v.VmSize), StringComparer.OrdinalIgnoreCase)
                .Union(allowedVmSizesWithoutComments.Intersect(supportedVmSizes.Select(v => v.VmFamily), StringComparer.OrdinalIgnoreCase))
                .Distinct()
                .ToList();

            var allowedVmSizesButNotSupported = allowedVmSizesWithoutComments.Except(allowedAndSupportedVmSizes).Distinct().ToList();

            if (allowedVmSizesButNotSupported.Any())
            {
                logger.LogWarning($"The following VM sizes or families are listed in {allowedVmSizesFilePath}, but are either misspelled or not supported in your region: {string.Join(", ", allowedVmSizesButNotSupported)}. These will be ignored.");

                var linesWithWarningsAdded = allowedVmSizesLines.ConvertAll(line =>
                    allowedVmSizesButNotSupported.Contains(line, StringComparer.OrdinalIgnoreCase)
                        ? $"{line} <-- WARNING: This VM size or family is either misspelled or not supported in your region. It will be ignored."
                        : line
                );

                var allowedVmSizesFileContentWithWarningsAdded = string.Join('\n', linesWithWarningsAdded);

                if (allowedVmSizesFileContentWithWarningsAdded != allowedVmSizesFileContent)
                {
                    try
                    {
                        await storageAccessProvider.UploadBlobAsync(allowedVmSizesFilePath, allowedVmSizesFileContentWithWarningsAdded);
                    }
                    catch
                    {
                        logger.LogWarning($"Failed to write warnings to {allowedVmSizesFilePath}.");
                    }
                }
            }

            if (allowedAndSupportedVmSizes.Any())
            {
                this.configuration["AllowedVmSizes"] = string.Join(',', allowedAndSupportedVmSizes);
            }
        }

        /// <summary>
        /// Combines the VM feature and price info with Batch quotas and produces the fixed-width list ready for uploading to .
        /// </summary>
        /// <param name="vmInfos">List of <see cref="VirtualMachineInformation"/></param>
        /// <param name="batchAccountQuotas">Batch quotas <see cref="AzureBatchAccountQuotas"/></param>
        /// <returns></returns>
        private static string VirtualMachineInfoToFixedWidthColumns(IEnumerable<VirtualMachineInformation> vmInfos, AzureBatchAccountQuotas batchAccountQuotas)
        {
            var vmSizes = vmInfos.Where(v => !v.LowPriority).Select(v => v.VmSize);

            var vmInfosAsStrings = vmSizes
                .Select(s => new { VmInfoWithDedicatedPrice = vmInfos.SingleOrDefault(l => l.VmSize == s && !l.LowPriority), PricePerHourLowPri = vmInfos.FirstOrDefault(l => l.VmSize == s && l.LowPriority)?.PricePerHour })
                .Select(v => new
                {
                    v.VmInfoWithDedicatedPrice.VmSize,
                    v.VmInfoWithDedicatedPrice.VmFamily,
                    PricePerHourDedicated = v.VmInfoWithDedicatedPrice.PricePerHour?.ToString("###0.000"),
                    PricePerHourLowPri = v.PricePerHourLowPri is not null ? v.PricePerHourLowPri?.ToString("###0.000") : "N/A",
                    MemoryInGiB = v.VmInfoWithDedicatedPrice.MemoryInGB?.ToString(),
                    NumberOfCores = v.VmInfoWithDedicatedPrice.NumberOfCores.ToString(),
                    ResourceDiskSizeInGiB = v.VmInfoWithDedicatedPrice.ResourceDiskSizeInGB.ToString(),
                    DedicatedQuota = batchAccountQuotas.DedicatedCoreQuotaPerVMFamilyEnforced
                        ? batchAccountQuotas.DedicatedCoreQuotaPerVMFamily.FirstOrDefault(q => q.Name.Equals(v.VmInfoWithDedicatedPrice.VmFamily, StringComparison.OrdinalIgnoreCase))?.CoreQuota.ToString() ?? "N/A"
                        : batchAccountQuotas.DedicatedCoreQuota.ToString()
                });

            vmInfosAsStrings = vmInfosAsStrings.Prepend(new { VmSize = string.Empty, VmFamily = string.Empty, PricePerHourDedicated = "dedicated", PricePerHourLowPri = "low pri", MemoryInGiB = "(GiB)", NumberOfCores = string.Empty, ResourceDiskSizeInGiB = "(GiB)", DedicatedQuota = $"quota {(batchAccountQuotas.DedicatedCoreQuotaPerVMFamilyEnforced ? "(per fam.)" : "(total)")}" });
            vmInfosAsStrings = vmInfosAsStrings.Prepend(new { VmSize = "VM Size", VmFamily = "Family", PricePerHourDedicated = "$/hour", PricePerHourLowPri = "$/hour", MemoryInGiB = "Memory", NumberOfCores = "CPUs", ResourceDiskSizeInGiB = "Disk", DedicatedQuota = "Dedicated CPU" });

            var sizeColWidth = vmInfosAsStrings.Max(v => v.VmSize.Length);
            var seriesColWidth = vmInfosAsStrings.Max(v => v.VmFamily.Length);
            var priceDedicatedColumnWidth = vmInfosAsStrings.Max(v => v.PricePerHourDedicated.Length);
            var priceLowPriColumnWidth = vmInfosAsStrings.Max(v => v.PricePerHourLowPri.Length);
            var memoryColumnWidth = vmInfosAsStrings.Max(v => v.MemoryInGiB.Length);
            var coresColumnWidth = vmInfosAsStrings.Max(v => v.NumberOfCores.Length);
            var diskColumnWidth = vmInfosAsStrings.Max(v => v.ResourceDiskSizeInGiB.Length);
            var dedicatedQuotaColumnWidth = vmInfosAsStrings.Max(v => v.DedicatedQuota.Length);

            var fixedWidthVmInfos = vmInfosAsStrings.Select(v => $"{v.VmSize.PadRight(sizeColWidth)} {v.VmFamily.PadRight(seriesColWidth)} {v.PricePerHourDedicated.PadLeft(priceDedicatedColumnWidth)}  {v.PricePerHourLowPri.PadLeft(priceLowPriColumnWidth)}  {v.MemoryInGiB.PadLeft(memoryColumnWidth)}  {v.NumberOfCores.PadLeft(coresColumnWidth)}  {v.ResourceDiskSizeInGiB.PadLeft(diskColumnWidth)}  {v.DedicatedQuota.PadLeft(dedicatedQuotaColumnWidth)}");

            return string.Join('\n', fixedWidthVmInfos);
        }
    }
}
