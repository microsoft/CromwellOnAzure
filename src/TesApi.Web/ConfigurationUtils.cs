using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Tes.Models;

namespace TesApi.Web
{
    public class ConfigurationUtils
    {
        private readonly IConfiguration configuration;
        private readonly IAzureProxy azureProxy;
        private readonly IStorageAccessProvider storageAccessProvider;
        private readonly ILogger logger;

        public ConfigurationUtils(IConfiguration configuration, IAzureProxy azureProxy, IStorageAccessProvider storageAccessProvider, ILogger logger)
        {
            this.configuration = configuration;
            this.azureProxy = azureProxy;
            this.storageAccessProvider = storageAccessProvider;
            this.logger = logger;
        }

        // TODO TONY - split in three vm info / quota retrieval, allowed file processing, supported file processing
        public async Task<List<string>> ProcessAllowedVmSizesConfigurationAndGetAllowedVmSizesAsync()
        {
            var defaultStorageAccountName = configuration["DefaultStorageAccountName"];

            var supportedVmSizesFilePath = $"/{defaultStorageAccountName}/configuration/supported-vm-sizes";
            var supportedVmSizes = (await azureProxy.GetVmSizesAndPricesAsync()).OrderBy(v => v.VmFamily).ThenBy(v => v.VmSize).ToList();
            var batchAccountQuotas = await azureProxy.GetBatchAccountQuotasAsync();
            var supportedVmSizesFileContent = string.Join('\n', VirtualMachineInfoToFixedWidthColumns(supportedVmSizes, batchAccountQuotas));

            try
            {
                await storageAccessProvider.UploadBlobAsync(supportedVmSizesFilePath, supportedVmSizesFileContent);
            }
            catch
            {
                logger.LogWarning($"Failed to write {supportedVmSizesFilePath}. Updated VM size information will not be available in the configuration directory. This will not impact the workflow execution.");
            }

            var allowedVmSizesFilePath = $"/{defaultStorageAccountName}/configuration/allowed-vm-sizes";
            var allowedVmSizesFileContent = await storageAccessProvider.DownloadBlobAsync(allowedVmSizesFilePath);

            if (allowedVmSizesFileContent == null)
            {
                logger.LogWarning($"Unable to read from {allowedVmSizesFilePath}. All supported VM sizes will be eligible for Azure Batch task scheduling.");
                return null;
            }

            // Read the file and remove any warnings (those start with "<" following the VM size name)
            var allowedVmSizesLines = allowedVmSizesFileContent
                .Split(new[] { '\r', '\n' })
                .Select(line => line.Split('<', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).FirstOrDefault() ?? string.Empty)
                .ToList();

            var allowedVmSizesWithoutComments = allowedVmSizesLines
                .Select(line => line.Trim())
                .Where(line => !string.IsNullOrWhiteSpace(line) && !line.StartsWith("#"))
                .ToList();

            var allowedAndSupportedVmSizes = allowedVmSizesWithoutComments.Intersect(supportedVmSizes.Select(v => v.VmSize), StringComparer.OrdinalIgnoreCase).Distinct().ToList();
            var allowedVmSizesButNotSupported = allowedVmSizesWithoutComments.Except(allowedAndSupportedVmSizes).Distinct().ToList();

            if (allowedVmSizesButNotSupported.Any())
            {
                logger.LogWarning($"The following VM sizes are listed in {allowedVmSizesFilePath}, but are misspelled or not currently supported in your region: {string.Join(", ", allowedVmSizesButNotSupported)}. These will be ignored.");

                var linesWithWarningsAdded = allowedVmSizesLines.ConvertAll(line =>
                    allowedVmSizesButNotSupported.Contains(line, StringComparer.OrdinalIgnoreCase)
                        ? $"{line} <-- WARNING: This VM size is either misspelled or not supported in your region. It will be ignored."
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

            return allowedAndSupportedVmSizes;
        }

        public IEnumerable<string> VirtualMachineInfoToFixedWidthColumns(IEnumerable<VirtualMachineInfo> vmInfos, AzureBatchAccountQuotas batchAccountQuotas)
        {
            var vmSizes = vmInfos.Where(v => !v.LowPriority).Select(v => v.VmSize);

            var vmInfosAsStrings = vmSizes
                .Select(s => new { VmInfoWithDedicatedPrice = vmInfos.SingleOrDefault(l => l.VmSize == s && !l.LowPriority), PricePerHourLowPri = vmInfos.FirstOrDefault(l => l.VmSize == s && l.LowPriority)?.PricePerHour })
                .Select(v => new
                {
                    v.VmInfoWithDedicatedPrice.VmSize,
                    v.VmInfoWithDedicatedPrice.VmFamily,
                    PricePerHourDedicated = v.VmInfoWithDedicatedPrice.PricePerHour?.ToString("###0.000"),
                    PricePerHourLowPri = v.PricePerHourLowPri != null ? v.PricePerHourLowPri?.ToString("###0.000") : "N/A",
                    MemoryInGiB = v.VmInfoWithDedicatedPrice.MemoryInGB?.ToString(),
                    NumberOfCores = v.VmInfoWithDedicatedPrice.NumberOfCores.ToString(),
                    ResourceDiskSizeInGiB = v.VmInfoWithDedicatedPrice.ResourceDiskSizeInGB.ToString(),
                    DedicatedQuota = batchAccountQuotas.DedicatedCoreQuotaPerVMFamilyEnforced 
                        ? batchAccountQuotas.DedicatedCoreQuotaPerVMFamily.FirstOrDefault(q => q.Name.Equals(v.VmInfoWithDedicatedPrice.VmFamily, StringComparison.OrdinalIgnoreCase))?.CoreQuota.ToString() ?? "N/A"
                        : batchAccountQuotas.DedicatedCoreQuota.ToString()
                });

            vmInfosAsStrings = vmInfosAsStrings.Prepend(new { VmSize = "", VmFamily = "", PricePerHourDedicated = "dedicated", PricePerHourLowPri = "low pri", MemoryInGiB = "(GiB)", NumberOfCores = "", ResourceDiskSizeInGiB = "(GiB)", DedicatedQuota = $"quota {(batchAccountQuotas.DedicatedCoreQuotaPerVMFamilyEnforced ? "(per fam.)" : "(total)")}" });
            vmInfosAsStrings = vmInfosAsStrings.Prepend(new { VmSize = "VM Size", VmFamily = "Family", PricePerHourDedicated = "$/hour", PricePerHourLowPri = "$/hour", MemoryInGiB = "Memory", NumberOfCores = "CPUs", ResourceDiskSizeInGiB = "Disk", DedicatedQuota = "Dedicated CPU" });

            var sizeColWidth = vmInfosAsStrings.Max(v => v.VmSize.Length);
            var seriesColWidth = vmInfosAsStrings.Max(v => v.VmFamily.Length);
            var priceDedicatedColumnWidth = vmInfosAsStrings.Max(v => v.PricePerHourDedicated.Length);
            var priceLowPriColumnWidth = vmInfosAsStrings.Max(v => v.PricePerHourLowPri.Length);
            var memoryColumnWidth = vmInfosAsStrings.Max(v => v.MemoryInGiB.Length);
            var coresColumnWidth = vmInfosAsStrings.Max(v => v.NumberOfCores.Length);
            var diskColumnWidth = vmInfosAsStrings.Max(v => v.ResourceDiskSizeInGiB.Length);
            var dedicatedQuotaColumnWidth = vmInfosAsStrings.Max(v => v.DedicatedQuota.Length);

            return vmInfosAsStrings.Select(v => $"{v.VmSize.PadRight(sizeColWidth)} {v.VmFamily.PadRight(seriesColWidth)} {v.PricePerHourDedicated.PadLeft(priceDedicatedColumnWidth)}  {v.PricePerHourLowPri.PadLeft(priceLowPriColumnWidth)}  {v.MemoryInGiB.PadLeft(memoryColumnWidth)}  {v.NumberOfCores.PadLeft(coresColumnWidth)}  {v.ResourceDiskSizeInGiB.PadLeft(diskColumnWidth)}  {v.DedicatedQuota.PadLeft(dedicatedQuotaColumnWidth)}");
        }
    }
}
