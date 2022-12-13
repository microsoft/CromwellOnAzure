using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using LazyCache;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Tes.Models;

namespace TesApi.Web.Management
{
    public class PriceApiBatchSkuInformationProvider : IBatchSkuInformationProvider
    {
        private readonly PriceApiClient priceApiClient;
        private readonly IAppCache appCache;
        private readonly ILogger logger;

        public PriceApiBatchSkuInformationProvider(IAppCache appCache, PriceApiClient priceApiClient, ILogger<PriceApiBatchSkuInformationProvider> logger)
        {
            ArgumentNullException.ThrowIfNull(priceApiClient);
            ArgumentNullException.ThrowIfNull(logger);

            this.appCache = appCache;
            this.priceApiClient = priceApiClient;
            this.logger = logger;
        }

        public PriceApiBatchSkuInformationProvider(PriceApiClient priceApiClient, ILogger<PriceApiBatchSkuInformationProvider> logger) : this(null,
            priceApiClient, logger)
        {
        }

        public async Task<List<VirtualMachineInformation>> GetVmSizesAndPricesAsync(string region)
        {
            if (appCache is null)
            {
                return await GetVmSizesAndPricesAsyncImpl(region);
            }

            logger.LogInformation("Trying to get pricing information from the cache.");

            return await appCache.GetOrAddAsync<List<VirtualMachineInformation>>(region, async () => await GetVmSizesAndPricesAsync(region));

        }

        public async Task<List<VirtualMachineInformation>> GetVmSizesAndPricesAsyncImpl(string region)
        {
            logger.LogInformation("Getting VM sizes and price information for region:{0}", region);

            var localVmSizeInfoForBatchSupportedSkus = await GetLocalVmSizeInformationForBatchSupportedSkusAsync();
            var pricingItems = await priceApiClient.GetAllPricingInformationForNonWindowsAndNonSpotVmsAsync(region).ToListAsync();

            logger.LogInformation("Received {0} pricing items}", pricingItems.Count);

            var vmInfoList = new List<VirtualMachineInformation>();

            foreach (var vm in localVmSizeInfoForBatchSupportedSkus)
            {

                var instancePricingInfo = pricingItems.Where(p => p.armSkuName == vm.VmSize);
                var normalPriorityInfo = instancePricingInfo.FirstOrDefault(s =>
                    s.skuName.Contains(" Low Priority", StringComparison.OrdinalIgnoreCase));
                var lowPriorityInfo = instancePricingInfo.FirstOrDefault(s =>
                    !s.skuName.Contains(" Low Priority", StringComparison.OrdinalIgnoreCase));

                if (lowPriorityInfo is not null)
                {
                    vmInfoList.Add(CreateVirtualMachineInfoFromReference(vm, true, Convert.ToDecimal(lowPriorityInfo.unitPrice)));
                }

                if (normalPriorityInfo is not null)
                {
                    vmInfoList.Add(CreateVirtualMachineInfoFromReference(vm, false, Convert.ToDecimal(normalPriorityInfo.unitPrice)));
                }
            }
            logger.LogInformation("Returning {0} Vm information entries with pricing for Azure Batch Supported Vm types}", vmInfoList.Count);

            return vmInfoList;

        }

        private VirtualMachineInformation CreateVirtualMachineInfoFromReference(VirtualMachineInformation vmReference, bool isLowPriority, decimal pricePerHour)
        {
            var spotVirtualMachineInformation = new VirtualMachineInformation()
            {
                LowPriority = isLowPriority,
                MaxDataDiskCount = vmReference.MaxDataDiskCount,
                MemoryInGB = vmReference.MemoryInGB,
                NumberOfCores = vmReference.NumberOfCores,
                PricePerHour = pricePerHour,
                ResourceDiskSizeInGB = vmReference.ResourceDiskSizeInGB,
                VmFamily = vmReference.VmFamily,
                VmSize = vmReference.VmSize
            };
            return spotVirtualMachineInformation;
        }

        private async Task<List<VirtualMachineInformation>> GetLocalVmSizeInformationForBatchSupportedSkusAsync()
        {
            return JsonConvert.DeserializeObject<List<VirtualMachineInformation>>(await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "BatchSupportedVmSizeInformation.json")));
        }
    }
}
