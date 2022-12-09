using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Tes.Models;

namespace TesApi.Web.Management
{
    /// <summary>
    /// Batch Sku information provider using the ARM APIS.
    /// </summary>
    public class ArmBatchSkuInformationProvider : IBatchSkuInformationProvider
    {
        private readonly IAzureProxy azureProxy;

        public ArmBatchSkuInformationProvider(IAzureProxy azureProxy)
        {
            this.azureProxy = azureProxy ?? throw new ArgumentNullException(nameof(azureProxy));
        }

        public Task<List<VirtualMachineInformation>> GetVmSizesAndPricesAsync(string region)
        {
            return azureProxy.GetVmSizesAndPricesAsync();
        }
    }
}
