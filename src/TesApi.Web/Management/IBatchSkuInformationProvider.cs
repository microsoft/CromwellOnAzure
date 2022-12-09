using System.Collections.Generic;
using System.Threading.Tasks;
using Tes.Models;

namespace TesApi.Web.Management
{
    /// <summary>
    /// Provider of pricing and size information of the Vm SKUs supported by Batch.
    /// </summary>
    public interface IBatchSkuInformationProvider
    {
        public Task<List<VirtualMachineInformation>> GetVmSizesAndPricesAsync(string region);
    }
}
