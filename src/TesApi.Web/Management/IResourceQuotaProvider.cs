using System.Threading.Tasks;
using Tes.Models;

namespace TesApi.Web.Management
{
    /// <summary>
    /// Provides quota information for a given resource. 
    /// </summary>
    public interface IResourceQuotaProvider
    {
        public Task<BatchAccountQuotas> GetBatchAccountQuotaInformationAsync(VirtualMachineInformation virtualMachineInformation);
    }
}
