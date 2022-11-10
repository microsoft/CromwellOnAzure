using System.Threading.Tasks;
using Tes.Models;

namespace TesApi.Web.Management
{
    /// <summary>
    /// Provides the ability to verify if the quota is available for a given SKU
    /// </summary>
    public interface IResourceQuotaVerifier
    {
        /// <summary>
        /// Checks if the current quota allows fullfiment of the requested VM SKU. 
        /// </summary>
        /// <param name="virtualMachineInformation"></param>
        /// <returns></returns>
        /// <exception cref="AzureBatchQuotaMaxedOutException">Thrown when a max quota condition was identified</exception>
        Task CheckBatchAccountQuotasAsync(VirtualMachineInformation virtualMachineInformation);
    }
}
