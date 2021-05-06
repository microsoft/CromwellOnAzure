// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.Management.Batch.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Tes.Models;
using TesApi.Web;

namespace TesApi.Tests
{
    [TestClass]
    public class ConfigurationUtilsTests
    {
        [TestMethod]
        public async Task Xyz()
        {
            var mockConfiguration = GetMockConfig();
            var mockAzureProxy = GetMockAzureProxy();
            var mockLogger = new Mock<ILogger>();

            var configurationUtils = new ConfigurationUtils(
                mockConfiguration,
                mockAzureProxy.Object, 
                new StorageAccessProvider(mockLogger.Object, mockConfiguration, mockAzureProxy.Object),
                mockLogger.Object);

            var allowedVmSizes = await configurationUtils.ProcessAllowedVmSizesConfigurationAndGetAllowedVmSizesAsync();

            var expectedAllowedVmSizesFileContent =
                "VmSize1\n" +
                "VmSize2\n" +
                "VmSizeNonExistent <-- WARNING: This VM size is misspelled or curently not supported in your region. It will be ignored.";

            var expectedSupportedVmSizesFileContent =
                "VM Size Family       $/hour   $/hour  Memory  CPUs  Disk     Dedicated CPU\n" +
                "                  dedicated  low pri    (GB)        (GB)  quota (per fam.)\n" +
                "VmSize1 VmFamily1    11.000   22.000     3.0     2    20               100\n" +
                "VmSize2 VmFamily2    33.000   44.000     6.0     4    40                 0\n" +
                "VmSize3 VmFamily3    55.000      N/A    12.0     8    80               300";

            Assert.AreEqual(2, allowedVmSizes.Count);
            mockAzureProxy.Verify(m => m.UploadBlobAsync(It.Is<Uri>(x => x.AbsoluteUri.Contains("allowed-vm-sizes")), It.Is<string>(s => s.Equals(expectedAllowedVmSizesFileContent))), Times.Exactly(1));
            mockAzureProxy.Verify(m => m.UploadBlobAsync(It.Is<Uri>(x => x.AbsoluteUri.Contains("supported-vm-sizes")), It.Is<string>(s => s.Equals(expectedSupportedVmSizesFileContent))), Times.Exactly(1));
        }

        private static IConfiguration GetMockConfig()
        {
            var config = new ConfigurationBuilder().AddInMemoryCollection().Build();
            config["DefaultStorageAccountName"] = "defaultstorageaccount";

            return config;
        }

        private static Mock<IAzureProxy> GetMockAzureProxy()
        {
            var vmInfos = new List<VirtualMachineInfo> {
                    new VirtualMachineInfo { VmSize = "VmSize1", VmFamily = "VmFamily1", LowPriority = false, NumberOfCores = 2, MemoryInGB = 3, ResourceDiskSizeInGB = 20, PricePerHour = 11 },
                    new VirtualMachineInfo { VmSize = "VmSize1", VmFamily = "VmFamily1", LowPriority = true, NumberOfCores = 2, MemoryInGB = 3, ResourceDiskSizeInGB = 20, PricePerHour = 22 },
                    new VirtualMachineInfo { VmSize = "VmSize2", VmFamily = "VmFamily2", LowPriority = false, NumberOfCores = 4, MemoryInGB = 6, ResourceDiskSizeInGB = 40, PricePerHour = 33 },
                    new VirtualMachineInfo { VmSize = "VmSize2", VmFamily = "VmFamily2", LowPriority = true, NumberOfCores = 4, MemoryInGB = 6, ResourceDiskSizeInGB = 40, PricePerHour = 44 },
                    new VirtualMachineInfo { VmSize = "VmSize3", VmFamily = "VmFamily3", LowPriority = false, NumberOfCores = 8, MemoryInGB = 12, ResourceDiskSizeInGB = 80, PricePerHour = 55 }
                };

            var dedicatedCoreQuotaPerVMFamily = new[] { new VirtualMachineFamilyCoreQuota("VmFamily1", 100), new VirtualMachineFamilyCoreQuota("VmFamily2", 0), new VirtualMachineFamilyCoreQuota("VmFamily3", 300) };

            var batchQuotas = new AzureBatchAccountQuotas { 
                ActiveJobAndJobScheduleQuota = 1, 
                PoolQuota = 1, 
                DedicatedCoreQuota = 5, 
                LowPriorityCoreQuota = 10, 
                DedicatedCoreQuotaPerVMFamilyEnforced = true, 
                DedicatedCoreQuotaPerVMFamily = dedicatedCoreQuotaPerVMFamily };

            var allowedVmSizes = new[] { "VmSize1", "VmSize2", "VmSizeNonExistent" };

            var storageAccountInfos = new Dictionary<string, StorageAccountInfo> {
                { 
                    "defaultstorageaccount", 
                    new StorageAccountInfo { Name = "defaultstorageaccount", Id = "Id", BlobEndpoint = "https://defaultstorageaccount.blob.core.windows.net/", SubscriptionId = "SubId" }
                } 
             };

            var azureProxy = new Mock<IAzureProxy>();

            azureProxy.Setup(a => a.GetVmSizesAndPricesAsync()).Returns(Task.FromResult(vmInfos));
            azureProxy.Setup(a => a.GetBatchAccountQuotasAsync()).Returns(Task.FromResult(batchQuotas));
            azureProxy.Setup(a => a.DownloadBlobAsync(It.IsAny<Uri>())).Returns(Task.FromResult(string.Join('\n', allowedVmSizes)));
            azureProxy.Setup(a => a.GetStorageAccountInfoAsync("defaultstorageaccount")).Returns(Task.FromResult(storageAccountInfos["defaultstorageaccount"]));
            azureProxy.Setup(a => a.GetStorageAccountKeyAsync(It.IsAny<StorageAccountInfo>())).Returns(Task.FromResult("Key1"));

            return azureProxy;
        }
    }
}
