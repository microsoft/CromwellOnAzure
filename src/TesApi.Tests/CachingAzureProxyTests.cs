// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;
using System.Threading.Tasks;
using LazyCache;
using LazyCache.Mocks;
using LazyCache.Providers;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using TesApi.Models;
using TesApi.Web;

namespace TesApi.Tests
{
    [TestClass]
    public class CachingAzureProxyTests
    {
        private readonly IAppCache cache = new CachingService(new MemoryCacheProvider(new MemoryCache(new MemoryCacheOptions())));
        
        [TestMethod]
        public async Task GetBatchAccountQuotasAsync_UsesCache()
        {
            var azureProxy = GetMockAzureProxy();
            var batchQuotas = new AzureBatchAccountQuotas { ActiveJobAndJobScheduleQuota = 1, PoolQuota = 1, DedicatedCoreQuota = 5, LowPriorityCoreQuota = 10 };
            azureProxy.Setup(a => a.GetBatchAccountQuotasAsync()).Returns(Task.FromResult(batchQuotas));
            var cachingAzureProxy = new CachingAzureProxy(azureProxy.Object, cache, new Mock<ILogger<CachingAzureProxy>>().Object);

            var quotas1 = await cachingAzureProxy.GetBatchAccountQuotasAsync();
            var quotas2 = await cachingAzureProxy.GetBatchAccountQuotasAsync();

            azureProxy.Verify(mock => mock.GetBatchAccountQuotasAsync(), Times.Once());
            Assert.AreEqual(batchQuotas, quotas1);
            Assert.AreEqual(quotas1, quotas2);
        }

        [TestMethod]
        public async Task GetStorageAccountKeyAsync_UsesCache()
        {
            var azureProxy = GetMockAzureProxy();
            var storageAccountInfo = new StorageAccountInfo { Name = "defaultstorageaccount", Id = "Id", BlobEndpoint = "https://defaultstorageaccount/", SubscriptionId = "SubId" };
            var storageAccountKey = "key";
            azureProxy.Setup(a => a.GetStorageAccountKeyAsync(It.IsAny<StorageAccountInfo>())).Returns(Task.FromResult(storageAccountKey));
            var cachingAzureProxy = new CachingAzureProxy(azureProxy.Object, cache, new Mock<ILogger<CachingAzureProxy>>().Object);

            var key1 = await cachingAzureProxy.GetStorageAccountKeyAsync(storageAccountInfo);
            var key2 = await cachingAzureProxy.GetStorageAccountKeyAsync(storageAccountInfo);

            azureProxy.Verify(mock => mock.GetStorageAccountKeyAsync(storageAccountInfo), Times.Once());
            Assert.AreEqual(storageAccountKey, key1);
            Assert.AreEqual(key1, key2);
        }

        [TestMethod]
        public async Task GetVMSizesAndPricesAsync_UsesCache()
        {
            var azureProxy = GetMockAzureProxy();
            var cachingAzureProxy = new CachingAzureProxy(azureProxy.Object, cache, new Mock<ILogger<CachingAzureProxy>>().Object);

            var vmSizesAndPrices1 = await cachingAzureProxy.GetVmSizesAndPricesAsync();
            var vmSizesAndPrices2 = await cachingAzureProxy.GetVmSizesAndPricesAsync();

            azureProxy.Verify(mock => mock.GetVmSizesAndPricesAsync(), Times.Once());
            Assert.AreEqual(vmSizesAndPrices1, vmSizesAndPrices2);
            Assert.AreEqual(4, vmSizesAndPrices1.Count);
        }

        [TestMethod]
        public async Task GetStorageAccountInfoAsync_UsesCache()
        {
            var azureProxy = GetMockAzureProxy();
            var storageAccountInfo = new StorageAccountInfo { Name = "defaultstorageaccount", Id = "Id", BlobEndpoint = "https://defaultstorageaccount/", SubscriptionId = "SubId" };

            azureProxy.Setup(a => a.GetStorageAccountInfoAsync(It.IsAny<string>())).Returns(Task.FromResult(storageAccountInfo));
            var cachingAzureProxy = new CachingAzureProxy(azureProxy.Object, cache, new Mock<ILogger<CachingAzureProxy>>().Object);

            var info1 = await cachingAzureProxy.GetStorageAccountInfoAsync("defaultstorageaccount");
            var info2 = await cachingAzureProxy.GetStorageAccountInfoAsync("defaultstorageaccount");

            azureProxy.Verify(mock => mock.GetStorageAccountInfoAsync("defaultstorageaccount"), Times.Once());
            Assert.AreEqual(storageAccountInfo, info1);
            Assert.AreEqual(info1, info2);
        }

        [TestMethod]
        public async Task GetStorageAccountInfoAsync_NullInfo_DoesNotSetCache()
        {
            var azureProxy = GetMockAzureProxy();

            azureProxy.Setup(a => a.GetStorageAccountInfoAsync(It.IsAny<string>())).Returns(Task.FromResult((StorageAccountInfo)null));
            var cachingAzureProxy = new CachingAzureProxy(azureProxy.Object, cache, new Mock<ILogger<CachingAzureProxy>>().Object);
            var info1 = await cachingAzureProxy.GetStorageAccountInfoAsync("defaultstorageaccount");

            var storageAccountInfo = new StorageAccountInfo { Name = "defaultstorageaccount", Id = "Id", BlobEndpoint = "https://defaultstorageaccount/", SubscriptionId = "SubId" };
            azureProxy.Setup(a => a.GetStorageAccountInfoAsync(It.IsAny<string>())).Returns(Task.FromResult(storageAccountInfo));
            var info2 = await cachingAzureProxy.GetStorageAccountInfoAsync("defaultstorageaccount");

            azureProxy.Verify(mock => mock.GetStorageAccountInfoAsync("defaultstorageaccount"), Times.Exactly(2));
            Assert.IsNull(info1);
            Assert.AreEqual(storageAccountInfo, info2);
        }

        [TestMethod]
        public async Task GetContainerRegistryInfoAsync_UsesCache()
        {
            var azureProxy = GetMockAzureProxy();
            var containerRegistryInfo = new ContainerRegistryInfo { RegistryServer = "registryServer1", Username = "default", Password = "placeholder" };

            azureProxy.Setup(a => a.GetContainerRegistryInfoAsync(It.IsAny<string>())).Returns(Task.FromResult(containerRegistryInfo));
            var cachingAzureProxy = new CachingAzureProxy(azureProxy.Object, cache, new Mock<ILogger<CachingAzureProxy>>().Object);

            var info1 = await cachingAzureProxy.GetContainerRegistryInfoAsync("registryServer1/imageName1:tag1");
            var info2 = await cachingAzureProxy.GetContainerRegistryInfoAsync("registryServer1/imageName1:tag1");

            azureProxy.Verify(mock => mock.GetContainerRegistryInfoAsync("registryServer1/imageName1:tag1"), Times.Once());
            Assert.AreEqual(containerRegistryInfo, info1);
            Assert.AreEqual(info1, info2);
        }

        [TestMethod]
        public async Task GetContainerRegistryInfoAsync_NullInfo_DoesNotSetCache()
        {
            var azureProxy = GetMockAzureProxy();

            azureProxy.Setup(a => a.GetContainerRegistryInfoAsync(It.IsAny<string>())).Returns(Task.FromResult((ContainerRegistryInfo)null));
            var cachingAzureProxy = new CachingAzureProxy(azureProxy.Object, cache, new Mock<ILogger<CachingAzureProxy>>().Object);
            var info1 = await cachingAzureProxy.GetContainerRegistryInfoAsync("registryServer1/imageName1:tag1");

            var containerRegistryInfo = new ContainerRegistryInfo { RegistryServer = "registryServer1", Username = "default", Password = "placeholder" };
            azureProxy.Setup(a => a.GetContainerRegistryInfoAsync(It.IsAny<string>())).Returns(Task.FromResult(containerRegistryInfo));
            var info2 = await cachingAzureProxy.GetContainerRegistryInfoAsync("registryServer1/imageName1:tag1");

            azureProxy.Verify(mock => mock.GetContainerRegistryInfoAsync("registryServer1/imageName1:tag1"), Times.Exactly(2));
            Assert.IsNull(info1);
            Assert.AreEqual(containerRegistryInfo, info2);
        }

        private Mock<IAzureProxy> GetMockAzureProxy()
        {
            var azureProxy = new Mock<IAzureProxy>();

            azureProxy.Setup(a => a.GetVmSizesAndPricesAsync()).Returns(Task.FromResult(
                new List<VirtualMachineInfo> {
                    new VirtualMachineInfo { VmSize = "VmSizeLowPri1", LowPriority = true, NumberOfCores = 1, MemoryInGB = 4, ResourceDiskSizeInGB = 20, PricePerHour = 1 },
                    new VirtualMachineInfo { VmSize = "VmSizeLowPri2", LowPriority = true, NumberOfCores = 2, MemoryInGB = 8, ResourceDiskSizeInGB = 40, PricePerHour = 2 },
                    new VirtualMachineInfo { VmSize = "VmSizeDedicated1", LowPriority = false, NumberOfCores = 1, MemoryInGB = 4, ResourceDiskSizeInGB = 20, PricePerHour = 11 },
                    new VirtualMachineInfo { VmSize = "VmSizeDedicated2", LowPriority = false, NumberOfCores = 2, MemoryInGB = 8, ResourceDiskSizeInGB = 40, PricePerHour = 22 }
                }));

            return azureProxy;
        }
    }
}
