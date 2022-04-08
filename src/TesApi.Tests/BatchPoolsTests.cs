// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Management.Batch.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using TesApi.Web;

namespace TesApi.Tests
{
    [TestClass]
    public class BatchPoolsTests
    {
        [TestMethod]
        public async Task GetOrAddDoesNotAddExistingAvailablePool()
        {
            using var serviceProvider = GetServiceProvider();
            var pools = serviceProvider.GetT();
            var info = await AddPool(pools);
            var keyCount = await ((IBatchPoolsImpl)pools).GetPoolGroups().CountAsync();
            var key = await ((IBatchPoolsImpl)pools).GetPoolGroups().SelectAwait(g => ValueTask.FromResult(g.Key)).FirstAsync();
            var count = await pools.GetPoolsAsync().CountAsync();
            serviceProvider.AzureProxy.Verify(mock => mock.CreateBatchPoolAsync(It.IsAny<Pool>()), Times.Once);

            var pool = await pools.GetOrAddAsync(key, id => ValueTask.FromResult(new Pool(name: id)));
            await pool.ServicePoolAsync(IBatchPool.ServiceKind.Update);

            Assert.AreEqual(await pools.GetPoolsAsync().CountAsync(), count);
            Assert.AreEqual(await ((IBatchPoolsImpl)pools).GetPoolGroups().CountAsync(), keyCount);
            //Assert.AreSame(info, pool);
            Assert.AreEqual(info.Pool.PoolId, pool.Pool.PoolId);
            serviceProvider.AzureProxy.Verify(mock => mock.CreateBatchPoolAsync(It.IsAny<Pool>()), Times.Once);
        }

        [TestMethod]
        public async Task GetOrAddDoesAddWithExistingUnavailablePool()
        {
            using var serviceProvider = GetServiceProvider();
            var pools = serviceProvider.GetT();
            var info = await AddPool(pools);
            ((IBatchPoolImpl)info).TestSetAvailable(false);
            await info.ServicePoolAsync(IBatchPool.ServiceKind.Update);
            var keyCount = await ((IBatchPoolsImpl)pools).GetPoolGroups().CountAsync();
            var key = await ((IBatchPoolsImpl)pools).GetPoolGroups().SelectAwait(g => ValueTask.FromResult(g.Key)).FirstAsync();
            var count = await pools.GetPoolsAsync().CountAsync();

            var pool = await pools.GetOrAddAsync(key, id => ValueTask.FromResult(new Pool(name: id)));
            await pool.ServicePoolAsync(IBatchPool.ServiceKind.Update);

            Assert.AreNotEqual(await pools.GetPoolsAsync().CountAsync(), count);
            Assert.AreEqual(await ((IBatchPoolsImpl)pools).GetPoolGroups().CountAsync(), keyCount);
            //Assert.AreNotSame(info, pool);
            Assert.AreNotEqual(info.Pool.PoolId, pool.Pool.PoolId);
        }


        [TestMethod]
        public async Task TryGetReturnsTrueAndCorrectPool()
        {
            using var serviceProvider = GetServiceProvider();
            var pools = serviceProvider.GetT();
            var info = await AddPool(pools);

            var result = pools.TryGet(info.Pool.PoolId, out var pool);

            Assert.IsTrue(result);
            //Assert.AreSame(infoPoolId, pool);
            Assert.AreEqual(info.Pool.PoolId, pool.Pool.PoolId);
        }

        [TestMethod]
        public async Task TryGetReturnsFalseWhenPoolIdNotPresent()
        {
            using var serviceProvider = GetServiceProvider();
            var pools = serviceProvider.GetT();
            _ = await AddPool(pools);

            var result = pools.TryGet("key2", out _);

            Assert.IsFalse(result);
        }

        [TestMethod]
        public async Task TryGetReturnsFalseWhenNoPoolIsAvailable()
        {
            using var serviceProvider = GetServiceProvider();
            var pools = serviceProvider.GetT();
            var pool = await AddPool(pools);
            ((IBatchPoolImpl)pool).TestSetAvailable(false);

            var result = pools.TryGet("key1", out _);

            Assert.IsFalse(result);
        }

        [TestMethod]
        public Task TryGetReturnsFalseWhenPoolIdIsNull()
        {
            using var serviceProvider = GetServiceProvider();
            var pools = serviceProvider.GetT();

            var result = pools.TryGet(null, out _);

            Assert.IsFalse(result);
            return Task.CompletedTask;
        }

        [TestMethod]
        public async Task UnavailablePoolsAreRemoved()
        {
            var poolId = string.Empty;
            var azureProxyMock = AzureProxyReturnValues.Get();
            azureProxyMock.AzureProxyDeleteBatchPool = id => poolId = id;

            using var serviceProvider = GetServiceProvider(azureProxyMock);
            var pools = serviceProvider.GetT();
            var pool = await AddPool(pools);
            Assert.IsTrue(pools.IsPoolAvailable("key1"));
            ((IBatchPoolImpl)pool).TestSetAvailable(false);
            await pool.ServicePoolAsync(IBatchPool.ServiceKind.Update);
            Assert.IsFalse(pools.IsPoolAvailable("key1"));
            Assert.IsTrue(await pools.GetPoolsAsync().AnyAsync());

            await pool.ServicePoolAsync(IBatchPool.ServiceKind.RemovePoolIfEmpty);

            Assert.AreEqual(pool.Pool.PoolId, poolId);
            Assert.IsFalse(pools.IsPoolAvailable("key1"));
            Assert.IsFalse(await pools.GetPoolsAsync().AnyAsync());
        }

        private TestServices.TestServiceProvider<BatchPools> GetServiceProvider(AzureProxyReturnValues azureProxyReturn = default)
            => new(wrapAzureProxy: true, configuration: GetMockConfig(), azureProxy: PrepareMockAzureProxy(azureProxyReturn ?? AzureProxyReturnValues.Get()), batchPoolRepositoryArgs: ("endpoint", "key", "databaseId", "containerId", "partitionKeyValue"));

        private static async Task<IBatchPool> AddPool(IBatchPools batchPools)
            => await batchPools.GetOrAddAsync("key1", id => ValueTask.FromResult(new Pool(name: id, displayName: "display1", vmSize: "vmSize1")));

        private class AzureProxyReturnValues
        {
            internal static AzureProxyReturnValues Get()
                => new();

            internal AzureBatchAccountQuotas BatchQuotas { get; set; } = new() { PoolQuota = 1, DedicatedCoreQuotaPerVMFamily = new List<VirtualMachineFamilyCoreQuota>() };
            internal int ActivePoolCount { get; set; } = 0;

            internal Action<string> AzureProxyDeleteBatchPool { get; set; } = poolId => { };
        }

        private Action<Mock<IAzureProxy>> PrepareMockAzureProxy(AzureProxyReturnValues azureProxyReturnValues)
        {
            return azureProxy =>
            {
                azureProxy.Setup(a => a.GetBatchAccountQuotasAsync()).Returns(Task.FromResult(azureProxyReturnValues.BatchQuotas));
                azureProxy.Setup(a => a.GetBatchActivePoolCount()).Returns(azureProxyReturnValues.ActivePoolCount);
                azureProxy.Setup(a => a.CreateBatchPoolAsync(It.IsAny<Pool>())).Returns((Pool p) => Task.FromResult(new PoolInformation { PoolId = p.Name }));
                azureProxy.Setup(a => a.DeleteBatchPoolAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).Callback<string, CancellationToken>(DeleteBatchPoolCallback);
            };

            void DeleteBatchPoolCallback(string poolId, CancellationToken cancellationToken)
                => azureProxyReturnValues.AzureProxyDeleteBatchPool?.Invoke(poolId);
        }

        private static IEnumerable<(string Key, string Value)> GetMockConfig()
            => Enumerable
                .Empty<(string Key, string Value)>()
                //.Append(("BatchPoolIdleNodeTime", "0.3"))
                //.Append(("BatchPoolIdlePoolTime", "0.6"))
                //.Append(("BatchPoolRotationForcedTime", "0.000694444"))
            ;
    }
}
