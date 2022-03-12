// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Management.Batch.Models;
using Microsoft.Extensions.Configuration;
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
            var mockAzureProxy = GetMockAzureProxy(AzureProxyReturnValues.Get());
            var (pools, info) = await ConfigureBatchPools(true, mockAzureProxy: mockAzureProxy);
            var keyCount = ((IBatchPoolsImpl)pools).ManagedBatchPools.Count;
            var key = ((IBatchPoolsImpl)pools).ManagedBatchPools.Keys.First();
            var count = ((IBatchPoolsImpl)pools).ManagedBatchPools.Values.SelectMany(q => q).Count();
            Assert.AreEqual(1, mockAzureProxy.Invocations.Count(i => i.Method.Name.Equals(nameof(IAzureProxy.CreateBatchPoolAsync))));

            var pool = await pools.GetOrAddAsync(key, id => new Pool(name: id));

            Assert.AreEqual(((IBatchPoolsImpl)pools).ManagedBatchPools.Values.SelectMany(q => q).Count(), count);
            Assert.AreEqual(((IBatchPoolsImpl)pools).ManagedBatchPools.Count, keyCount);
            Assert.AreSame(info, pool);
            Assert.AreEqual(1, mockAzureProxy.Invocations.Count(i => i.Method.Name.Equals(nameof(IAzureProxy.CreateBatchPoolAsync))));
        }

        [TestMethod]
        public async Task GetOrAddDoesAddWithExistingUnavailablePool()
        {
            var (pools, info) = await ConfigureBatchPools(true);
            ((IBatchPoolImpl)info).TestSetAvailable(false);
            var keyCount = ((IBatchPoolsImpl)pools).ManagedBatchPools.Count;
            var key = ((IBatchPoolsImpl)pools).ManagedBatchPools.Keys.First();
            var count = ((IBatchPoolsImpl)pools).ManagedBatchPools.Values.SelectMany(q => q).Count();

            var pool = await pools.GetOrAddAsync(key, id => new Pool(name: id));

            Assert.AreNotEqual(((IBatchPoolsImpl)pools).ManagedBatchPools.Values.SelectMany(q => q).Count(), count);
            Assert.AreEqual(((IBatchPoolsImpl)pools).ManagedBatchPools.Count, keyCount);
            Assert.AreNotSame(info, pool);
        }

        [TestMethod]
        public async Task GetOrAddCloudPoolDoesNotAddExistingPool()
        {
            var (pools, info) = await ConfigureBatchPools(true);

            var cloudPool = new Mock<CloudPool>().Object;
            cloudPool.VirtualMachineSize = "vmSize1";
            cloudPool.AutoScaleEnabled = false;
            cloudPool.Id = info.Pool.PoolId;

            var count = ((IBatchPoolsImpl)pools).ManagedBatchPools.Values.SelectMany(q => q).Count();
            var pool = await pools.GetOrAddAsync(cloudPool);

            Assert.AreEqual(((IBatchPoolsImpl)pools).ManagedBatchPools.Values.SelectMany(q => q).Count(), count);
            Assert.AreSame(info, pool);
        }

        [TestMethod]
        public async Task GetOrAddCloudPoolDoesAddNonexistingPool()
        {
            var (pools, _) = await ConfigureBatchPools(false);

            var cloudPool = new Mock<CloudPool>().Object;
            cloudPool.VirtualMachineSize = "vmSize1";
            cloudPool.AutoScaleEnabled = false;
            cloudPool.Id = @"key1-_____________";

            var count = ((IBatchPoolsImpl)pools).ManagedBatchPools.Values.SelectMany(q => q).Count();
            var pool = await pools.GetOrAddAsync(cloudPool);

            Assert.AreNotEqual(((IBatchPoolsImpl)pools).ManagedBatchPools.Values.SelectMany(q => q).Count(), count);
            Assert.AreEqual(pool.Pool.PoolId, cloudPool.Id);
        }

        [TestMethod]
        public async Task GetOrAddCloudPoolDoesNotAddAutoscalingPool()
        {
            var (pools, _) = await ConfigureBatchPools(false);

            var cloudPool = new Mock<CloudPool>().Object;
            cloudPool.VirtualMachineSize = "vmSize1";
            cloudPool.AutoScaleEnabled = true;
            cloudPool.Id = "key1-_____________";

            var count = ((IBatchPoolsImpl)pools).ManagedBatchPools.Values.SelectMany(q => q).Count();
            var pool = await pools.GetOrAddAsync(cloudPool);

            Assert.AreEqual(((IBatchPoolsImpl)pools).ManagedBatchPools.Values.SelectMany(q => q).Count(), count);
            Assert.IsNull(pool);
        }

        [TestMethod]
        public async Task TryGetReturnsTrueAndCorrectPool()
        {
            var (pools, info) = await ConfigureBatchPools(true);

            var result = pools.TryGet(info.Pool.PoolId, out var pool);

            Assert.IsTrue(result);
            Assert.AreSame(info, pool);
        }

        [TestMethod]
        public async Task TryGetReturnsFalseWhenPoolIdNotPresent()
        {
            var (pools, _) = await ConfigureBatchPools(true);

            var result = pools.TryGet("key2", out _);

            Assert.IsFalse(result);
        }

        [TestMethod]
        public async Task TryGetReturnsFalseWhenNoPoolIsAvailable()
        {
            var (pools, pool) = await ConfigureBatchPools(true);
            ((IBatchPoolImpl)pool).TestSetAvailable(false);

            var result = pools.TryGet("key1", out _);

            Assert.IsFalse(result);
        }

        [TestMethod]
        public async Task TryGetReturnsFalseWhenPoolIdIsNull()
        {
            var (pools, _) = await ConfigureBatchPools(false);

            var result = pools.TryGet(null, out _);

            Assert.IsFalse(result);
        }

        [TestMethod]
        public async Task UnavailablePoolsAreRemoved()
        {
            var (pools, pool) = await ConfigureBatchPools(true);
            Assert.IsTrue(pools.IsPoolAvailable("key1"));
            ((IBatchPoolImpl)pool).TestSetAvailable(false);
            Assert.IsFalse(pools.IsPoolAvailable("key1"));
            Assert.IsFalse(pools.IsEmpty);

            var poolId = string.Empty;
            try
            {
                AzureProxyDeleteBatchPool = SavePoolId;
                await pool.ServicePoolAsync(IBatchPool.ServiceKind.RemovePoolIfEmpty);
            }
            finally
            {
                AzureProxyDeleteBatchPool = default;
            }

            Assert.AreEqual(pool.Pool.PoolId, poolId);
            Assert.IsFalse(pools.IsPoolAvailable("key1"));
            Assert.IsTrue(pools.IsEmpty);

            void SavePoolId(string id)
                => poolId = id;
        }

        private async Task<(IBatchPools pools, IBatchPool pool)> ConfigureBatchPools(bool createInitialPool, AzureProxyReturnValues azureProxyReturnValues = default, IConfiguration configuration = default, Mock<IAzureProxy> mockAzureProxy = default)
        {
            IBatchPool info = default;
            var result = new BatchPools((mockAzureProxy ?? GetMockAzureProxy(azureProxyReturnValues ?? AzureProxyReturnValues.Get())).Object, new Mock<ILogger>().Object, configuration ?? GetMockConfig());
            if (createInitialPool)
            {
                info = await result.GetOrAddAsync("key1", id => new Pool(name: id, displayName: "display1", vmSize: "vmSize1"));
            }
            return (result, info);
        }

        private class AzureProxyReturnValues
        {
            internal static AzureProxyReturnValues Get()
                => new();

            internal AzureBatchAccountQuotas BatchQuotas { get; set; } = new() { PoolQuota = 1, DedicatedCoreQuotaPerVMFamily = new List<VirtualMachineFamilyCoreQuota>() };
            internal int ActivePoolCount { get; set; } = 0;
        }

        private Mock<IAzureProxy> GetMockAzureProxy(AzureProxyReturnValues azureProxyReturnValues)
        {
            var azureProxy = new Mock<IAzureProxy>();

            azureProxy.Setup(a => a.GetBatchAccountQuotasAsync()).Returns(Task.FromResult(azureProxyReturnValues.BatchQuotas));
            azureProxy.Setup(a => a.GetBatchActivePoolCount()).Returns(azureProxyReturnValues.ActivePoolCount);
            azureProxy.Setup(a => a.CreateBatchPoolAsync(It.IsAny<Pool>())).Returns((Pool p) => Task.FromResult(new PoolInformation { PoolId = p.Name }));
            azureProxy.Setup(a => a.DeleteBatchPoolAsync(It.IsAny<string>(), It.IsAny<System.Threading.CancellationToken>())).Callback<string, System.Threading.CancellationToken>(DeleteBatchPoolCallback);

            return azureProxy;

            void DeleteBatchPoolCallback(string poolId, System.Threading.CancellationToken cancellationToken)
                => AzureProxyDeleteBatchPool?.Invoke(poolId);
        }

        private Action<string> AzureProxyDeleteBatchPool;

        private static IConfiguration GetMockConfig()
        {
            var config = new ConfigurationBuilder().AddInMemoryCollection().Build();
            //config["BatchPoolIdleNodeTime"] = "0.3";
            //config["BatchPoolIdlePoolTime"] = "0.6";
            //config["BatchPoolRotationForcedTime"] = "0.000694444";

            return config;
        }
    }
}
