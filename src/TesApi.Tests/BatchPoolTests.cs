// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using LazyCache;
using LazyCache.Providers;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Azure.Management.Batch.Models;
using Microsoft.Azure.Management.Sql.Fluent;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Newtonsoft.Json;
using Tes.Extensions;
using Tes.Models;
using TesApi.Web;

namespace TesApi.Tests
{
    [TestClass]
    public class BatchPoolTests
    {
        //[TestInitialize]
        //public 

        [TestMethod]
        public async Task PrepareNodeIncrementsLowPriorityTargetWhenNoNodeIsIdle()
        {
            var pool = await CreateBatchPoolAsync();

            AffinityInformation info = default;
            try
            {
                AzureProxyForEachComputeNodeAsync = ForEachAction;
                info = await pool.PrepareNodeAsync(true);
            }
            finally
            {
                AzureProxyForEachComputeNodeAsync = default;
            }

            Assert.IsNull(info);
            Assert.AreEqual(1, ((IBatchPoolImpl)pool).TestTargetLowPriority);
            Assert.AreEqual(0, ((IBatchPoolImpl)pool).TestTargetDedicated);

            void ForEachAction(string id, Action<ComputeNode> action, ODATADetailLevel detail, CancellationToken cancel)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                Assert.IsNotNull(detail);
                Assert.IsNotNull(detail.SelectClause);
            }
        }

        [TestMethod]
        public async Task PrepareNodeIncrementsDedicatedTargetWhenNoNodeIsIdle()
        {
            var pool = await CreateBatchPoolAsync();

            AffinityInformation info = default;
            try
            {
                AzureProxyForEachComputeNodeAsync = ForEachAction;
                info = await pool.PrepareNodeAsync(false);
            }
            finally
            {
                AzureProxyForEachComputeNodeAsync = default;
            }

            Assert.IsNull(info);
            Assert.AreEqual(1, ((IBatchPoolImpl)pool).TestTargetDedicated);
            Assert.AreEqual(0, ((IBatchPoolImpl)pool).TestTargetLowPriority);

            void ForEachAction(string id, Action<ComputeNode> action, ODATADetailLevel detail, CancellationToken cancel)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                Assert.IsNotNull(detail);
                Assert.IsNotNull(detail.SelectClause);
            }
        }

        // Mock cannot setup any of the properties on ComputeNode, so we can't fully test the remaining scenarios involving PrepareNodeAsync() (or ReleaseNode(), for that matter).

        //[TestMethod]
        //public async Task PrepareNodeIncrementsLowPriorityTargetWhenNoLowPriorityNodeIsIdle()
        //{
        //    var reboots = 0;
        //    var nodes = new ComputeNode[]
        //    {
        //        GenerateNode("NodeOneLoPriRunning", false, false),
        //        GenerateNode("NodeOneDedicatedRunning", true, false),
        //        GenerateNode("NodeTwoLoPriRunning", false, false),
        //        GenerateNode("NodeTwoDedicatedRunning", true, false),
        //        GenerateNode("NodeOneLoPriIdle", false, true, true),
        //        GenerateNode("NodeOneDedicatedIdle", true, true),
        //        GenerateNode("NodeTwoLoPriIdle", false, true),
        //        GenerateNode("NodeTwoDedicatedIdle", true, true),
        //    };
        //    var pool = await CreateBatchPoolAsync();

        //    AffinityInformation info = default;
        //    try
        //    {
        //        AzureProxyForEachComputeNodeAsync = ForEachAction;
        //        info = await pool.PrepareNodeAsync(true);
        //    }
        //    finally
        //    {
        //        AzureProxyForEachComputeNodeAsync = default;
        //    }

        //    Assert.IsNull(info);
        //    Assert.AreEqual(0, reboots);
        //    Assert.AreEqual(1, ((IBatchPoolImpl)pool).TestTargetLowPriority);
        //    Assert.AreEqual(0, ((IBatchPoolImpl)pool).TestTargetDedicated);

        //    void ForEachAction(string id, Action<ComputeNode> action, ODATADetailLevel detail, CancellationToken cancel)
        //    {
        //        Assert.AreEqual(pool.Pool.PoolId, id);
        //        Assert.IsNotNull(detail);
        //        Assert.IsNotNull(detail.SelectClause);
        //        foreach (var node in nodes.Where(n => n.State == ComputeNodeState.Idle))
        //        {
        //            action(node);
        //        }
        //    }

        //    ComputeNode GenerateNode(string Affinity, bool isDedicated, bool isIdle, bool isCandidate = false)
        //    {
        //        var node = new Mock<ComputeNode>();
        //        node.Setup(x => x.AffinityId).Returns(Affinity);
        //        node.Setup(x => x.IsDedicated).Returns(isDedicated);
        //        node.Setup(x => x.State).Returns(isIdle ? ComputeNodeState.Idle : ComputeNodeState.Running);
        //        node.Setup(x => x.RebootAsync(It.IsAny<ComputeNodeRebootOption>(), It.IsAny<IEnumerable<BatchClientBehavior>>(), It.IsAny<CancellationToken>()))
        //            .Callback<ComputeNodeRebootOption, IEnumerable<BatchClientBehavior>, CancellationToken>((o, b, c) => { if (isCandidate) { ++reboots; } else { Assert.Fail("Should not be rebooted."); } })
        //            .Returns(Task.CompletedTask);
        //        return node.Object;
        //    }
        //}

        [TestMethod]
        public async Task SyncSizeSetsIBatchPoolState()
        {
            var pool = await CreateBatchPoolAsync();

            try
            {
                AzureProxyGetComputeNodeTargets = ValidateComputeNodeTargets;
                await pool.ServicePoolAsync(IBatchPool.ServiceKind.SyncSize);
            }
            finally
            {
                AzureProxyGetComputeNodeTargets = default;
            }

            Assert.AreEqual(3, ((IBatchPoolImpl)pool).TestTargetLowPriority);
            Assert.AreEqual(2, ((IBatchPoolImpl)pool).TestTargetDedicated);

            void ValidateComputeNodeTargets(string id)
                => Assert.AreEqual(pool.Pool.PoolId, id);
        }

        // Test the actions performed by BatchPoolService

        [TestMethod]
        public async Task ResizeDoesNothingWhenTargetsNotChanged()
        {
            var pool = await CreateBatchPoolAsync();

            try
            {
                AzureProxySetComputeNodeTargets = SetTargets;
                AzureProxyGetAllocationState = GetState;
                await Task.Delay(BatchPoolService.ResizeInterval + TimeSpan.FromSeconds(1));
                await pool.ServicePoolAsync(IBatchPool.ServiceKind.Resize);
            }
            finally
            {
                AzureProxySetComputeNodeTargets = default;
                AzureProxyGetAllocationState = default;
            }

            Assert.AreEqual(0, ((IBatchPoolImpl)pool).TestTargetLowPriority);
            Assert.AreEqual(0, ((IBatchPoolImpl)pool).TestTargetDedicated);

            Microsoft.Azure.Batch.Common.AllocationState GetState(string id)
            {
                Assert.Fail();
                return Microsoft.Azure.Batch.Common.AllocationState.Steady;
            }

            void SetTargets(string id, int? loPri, int? dedic)
                => Assert.Fail();
        }

        [TestMethod]
        public async Task ResizeDoesNothingWhenStateIsntSteady()
        {
            var pool = await CreateBatchPoolAsync();

            try
            {
                AzureProxySetComputeNodeTargets = SetTargets;
                AzureProxyGetAllocationState = GetState;
                _ = await pool.PrepareNodeAsync(false);
                await Task.Delay(BatchPoolService.ResizeInterval + TimeSpan.FromSeconds(1));
                await pool.ServicePoolAsync(IBatchPool.ServiceKind.Resize);
            }
            finally
            {
                AzureProxySetComputeNodeTargets = default;
                AzureProxyGetAllocationState = default;
            }

            Assert.AreEqual(0, ((IBatchPoolImpl)pool).TestTargetLowPriority);
            Assert.AreEqual(1, ((IBatchPoolImpl)pool).TestTargetDedicated);

            Microsoft.Azure.Batch.Common.AllocationState GetState(string id)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                return Microsoft.Azure.Batch.Common.AllocationState.Resizing;
            }

            void SetTargets(string id, int? loPri, int? dedic)
                => Assert.Fail();
        }

        [TestMethod]
        public async Task ResizeSetsBothTargetsWhenStateIsSteady()
        {
            var setTargetsCalled = 0;
            var pool = await CreateBatchPoolAsync();

            try
            {
                AzureProxySetComputeNodeTargets = SetTargets;
                AzureProxyGetAllocationState = GetState;
                _ = await pool.PrepareNodeAsync(false);
                await Task.Delay(BatchPoolService.ResizeInterval + TimeSpan.FromSeconds(1));
                await pool.ServicePoolAsync(IBatchPool.ServiceKind.Resize);
            }
            finally
            {
                AzureProxySetComputeNodeTargets = default;
                AzureProxyGetAllocationState = default;
            }

            Assert.AreEqual(1, setTargetsCalled);
            Assert.AreEqual(0, ((IBatchPoolImpl)pool).TestTargetLowPriority);
            Assert.AreEqual(1, ((IBatchPoolImpl)pool).TestTargetDedicated);

            Microsoft.Azure.Batch.Common.AllocationState GetState(string id)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                return Microsoft.Azure.Batch.Common.AllocationState.Steady;
            }

            void SetTargets(string id, int? loPri, int? dedic)
            {
                ++setTargetsCalled;
                Assert.AreEqual(pool.Pool.PoolId, id);
                Assert.AreEqual(0, loPri);
                Assert.AreEqual(1, dedic);
            }
        }

        [TestMethod]
        public async Task RotateDoesNothingWhenPoolIsNotAvailable()
        {
            var pool = await CreateBatchPoolAsync();
            ((IBatchPoolImpl)pool).TestSetAvailable(false);

            await pool.ServicePoolAsync(IBatchPool.ServiceKind.Rotate);

            Assert.IsFalse(pool.IsAvailable);
        }

        [TestMethod]
        public async Task RotateMarksPoolUnavailableWhenIdleForIdlePeriod()
        {
            var pool = await CreateBatchPoolAsync();

            await Task.Delay(((IBatchPoolImpl)pool).TestIdlePoolTime);
            await pool.ServicePoolAsync(IBatchPool.ServiceKind.Rotate);

            Assert.IsFalse(pool.IsAvailable);
            Assert.AreEqual(0, ((IBatchPoolImpl)pool).TestTargetDedicated + ((IBatchPoolImpl)pool).TestTargetLowPriority);
        }

        [TestMethod]
        public async Task RotateMarksPoolUnavailableWhenRotateIntervalHasPassed()
        {
            var pool = await CreateBatchPoolAsync();
            _ = await pool.PrepareNodeAsync(false);

            await Task.Delay(((IBatchPoolImpl)pool).TestRotatePoolTime);
            await pool.ServicePoolAsync(IBatchPool.ServiceKind.Rotate);

            Assert.IsFalse(pool.IsAvailable);
            Assert.AreNotEqual(0, ((IBatchPoolImpl)pool).TestTargetDedicated + ((IBatchPoolImpl)pool).TestTargetLowPriority);
        }

        private async Task<IBatchPool> CreateBatchPoolAsync()
            => await new BatchPools(GetMockAzureProxy(AzureProxyReturnValues.Get()).Object, new Mock<ILogger<BatchPools>>().Object, GetMockConfig(), GetBatchPoolFactory())
                .GetOrAddAsync("key1", id => new Pool(name: id, displayName: "display1", vmSize: "vmSize1"));

        internal static BatchPoolFactory GetBatchPoolFactory()
        {
            var services = new ServiceCollection();
            services.AddSingleton(_ => new Mock<ILogger<BatchPool>>().Object);
            return new BatchPoolFactory(services.BuildServiceProvider());
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
            azureProxy.Setup(a => a.ForEachComputeNodeAsync(It.IsAny<string>(), It.IsAny<Action<ComputeNode>>(), It.IsAny<DetailLevel>(), It.IsAny<CancellationToken>())).Callback<string, Action<ComputeNode>, DetailLevel, CancellationToken>((id, action, detail, cancel) => AzureProxyForEachComputeNodeAsync?.Invoke(id, action, detail as ODATADetailLevel, cancel));
            azureProxy.Setup(a => a.GetComputeNodeTargets(It.IsAny<string>())).Callback<string>(id => AzureProxyGetComputeNodeTargets?.Invoke(id)).Returns(() => (3, 2));
            azureProxy.Setup(a => a.GetAllocationStateAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).Returns((string id, CancellationToken token) => Task.FromResult(AzureProxyGetAllocationState?.Invoke(id)));
            azureProxy.Setup(a => a.SetComputeNodeTargetsAsync(It.IsAny<string>(), It.IsAny<int?>(), It.IsAny<int?>(), It.IsAny<CancellationToken>())).Callback<string, int?, int?, CancellationToken>((id, loPri, dedic, cancel) => AzureProxySetComputeNodeTargets?.Invoke(id, loPri, dedic)).Returns(Task.CompletedTask);

            return azureProxy;
        }

        private Action<string, Action<ComputeNode>, ODATADetailLevel, CancellationToken> AzureProxyForEachComputeNodeAsync;
        private Action<string> AzureProxyGetComputeNodeTargets;
        private Func<string, Microsoft.Azure.Batch.Common.AllocationState> AzureProxyGetAllocationState;
        private Action<string, int?, int?> AzureProxySetComputeNodeTargets;

        private static IConfiguration GetMockConfig()
        {
            var config = new ConfigurationBuilder().AddInMemoryCollection().Build();
            config["BatchPoolIdleNodeTime"] = "0.01";
            config["BatchPoolIdlePoolTime"] = "0.015";
            config["BatchPoolRotationForcedTime"] = "0.000011575";

            return config;
        }
    }
}
