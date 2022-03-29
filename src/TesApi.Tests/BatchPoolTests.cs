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
        private const string AffinityPrefix = "AP-";

        [TestMethod]
        public async Task PrepareNodeIncrementsLowPriorityTargetWhenNoNodeIsIdle()
        {
            var pool = await CreateBatchPoolAsync();

            AffinityInformation info = default;
            try
            {
                AzureProxyListComputeNodesAsync = ListComputeNodes;
                info = await pool.PrepareNodeAsync(true);
            }
            finally
            {
                AzureProxyListComputeNodesAsync = default;
            }

            Assert.IsNull(info);
            Assert.AreEqual(1, ((IBatchPoolImpl)pool).TestTargetLowPriority);
            Assert.AreEqual(0, ((IBatchPoolImpl)pool).TestTargetDedicated);

            IAsyncEnumerable<ComputeNode> ListComputeNodes(string id, ODATADetailLevel detail)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                Assert.IsNotNull(detail);
                Assert.IsNotNull(detail.SelectClause);
                return AsyncEnumerable.Empty<ComputeNode>();
            }
        }

        [TestMethod]
        public async Task PrepareNodeIncrementsDedicatedTargetWhenNoNodeIsIdle()
        {
            var pool = await CreateBatchPoolAsync();

            AffinityInformation info = default;
            try
            {
                AzureProxyListComputeNodesAsync = ListComputeNodes;
                info = await pool.PrepareNodeAsync(false);
            }
            finally
            {
                AzureProxyListComputeNodesAsync = default;
            }

            Assert.IsNull(info);
            Assert.AreEqual(1, ((IBatchPoolImpl)pool).TestTargetDedicated);
            Assert.AreEqual(0, ((IBatchPoolImpl)pool).TestTargetLowPriority);

            IAsyncEnumerable<ComputeNode> ListComputeNodes(string id, ODATADetailLevel detail)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                Assert.IsNotNull(detail);
                Assert.IsNotNull(detail.SelectClause);
                return AsyncEnumerable.Empty<ComputeNode>();
            }
        }

        [TestMethod]
        public async Task PrepareNodeIncrementsLowPriorityTargetWhenNoLowPriorityNodeIsIdle()
        {
            var reboots = 0;
            var pool = await CreateBatchPoolAsync();
            var nodes = new ComputeNode[]
            {
                GenerateNode(pool.Pool.PoolId, "NodeOneLoPriRunning", false, false),
                GenerateNode(pool.Pool.PoolId, "NodeOneDedicatedRunning", true, false),
                GenerateNode(pool.Pool.PoolId, "NodeTwoLoPriRunning", false, false),
                GenerateNode(pool.Pool.PoolId, "NodeTwoDedicatedRunning", true, false),
                GenerateNode(pool.Pool.PoolId, "NodeOneLoPriIdle", false, false),
                GenerateNode(pool.Pool.PoolId, "NodeOneDedicatedIdle", true, true),
                GenerateNode(pool.Pool.PoolId, "NodeTwoLoPriIdle", false, false),
                GenerateNode(pool.Pool.PoolId, "NodeTwoDedicatedIdle", true, true),
            };

            AffinityInformation info = default;
            try
            {
                IsRebootCandidate = (isDedicated, isIdle) => isIdle && !isDedicated;
                RebootCalled = rebootCalled;
                AzureProxyListComputeNodesAsync = ListComputeNodes;
                info = await pool.PrepareNodeAsync(true);
            }
            finally
            {
                AzureProxyListComputeNodesAsync = default;
                IsRebootCandidate = default;
                RebootCalled = default;
            }

            Assert.IsNull(info);
            Assert.AreEqual(0, reboots);
            Assert.AreEqual(1, ((IBatchPoolImpl)pool).TestTargetLowPriority);
            Assert.AreEqual(0, ((IBatchPoolImpl)pool).TestTargetDedicated);

            void rebootCalled()
                => ++reboots;

            IAsyncEnumerable<ComputeNode> ListComputeNodes(string id, ODATADetailLevel detail)
                => ListComputeNodesAsync(nodes, id, detail, ValidateLCN, c => c.State == ComputeNodeState.Idle);

            void ValidateLCN(string id, ODATADetailLevel detail)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                Assert.IsNotNull(detail);
                Assert.IsNotNull(detail.SelectClause);
            }
        }

        [TestMethod]
        public async Task PrepareNodeIncrementsLowPriorityTargetWhenNoDedicatedNodeIsIdle()
        {
            var reboots = 0;
            var pool = await CreateBatchPoolAsync();
            var nodes = new ComputeNode[]
            {
                GenerateNode(pool.Pool.PoolId, "NodeOneLoPriRunning", false, false),
                GenerateNode(pool.Pool.PoolId, "NodeOneDedicatedRunning", true, false),
                GenerateNode(pool.Pool.PoolId, "NodeTwoLoPriRunning", false, false),
                GenerateNode(pool.Pool.PoolId, "NodeTwoDedicatedRunning", true, false),
                GenerateNode(pool.Pool.PoolId, "NodeOneLoPriIdle", false, true),
                GenerateNode(pool.Pool.PoolId, "NodeOneDedicatedIdle", true, false),
                GenerateNode(pool.Pool.PoolId, "NodeTwoLoPriIdle", false, true),
                GenerateNode(pool.Pool.PoolId, "NodeTwoDedicatedIdle", true, false),
            };

            AffinityInformation info = default;
            try
            {
                IsRebootCandidate = (isDedicated, isIdle) => isIdle && isDedicated;
                RebootCalled = rebootCalled;
                AzureProxyListComputeNodesAsync = ListComputeNodes;
                info = await pool.PrepareNodeAsync(false);
            }
            finally
            {
                AzureProxyListComputeNodesAsync = default;
                IsRebootCandidate = default;
                RebootCalled = default;
            }

            Assert.IsNull(info);
            Assert.AreEqual(0, reboots);
            Assert.AreEqual(0, ((IBatchPoolImpl)pool).TestTargetLowPriority);
            Assert.AreEqual(1, ((IBatchPoolImpl)pool).TestTargetDedicated);

            void rebootCalled()
                => ++reboots;

            IAsyncEnumerable<ComputeNode> ListComputeNodes(string id, ODATADetailLevel detail)
                => ListComputeNodesAsync(nodes, id, detail, ValidateLCN, c => c.State == ComputeNodeState.Idle);

            void ValidateLCN(string id, ODATADetailLevel detail)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                Assert.IsNotNull(detail);
                Assert.IsNotNull(detail.SelectClause);
            }
        }

        [TestMethod]
        public async Task PrepareNodeReturnsLowPriorityTargetWhenAtLeastOneNodeIsIdle()
        {
            var reboots = 0;
            var pool = await CreateBatchPoolAsync();
            var nodes = new ComputeNode[]
            {
                GenerateNode(pool.Pool.PoolId, "NodeOneLoPriRunning", false, false),
                GenerateNode(pool.Pool.PoolId, "NodeOneDedicatedRunning", true, false),
                GenerateNode(pool.Pool.PoolId, "NodeTwoLoPriRunning", false, false),
                GenerateNode(pool.Pool.PoolId, "NodeTwoDedicatedRunning", true, false),
                GenerateNode(pool.Pool.PoolId, "NodeOneLoPriIdle", false, true),
                GenerateNode(pool.Pool.PoolId, "NodeOneDedicatedIdle", true, true),
                GenerateNode(pool.Pool.PoolId, "NodeTwoLoPriIdle", false, true),
                GenerateNode(pool.Pool.PoolId, "NodeTwoDedicatedIdle", true, true),
            };

            AffinityInformation info = default;
            try
            {
                IsRebootCandidate = (isDedicated, isIdle) => isIdle && !isDedicated;
                RebootCalled = rebootCalled;
                AzureProxyListComputeNodesAsync = ListComputeNodes;
                info = await pool.PrepareNodeAsync(true);
            }
            finally
            {
                AzureProxyListComputeNodesAsync = default;
                IsRebootCandidate = default;
                RebootCalled = default;
            }

            Assert.AreEqual(AffinityPrefix + "NodeOneLoPriIdle", info.AffinityId);
            Assert.AreEqual(1, reboots);
            Assert.AreEqual(0, ((IBatchPoolImpl)pool).TestTargetLowPriority);
            Assert.AreEqual(0, ((IBatchPoolImpl)pool).TestTargetDedicated);

            void rebootCalled()
                => ++reboots;

            IAsyncEnumerable<ComputeNode> ListComputeNodes(string id, ODATADetailLevel detail)
                => ListComputeNodesAsync(nodes, id, detail, ValidateLCN, c => c.State == ComputeNodeState.Idle);

            void ValidateLCN(string id, ODATADetailLevel detail)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                Assert.IsNotNull(detail);
                Assert.IsNotNull(detail.SelectClause);
            }
        }

        [TestMethod]
        public async Task PrepareNodeReturnsDedictatedTargetWhenAtLeastOneNodeIsIdle()
        {
            var reboots = 0;
            var pool = await CreateBatchPoolAsync();
            var nodes = new ComputeNode[]
            {
                GenerateNode(pool.Pool.PoolId, "NodeOneLoPriRunning", false, false),
                GenerateNode(pool.Pool.PoolId, "NodeOneDedicatedRunning", true, false),
                GenerateNode(pool.Pool.PoolId, "NodeTwoLoPriRunning", false, false),
                GenerateNode(pool.Pool.PoolId, "NodeTwoDedicatedRunning", true, false),
                GenerateNode(pool.Pool.PoolId, "NodeOneLoPriIdle", false, true),
                GenerateNode(pool.Pool.PoolId, "NodeOneDedicatedIdle", true, true),
                GenerateNode(pool.Pool.PoolId, "NodeTwoLoPriIdle", false, true),
                GenerateNode(pool.Pool.PoolId, "NodeTwoDedicatedIdle", true, true),
            };

            AffinityInformation info = default;
            try
            {
                IsRebootCandidate = (isDedicated, isIdle) => isIdle && isDedicated;
                RebootCalled = rebootCalled;
                AzureProxyListComputeNodesAsync = ListComputeNodes;
                info = await pool.PrepareNodeAsync(false);
            }
            finally
            {
                AzureProxyListComputeNodesAsync = default;
                IsRebootCandidate = default;
                RebootCalled = default;
            }

            Assert.AreEqual(AffinityPrefix + "NodeOneDedicatedIdle", info.AffinityId);
            Assert.AreEqual(1, reboots);
            Assert.AreEqual(0, ((IBatchPoolImpl)pool).TestTargetLowPriority);
            Assert.AreEqual(0, ((IBatchPoolImpl)pool).TestTargetDedicated);

            void rebootCalled()
                => ++reboots;

            IAsyncEnumerable<ComputeNode> ListComputeNodes(string id, ODATADetailLevel detail)
                => ListComputeNodesAsync(nodes, id, detail, ValidateLCN, c => c.State == ComputeNodeState.Idle);

            void ValidateLCN(string id, ODATADetailLevel detail)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                Assert.IsNotNull(detail);
                Assert.IsNotNull(detail.SelectClause);
            }
        }

        [TestMethod]
        public async Task PrepareNodeDoesNotDuplicateNodes()
        {
            var reboots = 0;
            var pool = await CreateBatchPoolAsync();
            var nodes = new ComputeNode[]
            {
                GenerateNode(pool.Pool.PoolId, "NodeOneLoPriRunning", false, false),
                GenerateNode(pool.Pool.PoolId, "NodeOneDedicatedRunning", true, false),
                GenerateNode(pool.Pool.PoolId, "NodeTwoLoPriRunning", false, false),
                GenerateNode(pool.Pool.PoolId, "NodeTwoDedicatedRunning", true, false),
                GenerateNode(pool.Pool.PoolId, "NodeOneLoPriIdle", false, true),
                GenerateNode(pool.Pool.PoolId, "NodeOneDedicatedIdle", true, true),
                GenerateNode(pool.Pool.PoolId, "NodeTwoLoPriIdle", false, true),
                GenerateNode(pool.Pool.PoolId, "NodeTwoDedicatedIdle", true, true),
            };

            try
            {
                IsRebootCandidate = (isDedicated, isIdle) => isIdle;
                RebootCalled = rebootCalled;
                AzureProxyListComputeNodesAsync = ListComputeNodes;
                _ = await pool.PrepareNodeAsync(true);
                _ = await pool.PrepareNodeAsync(true);
            }
            finally
            {
                AzureProxyListComputeNodesAsync = default;
                IsRebootCandidate = default;
                RebootCalled = default;
            }

            Assert.IsTrue(((IBatchPoolImpl)pool).TestIsNodeReserved(AffinityPrefix + "NodeTwoLoPriIdle"));
            Assert.IsTrue(((IBatchPoolImpl)pool).TestIsNodeReserved(AffinityPrefix + "NodeOneLoPriIdle"));
            Assert.AreEqual(2, ((IBatchPoolImpl)pool).TestNodeReservationCount);

            void rebootCalled()
                => ++reboots;

            IAsyncEnumerable<ComputeNode> ListComputeNodes(string id, ODATADetailLevel detail)
                => ListComputeNodesAsync(nodes, id, detail, ValidateLCN, c => c.State == ComputeNodeState.Idle);

            void ValidateLCN(string id, ODATADetailLevel detail)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                Assert.IsNotNull(detail);
                Assert.IsNotNull(detail.SelectClause);
            }
        }

        [TestMethod]
        public async Task ReleaseNodeRemovesNodeWhenReserved()
        {
            var reboots = 0;
            var pool = await CreateBatchPoolAsync();
            var nodes = new ComputeNode[]
            {
                GenerateNode(pool.Pool.PoolId, "NodeOneLoPriRunning", false, false),
                GenerateNode(pool.Pool.PoolId, "NodeOneDedicatedRunning", true, false),
                GenerateNode(pool.Pool.PoolId, "NodeTwoLoPriRunning", false, false),
                GenerateNode(pool.Pool.PoolId, "NodeTwoDedicatedRunning", true, false),
                GenerateNode(pool.Pool.PoolId, "NodeOneLoPriIdle", false, true),
                GenerateNode(pool.Pool.PoolId, "NodeOneDedicatedIdle", true, true),
                GenerateNode(pool.Pool.PoolId, "NodeTwoLoPriIdle", false, true),
                GenerateNode(pool.Pool.PoolId, "NodeTwoDedicatedIdle", true, true),
            };

            try
            {
                IsRebootCandidate = (isDedicated, isIdle) => isIdle;
                RebootCalled = rebootCalled;
                AzureProxyListComputeNodesAsync = ListComputeNodes;
                _ = await pool.PrepareNodeAsync(true);
                _ = await pool.PrepareNodeAsync(false);

                Assert.IsTrue(((IBatchPoolImpl)pool).TestIsNodeReserved(AffinityPrefix + "NodeOneDedicatedIdle"));

                pool.ReleaseNode(new AffinityInformation(AffinityPrefix + "NodeOneDedicatedIdle"));
            }
            finally
            {
                AzureProxyListComputeNodesAsync = default;
                IsRebootCandidate = default;
                RebootCalled = default;
            }

            Assert.IsFalse(((IBatchPoolImpl)pool).TestIsNodeReserved(AffinityPrefix + "NodeOneDedicatedIdle"));
            Assert.IsTrue(((IBatchPoolImpl)pool).TestIsNodeReserved(AffinityPrefix + "NodeOneLoPriIdle"));
            Assert.AreEqual(1, ((IBatchPoolImpl)pool).TestNodeReservationCount);

            void rebootCalled()
                => ++reboots;

            IAsyncEnumerable<ComputeNode> ListComputeNodes(string id, ODATADetailLevel detail)
                => ListComputeNodesAsync(nodes, id, detail, ValidateLCN, c => c.State == ComputeNodeState.Idle);

            void ValidateLCN(string id, ODATADetailLevel detail)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                Assert.IsNotNull(detail);
                Assert.IsNotNull(detail.SelectClause);
            }
        }

        [TestMethod]
        public async Task ReleaseNodeDoesNotFailWhenNodeNotFound()
        {
            var reboots = 0;
            var pool = await CreateBatchPoolAsync();
            var nodes = new ComputeNode[]
            {
                GenerateNode(pool.Pool.PoolId, "NodeOneLoPriRunning", false, false),
                GenerateNode(pool.Pool.PoolId, "NodeOneDedicatedRunning", true, false),
                GenerateNode(pool.Pool.PoolId, "NodeTwoLoPriRunning", false, false),
                GenerateNode(pool.Pool.PoolId, "NodeTwoDedicatedRunning", true, false),
                GenerateNode(pool.Pool.PoolId, "NodeOneLoPriIdle", false, true),
                GenerateNode(pool.Pool.PoolId, "NodeOneDedicatedIdle", true, true),
                GenerateNode(pool.Pool.PoolId, "NodeTwoLoPriIdle", false, true),
                GenerateNode(pool.Pool.PoolId, "NodeTwoDedicatedIdle", true, true),
            };

            try
            {
                IsRebootCandidate = (isDedicated, isIdle) => isIdle;
                RebootCalled = rebootCalled;
                AzureProxyListComputeNodesAsync = ListComputeNodes;
                _ = await pool.PrepareNodeAsync(true);
                _ = await pool.PrepareNodeAsync(false);

                Assert.AreEqual(2, ((IBatchPoolImpl)pool).TestNodeReservationCount);

                pool.ReleaseNode(new AffinityInformation(AffinityPrefix + "NodeTwoLoPriRunning"));
            }
            finally
            {
                AzureProxyListComputeNodesAsync = default;
                IsRebootCandidate = default;
                RebootCalled = default;
            }

            Assert.AreEqual(2, ((IBatchPoolImpl)pool).TestNodeReservationCount);

            void rebootCalled()
                => ++reboots;

            IAsyncEnumerable<ComputeNode> ListComputeNodes(string id, ODATADetailLevel detail)
                => ListComputeNodesAsync(nodes, id, detail, ValidateLCN, c => c.State == ComputeNodeState.Idle);

            void ValidateLCN(string id, ODATADetailLevel detail)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                Assert.IsNotNull(detail);
                Assert.IsNotNull(detail.SelectClause);
            }
        }

        [TestMethod]
        public async Task SyncSizeSetsIBatchPoolState()
        {
            var pool = await CreateBatchPoolAsync();

            try
            {
                AzureProxyReturnComputeNodeTargets = () => (3, 2);
                AzureProxyGetComputeNodeTargets = ValidateComputeNodeTargets;
                await pool.ServicePoolAsync(IBatchPool.ServiceKind.SyncSize);
            }
            finally
            {
                AzureProxyGetComputeNodeTargets = default;
                AzureProxyReturnComputeNodeTargets = default;
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
        public async Task RemoveNodeIfIdleRemovesNodesWhenNodesAreIdle()
        {
            DateTime expiryTime = default;
            var reboots = 0;
            var pool = await CreateBatchPoolAsync();
            var nodes = new ComputeNode[]
            {
                GenerateNode(pool.Pool.PoolId, "NodeOneLoPriRunning", false, false),
                GenerateNode(pool.Pool.PoolId, "NodeOneDedicatedRunning", true, false),
                GenerateNode(pool.Pool.PoolId, "NodeTwoLoPriRunning", false, false),
                GenerateNode(pool.Pool.PoolId, "NodeTwoDedicatedRunning", true, false),
                GenerateNode(pool.Pool.PoolId, "NodeOneLoPriIdle", false, true),
                GenerateNode(pool.Pool.PoolId, "NodeOneDedicatedIdle", true, true),
                GenerateNode(pool.Pool.PoolId, "NodeTwoLoPriIdle", false, true),
                GenerateNode(pool.Pool.PoolId, "NodeTwoDedicatedIdle", true, true),
            };

            try
            {
                AzureProxyGetAllocationState = GetState;
                AzureProxyReturnComputeNodeTargets = () => (4, 4);
                AzureProxyGetComputeNodeTargets = ValidateComputeNodeTargets;
                IsRebootCandidate = (isDedicated, isIdle) => isIdle;
                RebootCalled = rebootCalled;
                AzureProxyListComputeNodesAsync = ListComputeNodesSyncSize;
                _ = await pool.PrepareNodeAsync(true);
                _ = await pool.PrepareNodeAsync(true);
                _ = await pool.PrepareNodeAsync(false);
                _ = await pool.PrepareNodeAsync(false);
                await pool.ServicePoolAsync(IBatchPool.ServiceKind.SyncSize);
                AzureProxyListComputeNodesAsync = ListComputeNodesRemoveNode;
                AzureProxyDeleteBatchComputeNodes = DeleteComputeNodes;

                Assert.AreEqual(4, ((IBatchPoolImpl)pool).TestNodeReservationCount);
                Assert.AreEqual(4, ((IBatchPoolImpl)pool).TestTargetLowPriority);
                Assert.AreEqual(4, ((IBatchPoolImpl)pool).TestTargetDedicated);
                pool.ReleaseNode(new AffinityInformation(AffinityPrefix + "NodeOneDedicatedIdle"));
                Assert.AreEqual(3, ((IBatchPoolImpl)pool).TestNodeReservationCount);

                // Ensure nodes are not deleted prematurely
                expiryTime = DateTime.UtcNow - ((IBatchPoolImpl)pool).TestIdleNodeTime;
                await pool.ServicePoolAsync(IBatchPool.ServiceKind.RemoveNodeIfIdle);
                Assert.AreEqual(3, ((IBatchPoolImpl)pool).TestNodeReservationCount);
                Assert.AreEqual(4, ((IBatchPoolImpl)pool).TestTargetLowPriority);
                Assert.AreEqual(4, ((IBatchPoolImpl)pool).TestTargetDedicated);

                await Task.Delay(((IBatchPoolImpl)pool).TestIdleNodeTime + TimeSpan.FromMilliseconds(50));
                expiryTime = DateTime.UtcNow - ((IBatchPoolImpl)pool).TestIdleNodeTime;

                await pool.ServicePoolAsync(IBatchPool.ServiceKind.RemoveNodeIfIdle);
            }
            finally
            {
                AzureProxyDeleteBatchComputeNodes = default;
                AzureProxyListComputeNodesAsync = default;
                IsRebootCandidate = default;
                RebootCalled = default;
                AzureProxyReturnComputeNodeTargets = default;
                AzureProxyGetComputeNodeTargets = default;
                AzureProxyGetAllocationState = default;
            }

            Assert.AreEqual(0, ((IBatchPoolImpl)pool).TestNodeReservationCount);
            Assert.AreEqual(2, ((IBatchPoolImpl)pool).TestTargetLowPriority);
            Assert.AreEqual(2, ((IBatchPoolImpl)pool).TestTargetDedicated);


            Microsoft.Azure.Batch.Common.AllocationState GetState(string id)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                return Microsoft.Azure.Batch.Common.AllocationState.Steady;
            }

            void ValidateComputeNodeTargets(string id)
                => Assert.AreEqual(pool.Pool.PoolId, id);

            void rebootCalled()
                => ++reboots;

            IAsyncEnumerable<ComputeNode> ListComputeNodesSyncSize(string id, ODATADetailLevel detail)
                => ListComputeNodesAsync(nodes, id, detail, ValidateLCNSyncSize, c => c.State == ComputeNodeState.Idle);

            void ValidateLCNSyncSize(string id, ODATADetailLevel detail)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                Assert.IsNotNull(detail);
                Assert.AreEqual("id,affinityId,isDedicated", detail.SelectClause);
                Assert.AreEqual("state eq 'idle'", detail.FilterClause);
            }

            IAsyncEnumerable<ComputeNode> ListComputeNodesRemoveNode(string id, ODATADetailLevel detail)
                => ListComputeNodesAsync(nodes, id, detail, ValidateLCNRemoveNode, c => c.State == ComputeNodeState.Idle && c.StateTransitionTime < expiryTime);

            void ValidateLCNRemoveNode(string id, ODATADetailLevel detail)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                Assert.IsNotNull(detail);
                Assert.AreEqual("id,affinityId,isDedicated", detail.SelectClause);
                Assert.AreEqual($"state eq 'idle' and stateTransitionTime lt DateTime'{expiryTime.ToString("yyyy-MM-dd'T'HH:mm:ssZ", System.Globalization.CultureInfo.InvariantCulture)}'", detail.FilterClause);
            }

            void DeleteComputeNodes(string poolId, IEnumerable<ComputeNode> computeNodes, CancellationToken cancellationToken)
            {
                var nodes = computeNodes.Select(n => n.Id).ToList();
                Assert.AreEqual(pool.Pool.PoolId, poolId);
                Assert.AreEqual(4, nodes.Count);
                Assert.IsTrue(nodes.Contains("NodeOneDedicatedIdle"));
                Assert.IsTrue(nodes.Contains("NodeOneLoPriIdle"));
                Assert.IsTrue(nodes.Contains("NodeTwoLoPriIdle"));
                Assert.IsTrue(nodes.Contains("NodeTwoDedicatedIdle"));
                Assert.IsFalse(nodes.Contains("NodeOneLoPriRunning"));
                Assert.IsFalse(nodes.Contains("NodeOneDedicatedRunning"));
                Assert.IsFalse(nodes.Contains("NodeTwoLoPriRunning"));
                Assert.IsFalse(nodes.Contains("NodeTwoDedicatedRunning"));
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

        [TestMethod]
        public async Task RemovePoolIfEmptyDoesNotDeletePoolIfPoolIsAvailable()
        {
            var pool = await CreateBatchPoolAsync();

            try
            {
                AzureProxyDeleteBatchPool = (poolId, cancellationToken) => Assert.Fail();
                await pool.ServicePoolAsync(IBatchPool.ServiceKind.RemovePoolIfEmpty);
            }
            finally
            {
                AzureProxyDeleteBatchPool = default;
            }
        }

        [TestMethod]
        public async Task RemovePoolIfEmptyDoesNotDeletePoolIfPoolHasComputeNodes()
        {
            var pool = await CreateBatchPoolAsync();
            ((IBatchPoolImpl)pool).TestSetAvailable(false);

            try
            {
                AzureProxyGetCurrentComputeNodes = () => (0, 1);
                AzureProxyDeleteBatchPool = (poolId, cancellationToken) => Assert.Fail();
                await pool.ServicePoolAsync(IBatchPool.ServiceKind.RemovePoolIfEmpty);
            }
            finally
            {
                AzureProxyDeleteBatchPool = default;
                AzureProxyGetCurrentComputeNodes = default;
            }
        }

        [TestMethod]
        public async Task RemovePoolIfEmptyDeletesPoolIfPoolIsNotAvailableAndHasNoComputeNodes()
        {
            var pool = await CreateBatchPoolAsync();
            ((IBatchPoolImpl)pool).TestSetAvailable(false);
            var isDeleted = false;

            try
            {
                AzureProxyDeleteBatchPool = DeletePool;
                await pool.ServicePoolAsync(IBatchPool.ServiceKind.RemovePoolIfEmpty);
            }
            finally
            {
                AzureProxyDeleteBatchPool = default;
            }

            Assert.IsTrue(isDeleted);

            void DeletePool(string poolId, CancellationToken cancellationToken)
            {
                Assert.AreEqual(poolId, pool.Pool.PoolId);
                isDeleted = true;
            }
        }

        private async Task<IBatchPool> CreateBatchPoolAsync()
        {
            var azureProxy = GetMockAzureProxy(AzureProxyReturnValues.Get()).Object;
            return await new BatchPools(azureProxy, new Mock<ILogger<BatchPools>>().Object, GetMockConfig(), GetBatchPoolFactory(azureProxy))
                           .GetOrAddAsync("key1", id => new Pool(name: id, displayName: "display1", vmSize: "vmSize1"));
        }

        internal static BatchPoolFactory GetBatchPoolFactory(IAzureProxy azureProxy)
        {
            var services = new ServiceCollection();
            services.AddSingleton(azureProxy);
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
            azureProxy.Setup(a => a.ListComputeNodesAsync(It.IsAny<string>(), It.IsAny<DetailLevel>())).Returns<string, ODATADetailLevel>((poolId, detailLevel) => AzureProxyListComputeNodesAsync(poolId, detailLevel));
            azureProxy.Setup(a => a.DeleteBatchComputeNodesAsync(It.IsAny<string>(), It.IsAny<IEnumerable<ComputeNode>>(), It.IsAny<CancellationToken>())).Callback<string, IEnumerable<ComputeNode>, CancellationToken>((poolId, computeNodes, cancellationToken) => AzureProxyDeleteBatchComputeNodes(poolId, computeNodes, cancellationToken)).Returns(Task.CompletedTask);
            azureProxy.Setup(a => a.GetComputeNodeTargets(It.IsAny<string>())).Callback<string>(id => AzureProxyGetComputeNodeTargets?.Invoke(id)).Returns(() => AzureProxyReturnComputeNodeTargets());
            azureProxy.Setup(a => a.GetCurrentComputeNodesAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).Returns(() => Task.FromResult<(int? lowPriorityNodes, int? dedicatedNodes)>(AzureProxyGetCurrentComputeNodes?.Invoke() ?? (null, null)));
            azureProxy.Setup(a => a.DeleteBatchPoolAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).Callback<string, CancellationToken>((poolId, cancellationToken) => AzureProxyDeleteBatchPool?.Invoke(poolId, cancellationToken)).Returns(Task.CompletedTask);
            azureProxy.Setup(a => a.GetAllocationStateAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).Returns((string id, CancellationToken token) => Task.FromResult(AzureProxyGetAllocationState?.Invoke(id)));
            azureProxy.Setup(a => a.SetComputeNodeTargetsAsync(It.IsAny<string>(), It.IsAny<int?>(), It.IsAny<int?>(), It.IsAny<CancellationToken>())).Callback<string, int?, int?, CancellationToken>((id, loPri, dedic, cancel) => AzureProxySetComputeNodeTargets?.Invoke(id, loPri, dedic)).Returns(Task.CompletedTask);

            return azureProxy;
        }

        private Func<string, ODATADetailLevel, IAsyncEnumerable<ComputeNode>> AzureProxyListComputeNodesAsync;
        private Action<string> AzureProxyGetComputeNodeTargets;
        private Action<string, IEnumerable<ComputeNode>, CancellationToken> AzureProxyDeleteBatchComputeNodes;
        private Func<string, Microsoft.Azure.Batch.Common.AllocationState> AzureProxyGetAllocationState;
        private Func<(int, int)> AzureProxyReturnComputeNodeTargets;
        private Action<string, int?, int?> AzureProxySetComputeNodeTargets;
        private Func<(int?, int?)> AzureProxyGetCurrentComputeNodes;
        private Action<string, CancellationToken> AzureProxyDeleteBatchPool;
        private Func<bool, bool, bool> IsRebootCandidate;
        private Action RebootCalled;

        private static IConfiguration GetMockConfig()
        {
            var config = new ConfigurationBuilder().AddInMemoryCollection().Build();
            config["BatchPoolIdleNodeTime"] = "0.01";
            config["BatchPoolIdlePoolTime"] = "0.015";
            config["BatchPoolRotationForcedTime"] = "0.000011575";

            return config;
        }

        private ComputeNode GenerateNode(string poolId, string id, bool isDedicated, bool isIdle)
        {
            var computeNodeOperations = new Mock<Microsoft.Azure.Batch.Protocol.IComputeNodeOperations>();
            computeNodeOperations.Setup(x => x.RebootWithHttpMessagesAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<Microsoft.Azure.Batch.Protocol.Models.ComputeNodeRebootOption?>(), It.IsAny<Microsoft.Azure.Batch.Protocol.Models.ComputeNodeRebootOptions>(), It.IsAny<Dictionary<string, List<string>>>(), It.IsAny<CancellationToken>()))
                .Callback<string, string, Microsoft.Azure.Batch.Protocol.Models.ComputeNodeRebootOption?, Microsoft.Azure.Batch.Protocol.Models.ComputeNodeRebootOptions, Dictionary<string, List<string>>, CancellationToken>((poolId, nodeId, nodeRebootOption, computeNodeRebootOptions, customHeaders, cancelationToken) =>
                {
                    if (IsRebootCandidate?.Invoke(isDedicated, isIdle) ?? false)
                    {
                        RebootCalled();
                    }
                    else
                    {
                        Assert.Fail("Should not be rebooted.");
                    }
                })
                .Returns(Task.FromResult(new Microsoft.Rest.Azure.AzureOperationHeaderResponse<Microsoft.Azure.Batch.Protocol.Models.ComputeNodeRebootHeaders>()));
            var batchServiceClient = new MockServiceClient(computeNodeOperations.Object);
            var protocolLayer = typeof(Microsoft.Azure.Batch.Protocol.BatchServiceClient).Assembly.GetType("Microsoft.Azure.Batch.ProtocolLayer").GetConstructor(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic, null, new Type[] { typeof(Microsoft.Azure.Batch.Protocol.BatchServiceClient) }, null)
                .Invoke(new object[] { batchServiceClient });
            var parentClient = (BatchClient)typeof(BatchClient).GetConstructor(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic, null, new Type[] { typeof(Microsoft.Azure.Batch.Protocol.BatchServiceClient).Assembly.GetType("Microsoft.Azure.Batch.IProtocolLayer") }, null)
                .Invoke(new object[] { protocolLayer });
            var modelNode = new Microsoft.Azure.Batch.Protocol.Models.ComputeNode(stateTransitionTime: DateTime.UtcNow, id: id, affinityId: AffinityPrefix + id, isDedicated: isDedicated, state: isIdle ? Microsoft.Azure.Batch.Protocol.Models.ComputeNodeState.Idle : Microsoft.Azure.Batch.Protocol.Models.ComputeNodeState.Running);
            var node = (ComputeNode)typeof(ComputeNode).GetConstructor(System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance, default, new Type[] { typeof(BatchClient), typeof(string), typeof(Microsoft.Azure.Batch.Protocol.Models.ComputeNode), typeof(IEnumerable<BatchClientBehavior>) }, default)
                .Invoke(new object[] { parentClient, poolId, modelNode, null });
            return node;
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        static async IAsyncEnumerable<ComputeNode> ListComputeNodesAsync(ComputeNode[] nodes, string id, ODATADetailLevel detail, Action<string, ODATADetailLevel> validate, Predicate<ComputeNode> select = default)
        {
            _ = validate ?? throw new ArgumentNullException(nameof(validate));

            validate(id, detail);
            foreach (var node in nodes.Where(n => select?.Invoke(n) ?? true))
            {
                yield return node;
            }
        }
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously

        private sealed class MockServiceClient : Microsoft.Azure.Batch.Protocol.BatchServiceClient
        {
            private readonly Microsoft.Azure.Batch.Protocol.IComputeNodeOperations computeNode;

            public MockServiceClient(Microsoft.Azure.Batch.Protocol.IComputeNodeOperations computeNode)
            {
                this.computeNode = computeNode ?? throw new ArgumentNullException(nameof(computeNode));
            }

            public override Microsoft.Azure.Batch.Protocol.IComputeNodeOperations ComputeNode => computeNode;
        }
    }
}
