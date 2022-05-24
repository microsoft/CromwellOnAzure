// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Azure.Management.Batch.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using TesApi.Web;

namespace TesApi.Tests
{
    [TestClass]
    public class BatchPoolTests
    {
        private const string AffinityPrefix = "AP-";

        [TestMethod]
        public async Task PrepareNodeMakesReservationWhenPoolIsEmpty()
        {
            IBatchPool pool = default;

            var azureProxy = AzureProxyReturnValues.Get();
            azureProxy.AzureProxyListComputeNodesAsync = ListComputeNodes;
            using var services = GetServiceProvider(azureProxy);
            pool = await AddPool(services.GetT());

            AffinityInformation info = default;

            try { info = await pool.PrepareNodeAsync("JobId1", true); }
            catch (AzureBatchQuotaMaxedOutException) { }

            Assert.IsNull(info);
            Assert.AreEqual(0, ((BatchPool)pool).TestTargetLowPriority);
            Assert.AreEqual(0, ((BatchPool)pool).TestTargetDedicated);
            Assert.AreEqual(1, ((BatchPool)pool).TestPendingReservationsCount);

            IAsyncEnumerable<ComputeNode> ListComputeNodes(string id, ODATADetailLevel detail)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                Assert.IsNotNull(detail);
                Assert.IsNotNull(detail.SelectClause);
                return AsyncEnumerable.Empty<ComputeNode>();
            }
        }

        [TestMethod]
        public async Task PrepareNodeMakesOnlyOneReservationPerJobWhenPoolIsNotResized()
        {
            IBatchPool pool = default;

            var azureProxy = AzureProxyReturnValues.Get();
            azureProxy.AzureProxyListComputeNodesAsync = ListComputeNodes;
            using var services = GetServiceProvider(azureProxy);
            pool = await AddPool(services.GetT());

            AffinityInformation info = default;
            try
            {
                _ = await pool.PrepareNodeAsync("JobId1", true);
            }
            catch (AzureBatchQuotaMaxedOutException)
            {
                try { info = await pool.PrepareNodeAsync("JobId1", true); }
                catch (AzureBatchQuotaMaxedOutException) { }
            }

            Assert.IsNull(info);
            Assert.AreEqual(0, ((BatchPool)pool).TestTargetLowPriority);
            Assert.AreEqual(0, ((BatchPool)pool).TestTargetDedicated);
            Assert.AreEqual(1, ((BatchPool)pool).TestPendingReservationsCount);

            IAsyncEnumerable<ComputeNode> ListComputeNodes(string id, ODATADetailLevel detail)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                Assert.IsNotNull(detail);
                Assert.IsNotNull(detail.SelectClause);
                return AsyncEnumerable.Empty<ComputeNode>();
            }
        }

        [TestMethod]
        public async Task PrepareNodeMakesMultipleReservationsWhenPoolIsNotResized()
        {
            IBatchPool pool = default;

            var azureProxy = AzureProxyReturnValues.Get();
            azureProxy.AzureProxyListComputeNodesAsync = ListComputeNodes;
            using var services = GetServiceProvider(azureProxy);
            pool = await AddPool(services.GetT());

            AffinityInformation info = default;
            try
            {
                _ = await pool.PrepareNodeAsync("JobId1", true);
            }
            catch (AzureBatchQuotaMaxedOutException)
            {
                try { info = await pool.PrepareNodeAsync("JobId2", true); }
                catch (AzureBatchQuotaMaxedOutException) { }
            }

            Assert.IsNull(info);
            Assert.AreEqual(0, ((BatchPool)pool).TestTargetLowPriority);
            Assert.AreEqual(0, ((BatchPool)pool).TestTargetDedicated);
            Assert.AreEqual(2, ((BatchPool)pool).TestPendingReservationsCount);

            IAsyncEnumerable<ComputeNode> ListComputeNodes(string id, ODATADetailLevel detail)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                Assert.IsNotNull(detail);
                Assert.IsNotNull(detail.SelectClause);
                return AsyncEnumerable.Empty<ComputeNode>();
            }
        }

        [TestMethod]
        public async Task PrepareNodeIncrementsLowPriorityTargetWhenNoNodeIsIdle()
        {
            var setTargetsCalled = false;
            IBatchPool pool = default;

            var azureProxy = AzureProxyReturnValues.Get();
            azureProxy.AzureProxyGetAllocationState = id => Microsoft.Azure.Batch.Common.AllocationState.Steady;
            azureProxy.AzureProxySetComputeNodeTargets = (id, loPri, dedic) => setTargetsCalled = true;
            azureProxy.AzureProxyListComputeNodesAsync = ListComputeNodes;
            using var services = GetServiceProvider(azureProxy);
            pool = await AddPool(services.GetT());

            AffinityInformation info = default;
            try
            {
                _ = await pool.PrepareNodeAsync("JobId1", true);
            }
            catch (AzureBatchQuotaMaxedOutException)
            {
                await pool.ServicePoolAsync();
                info = await pool.PrepareNodeAsync("JobId1", true);
            }

            Assert.IsNotNull(info);
            Assert.IsTrue(setTargetsCalled);
            Assert.AreEqual(1, ((BatchPool)pool).TestNodeReservationCount);

            IAsyncEnumerable<ComputeNode> ListComputeNodes(string id, ODATADetailLevel detail)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                Assert.IsNotNull(detail);
                Assert.IsNotNull(detail.SelectClause);
                var result = AsyncEnumerable.Empty<ComputeNode>();
                if (setTargetsCalled)
                {
                    result = result.Append(GenerateNode(pool.Pool.PoolId, "ComputeNode1", false, true));
                }
                return result;
            }
        }

        [TestMethod]
        public async Task PrepareNodeAssignsRunningNodeWhenNoUnassignedNodeIsIsIdle()
        {
            var setTargetsCalled = false;
            IBatchPool pool = default;

            var azureProxy = AzureProxyReturnValues.Get();
            azureProxy.AzureProxyGetAllocationState = id => Microsoft.Azure.Batch.Common.AllocationState.Steady;
            azureProxy.AzureProxySetComputeNodeTargets = (id, loPri, dedic) => setTargetsCalled = true;
            azureProxy.AzureProxyListComputeNodesAsync = ListComputeNodes;
            using var services = GetServiceProvider(azureProxy);
            pool = await AddPool(services.GetT());

            AffinityInformation info = default;
            try
            {
                info = await pool.PrepareNodeAsync("JobId1", true);
            }
            catch (AzureBatchQuotaMaxedOutException)
            {
                try { _ = await pool.PrepareNodeAsync("JobId2", true); }
                catch (AzureBatchQuotaMaxedOutException) { }

                await pool.ServicePoolAsync();

                try { _ = await pool.PrepareNodeAsync("JobId2", true); }
                catch (AzureBatchQuotaMaxedOutException) { }
                try { info ??= await pool.PrepareNodeAsync("JobId1", true); }
                catch (AzureBatchQuotaMaxedOutException) { }
            }

            Assert.IsNotNull(info);
            Assert.IsTrue(setTargetsCalled);
            Assert.AreEqual(0, ((BatchPool)pool).TestPendingReservationsCount);
            Assert.AreEqual(2, ((BatchPool)pool).TestNodeReservationCount);
            Assert.AreEqual(AffinityPrefix + "ComputeNode1", info.AffinityId);

            IAsyncEnumerable<ComputeNode> ListComputeNodes(string id, ODATADetailLevel detail)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                Assert.IsNotNull(detail);
                Assert.IsNotNull(detail.SelectClause);
                var result = AsyncEnumerable.Empty<ComputeNode>();
                if (setTargetsCalled)
                {
                    result = result.Append(GenerateNode(pool.Pool.PoolId, "ComputeNode1", false, false));
                    result = result.Append(GenerateNode(pool.Pool.PoolId, "ComputeNode2", false, true));
                }
                return result;
            }
        }

        [TestMethod]
        public async Task PrepareNodeIncrementsDedicatedTargetWhenNoNodeIsIdle()
        {
            var azureProxy = AzureProxyReturnValues.Get();
            using var services = GetServiceProvider(azureProxy);
            var pool = await AddPool(services.GetT());
            var setTargetsCalled = false;
            azureProxy.AzureProxyGetAllocationState = id => Microsoft.Azure.Batch.Common.AllocationState.Steady;
            azureProxy.AzureProxySetComputeNodeTargets = (id, loPri, dedic) => setTargetsCalled = true;
            azureProxy.AzureProxyListComputeNodesAsync = ListComputeNodes;

            AffinityInformation info = default;
            try
            {
                _ = await pool.PrepareNodeAsync("JobId1", false);
            }
            catch (AzureBatchQuotaMaxedOutException)
            {
                await pool.ServicePoolAsync(IBatchPool.ServiceKind.Resize);
                info = await pool.PrepareNodeAsync("JobId1", false);
            }

            Assert.IsNotNull(info);
            Assert.IsTrue(setTargetsCalled);
            Assert.AreEqual(1, ((BatchPool)pool).TestNodeReservationCount);
            Assert.AreEqual(1, ((BatchPool)pool).TestTargetDedicated);
            Assert.AreEqual(0, ((BatchPool)pool).TestTargetLowPriority);

            IAsyncEnumerable<ComputeNode> ListComputeNodes(string id, ODATADetailLevel detail)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                Assert.IsNotNull(detail);
                Assert.IsNotNull(detail.SelectClause);
                var result = AsyncEnumerable.Empty<ComputeNode>();
                if (setTargetsCalled)
                {
                    result = result.Append(GenerateNode(pool.Pool.PoolId, "ComputeNode1", true, true));
                }
                return result;
            }
        }

        [TestMethod]
        public async Task PrepareNodeDoesNotDuplicateNodes()
        {
            var azureProxy = AzureProxyReturnValues.Get();
            using var services = GetServiceProvider(azureProxy);
            var pool = await AddPool(services.GetT());
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

            azureProxy.AzureProxyListComputeNodesAsync = ListComputeNodes;

            _ = await pool.PrepareNodeAsync("JobId1", true);
            _ = await pool.PrepareNodeAsync("JobId2", true);

            Assert.IsTrue(((BatchPool)pool).TestIsNodeReserved(AffinityPrefix + "NodeTwoLoPriIdle"));
            Assert.IsTrue(((BatchPool)pool).TestIsNodeReserved(AffinityPrefix + "NodeOneLoPriIdle"));
            Assert.AreEqual(2, ((BatchPool)pool).TestNodeReservationCount);

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
            var azureProxy = AzureProxyReturnValues.Get();
            using var services = GetServiceProvider(azureProxy);
            var pool = await AddPool(services.GetT());
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

            azureProxy.AzureProxyListComputeNodesAsync = ListComputeNodes;
            _ = await pool.PrepareNodeAsync("JobId1", true);
            _ = await pool.PrepareNodeAsync("JobId2", false);

            Assert.IsTrue(((BatchPool)pool).TestIsNodeReserved(AffinityPrefix + "NodeOneDedicatedIdle"));

            pool.ReleaseNode(new AffinityInformation(AffinityPrefix + "NodeOneDedicatedIdle"));

            Assert.IsFalse(((BatchPool)pool).TestIsNodeReserved(AffinityPrefix + "NodeOneDedicatedIdle"));
            Assert.IsTrue(((BatchPool)pool).TestIsNodeReserved(AffinityPrefix + "NodeOneLoPriIdle"));
            Assert.AreEqual(1, ((BatchPool)pool).TestNodeReservationCount);

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
            var azureProxy = AzureProxyReturnValues.Get();
            using var services = GetServiceProvider(azureProxy);
            var pool = await AddPool(services.GetT());
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

            azureProxy.AzureProxyListComputeNodesAsync = ListComputeNodes;
            _ = await pool.PrepareNodeAsync("JobId1", true);
            _ = await pool.PrepareNodeAsync("JobId2", false);

            Assert.AreEqual(2, ((BatchPool)pool).TestNodeReservationCount);

            pool.ReleaseNode(new AffinityInformation(AffinityPrefix + "NodeTwoLoPriRunning"));

            Assert.AreEqual(2, ((BatchPool)pool).TestNodeReservationCount);

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
        public async Task SyncSizeSetsBatchPoolState()
        {
            var azureProxy = AzureProxyReturnValues.Get();
            using var services = GetServiceProvider(azureProxy);
            var pool = await AddPool(services.GetT());

            azureProxy.AzureProxyReturnComputeNodeTargets = () => (3, 2);
            azureProxy.AzureProxyGetComputeNodeTargets = ValidateComputeNodeTargets;
            await pool.ServicePoolAsync(IBatchPool.ServiceKind.SyncSize);

            Assert.AreEqual(3, ((BatchPool)pool).TestTargetLowPriority);
            Assert.AreEqual(2, ((BatchPool)pool).TestTargetDedicated);

            void ValidateComputeNodeTargets(string id)
                => Assert.AreEqual(pool.Pool.PoolId, id);
        }

        // Test the actions performed by BatchPoolService

        [TestMethod]
        public async Task ResizeDoesNothingWhenNoReservationsAreAddedNorTargetsChanged()
        {
            var azureProxy = AzureProxyReturnValues.Get();
            using var services = GetServiceProvider(azureProxy);
            var pool = await AddPool(services.GetT());

            azureProxy.AzureProxySetComputeNodeTargets = SetTargets;
            azureProxy.AzureProxyGetAllocationState = GetState;
            await pool.ServicePoolAsync(IBatchPool.ServiceKind.Resize);

            Assert.AreEqual(0, ((BatchPool)pool).TestNodeReservationCount);
            Assert.AreEqual(0, ((BatchPool)pool).TestTargetLowPriority);
            Assert.AreEqual(0, ((BatchPool)pool).TestTargetDedicated);

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
            var azureProxy = AzureProxyReturnValues.Get();
            using var services = GetServiceProvider(azureProxy);
            var pool = await AddPool(services.GetT());

            azureProxy.AzureProxySetComputeNodeTargets = SetTargets;
            azureProxy.AzureProxyGetAllocationState = GetState;
            try { _ = await pool.PrepareNodeAsync("JobId1", false); } catch (AzureBatchQuotaMaxedOutException) { }
            await pool.ServicePoolAsync(IBatchPool.ServiceKind.Resize);

            Assert.AreEqual(0, ((BatchPool)pool).TestNodeReservationCount);
            Assert.AreEqual(0, ((BatchPool)pool).TestTargetLowPriority);
            Assert.AreEqual(1, ((BatchPool)pool).TestTargetDedicated);

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
            var azureProxy = AzureProxyReturnValues.Get();
            using var services = GetServiceProvider(azureProxy);
            var pool = await AddPool(services.GetT());

            azureProxy.AzureProxyListComputeNodesAsync = (i, d) => AsyncEnumerable.Empty<ComputeNode>();
            azureProxy.AzureProxySetComputeNodeTargets = SetTargets;
            azureProxy.AzureProxyGetAllocationState = GetState;
            try { _ = await pool.PrepareNodeAsync("JobId1", false); } catch (AzureBatchQuotaMaxedOutException) { }
            await pool.ServicePoolAsync(IBatchPool.ServiceKind.Resize);

            Assert.AreEqual(1, setTargetsCalled);
            Assert.AreEqual(1, ((BatchPool)pool).TestPendingReservationsCount);
            Assert.AreEqual(0, ((BatchPool)pool).TestTargetLowPriority);
            Assert.AreEqual(1, ((BatchPool)pool).TestTargetDedicated);

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
            var azureProxy = AzureProxyReturnValues.Get();
            azureProxy.AzureProxyReturnComputeNodeTargets = () => (4, 4);
            var services = GetServiceProvider(azureProxy);
            var pool = await AddPool(services.GetT());

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

            azureProxy.AzureProxyListComputeNodesAsync = ListComputeNodesSyncSize;
            azureProxy.AzureProxyGetComputeNodeTargets = ValidateComputeNodeTargets;
            azureProxy.AzureProxyGetAllocationState = GetState;

            _ = await pool.PrepareNodeAsync("JobId1", true);
            _ = await pool.PrepareNodeAsync("JobId2", true);
            _ = await pool.PrepareNodeAsync("JobId3", false);
            _ = await pool.PrepareNodeAsync("JobId4", false);
            await pool.ServicePoolAsync(IBatchPool.ServiceKind.SyncSize);
            azureProxy.AzureProxyListComputeNodesAsync = ListComputeNodesRemoveNode;
            azureProxy.AzureProxyDeleteBatchComputeNodes = DeleteComputeNodes;

            Assert.AreEqual(4, ((BatchPool)pool).TestNodeReservationCount);
            Assert.AreEqual(4, ((BatchPool)pool).TestTargetLowPriority);
            Assert.AreEqual(4, ((BatchPool)pool).TestTargetDedicated);
            pool.ReleaseNode(new AffinityInformation(AffinityPrefix + "NodeOneDedicatedIdle"));
            Assert.AreEqual(3, ((BatchPool)pool).TestNodeReservationCount);

            // Ensure nodes are not deleted prematurely
            expiryTime = DateTime.UtcNow - ((BatchPool)pool).TestIdleNodeTime;
            await pool.ServicePoolAsync(IBatchPool.ServiceKind.RemoveNodeIfIdle);
            Assert.AreEqual(3, ((BatchPool)pool).TestNodeReservationCount);
            Assert.AreEqual(4, ((BatchPool)pool).TestTargetLowPriority);
            Assert.AreEqual(4, ((BatchPool)pool).TestTargetDedicated);

            nodes = TimeShift(((BatchPool)pool).TestIdleNodeTime, pool, nodes).ToArray();
            expiryTime = DateTime.UtcNow - ((BatchPool)pool).TestIdleNodeTime;

            await pool.ServicePoolAsync(IBatchPool.ServiceKind.RemoveNodeIfIdle);

            Assert.AreEqual(0, ((BatchPool)pool).TestNodeReservationCount);
            Assert.AreEqual(2, ((BatchPool)pool).TestTargetLowPriority);
            Assert.AreEqual(2, ((BatchPool)pool).TestTargetDedicated);


            Microsoft.Azure.Batch.Common.AllocationState GetState(string id)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                return Microsoft.Azure.Batch.Common.AllocationState.Steady;
            }

            void ValidateComputeNodeTargets(string id)
                => Assert.AreEqual(pool.Pool.PoolId, id);

            IAsyncEnumerable<ComputeNode> ListComputeNodesSyncSize(string id, ODATADetailLevel detail)
                => ListComputeNodesAsync(nodes, id, detail, ValidateLCNSyncSize, c => c.State == ComputeNodeState.Idle);

            void ValidateLCNSyncSize(string id, ODATADetailLevel detail)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                Assert.IsNotNull(detail);
                Assert.AreEqual("id,state,affinityId,isDedicated", detail.SelectClause);
                Assert.AreEqual("state ne 'unusable' and state ne 'starttaskfailed' and state ne 'unknown' and state ne 'leavingpool' and state ne 'offline' and state ne 'preempted'", detail.FilterClause);
            }

            IAsyncEnumerable<ComputeNode> ListComputeNodesRemoveNode(string id, ODATADetailLevel detail)
                => ListComputeNodesAsync(nodes, id, detail, ValidateLCNRemoveNode, c => c.State == ComputeNodeState.Idle && c.StateTransitionTime < expiryTime);

            void ValidateLCNRemoveNode(string id, ODATADetailLevel detail)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                Assert.IsNotNull(detail);
                Assert.AreEqual("id,affinityId,isDedicated", detail.SelectClause);
                //Assert.AreEqual($"state eq 'idle' and stateTransitionTime lt DateTime'{expiryTime.ToString("yyyy-MM-dd'T'HH:mm:ssZ", System.Globalization.CultureInfo.InvariantCulture)}'", detail.FilterClause);
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
            var services = GetServiceProvider();
            var pool = await AddPool(services.GetT());
            ((BatchPool)pool).TestSetAvailable(false);

            await pool.ServicePoolAsync(IBatchPool.ServiceKind.Rotate);

            Assert.IsFalse(pool.IsAvailable);
        }

        [TestMethod]
        public async Task RotateMarksPoolUnavailableWhenIdleForIdlePeriod()
        {
            var services = GetServiceProvider();
            var pool = await AddPool(services.GetT());

            TimeShift(((BatchPool)pool).TestIdlePoolTime, pool);
            await pool.ServicePoolAsync(IBatchPool.ServiceKind.Rotate);

            Assert.IsFalse(pool.IsAvailable);
            Assert.AreEqual(0, ((BatchPool)pool).TestTargetDedicated + ((BatchPool)pool).TestTargetLowPriority);
        }

        [TestMethod]
        public async Task RotateMarksPoolUnavailableWhenRotateIntervalHasPassed()
        {
            var azureProxy = AzureProxyReturnValues.Get();
            azureProxy.AzureProxyGetAllocationState = id => Microsoft.Azure.Batch.Common.AllocationState.Steady;
            azureProxy.AzureProxySetComputeNodeTargets = (id, loPri, dedic) => { };
            azureProxy.AzureProxyListComputeNodesAsync = (i, d) => AsyncEnumerable.Empty<ComputeNode>();
            var services = GetServiceProvider(azureProxy);
            var pool = await AddPool(services.GetT());

            try
            {
                _ = await pool.PrepareNodeAsync("JobId1", false);
            }
            catch (AzureBatchQuotaMaxedOutException)
            {
                await pool.ServicePoolAsync(IBatchPool.ServiceKind.Resize);
            }

            TimeShift(((BatchPool)pool).TestRotatePoolTime, pool);
            await pool.ServicePoolAsync(IBatchPool.ServiceKind.Rotate);

            Assert.IsFalse(pool.IsAvailable);
            Assert.AreNotEqual(0, ((BatchPool)pool).TestTargetDedicated + ((BatchPool)pool).TestTargetLowPriority);
        }

        [TestMethod]
        public async Task RemovePoolIfEmptyDoesNotDeletePoolIfPoolIsAvailable()
        {
            var azureProxy = AzureProxyReturnValues.Get();
            azureProxy.AzureProxyDeleteBatchPool = (poolId, cancellationToken) => Assert.Fail();
            var services = GetServiceProvider(azureProxy);
            var pool = await AddPool(services.GetT());

            await pool.ServicePoolAsync(IBatchPool.ServiceKind.RemovePoolIfEmpty);
        }

        [TestMethod]
        public async Task RemovePoolIfEmptyDoesNotDeletePoolIfPoolHasComputeNodes()
        {
            IBatchPool pool = default;
            var azureProxy = AzureProxyReturnValues.Get();
            azureProxy.AzureProxyGetCurrentComputeNodes = () => (0, 1);
            azureProxy.AzureProxyDeleteBatchPool = (poolId, cancellationToken) => Assert.Fail();
            var services = GetServiceProvider(azureProxy);
            pool = await AddPool(services.GetT());
            ((BatchPool)pool).TestSetAvailable(false);

            await pool.ServicePoolAsync(IBatchPool.ServiceKind.RemovePoolIfEmpty);
        }

        [TestMethod]
        public async Task RemovePoolIfEmptyDeletesPoolIfPoolIsNotAvailableAndHasNoComputeNodes()
        {
            IBatchPool pool = default;
            var azureProxy = AzureProxyReturnValues.Get();
            azureProxy.AzureProxyDeleteBatchPool = DeletePool;
            var services = GetServiceProvider(azureProxy);
            pool = await AddPool(services.GetT());
            ((BatchPool)pool).TestSetAvailable(false);
            var isDeleted = false;

            await pool.ServicePoolAsync(IBatchPool.ServiceKind.RemovePoolIfEmpty);

            Assert.IsTrue(isDeleted);

            void DeletePool(string poolId, CancellationToken cancellationToken)
            {
                Assert.AreEqual(poolId, pool.Pool.PoolId);
                isDeleted = true;
            }
        }


        private static TestServices.TestServiceProvider<BatchScheduler> GetServiceProvider(AzureProxyReturnValues azureProxyReturn = default)
            => new(wrapAzureProxy: true, configuration: GetMockConfig(), azureProxy: PrepareMockAzureProxy(azureProxyReturn ?? AzureProxyReturnValues.Get()), batchPoolRepositoryArgs: ("endpoint", "key", "databaseId", "containerId", "partitionKeyValue"));

        private static async Task<IBatchPool> AddPool(BatchScheduler batchPools)
            => await batchPools.GetOrAddAsync("key1", id => ValueTask.FromResult(new Pool(name: id, displayName: "display1", vmSize: "vmSize1")));

        private static void TimeShift(TimeSpan shift, IBatchPool pool)
            => ((BatchPool)pool).TimeShift(shift);

        private static IEnumerable<ComputeNode> TimeShift(TimeSpan shift, IBatchPool pool, IEnumerable<ComputeNode> nodes)
        {
            TimeShift(shift, pool);

            foreach (var node in nodes)
            {
                yield return GenerateNode(pool.Pool.PoolId, node.Id, node.IsDedicated == true, node.State == ComputeNodeState.Idle, (node.StateTransitionTime ?? DateTime.UtcNow) - shift);
            }
        }

        private class AzureProxyReturnValues
        {
            internal static AzureProxyReturnValues Get()
                => new();

            internal AzureBatchAccountQuotas BatchQuotas { get; set; } = new() { PoolQuota = 1, DedicatedCoreQuotaPerVMFamily = new List<VirtualMachineFamilyCoreQuota>() };
            internal int ActivePoolCount { get; set; } = 0;

            internal Func<string, ODATADetailLevel, IAsyncEnumerable<ComputeNode>> AzureProxyListComputeNodesAsync { get; set; } = (poolId, detailLevel) => AsyncEnumerable.Empty<ComputeNode>();
            internal Action<string> AzureProxyGetComputeNodeTargets { get; set; } = poolId => { };
            internal Action<string, IEnumerable<ComputeNode>, CancellationToken> AzureProxyDeleteBatchComputeNodes { get; set; } = (poolId, computeNodes, cancellationToken) => { };
            internal Func<string, Microsoft.Azure.Batch.Common.AllocationState> AzureProxyGetAllocationState { get; set; } = /*(poolId, cancellationToken)*/ poolId => Microsoft.Azure.Batch.Common.AllocationState.Steady;
            internal Func<(int, int)> AzureProxyReturnComputeNodeTargets { get; set; } = () => /*(int TargetLowPriority, int TargetDedicated)*/ (0, 0);
            internal Action<string, int?, int?> AzureProxySetComputeNodeTargets { get; set; } = (poolId, targetLowPriorityComputeNodes, targetDedicatedComputeNodes) => { };
            internal Func<(int?, int?)> AzureProxyGetCurrentComputeNodes { get; set; } = () => /*(int? lowPriorityNodes, int? dedicatedNodes)*/ (0, 0);
            internal Action<string, CancellationToken> AzureProxyDeleteBatchPool { get; set; } = (poolId, cancellationToken) => { };
        }

        private static Action<Mock<IAzureProxy>> PrepareMockAzureProxy(AzureProxyReturnValues azureProxyReturnValues)
            => azureProxy =>
            {
                azureProxy.Setup(a => a.GetBatchAccountQuotasAsync()).Returns(Task.FromResult(azureProxyReturnValues.BatchQuotas));
                azureProxy.Setup(a => a.GetBatchActivePoolCount()).Returns(azureProxyReturnValues.ActivePoolCount);
                azureProxy.Setup(a => a.CreateBatchPoolAsync(It.IsAny<Pool>())).Returns((Pool p) => Task.FromResult(new PoolInformation { PoolId = p.Name }));
                azureProxy.Setup(a => a.ListComputeNodesAsync(It.IsAny<string>(), It.IsAny<DetailLevel>())).Returns<string, ODATADetailLevel>((poolId, detailLevel) => azureProxyReturnValues.AzureProxyListComputeNodesAsync(poolId, detailLevel));
                azureProxy.Setup(a => a.DeleteBatchComputeNodesAsync(It.IsAny<string>(), It.IsAny<IEnumerable<ComputeNode>>(), It.IsAny<CancellationToken>())).Callback<string, IEnumerable<ComputeNode>, CancellationToken>((poolId, computeNodes, cancellationToken) => azureProxyReturnValues.AzureProxyDeleteBatchComputeNodes(poolId, computeNodes, cancellationToken)).Returns(Task.CompletedTask);
                azureProxy.Setup(a => a.GetBatchPoolAsync(It.IsAny<string>(), It.IsAny<DetailLevel>(), It.IsAny<CancellationToken>())).Returns((string id, DetailLevel detailLevel, CancellationToken cancellationToken) => Task.FromResult(GeneratePool(id)));
                azureProxy.Setup(a => a.GetComputeNodeTargets(It.IsAny<string>())).Callback<string>(id => azureProxyReturnValues.AzureProxyGetComputeNodeTargets?.Invoke(id)).Returns(() => azureProxyReturnValues.AzureProxyReturnComputeNodeTargets());
                azureProxy.Setup(a => a.GetCurrentComputeNodesAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).Returns(() => Task.FromResult<(int? lowPriorityNodes, int? dedicatedNodes)>(azureProxyReturnValues.AzureProxyGetCurrentComputeNodes?.Invoke() ?? (null, null)));
                azureProxy.Setup(a => a.DeleteBatchPoolAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).Callback<string, CancellationToken>((poolId, cancellationToken) => azureProxyReturnValues.AzureProxyDeleteBatchPool?.Invoke(poolId, cancellationToken)).Returns(Task.CompletedTask);
                azureProxy.Setup(a => a.GetAllocationStateAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).Returns((string id, CancellationToken token) => Task.FromResult(azureProxyReturnValues.AzureProxyGetAllocationState?.Invoke(id)));
                azureProxy.Setup(a => a.SetComputeNodeTargetsAsync(It.IsAny<string>(), It.IsAny<int?>(), It.IsAny<int?>(), It.IsAny<CancellationToken>())).Callback<string, int?, int?, CancellationToken>((id, loPri, dedic, cancel) => azureProxyReturnValues.AzureProxySetComputeNodeTargets?.Invoke(id, loPri, dedic)).Returns(Task.CompletedTask);
            };


        private static IEnumerable<(string Key, string Value)> GetMockConfig()
            => Enumerable
                .Empty<(string Key, string Value)>()
                .Append(("BatchPoolIdleNodeMinutes", "0.3"))
                .Append(("BatchPoolIdlePoolMinutes", "0.6"))
                .Append(("BatchPoolRotationForcedDays", "0.000694444"));

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

        // Below this line we use reflection and internal details of the Azure libraries in order to generate Mocks of CloudPool and ComputeNode. A newer version of the library is supposed to enable this scenario, so hopefully we can soon drop this code.
        internal static CloudPool GeneratePool(string id, int currentDedicatedNodes = default, int currentLowPriorityNodes = default, int targetDedicatedNodes = default, int targetLowPriorityNodes = default, Microsoft.Azure.Batch.Common.AllocationState allocationState = Microsoft.Azure.Batch.Common.AllocationState.Steady, IList<Microsoft.Azure.Batch.MetadataItem> metadata = default, DateTime creationTime = default)
        {
            if (default == creationTime)
            {
                creationTime = DateTime.UtcNow;
            }

            metadata ??= new List<Microsoft.Azure.Batch.MetadataItem>();

            var computeNodeOperations = new Mock<Microsoft.Azure.Batch.Protocol.IComputeNodeOperations>();
            var batchServiceClient = new MockServiceClient(computeNodeOperations.Object);
            var protocolLayer = typeof(Microsoft.Azure.Batch.Protocol.BatchServiceClient).Assembly.GetType("Microsoft.Azure.Batch.ProtocolLayer").GetConstructor(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic, null, new Type[] { typeof(Microsoft.Azure.Batch.Protocol.BatchServiceClient) }, null)
                .Invoke(new object[] { batchServiceClient });
            var parentClient = (BatchClient)typeof(BatchClient).GetConstructor(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic, null, new Type[] { typeof(Microsoft.Azure.Batch.Protocol.BatchServiceClient).Assembly.GetType("Microsoft.Azure.Batch.IProtocolLayer") }, null)
                .Invoke(new object[] { protocolLayer });
            var modelPool = new Microsoft.Azure.Batch.Protocol.Models.CloudPool(id: id, currentDedicatedNodes: currentDedicatedNodes, currentLowPriorityNodes: currentLowPriorityNodes, targetDedicatedNodes: targetDedicatedNodes, targetLowPriorityNodes: targetLowPriorityNodes, allocationState: (Microsoft.Azure.Batch.Protocol.Models.AllocationState)allocationState, metadata: metadata.Select(ConvertMetadata).ToList(), creationTime: creationTime);
            var pool = (CloudPool)typeof(CloudPool).GetConstructor(System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance, default, new Type[] { typeof(BatchClient), typeof(Microsoft.Azure.Batch.Protocol.Models.CloudPool), typeof(IEnumerable<BatchClientBehavior>) }, default)
                .Invoke(new object[] { parentClient, modelPool, null });
            return pool;

            static Microsoft.Azure.Batch.Protocol.Models.MetadataItem ConvertMetadata(Microsoft.Azure.Batch.MetadataItem item)
                => item is null ? default : new(item.Name, item.Value);
        }

        internal static ComputeNode GenerateNode(string poolId, string id, bool isDedicated, bool isIdle, DateTime stateTransitionTime = default)
        {
            if (default == stateTransitionTime)
            {
                stateTransitionTime = DateTime.UtcNow;
            }

            var computeNodeOperations = new Mock<Microsoft.Azure.Batch.Protocol.IComputeNodeOperations>();
            var batchServiceClient = new MockServiceClient(computeNodeOperations.Object);
            var protocolLayer = typeof(Microsoft.Azure.Batch.Protocol.BatchServiceClient).Assembly.GetType("Microsoft.Azure.Batch.ProtocolLayer").GetConstructor(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic, null, new Type[] { typeof(Microsoft.Azure.Batch.Protocol.BatchServiceClient) }, null)
                .Invoke(new object[] { batchServiceClient });
            var parentClient = (BatchClient)typeof(BatchClient).GetConstructor(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic, null, new Type[] { typeof(Microsoft.Azure.Batch.Protocol.BatchServiceClient).Assembly.GetType("Microsoft.Azure.Batch.IProtocolLayer") }, null)
                .Invoke(new object[] { protocolLayer });
            var modelNode = new Microsoft.Azure.Batch.Protocol.Models.ComputeNode(stateTransitionTime: stateTransitionTime, id: id, affinityId: AffinityPrefix + id, isDedicated: isDedicated, state: isIdle ? Microsoft.Azure.Batch.Protocol.Models.ComputeNodeState.Idle : Microsoft.Azure.Batch.Protocol.Models.ComputeNodeState.Running);
            var node = (ComputeNode)typeof(ComputeNode).GetConstructor(System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance, default, new Type[] { typeof(BatchClient), typeof(string), typeof(Microsoft.Azure.Batch.Protocol.Models.ComputeNode), typeof(IEnumerable<BatchClientBehavior>) }, default)
                .Invoke(new object[] { parentClient, poolId, modelNode, null });
            return node;
        }
    }
}
