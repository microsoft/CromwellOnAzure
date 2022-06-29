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
        public async Task ResizeDoesNothingWhenNoReservationsAreAddedNorTargetsChanged()
        {
            var azureProxy = AzureProxyReturnValues.Get();
            using var services = GetServiceProvider(azureProxy);
            var pool = await AddPool(services.GetT(), false);

            azureProxy.AzureProxySetComputeNodeTargets = static (string id, int? loPri, int? dedic) => Assert.Fail();
            azureProxy.AzureProxyGetComputeNodeAllocationState = static id => (Microsoft.Azure.Batch.Common.AllocationState.Steady, 0, 0);

            await pool.ServicePoolAsync(IBatchPool.ServiceKind.Resize);

            Assert.AreEqual(0, ((BatchPool)pool).TestPendingReservationsCount);
            Assert.AreEqual(0, ((BatchPool)pool).TestTargetLowPriority);
            Assert.AreEqual(0, ((BatchPool)pool).TestTargetDedicated);
        }

        [TestMethod]
        public async Task ResizeDoesNothingWhenStateIsntSteady()
        {
            var azureProxy = AzureProxyReturnValues.Get();
            using var services = GetServiceProvider(azureProxy);
            var pool = await AddPool(services.GetT(), false);

            azureProxy.AzureProxySetComputeNodeTargets = static (string id, int? loPri, int? dedic) => Assert.Fail();
            azureProxy.AzureProxyGetComputeNodeAllocationState = GetState;

            await pool.ServicePoolAsync(IBatchPool.ServiceKind.Resize);

            Assert.AreEqual(0, ((BatchPool)pool).TestPendingReservationsCount);
            Assert.AreEqual(0, ((BatchPool)pool).TestTargetLowPriority);
            Assert.AreEqual(1, ((BatchPool)pool).TestTargetDedicated);

            (Microsoft.Azure.Batch.Common.AllocationState?, int?, int?) GetState(string id)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                return (Microsoft.Azure.Batch.Common.AllocationState.Resizing, 0, 1);
            }
        }

        [TestMethod]
        public async Task ResizeSetsBothTargetsWhenStateIsSteady()
        {
            var setTargetsCalled = 0;
            var azureProxy = AzureProxyReturnValues.Get();
            using var services = GetServiceProvider(azureProxy);
            var pool = await AddPool(services.GetT(), false);

            azureProxy.AzureProxyListComputeNodesAsync = (i, d) => AsyncEnumerable.Empty<ComputeNode>();
            azureProxy.AzureProxySetComputeNodeTargets = SetTargets;
            azureProxy.AzureProxyGetComputeNodeAllocationState = GetState;
            var jobTime = DateTime.UtcNow;
            azureProxy.AzureProxyListJobs = detail => AsyncEnumerable.Empty<CloudJob>().Append(GenerateJob("job1", stateTransitionTime: jobTime));

            await pool.ServicePoolAsync(IBatchPool.ServiceKind.Resize);
            Assert.AreEqual(0, setTargetsCalled);
            azureProxy.AzureProxyListJobs = detail => AsyncEnumerable.Empty<CloudJob>().Append(GenerateJob("job1", stateTransitionTime: jobTime - BatchPoolService.RunInterval));

            await pool.ServicePoolAsync(IBatchPool.ServiceKind.Resize);

            Assert.AreEqual(1, setTargetsCalled);

            (Microsoft.Azure.Batch.Common.AllocationState?, int?, int?) GetState(string id)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                return (Microsoft.Azure.Batch.Common.AllocationState.Steady, 0, 0);
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
            var services = GetServiceProvider(azureProxy);
            var pool = await AddPool(services.GetT(), false);

            var nodes = new ComputeNodeGroup()
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

            azureProxy.AzureProxyGetComputeNodeAllocationState = id =>
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                return (Microsoft.Azure.Batch.Common.AllocationState.Steady, nodes.Count(n => n.IsDedicated == false), nodes.Count(n => n.IsDedicated == true));
            };

            azureProxy.AzureProxyListJobs = detail => AsyncEnumerable.Empty<CloudJob>()
                .Append(Mock.Of<CloudJob>())
                .Append(Mock.Of<CloudJob>())
                .Append(Mock.Of<CloudJob>())
                .Append(Mock.Of<CloudJob>());

            azureProxy.AzureProxyGetComputeNodeTargets = ValidateComputeNodeTargets;
            azureProxy.AzureProxyListComputeNodesAsync = ListComputeNodesRemoveNode;
            azureProxy.AzureProxyDeleteBatchComputeNodes = DeleteComputeNodes;

            Assert.AreEqual(4, ((BatchPool)pool).TestTargetLowPriority);
            Assert.AreEqual(4, ((BatchPool)pool).TestTargetDedicated);
            Assert.AreEqual(4, ((BatchPool)pool).TestPendingReservationsCount);

            // Ensure nodes are not deleted prematurely
            expiryTime = DateTime.UtcNow - ((BatchPool)pool).TestIdleNodeTime;
            await pool.ServicePoolAsync(IBatchPool.ServiceKind.RemoveNodeIfIdle);

            Assert.AreEqual(4, ((BatchPool)pool).TestTargetLowPriority);
            Assert.AreEqual(4, ((BatchPool)pool).TestTargetDedicated);

            // Timeshift
            var tmpNodes = nodes.ToList();
            foreach (var node in tmpNodes)
            {
                Assert.IsTrue(nodes.Remove(node));
            }

            foreach (var node in TimeShift(((BatchPool)pool).TestIdleNodeTime, pool, tmpNodes))
            {
                Assert.IsTrue(nodes.Add(node));
            }

            // Ensure idle nodes are deleted
            expiryTime = DateTime.UtcNow - ((BatchPool)pool).TestIdleNodeTime;
            await pool.ServicePoolAsync(IBatchPool.ServiceKind.RemoveNodeIfIdle);

            Assert.AreEqual(2, ((BatchPool)pool).TestTargetLowPriority);
            Assert.AreEqual(2, ((BatchPool)pool).TestTargetDedicated);


            void ValidateComputeNodeTargets(string id)
                => Assert.AreEqual(pool.Pool.PoolId, id);

            IAsyncEnumerable<ComputeNode> ListComputeNodesRemoveNode(string id, ODATADetailLevel detail)
                => ListComputeNodesAsync(nodes.ToArray(), id, detail, ValidateLCNRemoveNode, c => c.State == ComputeNodeState.Idle && c.StateTransitionTime < expiryTime);

            void ValidateLCNRemoveNode(string id, ODATADetailLevel detail)
            {
                Assert.AreEqual(pool.Pool.PoolId, id);
                Assert.IsNotNull(detail);
                Assert.AreEqual("id,state,startTaskInfo", detail.SelectClause);
            }

            void DeleteComputeNodes(string poolId, IEnumerable<ComputeNode> computeNodes, CancellationToken cancellationToken)
            {
                var nodesToRemove = computeNodes.ToList();
                var nodeIdsToRemove = nodesToRemove.Select(n => n.Id).ToList();
                Assert.AreEqual(pool.Pool.PoolId, poolId);
                Assert.AreEqual(4, nodeIdsToRemove.Count);
                Assert.IsTrue(nodeIdsToRemove.Contains("NodeOneDedicatedIdle"));
                Assert.IsTrue(nodeIdsToRemove.Contains("NodeOneLoPriIdle"));
                Assert.IsTrue(nodeIdsToRemove.Contains("NodeTwoLoPriIdle"));
                Assert.IsTrue(nodeIdsToRemove.Contains("NodeTwoDedicatedIdle"));
                Assert.IsFalse(nodeIdsToRemove.Contains("NodeOneLoPriRunning"));
                Assert.IsFalse(nodeIdsToRemove.Contains("NodeOneDedicatedRunning"));
                Assert.IsFalse(nodeIdsToRemove.Contains("NodeTwoLoPriRunning"));
                Assert.IsFalse(nodeIdsToRemove.Contains("NodeTwoDedicatedRunning"));

                foreach (var node in nodesToRemove)
                {
                    Assert.IsTrue(nodes.Remove(node));
                }
            }
        }

        [TestMethod]
        public async Task RotateDoesNothingWhenPoolIsNotAvailable()
        {
            var services = GetServiceProvider();
            var pool = await AddPool(services.GetT(), false);
            ((BatchPool)pool).TestSetAvailable(false);

            await pool.ServicePoolAsync(IBatchPool.ServiceKind.Rotate);

            Assert.IsFalse(pool.IsAvailable);
        }

        [TestMethod]
        public async Task RotateMarksPoolUnavailableWhenIdleForIdlePeriod()
        {
            var services = GetServiceProvider();
            var pool = await AddPool(services.GetT(), false);

            TimeShift(((BatchPool)pool).TestIdlePoolTime, pool);
            await pool.ServicePoolAsync(IBatchPool.ServiceKind.Rotate);

            Assert.IsFalse(pool.IsAvailable);
            Assert.AreEqual(0, ((BatchPool)pool).TestTargetDedicated + ((BatchPool)pool).TestTargetLowPriority);
        }

        [TestMethod]
        public async Task RotateMarksPoolUnavailableWhenRotateIntervalHasPassed()
        {
            var azureProxy = AzureProxyReturnValues.Get();
            azureProxy.AzureProxyGetComputeNodeAllocationState = id => (Microsoft.Azure.Batch.Common.AllocationState.Steady, 0, 1);
            azureProxy.AzureProxyGetCurrentComputeNodes = () => (0, 1);
            azureProxy.AzureProxyListJobs = detailLevel => AsyncEnumerable.Empty<CloudJob>().Append(GenerateJob("job1"));
            azureProxy.AzureProxySetComputeNodeTargets = (id, loPri, dedic) => { };
            azureProxy.AzureProxyListComputeNodesAsync = (i, d) => AsyncEnumerable.Empty<ComputeNode>();
            var services = GetServiceProvider(azureProxy);
            var pool = await AddPool(services.GetT(), false);
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
            var pool = await AddPool(services.GetT(), false);

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
            pool = await AddPool(services.GetT(), false);
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
            pool = await AddPool(services.GetT(), false);
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

        private static async Task<IBatchPool> AddPool(BatchScheduler batchPools, bool isPreemtable)
            => await batchPools.GetOrAddAsync("key1", isPreemtable, id => new Pool(name: id, displayName: "display1", vmSize: "vmSize1"));

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
            internal Func<string, (Microsoft.Azure.Batch.Common.AllocationState? AllocationState, int? TargetLowPriority, int? TargetDedicated)> AzureProxyGetComputeNodeAllocationState { get; set; } = id => (Microsoft.Azure.Batch.Common.AllocationState.Steady, 0, 0);
            internal Action<string, int?, int?> AzureProxySetComputeNodeTargets { get; set; } = (poolId, targetLowPriorityComputeNodes, targetDedicatedComputeNodes) => { };
            internal Func<(int? lowPriorityNodes, int? dedicatedNodes)> AzureProxyGetCurrentComputeNodes { get; set; } = () => (0, 0);
            internal Action<string, CancellationToken> AzureProxyDeleteBatchPool { get; set; } = (poolId, cancellationToken) => { };
            internal Func<ODATADetailLevel, IAsyncEnumerable<CloudJob>> AzureProxyListJobs { get; set; } = detailLevel => AsyncEnumerable.Empty<CloudJob>();
        }

        private static Action<Mock<IAzureProxy>> PrepareMockAzureProxy(AzureProxyReturnValues azureProxyReturnValues)
            => azureProxy =>
            {
                azureProxy.Setup(a => a.GetBatchAccountQuotasAsync()).Returns(Task.FromResult(azureProxyReturnValues.BatchQuotas));
                azureProxy.Setup(a => a.GetBatchActivePoolCount()).Returns(azureProxyReturnValues.ActivePoolCount);
                azureProxy.Setup(a => a.CreateBatchPoolAsync(It.IsAny<Pool>(), It.IsAny<bool>())).Returns((Pool p, bool _1) => Task.FromResult(new PoolInformation { PoolId = p.Name }));
                azureProxy.Setup(a => a.ListComputeNodesAsync(It.IsAny<string>(), It.IsAny<DetailLevel>())).Returns<string, ODATADetailLevel>((poolId, detailLevel) => azureProxyReturnValues.AzureProxyListComputeNodesAsync(poolId, detailLevel));
                azureProxy.Setup(a => a.ListJobsAsync(It.IsAny<DetailLevel>())).Returns<ODATADetailLevel>(detailLevel => azureProxyReturnValues.AzureProxyListJobs(detailLevel));
                azureProxy.Setup(a => a.DeleteBatchComputeNodesAsync(It.IsAny<string>(), It.IsAny<IEnumerable<ComputeNode>>(), It.IsAny<CancellationToken>())).Callback<string, IEnumerable<ComputeNode>, CancellationToken>((poolId, computeNodes, cancellationToken) => azureProxyReturnValues.AzureProxyDeleteBatchComputeNodes(poolId, computeNodes, cancellationToken)).Returns(Task.CompletedTask);
                azureProxy.Setup(a => a.GetBatchPoolAsync(It.IsAny<string>(), It.IsAny<DetailLevel>(), It.IsAny<CancellationToken>())).Returns((string id, DetailLevel detailLevel, CancellationToken cancellationToken) => Task.FromResult(GeneratePool(id)));
                azureProxy.Setup(a => a.GetComputeNodeAllocationStateAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).Returns((string poolId, CancellationToken _1) => Task.FromResult(azureProxyReturnValues.AzureProxyGetComputeNodeAllocationState(poolId)));
                azureProxy.Setup(a => a.GetCurrentComputeNodesAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).Returns(() => Task.FromResult<(int? lowPriorityNodes, int? dedicatedNodes)>(azureProxyReturnValues.AzureProxyGetCurrentComputeNodes?.Invoke() ?? (null, null)));
                azureProxy.Setup(a => a.DeleteBatchPoolAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).Callback<string, CancellationToken>((poolId, cancellationToken) => azureProxyReturnValues.AzureProxyDeleteBatchPool?.Invoke(poolId, cancellationToken)).Returns(Task.CompletedTask);
                azureProxy.Setup(a => a.SetComputeNodeTargetsAsync(It.IsAny<string>(), It.IsAny<int?>(), It.IsAny<int?>(), It.IsAny<CancellationToken>())).Callback<string, int?, int?, CancellationToken>((id, loPri, dedic, cancel) => azureProxyReturnValues.AzureProxySetComputeNodeTargets?.Invoke(id, loPri, dedic)).Returns(Task.CompletedTask);
            };


        private static IEnumerable<(string Key, string Value)> GetMockConfig()
            => Enumerable
                .Empty<(string Key, string Value)>()
                .Append(("BatchPoolIdleNodeMinutes", "0.3"))
                .Append(("BatchPoolIdlePoolDays", "0.000416667"))
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

        private class ComputeNodeGroup : KeyedGroup<ComputeNode, GroupableSet<ComputeNode>>
        {
            public ComputeNodeGroup() : base(c => c.Id, StringComparer.Ordinal) { }

            protected override Func<IEnumerable<ComputeNode>, GroupableSet<ComputeNode>> CreateSetFunc
                => e => new(e, new ComputeNodeEqualityComparer());

            private class ComputeNodeEqualityComparer : IEqualityComparer<ComputeNode>
            {
                bool IEqualityComparer<ComputeNode>.Equals(ComputeNode x, ComputeNode y)
                    => x.Id.Equals(y.Id);

                int IEqualityComparer<ComputeNode>.GetHashCode(ComputeNode obj)
                    => obj.Id.GetHashCode();
            }
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

        internal static CloudJob GenerateJob(string id, DateTime stateTransitionTime = default)
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
            var modelJob = new Microsoft.Azure.Batch.Protocol.Models.CloudJob(id: id, stateTransitionTime: stateTransitionTime, state: Microsoft.Azure.Batch.Protocol.Models.JobState.Active);
            var job = (CloudJob)typeof(CloudJob).GetConstructor(System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance, default, new Type[] { typeof(BatchClient), typeof(Microsoft.Azure.Batch.Protocol.Models.CloudJob), typeof(IEnumerable<BatchClientBehavior>) }, default)
                .Invoke(new object[] { parentClient, modelJob, Enumerable.Empty<BatchClientBehavior>() });
            return job;
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
