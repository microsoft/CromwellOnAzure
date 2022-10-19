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

        //[TestCategory("Batch Pools")]


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
        public async Task RotateMarksPoolUnavailableWhenRotateIntervalHasPassed()
        {
            var azureProxy = AzureProxyReturnValues.Get();
            azureProxy.AzureProxyGetComputeNodeAllocationState = id => (Microsoft.Azure.Batch.Common.AllocationState.Steady, true, 0, 1);
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
            => await batchPools.GetOrAddPoolAsync("key1", isPreemtable, id => ValueTask.FromResult(new Pool(name: id, displayName: "display1", vmSize: "vmSize1")));

        private static void TimeShift(TimeSpan shift, IBatchPool pool)
            => ((BatchPool)pool).TimeShift(shift);

        private class AzureProxyReturnValues
        {
            internal static AzureProxyReturnValues Get()
                => new();

            internal AzureBatchAccountQuotas BatchQuotas { get; set; } = new() { PoolQuota = 1, DedicatedCoreQuotaPerVMFamily = new List<VirtualMachineFamilyCoreQuota>() };
            internal int ActivePoolCount { get; set; } = 0;

            internal Func<string, ODATADetailLevel, IAsyncEnumerable<ComputeNode>> AzureProxyListComputeNodesAsync { get; set; } = (poolId, detailLevel) => AsyncEnumerable.Empty<ComputeNode>();
            internal Action<string> AzureProxyGetComputeNodeTargets { get; set; } = poolId => { };
            internal Dictionary<string, StorageAccountInfo> StorageAccountInfos { get; set; }
                = new Dictionary<string, StorageAccountInfo> { { "defaultstorageaccount", new StorageAccountInfo { Name = "defaultstorageaccount", Id = "Id", BlobEndpoint = "https://defaultstorageaccount.blob.core.windows.net/", SubscriptionId = "SubId" } } };
            internal string StorageAccountKey { get; set; } = "Key1";
            internal string DownloadedBlobContent { get; set; } = string.Empty;
            internal Action<string, IEnumerable<ComputeNode>, CancellationToken> AzureProxyDeleteBatchComputeNodes { get; set; } = (poolId, computeNodes, cancellationToken) => { };
            internal Func<string, (Microsoft.Azure.Batch.Common.AllocationState? AllocationState, bool? AutoScaleEnabled, int? TargetLowPriority, int? TargetDedicated)> AzureProxyGetComputeNodeAllocationState { get; set; } = id => (Microsoft.Azure.Batch.Common.AllocationState.Steady, true, 0, 0);
            internal Action<string, int?, int?> AzureProxySetComputeNodeTargets { get; set; } = (poolId, targetLowPriorityComputeNodes, targetDedicatedComputeNodes) => { };
            internal Func<(int? lowPriorityNodes, int? dedicatedNodes)> AzureProxyGetCurrentComputeNodes { get; set; } = () => (0, 0);
            internal Action<string, CancellationToken> AzureProxyDeleteBatchPool { get; set; } = (poolId, cancellationToken) => { };
            internal Func<ODATADetailLevel, IAsyncEnumerable<CloudJob>> AzureProxyListJobs { get; set; } = detailLevel => AsyncEnumerable.Empty<CloudJob>();

            private readonly Dictionary<string, IList<Microsoft.Azure.Batch.MetadataItem>> poolMetadata = new();

            internal void AzureProxyDeleteBatchPoolImpl(string poolId, CancellationToken cancellationToken)
            {
                _ = poolMetadata.Remove(poolId);
                AzureProxyDeleteBatchPool(poolId, cancellationToken);
            }

            internal PoolInformation CreateBatchPoolImpl(Pool pool)
            {
                var poolId = pool.Name;

                poolMetadata.Add(poolId, pool.Metadata?.Select(Convert).ToList());
                return new() { PoolId = poolId };

                static Microsoft.Azure.Batch.MetadataItem Convert(Microsoft.Azure.Management.Batch.Models.MetadataItem item)
                    => new(item.Name, item.Value);
            }

            internal CloudPool GetBatchPoolImpl(string poolId)
            {
                if (!poolMetadata.TryGetValue(poolId, out var items))
                {
                    items = null;
                }

                return GeneratePool(poolId, metadata: items);
            }
        }

        private static Action<Mock<IAzureProxy>> PrepareMockAzureProxy(AzureProxyReturnValues azureProxyReturnValues)
            => azureProxy =>
            {
                azureProxy.Setup(a => a.GetActivePoolsAsync(It.IsAny<string>())).Returns(AsyncEnumerable.Empty<CloudPool>());
                azureProxy.Setup(a => a.GetBatchAccountQuotasAsync()).Returns(Task.FromResult(azureProxyReturnValues.BatchQuotas));
                azureProxy.Setup(a => a.GetBatchActivePoolCount()).Returns(azureProxyReturnValues.ActivePoolCount);
                azureProxy.Setup(a => a.GetStorageAccountInfoAsync("defaultstorageaccount")).Returns(Task.FromResult(azureProxyReturnValues.StorageAccountInfos["defaultstorageaccount"]));
                azureProxy.Setup(a => a.GetStorageAccountKeyAsync(It.IsAny<StorageAccountInfo>())).Returns(Task.FromResult(azureProxyReturnValues.StorageAccountKey));
                azureProxy.Setup(a => a.DownloadBlobAsync(It.IsAny<Uri>())).Returns(Task.FromResult(azureProxyReturnValues.DownloadedBlobContent));
                azureProxy.Setup(a => a.CreateBatchPoolAsync(It.IsAny<Pool>(), It.IsAny<bool>())).Returns((Pool p, bool _1) => Task.FromResult(azureProxyReturnValues.CreateBatchPoolImpl(p)));
                azureProxy.Setup(a => a.ListComputeNodesAsync(It.IsAny<string>(), It.IsAny<DetailLevel>())).Returns<string, ODATADetailLevel>((poolId, detailLevel) => azureProxyReturnValues.AzureProxyListComputeNodesAsync(poolId, detailLevel));
                azureProxy.Setup(a => a.ListJobsAsync(It.IsAny<DetailLevel>())).Returns<ODATADetailLevel>(detailLevel => azureProxyReturnValues.AzureProxyListJobs(detailLevel));
                azureProxy.Setup(a => a.DeleteBatchComputeNodesAsync(It.IsAny<string>(), It.IsAny<IEnumerable<ComputeNode>>(), It.IsAny<CancellationToken>())).Callback<string, IEnumerable<ComputeNode>, CancellationToken>((poolId, computeNodes, cancellationToken) => azureProxyReturnValues.AzureProxyDeleteBatchComputeNodes(poolId, computeNodes, cancellationToken)).Returns(Task.CompletedTask);
                azureProxy.Setup(a => a.GetBatchPoolAsync(It.IsAny<string>(), It.IsAny<DetailLevel>(), It.IsAny<CancellationToken>())).Returns((string id, DetailLevel detailLevel, CancellationToken cancellationToken) => Task.FromResult(azureProxyReturnValues.GetBatchPoolImpl(id)));
                azureProxy.Setup(a => a.GetComputeNodeAllocationStateAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).Returns((string poolId, CancellationToken _1) => Task.FromResult(azureProxyReturnValues.AzureProxyGetComputeNodeAllocationState(poolId)));
                azureProxy.Setup(a => a.GetCurrentComputeNodesAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).Returns(() => Task.FromResult<(int? lowPriorityNodes, int? dedicatedNodes)>(azureProxyReturnValues.AzureProxyGetCurrentComputeNodes?.Invoke() ?? (null, null)));
                azureProxy.Setup(a => a.DeleteBatchPoolAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).Callback<string, CancellationToken>((poolId, cancellationToken) => azureProxyReturnValues.AzureProxyDeleteBatchPoolImpl(poolId, cancellationToken)).Returns(Task.CompletedTask);
                azureProxy.Setup(a => a.SetComputeNodeTargetsAsync(It.IsAny<string>(), It.IsAny<int?>(), It.IsAny<int?>(), It.IsAny<CancellationToken>())).Callback<string, int?, int?, CancellationToken>((id, loPri, dedic, cancel) => azureProxyReturnValues.AzureProxySetComputeNodeTargets?.Invoke(id, loPri, dedic)).Returns(Task.CompletedTask);
            };

        private static IEnumerable<(string Key, string Value)> GetMockConfig()
            => Enumerable
                .Empty<(string Key, string Value)>()
                .Append(("DefaultStorageAccountName", "defaultstorageaccount"))
                .Append(("BatchPoolIdlePoolDays", "0.000416667"))
                .Append(("BatchPoolRotationForcedDays", "0.000694444"));

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
