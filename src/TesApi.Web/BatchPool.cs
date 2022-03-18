// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace TesApi.Web
{
    internal interface IBatchPoolImpl
    {
        int TestTargetDedicated { get; }
        int TestTargetLowPriority { get; }
        void TestSetAvailable(bool available);
        TimeSpan TestIdlePoolTime { get; }
        TimeSpan TestRotatePoolTime { get; }
    }

    /// <summary>
    /// Represents a pool in an Azure Batch Account.
    /// </summary>
    public sealed class BatchPool : IBatchPool, IBatchPoolImpl
    {
        /// <summary>
        /// Indicates that the pool is available for new jobs/tasks.
        /// </summary>
        public bool IsAvailable { get; private set; } = true;

        /// <summary>
        /// Provides the <see cref="PoolInformation"/> for the pool.
        /// </summary>
        public PoolInformation Pool { get; }

        /// <summary>
        /// Provides the VmSize of the compute nodes this pool can manage
        /// </summary>
        public string VmSize { get; }

        /// <summary>
        /// Either reserves an idle compute node in the pool, or requests an additional compute node.
        /// </summary>
        /// <param name="isLowPriority">True if the task is low priority, False if dedicated.</param>
        /// <param name="cancellationToken"></param>
        /// <returns>An <see cref="AffinityInformation"/> describing the reserved compute node, or null if a new node is requested.</returns>
        public async Task<AffinityInformation> PrepareNodeAsync(bool isLowPriority, CancellationToken cancellationToken = default)
        {
            Task rebootTask = default;
            AffinityInformation result = default;
            try
            {
                await _batchPools.azureProxy.ForEachComputeNodeAsync(Pool.PoolId, ConsiderNode, detailLevel: new ODATADetailLevel(filterClause: $"state eq 'idle'", selectClause: "id,affinityId,isDedicated"), cancellationToken: cancellationToken);

                void ConsiderNode(ComputeNode node)
                {
                    if (result is null && isLowPriority == !node.IsDedicated)
                    {
                        lock (lockObj)
                        {
                            if (ReservedComputeNodes.Any(n => n.AffinityId.Equals(node.AffinityId)))
                            {
                                return;
                            }

                            logger.LogDebug("Reserving ComputeNode {NodeId}", node.Id);
                            result = new AffinityInformation(node.AffinityId);
                            ReservedComputeNodes.Add(result);
                        }
                        rebootTask = node.RebootAsync(rebootOption: ComputeNodeRebootOption.TaskCompletion, cancellationToken: CancellationToken.None);
                    }
                }
            }
            catch (Exception /*ex*/) // Don't try to reserve an existing idle node if there's any errors. Just reserve a new one.
            {
                // log
            }

            if (result is null)
            {
                lock (lockObj)
                {
                    switch (isLowPriority)
                    {
                        case true: ++TargetLowPriority; break;
                        case false: ++TargetDedicated; break;
                    }
                }

                try
                {
                    if (Interlocked.Increment(ref _resizeGuard) == 1)
                    {
                        try
                        {
                            await ServicePoolAsync(IBatchPool.ServiceKind.Resize, cancellationToken);
                        }
                        catch (Exception ex)
                        {
                            logger.LogError(ex, "Failed to resize pool {PoolId}", Pool.PoolId);
                        }
                    }
                }
                finally
                {
                    Interlocked.Decrement(ref _resizeGuard);
                }
            }
            else if (rebootTask is not null)
            {
                await rebootTask;
            }

            return result;
        }

        /// <summary>
        /// Releases a reserved compute node.
        /// </summary>
        /// <param name="affinity">The <see cref="AffinityInformation"/> of the compute node to release.</param>
        public void ReleaseNode(AffinityInformation affinity)
        {
            lock (lockObj)
            {
                ReservedComputeNodes.Remove(affinity);
            }
        }

        /// <summary>
        /// Method used by the {BatchPoolService} to maintain this <see cref="IBatchPool"/> in the <see cref="IBatchPools"/> service.
        /// </summary>
        /// <param name="serviceKind">The type of <see cref="IBatchPool.ServiceKind"/> service call.</param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task ServicePoolAsync(IBatchPool.ServiceKind serviceKind, CancellationToken cancellationToken = default)
        {
            switch (serviceKind)
            {
                case IBatchPool.ServiceKind.SyncSize:
                    lock (lockObj)
                    {
                        var (targetLowPriority, targetDedicated) = _batchPools.azureProxy.GetComputeNodeTargets(Pool.PoolId);
                        TargetLowPriority = targetLowPriority;
                        TargetDedicated = targetDedicated;
                        ResizeDirty = false;
                    }
                    break;

                case IBatchPool.ServiceKind.Resize:
                    {
                        (bool dirty, int lowPri, int dedicated) values = default;
                        lock (lockObj)
                        {
                            values = (ResizeDirty, TargetLowPriority, TargetDedicated);
                        }

                        if (values.dirty && (await _batchPools.azureProxy.GetAllocationStateAsync(Pool.PoolId, cancellationToken)) == AllocationState.Steady)
                        {
                            logger.LogDebug("Resizing {PoolId}", Pool.PoolId);
                            await _batchPools.azureProxy.SetComputeNodeTargetsAsync(Pool.PoolId, values.lowPri, values.dedicated, cancellationToken);
                            lock (lockObj)
                            {
                                ResizeDirty = TargetDedicated != values.dedicated || TargetLowPriority != values.lowPri;
                            }
                        }
                    }
                    break;

                case IBatchPool.ServiceKind.RemoveNodeIfIdle:
                    if ((await _batchPools.azureProxy.GetAllocationStateAsync(Pool.PoolId, cancellationToken)) == AllocationState.Steady)
                    {
                        var lowPriDecrementTarget = 0;
                        var dedicatedDecrementTarget = 0;
                        var nodesToRemove = Enumerable.Empty<ComputeNode>();
                        var expiryTime = DateTime.UtcNow - _batchPools.IdleNodeCheck;
                        await _batchPools.azureProxy.ForEachComputeNodeAsync(Pool.PoolId, n =>
                        {
                            logger.LogDebug("Found idle node {NodeId}", n.Id);
                            if (!ReservedComputeNodes.Any(r => r.AffinityId.Equals(n.AffinityId)))
                            {
                                nodesToRemove = nodesToRemove.Append(n);
                                switch (n.IsDedicated)
                                {
                                    case true:
                                        ++dedicatedDecrementTarget;
                                        break;

                                    case false:
                                        ++lowPriDecrementTarget;
                                        break;
                                }
                            }
                        }, detailLevel: new ODATADetailLevel(filterClause: $"state eq 'idle' and stateTransitionTime lt DateTime'{expiryTime.ToString("yyyy-MM-dd'T'HH:mm:ssZ", CultureInfo.InvariantCulture)}'", selectClause: "id,affinityId,isDedicated"), cancellationToken: cancellationToken);

                        // It's documented that a max of 100 nodes can be removed at a time. Group the nodes to remove in batches up to 100 in quantity.
                        var removeNodesTasks = Enumerable.Empty<Task>();
                        foreach (var nodes in nodesToRemove.Select((n, i) => (i, n)).GroupBy(t => t.i / 100).OrderBy(t => t.Key))
                        {
                            removeNodesTasks = removeNodesTasks.Append(_batchPools.azureProxy.DeleteBatchComputeNodesAsync(Pool.PoolId, nodes.Select(t => t.n), cancellationToken));
                        }

                        // Call each group serially. Start the first group.
                        var tasks = Task.Run(async () =>
                        {
                            foreach (var task in removeNodesTasks)
                            {
                                logger.LogDebug("Removing nodes from {PoolId}", Pool.PoolId);
                                await task;
                            }
                        }, cancellationToken);

                        // Mark the new target values. Removing the nodes will reduce the targets in the Azure CloudPool for us, so when this is done, the "resize" will be a no-op, and we won't accidentally remove a node we are expecting to use for a different task.
                        lock (lockObj)
                        {
                            TargetDedicated -= dedicatedDecrementTarget;
                            TargetLowPriority -= lowPriDecrementTarget;
                        }

                        // Return when all the groups are done
                        await tasks;
                    }
                    break;

                case IBatchPool.ServiceKind.Rotate:
                    if (IsAvailable)
                    {
                        IsAvailable = Creation + _batchPools.ForcePoolRotationAge >= DateTime.UtcNow &&
                            !(Changed + _batchPools.IdlePoolCheck < DateTime.UtcNow && TargetDedicated == 0 && TargetLowPriority == 0);
                    }
                    break;

                case IBatchPool.ServiceKind.RemovePoolIfEmpty:
                    if (!IsAvailable)
                    {
                        var (lowPriorityNodes, dedicatedNodes) = await _batchPools.azureProxy.GetCurrentComputeNodesAsync(Pool.PoolId, cancellationToken);
                        if ((lowPriorityNodes is null || lowPriorityNodes == 0) && (dedicatedNodes is null || dedicatedNodes == 0))
                        {
                            foreach (var queue in _batchPools.ManagedBatchPools.Values)
                            {
                                if (queue.Contains(this))
                                {
                                    // Keep all other entries in the same order by rotating them through the queue. Doing it while holding lockObj keeps the order of the queue's contents consistent from other APIs where order matters.
                                    lock (lockObj)
                                    {
                                        var entries = Enumerable.Empty<IBatchPool>();
                                        while (queue.TryDequeue(out var pool))
                                        {
                                            if (!ReferenceEquals(this, pool))
                                            {
                                                entries = entries.Append(pool);
                                            }
                                        }

                                        foreach (var entry in entries)
                                        {
                                            queue.Enqueue(entry);
                                        }
                                    }
                                }
                            }
                            await _batchPools.azureProxy.DeleteBatchPoolAsync(Pool.PoolId, cancellationToken);
                        }
                    }
                    break;
            }
        }

        /// <summary>
        /// Constructor of <see cref="BatchPool"/>
        /// </summary>
        /// <param name="poolInformation"></param>
        /// <param name="vmSize"></param>
        /// <param name="batchPools"></param>
        /// <param name="creationTime"></param>
        /// <param name="changedTime"></param>
        /// <param name="logger"></param>
        /// <exception cref="ArgumentException"></exception>
        public BatchPool(PoolInformation poolInformation, string vmSize, IBatchPools batchPools, DateTime? creationTime, DateTime? changedTime, ILogger<BatchPool> logger)
        {
            this.logger = logger;
            _batchPools = batchPools as IBatchPoolsImpl ?? throw new ArgumentException("batchPools must be of type IBatchPoolsImpl", nameof(batchPools));
            VmSize = vmSize;
            Pool = poolInformation;
            var now = DateTime.UtcNow;
            Creation = creationTime ?? now;
            Changed = changedTime ?? now;
        }

        private readonly ILogger logger;
        private DateTime Creation { get; }
        private DateTime Changed { get; set; }

        private bool ResizeDirty { get => _resizeDirty; set { _resizeDirty = value; if (value) { Changed = DateTime.UtcNow; } } }
        private volatile bool _resizeDirty = false;
        private int TargetLowPriority { get => _targetLowPriority; set { ResizeDirty |= value != _targetLowPriority; _targetLowPriority = value; } }
        private volatile int _targetLowPriority = 0;
        private int TargetDedicated { get => _targetDedicated; set { ResizeDirty |= value != _targetDedicated; _targetDedicated = value; } }
        private volatile int _targetDedicated = 0;

        private readonly IBatchPoolsImpl _batchPools;
        private readonly object lockObj = new();
        private volatile int _resizeGuard = 0;

        private List<AffinityInformation> ReservedComputeNodes { get; } = new();

        // For testing
        int IBatchPoolImpl.TestTargetDedicated => TargetDedicated;
        int IBatchPoolImpl.TestTargetLowPriority => TargetLowPriority;

        TimeSpan IBatchPoolImpl.TestIdlePoolTime
            => _batchPools.IdlePoolCheck;

        TimeSpan IBatchPoolImpl.TestRotatePoolTime
            => _batchPools.ForcePoolRotationAge;

        void IBatchPoolImpl.TestSetAvailable(bool available)
            => IsAvailable = available;
    }
}
