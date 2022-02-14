// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;

namespace TesApi.Web
{
    /// <summary>
    /// Represents a pool in an Azure Batch Account.
    /// </summary>
    public class BatchPool : IBatchPool
    {
        private static readonly TimeSpan IdleNodeCheck = TimeSpan.FromMinutes(5); // TODO: set this to an appropriate value
        private static readonly TimeSpan IdlePoolCheck = TimeSpan.FromMinutes(30); // TODO: set this to an appropriate value
        private static readonly TimeSpan ForcePoolRotationAge = TimeSpan.FromDays(60); // TODO: set this to an appropriate value

        /// <summary>
        /// Indicates that the pool is available for new jobs/tasks.
        /// </summary>
        public bool IsAvailable { get; private set; } = true;

        /// <summary>
        /// Provides the <see cref="PoolInformation"/> for the pool.
        /// </summary>
        public PoolInformation Pool { get; }

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
                await _batchPools.azureProxy.ForEachComputeNodeAsync(Pool.PoolId, ConsiderNode, detailLevel: new ODATADetailLevel(selectClause: "id,affinityId,state"), cancellationToken: cancellationToken);
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
                        catch (Exception /*ex*/)
                        {
                            // log
                        }
                    }
                }
                finally
                {
                    Interlocked.Decrement(ref _resizeGuard);
                }
            }
            else
            {
                await rebootTask; // Can be null
                                  // TODO: Wait until idle?
            }

            return result;

            void ConsiderNode(ComputeNode node)
            {
                if (result is null && node.State == ComputeNodeState.Idle)
                {
                    lock (lockObj)
                    {
                        if (ReservedComuteNodes.Any(n => n.AffinityId.Equals(node.AffinityId)))
                        {
                            return;
                        }

                        result = new AffinityInformation(node.AffinityId);
                        ReservedComuteNodes.Add(result);
                    }
                    rebootTask = node.RebootAsync(cancellationToken: CancellationToken.None);
                }
            }
        }

        /// <summary>
        /// Releases a reserved compute node.
        /// </summary>
        /// <param name="affinity">The <see cref="AffinityInformation"/> of the compute node to release.</param>
        public void ReleaseNode(AffinityInformation affinity)
        {
            lock (lockObj)
            {
                ReservedComuteNodes.Remove(affinity);
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
                case IBatchPool.ServiceKind.Resize:
                    {
                        (bool dirty, int lowPri, int dedicated) values = default;
                        lock (lockObj)
                        {
                            values = (ResizeDirty, TargetLowPriority, TargetDedicated);
                        }

                        if (values.dirty && (await _batchPools.azureProxy.GetAllocationStateAsync(Pool.PoolId, cancellationToken)) == AllocationState.Steady)
                        {
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
                        await _batchPools.azureProxy.ForEachComputeNodeAsync(Pool.PoolId, n =>
                        {
                            if (n.StateTransitionTime + IdleNodeCheck < DateTime.UtcNow && n.State == ComputeNodeState.Idle /*&& ReservedComuteNodes.Any(r => r.AffinityId.Equals(n.AffinityId))*/)
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
                        }, detailLevel: new ODATADetailLevel(selectClause: "id,affinityId,isDedicated,state,stateTransitionTime"), cancellationToken: cancellationToken);

                        lock (lockObj)
                        {
                            foreach (var node in ReservedComuteNodes.Where(r => nodesToRemove.Any(n => n.AffinityId.Equals(r.AffinityId))).ToList())
                            {
                                _ = ReservedComuteNodes.Remove(node);
                            }

                            TargetDedicated -= dedicatedDecrementTarget;
                            TargetLowPriority -= lowPriDecrementTarget;
                        }

                        await Task.WhenAll(nodesToRemove.Select(n => n.RemoveFromPoolAsync(cancellationToken: cancellationToken)).ToArray());
                    }
                    break;

                case IBatchPool.ServiceKind.Rotate:
                    if (IsAvailable)
                    {
                        IsAvailable = Creation + ForcePoolRotationAge >= DateTime.UtcNow &&
                            !(Changed + IdlePoolCheck < DateTime.UtcNow && TargetDedicated == 0 && TargetLowPriority == 0);
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
                                    lock (lockObj)
                                    {
                                        while (queue.TryDequeue(out var pool))
                                        {
                                            if (!ReferenceEquals(this, pool))
                                            {
                                                queue.Enqueue(pool);
                                            }
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
        /// TODO
        /// </summary>
        /// <returns></returns>
        internal static BatchPool Create(PoolInformation poolInformation, IBatchPoolsImpl batchPools)
            => new(poolInformation, batchPools);

        private BatchPool(PoolInformation poolInformation, IBatchPoolsImpl batchPools)
        {
            _batchPools = batchPools;
            Pool = poolInformation;
            Creation = Changed = DateTime.UtcNow;
        }

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

        private List<AffinityInformation> ReservedComuteNodes { get; } = new();
    }
}
