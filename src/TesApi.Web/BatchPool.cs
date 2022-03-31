// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Logging;

namespace TesApi.Web
{
    internal interface IBatchPoolImpl
    {
        int TestTargetDedicated { get; set; }
        int TestTargetLowPriority { get; set; }
        void TestSetAvailable(bool available);
        TimeSpan TestIdleNodeTime { get; }
        TimeSpan TestIdlePoolTime { get; }
        TimeSpan TestRotatePoolTime { get; }
        bool TestIsNodeReserved(string affinityId);
        int TestNodeReservationCount { get; }
        int TestPendingReservationsCount { get; }
    }

    /// <summary>
    /// Represents a pool in an Azure Batch Account.
    /// </summary>
    public sealed class BatchPool : IBatchPool, IBatchPoolImpl
    {
        /// <inheritdoc/>
        public bool IsAvailable { get; private set; } = true;

        /// <inheritdoc/>
        public PoolInformation Pool { get; }

        /// <inheritdoc/>
        public string VmSize { get; }

        /// <inheritdoc/>
        public async Task<AffinityInformation> PrepareNodeAsync(string jobId, bool isLowPriority, CancellationToken cancellationToken = default)
        {
            _ = jobId ?? throw new ArgumentNullException(nameof(jobId));
            AffinityInformation result = default;
            try
            {
                var cache = new ConcurrentDictionary<ComputeNodeState, List<ComputeNode>>();
                await foreach (var node in azureProxy
                    .ListComputeNodesAsync(
                        Pool.PoolId,
                        new ODATADetailLevel(filterClause: "state ne 'unusable' and state ne 'starttaskfailed' and state ne 'unknown' and state ne 'leavingpool' and state ne 'offline' and state ne 'preempted'", selectClause: "id,state,affinityId,isDedicated"))
                    .WithCancellation(cancellationToken))
                {
                    if (result is null && isLowPriority == !node.IsDedicated)
                    {
                        switch (node.State)
                        {
                            case ComputeNodeState.Idle:
                                result = TryAssignNode(node);
                                if (result is not null) { return result; }
                                break;

                            case ComputeNodeState.Rebooting:
                            case ComputeNodeState.Reimaging:
                            case ComputeNodeState.Running:
                            case ComputeNodeState.Creating:
                            case ComputeNodeState.Starting:
                            case ComputeNodeState.WaitingForStartTask:
                                cache.GetOrAdd(node.State.Value, s => new List<ComputeNode>()).Add(node);
                                break;

                            default:
                                throw new InvalidOperationException("Unexpected compute node state.");
                        }
                    }
                }

                if (result is null)
                {
                    var states = new[] { ComputeNodeState.WaitingForStartTask, ComputeNodeState.Starting, ComputeNodeState.Rebooting, ComputeNodeState.Creating, ComputeNodeState.Reimaging, ComputeNodeState.Running };
                    foreach (var state in states)
                    {
                        foreach (var node in cache.GetOrAdd(state, s => new List<ComputeNode>()))
                        {
                            // TODO: Consider adding some intelligence around stateTransitionTime
                            result = TryAssignNode(node);
                            if (result is not null) { return result; }
                        }
                    }
                }
            }
            catch (Exception /*ex*/) // Don't try to reserve an existing idle node if there're any errors. Just request a new one.
            {
                // log? TODO: determine
            }

            if (PendingReservations.TryAdd(jobId, new PendingReservation(isLowPriority)))
            {
                logger.LogInformation("Reservation requested for {JobId}.", jobId);
            }

            throw new AzureBatchQuotaMaxedOutException("Pool is being resized for this job.");

            AffinityInformation TryAssignNode(ComputeNode node)
            {
                lock (lockObj)
                {
                    var affinityId = new AffinityInformation(node.AffinityId);
                    if (ReservedComputeNodes.Contains(affinityId)) { return default; }

                    if (PendingReservations.TryRemove(jobId, out var reservation))
                    {
                        if (!reservation.IsRequested)
                        {
                            logger.LogWarning("Reservation granted to Job {JobId} (queued at {QueuedTime}) without corresponding pool resize request.", jobId, reservation.QueuedTime);
                        }
                    }
                    else
                    {
                        logger.LogWarning("Reservation granted to Job {JobId} without corresponding reservation.", jobId);
                    }

                    logger.LogInformation("Reserving ComputeNode {NodeId} ({AffinityId}) for {JobId}", node.Id, node.AffinityId, jobId);
                    ReservedComputeNodes.Add(affinityId);

                    return affinityId;
                }
            }
        }

        /// <inheritdoc/>
        public void ReleaseNode(AffinityInformation affinityInformation)
        {
            lock (lockObj)
            {
                if (ReservedComputeNodes.Remove(ReservedComputeNodes.FirstOrDefault(n => n.Equals(affinityInformation))))
                {
                    logger.LogDebug("Removing reservation for {AffinityId}", affinityInformation.AffinityId);
                }
            }
        }

        /// <inheritdoc/>
        public void ReleaseNode(string jobId)
        {
            if (PendingReservations.TryRemove(jobId, out var reservation))
            {
                if (reservation.IsRequested)
                {
                    lock (lockObj)
                    {
                        switch (reservation.IsLowPriority)
                        {
                            case false:
                                --TargetLowPriority;
                                break;
                            case true:
                                --TargetDedicated;
                                break;
                        }
                    }
                }
            }
        }

        /// <inheritdoc/>
        public async Task ScheduleReimage(ComputeNodeInformation nodeInformation, BatchTaskState taskState)
        {
            if (nodeInformation is not null)
            {
                var affinity = new AffinityInformation(nodeInformation.AffinityId);
                if (ReservedComputeNodes.Contains(affinity))
                {
                    switch (taskState)
                    {
                        case BatchTaskState.Initializing:
                            break;

                        case BatchTaskState.Running:
                        case BatchTaskState.CompletedSuccessfully:
                        case BatchTaskState.CompletedWithErrors:
                        case BatchTaskState.NodeFailedDuringStartupOrExecution:
                            if (ReservedComputeNodes.Contains(affinity))
                            {
                                if (await azureProxy.ReimageComputeNodeAsync(nodeInformation.PoolId, nodeInformation.ComputeNodeId, taskState switch
                                {
                                    BatchTaskState.Running => ComputeNodeReimageOption.TaskCompletion,
                                    _ => ComputeNodeReimageOption.Requeue,
                                }))
                                {
                                    ReleaseNode(affinity);
                                }
                            }
                            break;

                        case BatchTaskState.ActiveJobWithMissingAutoPool:
                        case BatchTaskState.JobNotFound:
                        case BatchTaskState.ErrorRetrievingJobs:
                        case BatchTaskState.MoreThanOneActiveJobFound:
                        case BatchTaskState.NodeAllocationFailed:
                        case BatchTaskState.NodePreempted:
                        case BatchTaskState.NodeUnusable:
                        case BatchTaskState.MissingBatchTask:
                            ReleaseNode(affinity);
                            break;
                    }
                }
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "Pattern")]
        private ValueTask ServicePoolSyncSizeAsync(CancellationToken cancellationToken = default)
        {
            lock (lockObj)
            {
                var (targetLowPriority, targetDedicated) = azureProxy.GetComputeNodeTargets(Pool.PoolId);
                TargetLowPriority = targetLowPriority;
                TargetDedicated = targetDedicated;
                ResizeDirty = PendingReservations.IsEmpty;
            }
            return ValueTask.CompletedTask;
        }

        private async ValueTask ServicePoolResizeAsync(CancellationToken cancellationToken = default)
        {
            foreach (var key in PendingReservations.Keys)
            {
                if (PendingReservations.TryGetValue(key, out var reservation))
                {
                    if (!reservation.IsRequested)
                    {
                        lock (lockObj)
                        {
                            reservation.IsRequested = true;
                            switch (reservation.IsLowPriority)
                            {
                                case false:
                                    ++TargetDedicated;
                                    break;
                                case true:
                                    ++TargetLowPriority;
                                    break;
                            }
                        }
                    }
                }
            }

            (bool dirty, int lowPri, int dedicated) values = default;
            lock (lockObj)
            {
                values = (ResizeDirty, TargetLowPriority, TargetDedicated);
            }

            DateTime cutoff;
            try
            {
                cutoff = PendingReservations.Select(p => p.Value.QueuedTime).Min();
            }
            catch (InvalidOperationException)
            {
                cutoff = Changed;
            }

            if (values.dirty && cutoff < DateTime.UtcNow - BatchPoolService.ResizeInterval && (await azureProxy.GetAllocationStateAsync(Pool.PoolId, cancellationToken)) == AllocationState.Steady)
            {
                logger.LogDebug("Resizing {PoolId}", Pool.PoolId);
                await azureProxy.SetComputeNodeTargetsAsync(Pool.PoolId, values.lowPri, values.dedicated, cancellationToken);
                lock (lockObj)
                {
                    ResizeDirty = TargetDedicated != values.dedicated || TargetLowPriority != values.lowPri;
                }
            }
        }

        private async ValueTask ServicePoolRemoveNodeIfIdleAsync(CancellationToken cancellationToken = default)
        {
            if ((await azureProxy.GetAllocationStateAsync(Pool.PoolId, cancellationToken)) == AllocationState.Steady)
            {
                var lowPriDecrementTarget = 0;
                var dedicatedDecrementTarget = 0;
                var nodesToRemove = Enumerable.Empty<ComputeNode>();
                var affinitiesToRemove = Enumerable.Empty<EquatableAffinityInformation>();
                var expiryTime = DateTime.UtcNow - _batchPools.IdleNodeCheck;
                await foreach (var node in azureProxy
                    .ListComputeNodesAsync(
                        Pool.PoolId,
                        new ODATADetailLevel(filterClause: $"state eq 'idle' and stateTransitionTime lt DateTime'{expiryTime.ToString("yyyy-MM-dd'T'HH:mm:ssZ", CultureInfo.InvariantCulture)}'", selectClause: "id,affinityId,isDedicated"))
                    .WithCancellation(cancellationToken))
                {
                    logger.LogDebug("Found idle node {NodeId}", node.Id);
                    nodesToRemove = nodesToRemove.Append(node);
                    switch (node.IsDedicated)
                    {
                        case true:
                            ++dedicatedDecrementTarget;
                            break;

                        case false:
                            ++lowPriDecrementTarget;
                            break;
                    }

                    var affinityId = new AffinityInformation(node.AffinityId);
                    if (ReservedComputeNodes.Contains(affinityId))
                    {
                        logger.LogTrace("Removing {ComputeNode} from reserved nodes list", node.Id);
                        affinitiesToRemove = affinitiesToRemove.Append(ReservedComputeNodes.FirstOrDefault(r => r.Equals(affinityId)));
                    }
                }

                // It's documented that a max of 100 nodes can be removed at a time. Group the nodes to remove in batches up to 100 in quantity.
                var removeNodesTasks = Enumerable.Empty<Task>();
                foreach (var nodes in nodesToRemove.Select((n, i) => (i, n)).GroupBy(t => t.i / 100).OrderBy(t => t.Key))
                {
                    removeNodesTasks = removeNodesTasks.Append(azureProxy.DeleteBatchComputeNodesAsync(Pool.PoolId, nodes.Select(t => t.n), cancellationToken));
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
                    foreach (var affinity in affinitiesToRemove)
                    {
                        _ = ReservedComputeNodes.Remove(affinity);
                    }
                }

                // Return when all the groups are done
                await tasks;
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "Pattern")]
        private ValueTask ServicePoolRotateAsync(CancellationToken cancellationToken = default)
        {
            if (IsAvailable)
            {
                IsAvailable = Creation + _batchPools.ForcePoolRotationAge >= DateTime.UtcNow &&
                    !(Changed + _batchPools.IdlePoolCheck < DateTime.UtcNow && TargetDedicated == 0 && TargetLowPriority == 0 && PendingReservations.IsEmpty);
            }
            return ValueTask.CompletedTask;
        }

        private async ValueTask ServicePoolRemovePoolIfEmptyAsync(CancellationToken cancellationToken = default)
        {
            if (!IsAvailable)
            {
                var (lowPriorityNodes, dedicatedNodes) = await azureProxy.GetCurrentComputeNodesAsync(Pool.PoolId, cancellationToken);
                if ((lowPriorityNodes is null || lowPriorityNodes == 0) && (dedicatedNodes is null || dedicatedNodes == 0) && PendingReservations.IsEmpty && ReservedComputeNodes.Count == 0)
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
                    await azureProxy.DeleteBatchPoolAsync(Pool.PoolId, cancellationToken);
                }
            }
        }

        /// <inheritdoc/>
        public async Task ServicePoolAsync(IBatchPool.ServiceKind serviceKind, CancellationToken cancellationToken = default)
        {
            switch (serviceKind)
            {
                case IBatchPool.ServiceKind.SyncSize:
                    await ServicePoolSyncSizeAsync(cancellationToken);
                    break;

                case IBatchPool.ServiceKind.Resize:
                    await ServicePoolResizeAsync(cancellationToken);
                    break;

                case IBatchPool.ServiceKind.RemoveNodeIfIdle:
                    await ServicePoolRemoveNodeIfIdleAsync(cancellationToken);
                    break;

                case IBatchPool.ServiceKind.Rotate:
                    await ServicePoolRotateAsync(cancellationToken);
                    break;

                case IBatchPool.ServiceKind.RemovePoolIfEmpty:
                    await ServicePoolRemovePoolIfEmptyAsync(cancellationToken);
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
        /// <param name="azureProxy"></param>
        /// <param name="logger"></param>
        /// <exception cref="ArgumentException"></exception>
        public BatchPool(PoolInformation poolInformation, string vmSize, IBatchPools batchPools, DateTime? creationTime, DateTime? changedTime, IAzureProxy azureProxy, ILogger<BatchPool> logger)
        {
            this.azureProxy = azureProxy;
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
        private readonly IAzureProxy azureProxy;
        private readonly object lockObj = new();

        private ConcurrentDictionary<string, PendingReservation> PendingReservations { get; } = new();
        private List<EquatableAffinityInformation> ReservedComputeNodes { get; } = new();

        private class PendingReservation
        {
            public PendingReservation(bool isLowPriority)
            {
                IsLowPriority = isLowPriority;
                QueuedTime = DateTime.UtcNow;
            }

            public bool IsLowPriority { get; }
            public DateTime QueuedTime { get; }

            public bool IsRequested { get; set; }
        }

        private class EquatableAffinityInformation : IEquatable<EquatableAffinityInformation>
        {
            private readonly string affinityId;
            public EquatableAffinityInformation(AffinityInformation affinityInformation) : this(affinityInformation?.AffinityId ?? throw new ArgumentNullException(nameof(affinityInformation))) => affinityId = affinityInformation.AffinityId;

            private EquatableAffinityInformation(string affinityId)
                => this.affinityId = affinityId;

            public override int GetHashCode()
                => affinityId.GetHashCode();

            public override bool Equals(object obj)
                => obj switch
                {
                    AffinityInformation affinityInformation => (affinityInformation as IEquatable<EquatableAffinityInformation>)?.Equals(this) ?? ((IEquatable<EquatableAffinityInformation>)this).Equals(affinityInformation),
                    string affinityId => affinityId.Equals(this.affinityId, StringComparison.OrdinalIgnoreCase),
                    _ => affinityId.Equals(obj),
                };

            bool IEquatable<EquatableAffinityInformation>.Equals(EquatableAffinityInformation other)
                => affinityId.Equals(other.affinityId, StringComparison.OrdinalIgnoreCase);

            public static implicit operator EquatableAffinityInformation(AffinityInformation affinityInformation)
                => new(affinityInformation?.AffinityId ?? throw new ArgumentException(null, nameof(affinityInformation)));
        }

        // For testing
        TimeSpan IBatchPoolImpl.TestIdleNodeTime
            => _batchPools.IdleNodeCheck;

        TimeSpan IBatchPoolImpl.TestIdlePoolTime
            => _batchPools.IdlePoolCheck;

        TimeSpan IBatchPoolImpl.TestRotatePoolTime
            => _batchPools.ForcePoolRotationAge;

        int IBatchPoolImpl.TestNodeReservationCount => ReservedComputeNodes.Count;
        int IBatchPoolImpl.TestPendingReservationsCount => PendingReservations.Count;


        int IBatchPoolImpl.TestTargetDedicated { get => TargetDedicated; set => TargetDedicated = value; }
        int IBatchPoolImpl.TestTargetLowPriority { get => TargetLowPriority; set => TargetLowPriority = value; }

        void IBatchPoolImpl.TestSetAvailable(bool available)
            => IsAvailable = available;

        bool IBatchPoolImpl.TestIsNodeReserved(string affinityId)
            => ReservedComputeNodes.Contains(new AffinityInformation(affinityId));
    }
}
