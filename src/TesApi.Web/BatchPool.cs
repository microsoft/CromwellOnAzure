// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Tes.Repository;

namespace TesApi.Web
{
    // Used for unit/module testing
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
        void TimeShift(TimeSpan shift);
    }

    /// <summary>
    /// Represents a pool in an Azure Batch Account.
    /// </summary>
    public sealed class BatchPool : IBatchPool, IBatchPoolImpl
    {
        #region IBatchPool
        /// <summary>
        /// TODO
        /// </summary>
        public PoolData RepositoryItem { get; private set; }

        IHasChangesRepositoryItem<PoolData> IHasRepositoryItem<PoolData>.ChangesRepositoryItem
            => _hasChangesRepository;

        void IHasRepositoryItem<PoolData>.ReplaceRepositoryItem(PoolData replacementItem)
        {
            SetPoolData(this, replacementItem);
            RepositoryItem = replacementItem;
        }

        private static void SetPoolData(BatchPool pool, PoolData data)
        {
            if (data.Changed != pool.Changed)
            {
                data.Changed = pool.Changed;
            }

            if (data.IsAvailable != pool.IsAvailable)
            {
                data.IsAvailable = pool.IsAvailable;
            }

            if (data.RequestedDedicatedNodes != pool.TargetDedicated)
            {
                data.RequestedDedicatedNodes = pool.TargetDedicated;
            }

            if (data.RequestedLowPriorityNodes != pool.TargetLowPriority)
            {
                data.RequestedLowPriorityNodes = pool.TargetLowPriority;
            }

            var reservations = pool.ReservedComputeNodes.Select(a => a.Affinity.AffinityId).ToList();
            if (!data.Reservations.SequenceEqual(reservations))
            {
                data.Reservations = reservations;
            }

            var pendingReservations = pool.PendingReservations.Select(p => new PendingReservationItem { JobId = p.Key, Created = p.Value.QueuedTime, IsDedicated = !p.Value.IsLowPriority, IsRequested = p.Value.IsRequested }).ToList();
            if (!data.PendingReservations.SequenceEqual(pendingReservations))
            {
                data.PendingReservations = pendingReservations;
            }
        }

        /// <inheritdoc/>
        public bool IsAvailable { get; private set; } = true;

        /// <inheritdoc/>
        public bool HasNoReservations
            => PendingReservations.Count == 0 && ReservedComputeNodes.Count == 0;

        /// <inheritdoc/>
        public PoolInformation Pool { get; }

        /// <inheritdoc/>
        public async ValueTask<AffinityInformation> PrepareNodeAsync(string jobId, bool isLowPriority, CancellationToken cancellationToken = default)
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
                                if (result is not null)
                                {
                                    await ServicePoolUpdateAsync(cancellationToken);
                                    return result;
                                }
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
                            if (result is not null)
                            {
                                await ServicePoolUpdateAsync(cancellationToken);
                                return result;
                            }
                        }
                    }
                }
            }
            catch (Exception /*ex*/) // Don't try to reserve an existing idle node if there're any errors. Just request a new one.
            {
                // log? TODO: determine
            }

            if (PendingReservations.TryAdd(jobId, new PendingReservation(jobId, isLowPriority)))
            {
                logger.LogInformation("Reservation requested for {JobId}.", jobId);
                await ServicePoolUpdateAsync(cancellationToken);
            }

            throw new AzureBatchQuotaMaxedOutException("Pool is being resized for this task");

            AffinityInformation TryAssignNode(ComputeNode node)
            {
                var affinityId = new AffinityInformation(node.AffinityId);
                if (ReservedComputeNodes.Contains(affinityId)) { return default; }

                if (PendingReservations.Remove(jobId, out var reservation))
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

        /// <inheritdoc/>
        public void ReleaseNode(AffinityInformation affinityInformation)
        {
            if (ReservedComputeNodes.Remove(ReservedComputeNodes.FirstOrDefault(n => n.Equals(affinityInformation))))
            {
                logger.LogDebug("Removing reservation for {AffinityId}", affinityInformation.AffinityId);
                ServicePoolUpdateAsync().AsTask().Wait();
            }
        }

        /// <inheritdoc/>
        public void ReleaseNode(string jobId)
        {
            if (PendingReservations.Remove(jobId, out var reservation))
            {
                if (reservation.IsRequested)
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
                ServicePoolUpdateAsync().AsTask().Wait();
            }
        }

        /// <inheritdoc/>
        public async ValueTask ScheduleReimage(ComputeNodeInformation nodeInformation, BatchTaskState taskState)
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

        /// <inheritdoc/>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        public ValueTask ServicePoolAsync(IBatchPool.ServiceKind serviceKind, CancellationToken cancellationToken = default)
            => serviceKind switch
            {
                IBatchPool.ServiceKind.Update => ServicePoolUpdateAsync(cancellationToken),
                IBatchPool.ServiceKind.SyncSize => ServicePoolSyncSizeAsync(cancellationToken),
                IBatchPool.ServiceKind.Resize => ServicePoolResizeAsync(cancellationToken),
                IBatchPool.ServiceKind.RemoveNodeIfIdle => ServicePoolRemoveNodeIfIdleAsync(cancellationToken),
                IBatchPool.ServiceKind.Rotate => ServicePoolRotateAsync(cancellationToken),
                IBatchPool.ServiceKind.RemovePoolIfEmpty => ServicePoolRemovePoolIfEmptyAsync(cancellationToken),
                IBatchPool.ServiceKind.ForceRemove => ServicePoolForceRemoveAsync(cancellationToken),
                _ => throw new InvalidOperationException(),
            };

        /// <inheritdoc/>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        public async ValueTask ServicePoolAsync(CancellationToken cancellationToken = default)
        {
            await ServicePoolAsync(IBatchPool.ServiceKind.RemovePoolIfEmpty, cancellationToken);
            await ServicePoolAsync(IBatchPool.ServiceKind.Rotate, cancellationToken);
            await ServicePoolAsync(IBatchPool.ServiceKind.Resize, cancellationToken);
            await ServicePoolAsync(IBatchPool.ServiceKind.RemoveNodeIfIdle, cancellationToken);
            await ServicePoolAsync(IBatchPool.ServiceKind.Update, cancellationToken);
        }
        #endregion

        #region ServicePool~Async implementations
        private ValueTask ServicePoolSyncSizeAsync(CancellationToken _1 = default)
        {
            var (targetLowPriority, targetDedicated) = azureProxy.GetComputeNodeTargets(Pool.PoolId);
            TargetLowPriority = targetLowPriority;
            TargetDedicated = targetDedicated;
            ResizeDirty = PendingReservations.Count == 0;
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

            (bool dirty, int lowPri, int dedicated) values = default;
            values = (ResizeDirty, TargetLowPriority, TargetDedicated);

            DateTime cutoff;
            try
            {
                cutoff = PendingReservations.Select(p => p.Value.QueuedTime).Min();
            }
            catch (InvalidOperationException)
            {
                cutoff = Changed;
            }

            if (values.dirty /*&& cutoff < DateTime.UtcNow - TimeSpan.FromSeconds(15)*/ && (await azureProxy.GetAllocationStateAsync(Pool.PoolId, cancellationToken)) == AllocationState.Steady)
            {
                logger.LogDebug("Resizing {PoolId}", Pool.PoolId);
                await azureProxy.SetComputeNodeTargetsAsync(Pool.PoolId, values.lowPri, values.dedicated, cancellationToken);
                ResizeDirty = TargetDedicated != values.dedicated || TargetLowPriority != values.lowPri;
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
                var expiryTime = DateTime.UtcNow - _idleNodeCheck;
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
                TargetDedicated -= dedicatedDecrementTarget;
                TargetLowPriority -= lowPriDecrementTarget;
                foreach (var affinity in affinitiesToRemove)
                {
                    _ = ReservedComputeNodes.Remove(affinity);
                }

                // Return when all the groups are done
                await tasks;
            }
        }

        private ValueTask ServicePoolRotateAsync(CancellationToken _1 = default)
        {
            if (IsAvailable)
            {
                var now = DateTime.UtcNow;
                IsAvailable = Creation + _forcePoolRotationAge > now &&
                    (Changed + _idlePoolCheck > now || TargetDedicated != 0 || TargetLowPriority != 0 || PendingReservations.Count != 0);
            }
            return ValueTask.CompletedTask;
        }

        private async ValueTask ServicePoolRemovePoolIfEmptyAsync(CancellationToken cancellationToken = default)
        {
            if (!IsAvailable)
            {
                var (lowPriorityNodes, dedicatedNodes) = await azureProxy.GetCurrentComputeNodesAsync(Pool.PoolId, cancellationToken);
                if ((lowPriorityNodes is null || lowPriorityNodes == 0) && (dedicatedNodes is null || dedicatedNodes == 0) && HasNoReservations)
                {
                    _isRemoved = true;
                    await azureProxy.DeleteBatchPoolAsync(Pool.PoolId, cancellationToken);
                    _ = await _batchPools.RemovePoolFromListAsync(this);
                }
            }
        }

        private async ValueTask ServicePoolForceRemoveAsync(CancellationToken _1)
        {
            IsAvailable = false;
            _isRemoved = true;
            _ = await _batchPools.RemovePoolFromListAsync(this);
        }

        private ValueTask ServicePoolUpdateAsync(CancellationToken _1 = default)
        {
            if (_isRemoved)
            {
                return ValueTask.CompletedTask;
            }

            SetPoolData(this, RepositoryItem);
            return ValueTask.CompletedTask;
        }
        #endregion

        #region Constructors
        /// <summary>
        /// Constructor of <see cref="BatchPool"/>
        /// </summary>
        /// <param name="poolInformation"></param>
        /// <param name="batchPools"></param>
        /// <param name="configuration"></param>
        /// <param name="azureProxy"></param>
        /// <param name="logger"></param>
        /// <exception cref="ArgumentException"></exception>
        public BatchPool(PoolInformation poolInformation, IBatchScheduler batchPools, IConfiguration configuration, IAzureProxy azureProxy, ILogger<BatchPool> logger)
            : this(new()
                {
                    PoolId = poolInformation.PoolId,
                    Created = DateTime.UtcNow,
                    IsAvailable = true,
                    Reservations = new(),
                    PendingReservations = new(),
                    Changed = DateTime.UtcNow // Please keep this at the bottom of this initialization list
                },
                poolInformation,
                batchPools,
                configuration,
                azureProxy,
                logger)
        { }

        /// <summary>
        /// Constructor of <see cref="BatchPool"/>
        /// </summary>
        /// <param name="poolData"></param>
        /// <param name="batchPools"></param>
        /// <param name="configuration"></param>
        /// <param name="azureProxy"></param>
        /// <param name="logger"></param>
        public BatchPool(PoolData poolData, IBatchScheduler batchPools, IConfiguration configuration, IAzureProxy azureProxy, ILogger<BatchPool> logger)
            : this(poolData,
                new PoolInformation { PoolId = poolData.PoolId },
                batchPools,
                configuration,
                azureProxy,
                logger)
        { }

        private BatchPool(PoolData data, PoolInformation poolId, IBatchScheduler batchPools, IConfiguration configuration, IAzureProxy azureProxy, ILogger<BatchPool> logger)
        {
            RepositoryItem = data ?? throw new ArgumentNullException(nameof(data));
            Pool = poolId ?? throw new ArgumentNullException(nameof(poolId));

            _idleNodeCheck = TimeSpan.FromMinutes(GetConfigurationValue(configuration, "BatchPoolIdleNodeMinutes", 0.125));
            _idlePoolCheck = TimeSpan.FromMinutes(GetConfigurationValue(configuration, "BatchPoolIdlePoolMinutes", 0.125));
            _forcePoolRotationAge = TimeSpan.FromDays(GetConfigurationValue(configuration, "BatchPoolRotationForcedDays", 60));

            this.azureProxy = azureProxy;
            this.logger = logger;
            _batchPools = batchPools as IBatchPoolsImpl ?? throw new ArgumentException("batchPools must be of type IBatchPoolsImpl", nameof(batchPools));
            _hasChangesRepository = new(this);

            Changed = RepositoryItem.Changed;
            IsAvailable = RepositoryItem.IsAvailable;
            TargetDedicated = RepositoryItem.RequestedDedicatedNodes;
            TargetLowPriority = RepositoryItem.RequestedLowPriorityNodes;
            ReservedComputeNodes = RepositoryItem.Reservations.Select<string, EquatableAffinityInformation>(r => new(new(r))).ToList();
            PendingReservations = RepositoryItem.PendingReservations.ToDictionary(p => p.JobId, p => new PendingReservation(p));
            ResizeDirty = false;

            // IConfiguration.GetValue<double>(string key, double defaultValue) throws an exception if the value is defined as blank
            static double GetConfigurationValue(IConfiguration configuration, string key, double defaultValue)
            {
                var value = configuration.GetValue(key, string.Empty);
                return string.IsNullOrWhiteSpace(value) ? defaultValue : double.Parse(value);
            }
        }
        #endregion

        #region Implementation state
        private readonly HasChangesRepositoryItem<PoolData> _hasChangesRepository;

        private readonly ILogger logger;
        private DateTime Creation
            => RepositoryItem.Created;
        private DateTime Changed { get; set; }

        private bool _isRemoved;

        private readonly TimeSpan _idleNodeCheck;
        private readonly TimeSpan _idlePoolCheck;
        private readonly TimeSpan _forcePoolRotationAge;

        private bool ResizeDirty { get => _resizeDirty; set { _resizeDirty = value; if (value) { Changed = DateTime.UtcNow; } } }
        private volatile bool _resizeDirty = false;
        private int TargetLowPriority { get => _targetLowPriority; set { ResizeDirty |= value != _targetLowPriority; _targetLowPriority = value; } }
        private volatile int _targetLowPriority = 0;
        private int TargetDedicated { get => _targetDedicated; set { ResizeDirty |= value != _targetDedicated; _targetDedicated = value; } }
        private volatile int _targetDedicated = 0;

        private readonly IBatchPoolsImpl _batchPools;
        private readonly IAzureProxy azureProxy;

        private IDictionary<string, PendingReservation> PendingReservations { get; }
        private List<EquatableAffinityInformation> ReservedComputeNodes { get; }
        #endregion

        #region Nested private classes
        private interface IPendingReservation
        {
            string JobId { get; }
            bool IsLowPriority { get; }
            DateTime QueuedTime { get; }
            bool IsRequested { get; set; }
        }

        private class PendingReservation : IPendingReservation
        {
            private PendingReservation(string jobId, bool isLowPriority, DateTime queued, bool requested)
                => Source = new() { JobId = jobId ?? throw new ArgumentNullException(nameof(jobId)), IsDedicated = !isLowPriority, Created = queued, IsRequested = requested };

            public PendingReservation(string jobId, bool isLowPriority) : this(jobId, isLowPriority, DateTime.UtcNow, false) { }

            internal PendingReservation(PendingReservationItem item) => Source = item;

            internal PendingReservationItem Source { get; }
            public bool IsLowPriority => !Source.IsDedicated;
            public DateTime QueuedTime => Source.Created;
            public bool IsRequested { get => Source.IsRequested; set => Source.IsRequested = value; }
            string IPendingReservation.JobId => Source.JobId;

            // For testing
            internal void TimeShift(TimeSpan shift)
                => Source.Created -= shift;
        }

        private class EquatableAffinityInformation : IEquatable<EquatableAffinityInformation>
        {
            private const StringComparison Comparison = StringComparison.OrdinalIgnoreCase; // TODO: change/rationize

            public EquatableAffinityInformation(AffinityInformation affinityInformation)
                => Affinity = affinityInformation ?? throw new ArgumentNullException(nameof(affinityInformation));

            public override int GetHashCode()
                => Affinity.AffinityId.GetHashCode();

            public AffinityInformation Affinity { get; }

            public override bool Equals(object obj)
                => obj switch
                {
                    AffinityInformation affinityInformation => (affinityInformation as IEquatable<EquatableAffinityInformation>)?.Equals(this) ?? ((IEquatable<EquatableAffinityInformation>)this).Equals(affinityInformation),
                    string affinityId => affinityId.Equals(Affinity.AffinityId, Comparison),
                    _ => Affinity.AffinityId.Equals(obj),
                };

            bool IEquatable<EquatableAffinityInformation>.Equals(EquatableAffinityInformation other)
                => Affinity.AffinityId.Equals(other.Affinity.AffinityId, Comparison);

            public static implicit operator EquatableAffinityInformation(AffinityInformation affinityInformation)
                => new(affinityInformation ?? throw new ArgumentException(null, nameof(affinityInformation)));
        }
        #endregion

        #region IBatchPoolImpl
        int IBatchPoolImpl.TestNodeReservationCount => ReservedComputeNodes.Count;
        int IBatchPoolImpl.TestPendingReservationsCount => PendingReservations.Count;


        int IBatchPoolImpl.TestTargetDedicated { get => TargetDedicated; set => TargetDedicated = value; }
        int IBatchPoolImpl.TestTargetLowPriority { get => TargetLowPriority; set => TargetLowPriority = value; }

        TimeSpan IBatchPoolImpl.TestIdleNodeTime
            => _idleNodeCheck;

        TimeSpan IBatchPoolImpl.TestIdlePoolTime
            => _idlePoolCheck;

        TimeSpan IBatchPoolImpl.TestRotatePoolTime
            => _forcePoolRotationAge;

        //PoolData IHasRepositoryItem<PoolData>.RepositoryItem
        //    => RepositoryItem;

        void IBatchPoolImpl.TestSetAvailable(bool available)
            => IsAvailable = available;

        bool IBatchPoolImpl.TestIsNodeReserved(string affinityId)
            => ReservedComputeNodes.Contains(new AffinityInformation(affinityId));

        void IBatchPoolImpl.TimeShift(TimeSpan shift)
        {
            RepositoryItem.Created -= shift;
            Changed -= shift;

            foreach (var reservation in PendingReservations)
            {
                reservation.Value.TimeShift(shift);
            }

            ServicePoolUpdateAsync().AsTask().Wait();
        }
        #endregion

        #region Repository classes
        /// <summary>
        /// Relevant <see cref="CloudPool"/> metadata.
        /// </summary>
        public sealed class PoolData : RepositoryItem<PoolData>, IEquatable<PoolData>
        {
            /// <summary>
            /// Batch pool id.
            /// </summary>
            [DataMember(Name = "id")]
            public string PoolId { get; set; }

            /// <summary>
            /// Pool availability for scheduling (false means pool will be deleted once drained).
            /// </summary>
            [DataMember(Name = "is_available")]
            public bool IsAvailable { get; set; }

            /// <summary>
            /// Time of pool creation.
            /// </summary>
            [DataMember(Name = "created")]
            public DateTime Created { get; set; }

            /// <summary>
            /// Time of pool creation.
            /// </summary>
            [DataMember(Name = "changed")]
            public DateTime Changed { get; set; }

            /// <summary>
            /// Number of dedicated <see cref="ComputeNode"/> required.
            /// </summary>
            [DataMember(Name = "dedicated")]
            public int RequestedDedicatedNodes { get; set; }

            /// <summary>
            /// Number of low priority <see cref="ComputeNode"/> required.
            /// </summary>
            [DataMember(Name = "low_priority")]
            public int RequestedLowPriorityNodes { get; set; }

            /// <summary>
            /// List of <see cref="ComputeNode.AffinityId"/> of reserved nodes.
            /// </summary>
            [DataMember(Name = "reservations")]
            public List<string> Reservations { get; set; }

            /// <summary>
            /// List of <see cref="PendingReservationItem"/>.
            /// </summary>
            [DataMember(Name = "pending_reservations")]
            public List<PendingReservationItem> PendingReservations { get; set; }

            /// <inheritdoc/>
            public bool Equals(PoolData data)
                => data is not null
                && PoolId == data.PoolId
                && IsAvailable == data.IsAvailable
                && Created == data.Created
                && Changed == data.Changed
                && RequestedDedicatedNodes == data.RequestedDedicatedNodes
                && RequestedLowPriorityNodes == data.RequestedLowPriorityNodes
                && (Reservations?.SequenceEqual(data.Reservations) ?? data.Reservations is null)
                && (PendingReservations?.SequenceEqual(data.PendingReservations) ?? data.PendingReservations is null);

            /// <inheritdoc/>
            public override bool Equals(object obj)
                => obj switch
                {
                    null => false,
                    PoolData item => Equals(item),
                    _ => false,
                };

            /// <inheritdoc/>
            public override int GetHashCode()
                => GetHashCodeFromData(PoolId, IsAvailable, Created, Changed, RequestedDedicatedNodes, RequestedLowPriorityNodes, Reservations, PendingReservations);

            /// <summary>
            /// Generates hashcode for a <see cref="PoolData"/> with the relevant data.
            /// </summary>
            /// <param name="poolId"></param>
            /// <param name="isAvailable"></param>
            /// <param name="created"></param>
            /// <param name="changed"></param>
            /// <param name="requestedDedicatedNodes"></param>
            /// <param name="requestedLowPriorityNodes"></param>
            /// <param name="reservations"></param>
            /// <param name="pendingReservations"></param>
            /// <returns></returns>
            public static int GetHashCodeFromData(string poolId, bool isAvailable, DateTime created, DateTime changed, int requestedDedicatedNodes, int requestedLowPriorityNodes, List<string> reservations, List<PendingReservationItem> pendingReservations)
                => Tuple.Create(poolId, isAvailable, created, changed, requestedDedicatedNodes, requestedLowPriorityNodes, reservations, pendingReservations).GetHashCode();
        }

        /// <summary>
        /// Pending reservation for a needed <see cref="ComputeNode"/>.
        /// </summary>
        public class PendingReservationItem : /*RepositoryItem<PendingReservationItem>,*/ IEquatable<PendingReservationItem>
        {
            /// <summary>
            /// TES job id.
            /// </summary>
            [DataMember(Name = "id")]
            public string JobId { get; set; }

            /// <summary>
            /// Queued time.
            /// </summary>
            [DataMember(Name = "created")]
            public DateTime Created { get; set; }

            /// <summary>
            /// Dedicated <see cref="ComputeNode"/> requested.
            /// </summary>
            [DataMember(Name = "dedicated")]
            public bool IsDedicated { get; set; }

            /// <summary>
            /// <see cref="CloudPool"/>'s "Targeted~" resize values include this reservation.
            /// </summary>
            [DataMember(Name = "requested")]
            public bool IsRequested { get; set; }

            /// <inheritdoc/>
            public bool Equals(PendingReservationItem other)
                => other is not null
                && JobId == other.JobId
                && Created == other.Created
                && IsDedicated == other.IsDedicated
                && IsRequested == other.IsRequested;

            /// <inheritdoc/>
            public override bool Equals(object obj)
                => obj switch
                {
                    null => false,
                    PendingReservationItem item => Equals(item),
                    _ => false,
                };

            /// <inheritdoc/>
            public override int GetHashCode()
                => GetHashCodeFromData(JobId, Created, IsDedicated, IsRequested);

            /// <summary>
            /// Generates hashcode for a <see cref="PendingReservationItem"/> with the relevant data.
            /// </summary>
            /// <param name="jobId"></param>
            /// <param name="created"></param>
            /// <param name="isDedicated"></param>
            /// <param name="isRequested"></param>
            /// <returns></returns>
            public static int GetHashCodeFromData(string jobId, DateTime created, bool isDedicated, bool isRequested)
                => Tuple.Create(jobId, created, isDedicated, isRequested).GetHashCode();
        }
        #endregion
    }
}
