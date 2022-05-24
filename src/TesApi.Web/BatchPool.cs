// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
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
    /// <summary>
    /// Represents a pool in an Azure Batch Account.
    /// </summary>
    public sealed class BatchPool : IBatchPool
    {
        internal const string PoolDataName = "CoA-TES-PoolData";

        #region IBatchPool
        /// <inheritdoc/>
        public bool IsAvailable { get; private set; } = true;

        /// <inheritdoc/>
        public async Task<bool> CanBeDeleted(CancellationToken cancellationToken = default)
        {
            if (!HasNoReservations)
            {
                return false;
            }

            await foreach (var node in azureProxy.ListComputeNodesAsync(Pool.PoolId, new ODATADetailLevel(selectClause: "state")).WithCancellation(cancellationToken))
            {
                switch (node.State)
                {
                    case ComputeNodeState.Rebooting:
                    case ComputeNodeState.Reimaging:
                    case ComputeNodeState.Running:
                    case ComputeNodeState.Creating:
                    case ComputeNodeState.Starting:
                    case ComputeNodeState.WaitingForStartTask:
                        return false;
                }
            }

            return true;
        }

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
                ServicePoolUpdateAsync(default).AsTask().Wait();
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
                ServicePoolUpdateAsync(default).AsTask().Wait();
            }
        }

        /// <inheritdoc/>
        public async ValueTask ScheduleReimage(ComputeNodeInformation nodeInformation, BatchTaskState taskState, AffinityInformation affinityInformation)
        {
            if (nodeInformation is not null)
            {
                var affinity = new AffinityInformation(nodeInformation?.AffinityId);
                if (ReservedComputeNodes.Contains(affinity))
                {
                    switch (taskState)
                    {
                        case BatchTaskState.Initializing:
                            if (nodeInformation is not null && nodeInformation.AffinityId != affinityInformation.AffinityId)
                            {
                                if (!ReservedComputeNodes.Contains(affinity))
                                {
                                    ReservedComputeNodes.Add(affinity);
                                }
                                ReleaseNode(affinityInformation);
                            }
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
        private ValueTask ServicePoolSyncSizeAsync(CancellationToken _1)
        {
            var (targetLowPriority, targetDedicated) = azureProxy.GetComputeNodeTargets(Pool.PoolId);
            TargetLowPriority = targetLowPriority;
            TargetDedicated = targetDedicated;
            ResizeDirty = PendingReservations.Count == 0;
            return ValueTask.CompletedTask;
        }

        private async ValueTask ServicePoolResizeAsync(CancellationToken cancellationToken)
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

                var tasks = RemoveNodes(nodesToRemove, cancellationToken);

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

        private ValueTask ServicePoolRotateAsync(CancellationToken _1)
        {
            if (IsAvailable)
            {
                var now = DateTime.UtcNow;
                IsAvailable = Creation + _forcePoolRotationAge > now &&
                    (Changed + _idlePoolCheck > now || TargetDedicated != 0 || TargetLowPriority != 0 || PendingReservations.Count != 0);
            }
            return ValueTask.CompletedTask;
        }

        private async ValueTask ServicePoolRemovePoolIfEmptyAsync(CancellationToken cancellationToken)
        {
            if (!IsAvailable)
            {
                var (lowPriorityNodes, dedicatedNodes) = await azureProxy.GetCurrentComputeNodesAsync(Pool.PoolId, cancellationToken);
                if ((lowPriorityNodes is null || lowPriorityNodes == 0) && (dedicatedNodes is null || dedicatedNodes == 0) && HasNoReservations)
                {
                    _isRemoved = true;
                    await azureProxy.DeleteBatchPoolAsync(Pool.PoolId, cancellationToken);
                    _ = _batchPools.RemovePoolFromList(this);
                }
            }
        }

        private async ValueTask ServicePoolUpdateAsync(CancellationToken cancellationToken)
        {
            if (_isRemoved)
            {
                return;
            }

            var cloudPool = await azureProxy.GetBatchPoolAsync(Pool.PoolId, new ODATADetailLevel { SelectClause = "id,metadata" }, cancellationToken);
            cloudPool.Metadata = SetPoolData(this, _poolData, cloudPool.Metadata).ToList();
            await azureProxy.CommitBatchPoolChangesAsync(cloudPool, cancellationToken);
        }
        #endregion

        #region Constructors
        /// <summary>
        /// Constructor of <see cref="BatchPool"/>
        /// </summary>
        /// <param name="poolInformation"></param>
        /// <param name="batchScheduler"></param>
        /// <param name="configuration"></param>
        /// <param name="azureProxy"></param>
        /// <param name="logger"></param>
        /// <exception cref="ArgumentException"></exception>
        public BatchPool(PoolInformation poolInformation, IBatchScheduler batchScheduler, IConfiguration configuration, IAzureProxy azureProxy, ILogger<BatchPool> logger)
            : this(new()
                {
                    IsAvailable = true,
                    Reservations = Enumerable.Empty<string>(),
                    PendingReservations = Enumerable.Empty<IPendingReservation>(),
                    Changed = DateTime.UtcNow // Please keep this at the bottom of this initialization list
                },
                poolInformation,
                batchScheduler,
                configuration,
                azureProxy,
                logger)
        { }

        /// <summary>
        /// Constructor of <see cref="BatchPool"/>
        /// </summary>
        /// <param name="poolId"></param>
        /// <param name="poolData"></param>
        /// <param name="batchScheduler"></param>
        /// <param name="configuration"></param>
        /// <param name="azureProxy"></param>
        /// <param name="logger"></param>
        public BatchPool(string poolId, PoolData poolData, IBatchScheduler batchScheduler, IConfiguration configuration, IAzureProxy azureProxy, ILogger<BatchPool> logger)
            : this(poolData,
                new PoolInformation { PoolId = poolId },
                batchScheduler,
                configuration,
                azureProxy,
                logger)
        { }

        private BatchPool(PoolData data, PoolInformation poolInfo, IBatchScheduler batchScheduler, IConfiguration configuration, IAzureProxy azureProxy, ILogger<BatchPool> logger)
        {
            _poolData = data ?? throw new ArgumentNullException(nameof(data));
            Pool = poolInfo ?? throw new ArgumentNullException(nameof(poolInfo));

            _idleNodeCheck = TimeSpan.FromMinutes(GetConfigurationValue(configuration, "BatchPoolIdleNodeMinutes", 0.125));
            _idlePoolCheck = TimeSpan.FromMinutes(GetConfigurationValue(configuration, "BatchPoolIdlePoolMinutes", 0.125));
            _forcePoolRotationAge = TimeSpan.FromDays(GetConfigurationValue(configuration, "BatchPoolRotationForcedDays", 60));

            this.azureProxy = azureProxy;
            this.logger = logger;
            _batchPools = batchScheduler as BatchScheduler ?? throw new ArgumentException("batchScheduler must be of type BatchScheduler", nameof(batchScheduler));
            var cloudPool = azureProxy.GetBatchPoolAsync(Pool.PoolId, new ODATADetailLevel { SelectClause = "id,creationTime,metadata" }).Result;

            Creation = cloudPool.CreationTime.Value;
            Changed = _poolData.Changed;
            IsAvailable = _poolData.IsAvailable;
            TargetDedicated = FindTarget(_poolData.RequestedDedicatedNodes, cloudPool.TargetDedicatedComputeNodes);
            TargetLowPriority = FindTarget(_poolData.RequestedLowPriorityNodes, cloudPool.TargetLowPriorityComputeNodes);
            ReservedComputeNodes = _poolData.Reservations.Select<string, EquatableAffinityInformation>(r => new(new(r))).ToList();
            PendingReservations = _poolData.PendingReservations.ToDictionary(p => p.JobId);
            ResizeDirty = false;
            cloudPool.Metadata = SetPoolData(this, _poolData, cloudPool.Metadata).ToList();
            azureProxy.CommitBatchPoolChangesAsync(cloudPool).Wait();

            // IConfiguration.GetValue<double>(string key, double defaultValue) throws an exception if the value is defined as blank
            static double GetConfigurationValue(IConfiguration configuration, string key, double defaultValue)
            {
                var value = configuration.GetValue(key, string.Empty);
                return string.IsNullOrWhiteSpace(value) ? defaultValue : double.Parse(value);
            }

            static int FindTarget(int poolDataTarget, int? cloudPoolTarget)
                => default == poolDataTarget ? cloudPoolTarget.GetValueOrDefault(poolDataTarget) : poolDataTarget;
        }
        #endregion

        #region Implementation methods
        /// <summary>
        /// TODO
        /// </summary>
        /// <param name="pool"></param>
        /// <returns></returns>
        public static PoolData GetPoolData(CloudPool pool)
        {
            var metadata = pool.Metadata.FirstOrDefault(t => PoolDataName.Equals(t.Name, StringComparison.Ordinal));
            using var reader = metadata is null ? default : new StringReader(metadata.Value);
            return BatchUtils.ReadJson<PoolData>(reader, () => default);
        }

        internal static IEnumerable<MetadataItem> SetPoolData(BatchPool pool, PoolData data, IEnumerable<MetadataItem> metadataItems)
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

            var pendingReservations = pool.PendingReservations.Values;
            if (!data.PendingReservations.SequenceEqual(pendingReservations))
            {
                data.PendingReservations = pendingReservations;
            }

            return Enumerable.Empty<MetadataItem>()
                .Append(new(PoolDataName, BatchUtils.WriteJson(data)))
                .Concat(metadataItems?.Where(t => !PoolDataName.Equals(t.Name, StringComparison.Ordinal)) ?? Enumerable.Empty<MetadataItem>());
        }

        internal bool IsJobQueued(string jobId, out IPendingReservation queuedRecord)
            => PendingReservations.TryGetValue(jobId, out queuedRecord);

        internal async ValueTask<IEnumerable<(string AffinityId, bool IsPreemptible, TaskFailureInformation FailureInfo, IBatchPool Pool)>> ServicePoolRemoveNodeIfStartTaskFailedAsync(CancellationToken cancellationToken = default)
        {
            var affinitiesToRemove = Enumerable.Empty<(string, bool, TaskFailureInformation, IBatchPool)>();
            var nodesToRemove = Enumerable.Empty<ComputeNode>();

            if ((await azureProxy.GetAllocationStateAsync(Pool.PoolId, cancellationToken)) == AllocationState.Steady)
            {
                await foreach (var node in azureProxy
                .ListComputeNodesAsync(
                    Pool.PoolId,
                    new ODATADetailLevel(filterClause: $"state eq 'starttaskfailed'", selectClause: "id,affinityId,isDedicated,startTaskInfo"))
                .WithCancellation(cancellationToken))
                {
                    logger.LogDebug("Found starttaskfailed node {NodeId}", node.Id);
                    nodesToRemove = nodesToRemove.Append(node);
                    DecrementTarget(node.IsDedicated == false);
                    affinitiesToRemove = affinitiesToRemove.Append((node.AffinityId, node.IsDedicated == false, node.StartTaskInformation.FailureInformation, this));
                    _ = ReservedComputeNodes.Remove(new AffinityInformation(node.AffinityId));
                }
            }

            await RemoveNodes(nodesToRemove, cancellationToken);
            return affinitiesToRemove;
        }

        internal void DecrementTarget(bool? isLowPriority)
        {
            switch (isLowPriority)
            {
                case true:
                    --TargetLowPriority;
                    break;
                case false:
                    --TargetDedicated;
                    break;
            }
        }

        private Task RemoveNodes(IEnumerable<ComputeNode> nodesToRemove, CancellationToken cancellationToken = default)
        {
            // It's documented that a max of 100 nodes can be removed at a time. Group the nodes to remove in batches up to 100 in quantity.
            var removeNodesTasks = Enumerable.Empty<Task>();
            foreach (var nodes in nodesToRemove.Select((n, i) => (n, i)).GroupBy(t => t.i / 100).OrderBy(t => t.Key))
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

            return tasks;
        }
        #endregion

        #region Implementation state
        private readonly PoolData _poolData;

        private readonly ILogger logger;
        private DateTime Creation { get; set; }
        private DateTime Changed { get; set; }

        private bool _isRemoved;
        private bool HasNoReservations
            => PendingReservations.Count == 0 && ReservedComputeNodes.Count == 0;

        private readonly TimeSpan _idleNodeCheck;
        private readonly TimeSpan _idlePoolCheck;
        private readonly TimeSpan _forcePoolRotationAge;

        private bool ResizeDirty { get => _resizeDirty; set { _resizeDirty = value; if (value) { Changed = DateTime.UtcNow; } } }
        private volatile bool _resizeDirty = false;
        private int TargetLowPriority { get => _targetLowPriority; set { ResizeDirty |= value != _targetLowPriority; _targetLowPriority = value; } }
        private volatile int _targetLowPriority = 0;
        private int TargetDedicated { get => _targetDedicated; set { ResizeDirty |= value != _targetDedicated; _targetDedicated = value; } }
        private volatile int _targetDedicated = 0;

        private readonly BatchScheduler _batchPools;
        private readonly IAzureProxy azureProxy;

        private IDictionary<string, IPendingReservation> PendingReservations { get; }
        private List<EquatableAffinityInformation> ReservedComputeNodes { get; }
        #endregion

        #region Nested private classes
        /// <summary>
        /// TODO
        /// </summary>
        public class PoolData
        {
            [Newtonsoft.Json.JsonProperty("changed")]
            internal DateTime Changed { get; set; }

            [Newtonsoft.Json.JsonProperty("isAvailable")]
            internal bool IsAvailable { get; set; }

            [Newtonsoft.Json.JsonProperty("requestedDedicated")]
            internal int RequestedDedicatedNodes { get; set; }

            [Newtonsoft.Json.JsonProperty("requestedLowPriority")]
            internal int RequestedLowPriorityNodes { get; set; }

            [Newtonsoft.Json.JsonProperty("reservations")]
            internal IEnumerable<string> Reservations { get; set; } = Enumerable.Empty<string>();

            [Newtonsoft.Json.JsonConverter(typeof(PendingReservationConverter))]
            [Newtonsoft.Json.JsonProperty("pendingReservations")]
            internal IEnumerable<IPendingReservation> PendingReservations { get; set; } = Enumerable.Empty<IPendingReservation>();
        }

        private class PendingReservationConverter : Newtonsoft.Json.JsonConverter
        {
            public override bool CanConvert(Type objectType)
                => objectType switch
                {
                    var x when x == typeof(IEnumerable<IPendingReservation>) => true,
                    _ => false,
                };

            public override object ReadJson(Newtonsoft.Json.JsonReader reader, Type objectType, object existingValue, Newtonsoft.Json.JsonSerializer serializer)
                => serializer.Deserialize<IEnumerable<PendingReservation>>(reader);

            public override void WriteJson(Newtonsoft.Json.JsonWriter writer, object value, Newtonsoft.Json.JsonSerializer serializer)
                => serializer.Serialize(writer, value);
        }

        internal interface IPendingReservation
        {
            string JobId { get; }
            bool IsLowPriority { get; }
            DateTime QueuedTime { get; }
            bool IsRequested { get; set; }

            void TimeShift(TimeSpan shift);
        }

        [Newtonsoft.Json.JsonObject]
        private class PendingReservation : IPendingReservation
        {
            internal PendingReservation(string jobId, bool isLowPriority, DateTime queued, bool requested)
            {
                JobId = jobId ?? throw new ArgumentNullException(nameof(jobId));
                IsLowPriority = isLowPriority;
                QueuedTime = queued;
                IsRequested = requested;
            }

            public PendingReservation(string jobId, bool isLowPriority) : this(jobId, isLowPriority, DateTime.UtcNow, false) { }

            [DataMember(Name = "isLowPriority")]
            public bool IsLowPriority { get; }

            [DataMember(Name = "created")]
            public DateTime QueuedTime { get; private set; }

            [DataMember(Name = "isRequested")]
            public bool IsRequested { get; set; }

            [DataMember(Name = "jobId")]
            public string JobId { get; }

            // For testing
            void IPendingReservation.TimeShift(TimeSpan shift)
                => QueuedTime -= shift;
        }

        private class EquatableAffinityInformation : IEquatable<EquatableAffinityInformation>
        {
            private const StringComparison Comparison = StringComparison.Ordinal;

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

        #region Used for unit/module testing
        internal int TestNodeReservationCount => ReservedComputeNodes.Count;
        internal int TestPendingReservationsCount => PendingReservations.Count;


        internal int TestTargetDedicated { get => TargetDedicated; set => TargetDedicated = value; }
        internal int TestTargetLowPriority { get => TargetLowPriority; set => TargetLowPriority = value; }

        internal TimeSpan TestIdleNodeTime
            => _idleNodeCheck;

        internal TimeSpan TestIdlePoolTime
            => _idlePoolCheck;

        internal TimeSpan TestRotatePoolTime
            => _forcePoolRotationAge;

        internal void TestSetAvailable(bool available)
            => IsAvailable = available;

        internal bool TestIsNodeReserved(string affinityId)
            => ReservedComputeNodes.Contains(new AffinityInformation(affinityId));

        internal void TimeShift(TimeSpan shift)
        {
            Creation -= shift;
            Changed -= shift;

            foreach (var reservation in PendingReservations)
            {
                reservation.Value.TimeShift(shift);
            }

            ServicePoolUpdateAsync(default).AsTask().Wait();
        }
        #endregion
    }
}
