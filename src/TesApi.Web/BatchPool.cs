// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace TesApi.Web
{
    /// <summary>
    /// Represents a pool in an Azure Batch Account.
    /// </summary>
    public sealed class BatchPool : IBatchPool
    {
        /// <summary>
        /// Minimum property set required for <see cref="CloudPool"/> provided to constructors of this class
        /// </summary>
        public const string CloudPoolSelectClause = "id,creationTime,metadata";

        internal const string PoolDataName = "CoA-TES-PoolData";

        #region IBatchPool
        /// <inheritdoc/>
        public bool IsAvailable { get; private set; } = true;

        /// <inheritdoc/>
        public bool IsPreemptable { get; }

        /// <inheritdoc/>
        public async ValueTask<bool> CanBeDeleted(CancellationToken cancellationToken = default)
        {
            if (await GetJobsAsync().AnyAsync(cancellationToken))
            {
                return false;
            }

            await foreach (var node in _azureProxy.ListComputeNodesAsync(Pool.PoolId, new ODATADetailLevel(selectClause: "state")).WithCancellation(cancellationToken))
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
        public TaskFailureInformation PopNextStartTaskFailure()
            => StartTaskFailures.TryDequeue(out var failure) ? failure : default;

        /// <inheritdoc/>
        public ResizeError PopNextResizeError()
            => ResizeErrors.TryDequeue(out var resizeError) ? resizeError : default;

        /// <inheritdoc/>
        public ValueTask ServicePoolAsync(IBatchPool.ServiceKind serviceKind, CancellationToken cancellationToken = default)
        {
            lock (_lockObj)
            {
                return serviceKind switch
                {
                    IBatchPool.ServiceKind.GetResizeErrors => ServicePoolGetResizeErrorsAsync(cancellationToken),
                    IBatchPool.ServiceKind.RemoveNodeIfIdle => ServicePoolRemoveNodeIfIdleAsync(cancellationToken),
                    IBatchPool.ServiceKind.RemovePoolIfEmpty => ServicePoolRemovePoolIfEmptyAsync(cancellationToken),
                    IBatchPool.ServiceKind.Resize => ServicePoolResizeAsync(cancellationToken),
                    IBatchPool.ServiceKind.Rotate => ServicePoolRotateAsync(cancellationToken),
                    IBatchPool.ServiceKind.Update => ServicePoolUpdateAsync(cancellationToken),
                    _ => throw new ArgumentOutOfRangeException(nameof(serviceKind)),
                };
            }
        }

        /// <inheritdoc/>
        public async ValueTask ServicePoolAsync(CancellationToken cancellationToken = default)
        {
            var exceptions = new List<Exception>();

            await PerformTask(ServicePoolAsync(IBatchPool.ServiceKind.GetResizeErrors, cancellationToken), cancellationToken);
            await PerformTask(ServicePoolAsync(IBatchPool.ServiceKind.RemovePoolIfEmpty, cancellationToken), cancellationToken);
            await PerformTask(ServicePoolAsync(IBatchPool.ServiceKind.Rotate, cancellationToken), cancellationToken);
            await PerformTask(ServicePoolAsync(IBatchPool.ServiceKind.RemoveNodeIfIdle, cancellationToken), cancellationToken);
            await PerformTask(ServicePoolAsync(IBatchPool.ServiceKind.Resize, cancellationToken), cancellationToken);
            await PerformTask(ServicePoolAsync(IBatchPool.ServiceKind.Update, cancellationToken), cancellationToken);

            if (_nodesToRemove is not null)
            {
                await RemoveNodesAsync(_nodesToRemove, cancellationToken);
                _nodesToRemove = default;
            }

            switch (exceptions.Count)
            {
                case 0:
                    return;

                case 1:
                    throw exceptions.First();

                default:
                    throw new AggregateException(exceptions.SelectMany(Flatten).ToArray());
            }

            static IEnumerable<Exception> Flatten(Exception ex)
                => ex switch
                {
                    AggregateException aggregateException => aggregateException.InnerExceptions,
                    _ => Enumerable.Empty<Exception>().Append(ex),
                };

            async ValueTask PerformTask(ValueTask serviceAction, CancellationToken cancellationToken)
            {
                if (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        await serviceAction;
                    }
                    catch (Exception ex)
                    {
                        exceptions.Add(ex);
                    }
                }
            }
        }
        #endregion

        #region ServicePool~Async implementations
        private async ValueTask ServicePoolGetResizeErrorsAsync(CancellationToken cancellationToken)
        {
            if ((await _azureProxy.GetComputeNodeAllocationStateAsync(Pool.PoolId, cancellationToken)).AllocationState == AllocationState.Steady)
            {
                if (!_resizeErrorsRetrieved)
                {
                    ResizeErrors.Clear();
                    var pool = await _azureProxy.GetBatchPoolAsync(Pool.PoolId, new ODATADetailLevel { SelectClause = "resizeErrors" }, cancellationToken);

                    foreach (var error in pool.ResizeErrors ?? Enumerable.Empty<ResizeError>())
                    {
                        ResizeErrors.Enqueue(error);
                    }

                    _resizeErrorsRetrieved = true;
                }
            }
            else
            {
                _resizeErrorsRetrieved = false;
            }
        }

        private async ValueTask ServicePoolResizeAsync(CancellationToken cancellationToken)
        {
            var (allocationState, targetLowPri, targetDedicated) = await _azureProxy.GetComputeNodeAllocationStateAsync(Pool.PoolId, cancellationToken);
            var currentTargetNodes = IsPreemptable ? targetLowPri ?? 0 : targetDedicated ?? 0;

            if (allocationState == AllocationState.Steady)
            {
                var jobCount = (await GetJobsAsync().CountAsync(cancellationToken));

                // This method never reduces the nodes target. ServicePoolRemoveNodeIfIdleAsync() removes nodes.
                if (jobCount > currentTargetNodes)
                {
                    _nodesToRemove = default; // Since we are upsizing, cancel any idle node removals (they are actually needed).
                    _logger.LogDebug("Resizing {PoolId} to contain at least {Nodes} nodes", Pool.PoolId, jobCount);
                    _resizeErrorsRetrieved = false;
                    await _azureProxy.SetComputeNodeTargetsAsync(
                        Pool.PoolId,
                        IsPreemptable ? jobCount : 0,
                        IsPreemptable ? 0 : jobCount,
                        cancellationToken);
                }
            }
        }

        private async ValueTask ServicePoolRemoveNodeIfIdleAsync(CancellationToken cancellationToken)
        {
            _nodesToRemove = default;

            if ((await _azureProxy.GetComputeNodeAllocationStateAsync(Pool.PoolId, cancellationToken)).AllocationState == AllocationState.Steady)
            {
                var nodesToRemove = Enumerable.Empty<ComputeNode>();
                var expiryTime = (DateTime.UtcNow - _idleNodeCheck).ToString("yyyy-MM-dd'T'HH:mm:ssZ", CultureInfo.InvariantCulture);

                await foreach (var node in _azureProxy
                    .ListComputeNodesAsync(
                        Pool.PoolId,
                        new ODATADetailLevel(
                            filterClause: $"state eq 'starttaskfailed' or (state eq 'idle' and stateTransitionTime le DateTime'{expiryTime}')",
                            selectClause: "id,state,stateTransitionTime,startTaskInfo"))
                    .WithCancellation(cancellationToken))
                {
                    switch (node.State)
                    {
                        case ComputeNodeState.Idle:
                            _logger.LogDebug("Found idle node {NodeId}", node.Id);
                            break;
                        case ComputeNodeState.StartTaskFailed:
                            _logger.LogDebug("Found starttaskfailed node {NodeId}", node.Id);
                            StartTaskFailures.Enqueue(node.StartTaskInformation.FailureInformation);
                            break;
                        default:
                            throw new InvalidOperationException($"Unexpected compute node state found for node {node.Id}/{node.State} (should never happen).");
                    }
                    nodesToRemove = nodesToRemove.Append(node);
                    _resizeErrorsRetrieved = false;
                }

                // It's documented that a max of 100 nodes can be removed at a time. Excess eligible nodes will be removed in a future call.
                // Prioritize removing "start task failed" nodes, followed by longest waiting nodes.
                var orderedNodes = nodesToRemove
                    .OrderByDescending(n => n.State)
                    .ThenBy(n => n.StateTransitionTime)
                    .Take(100)
                    .ToList();

                if (!orderedNodes.Any())
                { // No work to do
                    return;
                }
                else if (!orderedNodes.Any(n => n.State == ComputeNodeState.StartTaskFailed))
                { // No StartTaskFailed nodes. Defer all node removals (in case any idle nodes haven't been assigned yet)
                    _nodesToRemove = orderedNodes;
                    return;
                }
                // Even if there are both Idle nodes and StartTaskFailed nodes mixed together, remove them in one call. We can't scale the pool in either direction again anytime soon, and it's simply an egregious waste of money to defer removal of start task failed nodes.

                await RemoveNodesAsync(orderedNodes, cancellationToken);
            }
        }

        private async ValueTask ServicePoolRotateAsync(CancellationToken cancellationToken)
        {
            if (IsAvailable)
            {
                var now = DateTime.UtcNow;
                IsAvailable = Creation + _forcePoolRotationAge > now && (Changed + _idlePoolCheck > now || await GetJobsAsync().AnyAsync(cancellationToken));
            }
        }

        private async ValueTask ServicePoolRemovePoolIfEmptyAsync(CancellationToken cancellationToken)
        {
            if (!IsAvailable)
            {
                var (lowPriorityNodes, dedicatedNodes) = await _azureProxy.GetCurrentComputeNodesAsync(Pool.PoolId, cancellationToken);
                if ((lowPriorityNodes is null || lowPriorityNodes == 0) && (dedicatedNodes is null || dedicatedNodes == 0) && !await GetJobsAsync().AnyAsync(cancellationToken))
                {
                    _isRemoved = true;
                    await _azureProxy.DeleteBatchPoolAsync(Pool.PoolId, cancellationToken);
                    _ = _batchPools.RemovePoolFromList(this);
                }
            }
        }

        /// <summary>
        /// Updates pool metadata.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns><see cref="ValueTask"/> for purposes of awaiting the task.</returns>
        /// <remarks>This is expected to be the last method called from <see cref="BatchPoolService"/>.</remarks>
        private async ValueTask ServicePoolUpdateAsync(CancellationToken cancellationToken)
        {
            if (_isRemoved)
            {
                return;
            }

            try
            {
                var cloudPool = await _azureProxy.GetBatchPoolAsync(Pool.PoolId, new ODATADetailLevel { SelectClause = "id,metadata" }, cancellationToken);
                var hash = _poolData.GetHashCode();
                cloudPool.Metadata = SetPoolData(this, _poolData, cloudPool.Metadata).ToList();
                _isDirty |= _poolData.GetHashCode() != hash;

                if (_isDirty)
                {
                    await _azureProxy.CommitBatchPoolChangesAsync(cloudPool, cancellationToken);
                }

                _isDirty = false;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }
        }
        #endregion

        #region Constructors
        /// <summary>
        /// Constructor of <see cref="BatchPool"/> for new pools
        /// </summary>
        /// <param name="poolInformation"></param>
        /// <param name="isPreemptable"></param>
        /// <param name="batchScheduler"></param>
        /// <param name="configuration"></param>
        /// <param name="azureProxy"></param>
        /// <param name="logger"></param>
        /// <exception cref="ArgumentException"></exception>
        public BatchPool(PoolInformation poolInformation, bool isPreemptable, IBatchScheduler batchScheduler, IConfiguration configuration, IAzureProxy azureProxy, ILogger<BatchPool> logger)
            : this(azureProxy.GetBatchPoolAsync(poolInformation.PoolId, new ODATADetailLevel { SelectClause = CloudPoolSelectClause }).Result,
                new() { IsAvailable = true },
                poolInformation,
                isPreemptable,
                DateTime.UtcNow,
                batchScheduler,
                configuration,
                azureProxy,
                logger)
        { }

        /// <summary>
        /// Constructor of <see cref="BatchPool"/> for retrieved pools
        /// </summary>
        /// <param name="pool"></param>
        /// <param name="changed"></param>
        /// <param name="batchScheduler"></param>
        /// <param name="configuration"></param>
        /// <param name="azureProxy"></param>
        /// <param name="logger"></param>
        public BatchPool(CloudPool pool, DateTime changed, IBatchScheduler batchScheduler, IConfiguration configuration, IAzureProxy azureProxy, ILogger<BatchPool> logger)
            : this(pool,
                GetPoolData(pool),
                new() { PoolId = pool.Id },
                null,
                changed,
                batchScheduler,
                configuration,
                azureProxy,
                logger)
        { }

        /// <summary>
        /// Alternate constructor of <see cref="BatchPool"/> for new pools
        /// </summary>
        /// <param name="poolId"></param>
        /// <param name="batchScheduler"></param>
        /// <param name="configuration"></param>
        /// <param name="azureProxy"></param>
        /// <param name="logger"></param>
        /// <exception cref="ArgumentException"></exception>
        public BatchPool(string poolId, IBatchScheduler batchScheduler, IConfiguration configuration, IAzureProxy azureProxy, ILogger<BatchPool> logger)
            : this(null,
                new() { IsAvailable = false },
                new() { PoolId = poolId },
                null,
                DateTime.UtcNow,
                batchScheduler,
                configuration,
                azureProxy,
                logger)
        { }

        private BatchPool(CloudPool cloudPool, PoolData data, PoolInformation poolInfo, bool? isPreemptable, DateTime changed, IBatchScheduler batchScheduler, IConfiguration configuration, IAzureProxy azureProxy, ILogger<BatchPool> logger)
        {
            Pool = poolInfo ?? throw new ArgumentNullException(nameof(poolInfo));
            _poolData = data ?? new();

            _idleNodeCheck = TimeSpan.FromMinutes(GetConfigurationValue(configuration, "BatchPoolIdleNodeMinutes", 0.125));
            _idlePoolCheck = TimeSpan.FromDays(GetConfigurationValue(configuration, "BatchPoolIdlePoolDays", 0.03125));
            _forcePoolRotationAge = TimeSpan.FromDays(GetConfigurationValue(configuration, "BatchPoolRotationForcedDays", 30));

            this._azureProxy = azureProxy;
            this._logger = logger;
            _batchPools = batchScheduler as BatchScheduler ?? throw new ArgumentException("batchScheduler must be of type BatchScheduler", nameof(batchScheduler));

            Creation = cloudPool?.CreationTime.Value ?? changed;
            Changed = changed;
            IsPreemptable = isPreemptable ?? !_poolData.IsDedicated;
            _poolData.IsDedicated = !IsPreemptable;
            IsAvailable = _poolData.IsAvailable;
            var metadata = SetPoolData(this, _poolData, cloudPool?.Metadata).ToList();

            if (cloudPool is not null)
            {
                cloudPool.Metadata = metadata;
                azureProxy.CommitBatchPoolChangesAsync(cloudPool).Wait();
                _isDirty = false;
            }
            else
            {
                _isDirty = true;
            }

            // IConfiguration.GetValue<double>(string key, double defaultValue) throws an exception if the value is defined as blank
            static double GetConfigurationValue(IConfiguration configuration, string key, double defaultValue)
            {
                var value = configuration.GetValue(key, string.Empty);
                return string.IsNullOrWhiteSpace(value) ? defaultValue : double.Parse(value);
            }
        }
        #endregion

        #region Implementation methods
        /// <summary>
        /// Retrieve stored metadata from pool's metadata collection
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
            if (data.IsAvailable != pool.IsAvailable)
            {
                data.IsAvailable = pool.IsAvailable;
            }

            return Enumerable.Empty<MetadataItem>()
                .Concat(metadataItems?.Where(t => !PoolDataName.Equals(t.Name, StringComparison.Ordinal)) ?? Enumerable.Empty<MetadataItem>())
                .Append(new(PoolDataName, BatchUtils.WriteJson(data)));
        }

        internal IAsyncEnumerable<CloudJob> GetJobsAsync()
            => _azureProxy.ListJobsAsync(new ODATADetailLevel { SelectClause = "id,stateTransitionTime", FilterClause = $"state eq 'active' and executionInfo/poolId eq '{Pool.PoolId}'" });

        private async ValueTask RemoveNodesAsync(IList<ComputeNode> nodesToRemove, CancellationToken cancellationToken)
        {
            _logger.LogDebug("Removing {Nodes} nodes from {PoolId}", nodesToRemove.Count, Pool.PoolId);
            _resizeErrorsRetrieved = false;
            await _azureProxy.DeleteBatchComputeNodesAsync(Pool.PoolId, nodesToRemove, cancellationToken);
        }
        #endregion

        #region Implementation state
        private readonly object _lockObj = new();
        private readonly PoolData _poolData;
        private IList<ComputeNode> _nodesToRemove;

        private readonly ILogger _logger;
        private DateTime Creation { get; set; }
        private DateTime Changed { get; set; }

        private Queue<TaskFailureInformation> StartTaskFailures { get; } = new();
        private Queue<ResizeError> ResizeErrors { get; } = new();

        private bool _isRemoved;
        private bool _isDirty;
        private bool _resizeErrorsRetrieved;

        private readonly TimeSpan _idleNodeCheck;
        private readonly TimeSpan _idlePoolCheck;
        private readonly TimeSpan _forcePoolRotationAge;
        private readonly BatchScheduler _batchPools;
        private readonly IAzureProxy _azureProxy;
        #endregion

        #region Nested private classes
        /// <summary>
        /// Stateful data we need to survive reboots
        /// </summary>
        public class PoolData
        {
            [Newtonsoft.Json.JsonProperty("isAvailable")]
            internal bool IsAvailable { get; set; }

            [Newtonsoft.Json.JsonProperty("isDedicated")]
            internal bool IsDedicated { get; set; }
        }
        #endregion

        #region Used for unit/module testing
        internal int TestPendingReservationsCount => GetJobsAsync().CountAsync().AsTask().Result;

        internal int? TestTargetDedicated => _azureProxy.GetComputeNodeAllocationStateAsync(Pool.PoolId).Result.TargetDedicated;
        internal int? TestTargetLowPriority => _azureProxy.GetComputeNodeAllocationStateAsync(Pool.PoolId).Result.TargetLowPriority;

        internal TimeSpan TestIdleNodeTime
            => _idleNodeCheck;

        internal TimeSpan TestIdlePoolTime
            => _idlePoolCheck;

        internal TimeSpan TestRotatePoolTime
            => _forcePoolRotationAge;

        internal void TestSetAvailable(bool available)
            => IsAvailable = available;

        internal async ValueTask TestRemoveNodes()
        {
            if (_nodesToRemove is not null)
            {
                await RemoveNodesAsync(_nodesToRemove, default);
                _nodesToRemove = default;
            }
        }

        internal void TimeShift(TimeSpan shift)
        {
            Creation -= shift;
            Changed -= shift;
        }
        #endregion
    }
}
