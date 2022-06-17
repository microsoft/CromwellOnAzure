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
        public ValueTask ServicePoolAsync(IBatchPool.ServiceKind serviceKind, CancellationToken cancellationToken = default)
        {
            lock (this) // TODO: make lock object
            {
                return serviceKind switch
                {
                    IBatchPool.ServiceKind.Update => ServicePoolUpdateAsync(cancellationToken),
                    IBatchPool.ServiceKind.Resize => ServicePoolResizeAsync(cancellationToken),
                    IBatchPool.ServiceKind.RemoveNodeIfIdle => ServicePoolRemoveNodeIfIdleAsync(cancellationToken),
                    IBatchPool.ServiceKind.Rotate => ServicePoolRotateAsync(cancellationToken),
                    IBatchPool.ServiceKind.RemovePoolIfEmpty => ServicePoolRemovePoolIfEmptyAsync(cancellationToken),
                    _ => throw new ArgumentOutOfRangeException(nameof(serviceKind)),
                };
            }
        }

        /// <inheritdoc/>
        public async ValueTask<bool> ServicePoolAsync(CancellationToken cancellationToken = default)
        {
            var exceptions = new List<Exception>();
            var retVal = _isDirty || _isChanged;
            await PerformTask(ServicePoolAsync(IBatchPool.ServiceKind.RemovePoolIfEmpty, cancellationToken));
            await PerformTask(ServicePoolAsync(IBatchPool.ServiceKind.Rotate, cancellationToken));
            await PerformTask(ServicePoolAsync(IBatchPool.ServiceKind.Resize, cancellationToken));
            await PerformTask(ServicePoolAsync(IBatchPool.ServiceKind.RemoveNodeIfIdle, cancellationToken));
            await PerformTask(ServicePoolAsync(IBatchPool.ServiceKind.Update, cancellationToken));
            retVal |= _isDirty || _isChanged;
            _isChanged = false;

            return exceptions.Count switch
            {
                0 => retVal,
                1 => throw exceptions.First(),
                _ => throw new AggregateException(exceptions.SelectMany(Flatten).ToArray()),
            };

            static IEnumerable<Exception> Flatten(Exception ex)
                => ex switch
                {
                    AggregateException aggregateException => aggregateException.InnerExceptions,
                    _ => Enumerable.Empty<Exception>().Append(ex),
                };

            async ValueTask PerformTask(ValueTask serviceAction)
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
        #endregion

        #region ServicePool~Async implementations
        private async ValueTask ServicePoolResizeAsync(CancellationToken cancellationToken)
        {
            //DateTime cutoff;
            //try
            //{
            //    cutoff = Reservations.Min(p => p.Value.QueuedTime);
            //}
            //catch (InvalidOperationException)
            //{
            //    cutoff = Changed;
            //}

            var (allocationState, targetLowPri, targetDedicated) = await _azureProxy.GetComputeNodeAllocationStateAsync(Pool.PoolId, cancellationToken);

            if (/*cutoff < DateTime.UtcNow - TimeSpan.FromSeconds(15) &&*/ allocationState == AllocationState.Steady)
            {
                var nodes = await GetJobsAsync().CountAsync(cancellationToken);
                var lowPri = IsPreemptable ? Math.Max(targetLowPri ?? 0, nodes) : 0;
                var dedicated = IsPreemptable ? 0 : Math.Max(targetDedicated ?? 0, nodes);

                if (lowPri != (targetLowPri ?? 0) || dedicated != (targetDedicated ?? 0))
                {
                    _logger.LogDebug("Resizing {PoolId}", Pool.PoolId);
                    _isChanged = true;
                    await _azureProxy.SetComputeNodeTargetsAsync(Pool.PoolId, lowPri, dedicated, cancellationToken);
                }
            }
        }

        private async ValueTask ServicePoolRemoveNodeIfIdleAsync(CancellationToken cancellationToken)
        {
            if ((await _azureProxy.GetComputeNodeAllocationStateAsync(Pool.PoolId, cancellationToken)).AllocationState == AllocationState.Steady)
            {
                var nodesToRemove = Enumerable.Empty<ComputeNode>();
                var expiryTime = DateTime.UtcNow - _idleNodeCheck;

                await foreach (var node in _azureProxy
                    .ListComputeNodesAsync(
                        Pool.PoolId,
                        new ODATADetailLevel(filterClause: $"state eq 'starttaskfailed' or (state eq 'idle' and stateTransitionTime lt DateTime'{expiryTime.ToString("yyyy-MM-dd'T'HH:mm:ssZ", CultureInfo.InvariantCulture)}')", selectClause: "id,state,startTaskInfo"))
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
                    }
                    nodesToRemove = nodesToRemove.Append(node);
                }

                // It's documented that a max of 100 nodes can be removed at a time. Group the nodes to remove in batches up to 100 in quantity.
                var removeNodesTasks = Enumerable.Empty<Task>();
                foreach (var nodes in nodesToRemove.Select((n, i) => (n, i)).GroupBy(t => t.i / 100).OrderBy(t => t.Key))
                {
                    _isChanged = true;
                    removeNodesTasks = removeNodesTasks.Append(_azureProxy.DeleteBatchComputeNodesAsync(Pool.PoolId, nodes.Select(t => t.n), cancellationToken));
                }

                // Call each group serially.
                var tasks = Task.Run(async () =>
                {
                    foreach (var task in removeNodesTasks)
                    {
                        _logger.LogDebug("Removing nodes from {PoolId}", Pool.PoolId);
                        await task;
                    }
                }, cancellationToken);

                // Return when all the groups are done
                await tasks;
            }
        }

        private async ValueTask ServicePoolRotateAsync(CancellationToken cancellationToken)
        {
            if (IsAvailable)
            {
                var now = DateTime.UtcNow;
                IsAvailable = Creation + _forcePoolRotationAge > now && (Changed + _idlePoolCheck > now || await GetJobsAsync().AnyAsync(cancellationToken));
                _isChanged |= !IsAvailable;
            }
        }

        private async ValueTask ServicePoolRemovePoolIfEmptyAsync(CancellationToken cancellationToken)
        {
            if (!IsAvailable)
            {
                var (lowPriorityNodes, dedicatedNodes) = await _azureProxy.GetCurrentComputeNodesAsync(Pool.PoolId, cancellationToken);
                if ((lowPriorityNodes is null || lowPriorityNodes == 0) && (dedicatedNodes is null || dedicatedNodes == 0) && !await GetJobsAsync().AnyAsync(cancellationToken))
                {
                    _isChanged = true;
                    _isRemoved = true;
                    await _azureProxy.DeleteBatchPoolAsync(Pool.PoolId, cancellationToken);
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

            try
            {
                var cloudPool = await _azureProxy.GetBatchPoolAsync(Pool.PoolId, new ODATADetailLevel { SelectClause = "id,metadata" }, cancellationToken);
                var hash = _poolData.GetHashCode();
                cloudPool.Metadata = SetPoolData(this, _poolData, cloudPool.Metadata).ToList();
                _isDirty |= _poolData.GetHashCode() != hash;

                if (_isDirty)
                {
                    _isChanged = true;
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
        /// Alternate onstructor of <see cref="BatchPool"/> for new pools
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
            if (data.IsAvailable != pool.IsAvailable)
            {
                data.IsAvailable = pool.IsAvailable;
            }

            return Enumerable.Empty<MetadataItem>()
                .Concat(metadataItems?.Where(t => !PoolDataName.Equals(t.Name, StringComparison.Ordinal)) ?? Enumerable.Empty<MetadataItem>())
                .Append(new(PoolDataName, BatchUtils.WriteJson(data)));
        }

        internal IAsyncEnumerable<CloudJob> GetJobsAsync()
            => _azureProxy.ListJobsAsync(new ODATADetailLevel { FilterClause = $"state eq 'active' and executionInfo/poolId eq '{Pool.PoolId}'" });
        #endregion

        #region Implementation state
        private readonly PoolData _poolData;

        private readonly ILogger _logger;
        private DateTime Creation { get; set; }
        private DateTime Changed { get; set; }

        private Queue<TaskFailureInformation> StartTaskFailures { get; } = new();

        private bool _isChanged;
        private bool _isRemoved;
        private bool _isDirty;

        private readonly TimeSpan _idleNodeCheck;
        private readonly TimeSpan _idlePoolCheck;
        private readonly TimeSpan _forcePoolRotationAge;
        private readonly BatchScheduler _batchPools;
        private readonly IAzureProxy _azureProxy;
        #endregion

        #region Nested private classes
        /// <summary>
        /// TODO
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

        internal void TimeShift(TimeSpan shift)
        {
            Creation -= shift;
            Changed -= shift;
        }
        #endregion
    }
}
