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
    public sealed partial class BatchPool
    {
        /// <summary>
        /// Minimum property set required for <see cref="CloudPool"/> provided to constructors of this class
        /// </summary>
        public const string CloudPoolSelectClause = "id,creationTime,metadata";

        private readonly ILogger _logger;
        private readonly IAzureProxy _azureProxy;

        /// <summary>
        /// Constructor of <see cref="BatchPool"/> for new pools
        /// </summary>
        /// <param name="poolInformation"></param>
        /// <param name="batchScheduler"></param>
        /// <param name="configuration"></param>
        /// <param name="azureProxy"></param>
        /// <param name="logger"></param>
        /// <exception cref="ArgumentException"></exception>
        public BatchPool(PoolInformation poolInformation, IBatchScheduler batchScheduler, IConfiguration configuration, IAzureProxy azureProxy, ILogger<BatchPool> logger)
            : this(azureProxy.GetBatchPoolAsync(poolInformation.PoolId, new ODATADetailLevel { SelectClause = CloudPoolSelectClause }).Result,
                poolInformation,
                batchScheduler,
                configuration,
                azureProxy,
                logger)
        { }

        /// <summary>
        /// Constructor of <see cref="BatchPool"/> for retrieved pools
        /// </summary>
        /// <param name="pool"></param>
        /// <param name="batchScheduler"></param>
        /// <param name="configuration"></param>
        /// <param name="azureProxy"></param>
        /// <param name="logger"></param>
        public BatchPool(CloudPool pool, IBatchScheduler batchScheduler, IConfiguration configuration, IAzureProxy azureProxy, ILogger<BatchPool> logger)
            : this(pool,
                new() { PoolId = pool.Id },
                batchScheduler,
                configuration,
                azureProxy,
                logger)
        { }

        /// <summary>
        /// Alternate constructor of <see cref="BatchPool"/> for broken pools
        /// </summary>
        /// <param name="poolId"></param>
        /// <param name="batchScheduler"></param>
        /// <param name="configuration"></param>
        /// <param name="azureProxy"></param>
        /// <param name="logger"></param>
        /// <exception cref="ArgumentException"></exception>
        public BatchPool(string poolId, IBatchScheduler batchScheduler, IConfiguration configuration, IAzureProxy azureProxy, ILogger<BatchPool> logger)
            : this((CloudPool)default,
                new() { PoolId = poolId },
                batchScheduler,
                configuration,
                azureProxy,
                logger)
        { }

        private BatchPool(CloudPool cloudPool, PoolInformation poolInfo, IBatchScheduler batchScheduler, IConfiguration configuration, IAzureProxy azureProxy, ILogger<BatchPool> logger)
        {
            Pool = poolInfo ?? throw new ArgumentNullException(nameof(poolInfo));

            _idleNodeCheck = TimeSpan.FromMinutes(GetConfigurationValue(configuration, "BatchPoolIdleNodeMinutes", 0.125));
            _forcePoolRotationAge = TimeSpan.FromDays(GetConfigurationValue(configuration, "BatchPoolRotationForcedDays", 30));

            this._azureProxy = azureProxy;
            this._logger = logger;
            _batchPools = batchScheduler as BatchScheduler ?? throw new ArgumentException("batchScheduler must be of type BatchScheduler", nameof(batchScheduler));

            Creation = cloudPool?.CreationTime;
            IsAvailable = cloudPool is not null;

            // IConfiguration.GetValue<double>(string key, double defaultValue) throws an exception if the value is defined as blank
            static double GetConfigurationValue(IConfiguration configuration, string key, double defaultValue)
            {
                var value = configuration.GetValue(key, string.Empty);
                return string.IsNullOrWhiteSpace(value) ? defaultValue : double.Parse(value);
            }
        }

        private Queue<TaskFailureInformation> StartTaskFailures { get; } = new();
        private Queue<ResizeError> ResizeErrors { get; } = new();

        internal IAsyncEnumerable<CloudJob> GetJobsAsync()
            => _azureProxy.ListJobsAsync(new ODATADetailLevel { SelectClause = "id,stateTransitionTime", FilterClause = $"state eq 'active' and executionInfo/poolId eq '{Pool.PoolId}'" });

        private async ValueTask RemoveNodesAsync(IList<ComputeNode> nodesToRemove, CancellationToken cancellationToken)
        {
            _logger.LogDebug("Removing {Nodes} nodes from {PoolId}", nodesToRemove.Count, Pool.PoolId);
            _resizeErrorsRetrieved = false;
            await _azureProxy.DeleteBatchComputeNodesAsync(Pool.PoolId, nodesToRemove, cancellationToken);
        }
    }

    /// <content>
    /// Implements the various ServicePool* methods.
    /// </content>
    public sealed partial class BatchPool
    {
        private readonly TimeSpan _idleNodeCheck;
        private readonly TimeSpan _forcePoolRotationAge;
        private readonly BatchScheduler _batchPools;
        private bool _resizeErrorsRetrieved;

        private DateTime? Creation { get; set; }

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

        private async ValueTask ServicePoolRemoveNodeIfStartTaskFailedAsync(CancellationToken cancellationToken)
        {
            if ((await _azureProxy.GetComputeNodeAllocationStateAsync(Pool.PoolId, cancellationToken)).AllocationState == AllocationState.Steady)
            {
                var nodesToRemove = Enumerable.Empty<ComputeNode>();

                await foreach (var node in _azureProxy
                    .ListComputeNodesAsync(
                        Pool.PoolId,
                        new ODATADetailLevel(
                            filterClause: $"state eq 'starttaskfailed'",
                            selectClause: "id,startTaskInfo"))
                    .WithCancellation(cancellationToken))
                {
                    _logger.LogDebug("Found starttaskfailed node {NodeId}", node.Id);
                    StartTaskFailures.Enqueue(node.StartTaskInformation.FailureInformation);
                    nodesToRemove = nodesToRemove.Append(node);
                    _resizeErrorsRetrieved = false;
                }

                // It's documented that a max of 100 nodes can be removed at a time. Excess eligible nodes will be removed in a future call.
                // Prioritize removing "start task failed" nodes, followed by longest waiting nodes.
                var orderedNodes = nodesToRemove
                    .Take(100)
                    .ToList();

                if (!orderedNodes.Any())
                { // No work to do
                    return;
                }

                // Even if there are both Idle nodes and StartTaskFailed nodes mixed together, remove them in one call. We can't scale the pool in either direction again anytime soon, and it's simply an egregious waste of money to defer removal of start task failed nodes.

                await RemoveNodesAsync(orderedNodes, cancellationToken);
            }
        }

        private ValueTask ServicePoolRotateAsync(CancellationToken _1)
        {
            if (IsAvailable)
            {
                IsAvailable = Creation + _forcePoolRotationAge > DateTime.UtcNow;
            }

            return ValueTask.CompletedTask;
        }

        private async ValueTask ServicePoolRemovePoolIfEmptyAsync(CancellationToken cancellationToken)
        {
            if (!IsAvailable)
            {
                var (lowPriorityNodes, dedicatedNodes) = await _azureProxy.GetCurrentComputeNodesAsync(Pool.PoolId, cancellationToken);
                if ((lowPriorityNodes is null || lowPriorityNodes == 0) && (dedicatedNodes is null || dedicatedNodes == 0) && !await GetJobsAsync().AnyAsync(cancellationToken))
                {
                    await _azureProxy.DeleteBatchPoolAsync(Pool.PoolId, cancellationToken);
                    _ = _batchPools.RemovePoolFromList(this);
                }
            }
        }
    }

    /// <content>
    /// Implements the <see cref="IBatchPool"/> interface.
    /// </content>
    public sealed partial class BatchPool : IBatchPool
    {
        private readonly object _lockObj = new();

        /// <inheritdoc/>
        public bool IsAvailable { get; private set; } = true;

        /// <inheritdoc/>
        public PoolInformation Pool { get; }

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
        public ResizeError PopNextResizeError()
            => ResizeErrors.TryDequeue(out var resizeError) ? resizeError : default;

        /// <inheritdoc/>
        public TaskFailureInformation PopNextStartTaskFailure()
            => StartTaskFailures.TryDequeue(out var failure) ? failure : default;

        /// <inheritdoc/>
        public ValueTask ServicePoolAsync(IBatchPool.ServiceKind serviceKind, CancellationToken cancellationToken = default)
        {
            lock (_lockObj)
            {
                return serviceKind switch
                {
                    IBatchPool.ServiceKind.GetResizeErrors => ServicePoolGetResizeErrorsAsync(cancellationToken),
                    IBatchPool.ServiceKind.RemoveNodeIfStartTaskFailed => ServicePoolRemoveNodeIfStartTaskFailedAsync(cancellationToken),
                    IBatchPool.ServiceKind.RemovePoolIfEmpty => ServicePoolRemovePoolIfEmptyAsync(cancellationToken),
                    IBatchPool.ServiceKind.Rotate => ServicePoolRotateAsync(cancellationToken),
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
            await PerformTask(ServicePoolAsync(IBatchPool.ServiceKind.RemoveNodeIfStartTaskFailed, cancellationToken), cancellationToken);

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
    }

    /// <content>
    /// Used for unit/module testing.
    /// </content>
    public sealed partial class BatchPool
    {
        internal int TestPendingReservationsCount => GetJobsAsync().CountAsync().AsTask().Result;

        internal int? TestTargetDedicated => _azureProxy.GetComputeNodeAllocationStateAsync(Pool.PoolId).Result.TargetDedicated;
        internal int? TestTargetLowPriority => _azureProxy.GetComputeNodeAllocationStateAsync(Pool.PoolId).Result.TargetLowPriority;

        internal TimeSpan TestIdleNodeTime
            => _idleNodeCheck;

        internal TimeSpan TestRotatePoolTime
            => _forcePoolRotationAge;

        internal void TestSetAvailable(bool available)
            => IsAvailable = available;

        internal void TimeShift(TimeSpan shift)
            => Creation -= shift;
    }
}
