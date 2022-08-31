// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Azure.Management.AppService.Fluent.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using YamlDotNet.Core.Tokens;

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

            _idleNodeCheck = TimeSpan.FromMinutes(ConfigurationUtils.GetConfigurationValue(configuration, "BatchPoolIdleNodeMinutes", 0.125));
            _forcePoolRotationAge = batchScheduler.ForcePoolRotationAge;

            this._azureProxy = azureProxy;
            this._logger = logger;
            _batchPools = batchScheduler as BatchScheduler ?? throw new ArgumentException("batchScheduler must be of type BatchScheduler", nameof(batchScheduler));

            Creation = cloudPool?.CreationTime;
            IsAvailable = cloudPool is not null;
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
        private enum ScalingMode
        {
            AutoScaleEnabled,
            SettingManualScale,
            RemovingFailedNodes,
            SettingAutoScale
        }

        private ScalingMode _scalingMode = ScalingMode.AutoScaleEnabled;

        private readonly TimeSpan _idleNodeCheck;
        private readonly TimeSpan _forcePoolRotationAge;
        private readonly BatchScheduler _batchPools;
        private bool _resizeErrorsRetrieved;

        private DateTime? Creation { get; set; }

        private async ValueTask ServicePoolGetResizeErrorsAsync(CancellationToken cancellationToken)
        {
            if (_scalingMode == ScalingMode.AutoScaleEnabled && (await _azureProxy.GetComputeNodeAllocationStateAsync(Pool.PoolId, cancellationToken)).AllocationState == AllocationState.Steady)
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

        /// <summary>
        /// Generates a formula for Azure batch account auto-pool usage
        /// </summary>
        /// <param name="preemptable">false if compute nodes are dedicated, true otherwise.</param>
        /// <param name="initialTarget">Number of compute nodes to allocate the first time this formula is evaluated.</param>
        /// <remarks>Implements <see cref="IAzureProxy.BatchPoolAutoScaleFormulaFactory"/>.</remarks>
        /// <returns></returns>
        public static string AutoPoolFormula(bool preemptable, int initialTarget)
            => string.Format(@"
    $NodeDeallocationOption=taskcompletion;
    lifespan         = time() - time(""{1}"");
    span             = TimeInterval_Second * 90;
    startup          = TimeInterval_Minute * 2;
    ratio            = 10;
    {0} = (lifespan > startup ? min($PendingTasks.GetSample(span, ratio)) : {2});
    ", preemptable ? "$TargetLowPriorityNodes" : "$TargetDedicated", DateTime.UtcNow.ToString("r"), initialTarget);

        private async ValueTask ServicePoolRemoveFailedNodesAsync(CancellationToken cancellationToken)
        {
            if ((await _azureProxy.GetComputeNodeAllocationStateAsync(Pool.PoolId, cancellationToken)).AllocationState == AllocationState.Steady)
            {
                switch (_scalingMode)
                {
                    case ScalingMode.AutoScaleEnabled:
                        if (await GetNodes(false).GetAsyncEnumerator().MoveNextAsync())
                        {
                            //await _azureProxy.DisableAutoScaleAsync(Pool.PoolId);
                            _scalingMode = ScalingMode.SettingManualScale;
                        }
                        break;

                    case ScalingMode.SettingManualScale:
                        {
                            var quantity = 0;
                            var nodesToRemove = Enumerable.Empty<ComputeNode>();
                            var nodes = GetNodes(true).GetAsyncEnumerator();

                            // It's documented that a max of 100 nodes can be removed at a time. Excess eligible nodes will be removed in a future call.
                            while (await nodes.MoveNextAsync() && quantity++ < 100)
                            {
                                var node = nodes.Current;

                                switch (node.State)
                                {
                                    case ComputeNodeState.Unusable:
                                        _logger.LogDebug("Found unusable node {NodeId}", node.Id);
                                        goto default;

                                    case ComputeNodeState.StartTaskFailed:
                                        _logger.LogDebug("Found starttaskfailed node {NodeId}", node.Id);
                                        StartTaskFailures.Enqueue(node.StartTaskInformation.FailureInformation);
                                        goto default;

                                    case ComputeNodeState.Preempted:
                                        _logger.LogDebug("Found preempted node {NodeId}", node.Id);
                                        goto default;

                                    default:
                                        nodesToRemove = nodesToRemove.Append(node);
                                        _resizeErrorsRetrieved = false;
                                        break;
                                }
                            }

                            if (!nodesToRemove.Any())
                            { // How did we get here?
                                goto case ScalingMode.RemovingFailedNodes;
                            }

                            await RemoveNodesAsync(nodesToRemove.ToList(), cancellationToken);
                        }
                        _scalingMode = ScalingMode.RemovingFailedNodes;
                        break;

                    case ScalingMode.RemovingFailedNodes:
                        await _azureProxy.EnableBatchPoolAutoScaleAsync(Pool.PoolId, TimeSpan.FromMinutes(5), AutoPoolFormula, cancellationToken);
                        _scalingMode = ScalingMode.SettingAutoScale;
                        break;

                    case ScalingMode.SettingAutoScale:
                        ResizeErrors.Clear();
                        _resizeErrorsRetrieved = true;
                        _scalingMode = ScalingMode.AutoScaleEnabled;
                        break;
                }
            }

            ConfiguredCancelableAsyncEnumerable<ComputeNode> GetNodes(bool withState)
                => _azureProxy.ListComputeNodesAsync(Pool.PoolId, new ODATADetailLevel(filterClause: @"state eq 'starttaskfailed' or state eq 'preempted' or state eq 'unusable'", selectClause: withState ? @"id,state,startTaskInfo" : @"id")).WithCancellation(cancellationToken);
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
        private sealed class LockObj : IDisposable
        {
            private static readonly SemaphoreSlim lockObj = new(1, 1);

            public static async ValueTask<LockObj> Lock()
            {
                await lockObj.WaitAsync();
                return new LockObj();
            }

            public static async ValueTask<LockObj> Lock(TimeSpan timeout)
            {
                if (await lockObj.WaitAsync(timeout))
                {
                    return new LockObj();
                }

                throw new TimeoutException();
            }

            public void Dispose()
                => lockObj.Release();
        }

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
        public async ValueTask ServicePoolAsync(IBatchPool.ServiceKind serviceKind, CancellationToken cancellationToken = default)
        {
            using var @lock = await LockObj.Lock();
            Func<CancellationToken, ValueTask> func = serviceKind switch
            {
                IBatchPool.ServiceKind.GetResizeErrors => ServicePoolGetResizeErrorsAsync,
                IBatchPool.ServiceKind.RemoveFailedNodes => ServicePoolRemoveFailedNodesAsync,
                IBatchPool.ServiceKind.RemovePoolIfEmpty => ServicePoolRemovePoolIfEmptyAsync,
                IBatchPool.ServiceKind.Rotate => ServicePoolRotateAsync,
                _ => throw new ArgumentOutOfRangeException(nameof(serviceKind)),
            };

            await func(cancellationToken);
        }

        /// <inheritdoc/>
        public async ValueTask ServicePoolAsync(CancellationToken cancellationToken = default)
        {
            var exceptions = new List<Exception>();

            await PerformTask(ServicePoolAsync(IBatchPool.ServiceKind.GetResizeErrors, cancellationToken), cancellationToken);
            await PerformTask(ServicePoolAsync(IBatchPool.ServiceKind.RemoveFailedNodes, cancellationToken), cancellationToken);
            await PerformTask(ServicePoolAsync(IBatchPool.ServiceKind.Rotate, cancellationToken), cancellationToken);
            await PerformTask(ServicePoolAsync(IBatchPool.ServiceKind.RemovePoolIfEmpty, cancellationToken), cancellationToken);

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
                        RemoveMissingPools(ex);
                    }
                }
            }

            void RemoveMissingPools(Exception ex)
            {
                switch(ex)
                {
                    case AggregateException aggregateException:
                        foreach (var e in aggregateException.InnerExceptions)
                        {
                            RemoveMissingPools(e);
                        }
                        break;

                    case BatchException batchException:
                        if (batchException.RequestInformation.BatchError.Code == BatchErrorCodeStrings.PoolNotFound)
                        {
                            _logger.LogError(ex, "Batch pool {PoolId} is missing. Removing it from TES's active pool list.", Pool.PoolId);
                            _ = _batchPools.RemovePoolFromList(this);
                        }
                        break;
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
