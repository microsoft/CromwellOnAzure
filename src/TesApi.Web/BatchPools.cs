// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection.Metadata.Ecma335;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TesApi.Web
{
    /// <summary>
    /// TODO
    /// </summary>
    public static class BatchPools
    {
        /// <summary>
        /// TODO
        /// </summary>
        public static bool IsEmpty
            => ManagedBatchPools.IsEmpty;

        /// <summary>
        /// TODO
        /// </summary>
        /// <param name="azureProxy"></param>
        /// <param name="key"></param>
        /// <param name="valueFactory"></param>
        /// <returns></returns>
        public static async Task<BatchPool> GetOrAdd(IAzureProxy azureProxy, string key, Func<string, Task<PoolInformation>> valueFactory)
        {
            _ = valueFactory ?? throw new ArgumentNullException(nameof(valueFactory));
            var queue = ManagedBatchPools.GetOrAdd(key, k => new());
            var result = queue.LastOrDefault(Available);
            if (result is null)
            {
                await Task.Run(() =>
                {
                    lock (syncObject)
                    {
                        result = queue.LastOrDefault(Available);
                        if (result is null)
                        {
                            result = BatchPool.Create(valueFactory($"{key}-{Guid.NewGuid()}").Result, azureProxy);
                            queue.Enqueue(result);
                        }
                    }
                });
            }
            return result;

            static bool Available(BatchPool pool)
                => pool.IsAvailable;
        }

        /// <summary>
        /// TODO
        /// </summary>
        /// <param name="poolId"></param>
        /// <param name="batchPool"></param>
        /// <returns></returns>
        public static bool TryGet(string poolId, out BatchPool batchPool)
        {
            batchPool = ManagedBatchPools.Values.SelectMany(q => q).FirstOrDefault(p => p.Pool.PoolId.Equals(poolId, StringComparison.OrdinalIgnoreCase));
            return batchPool is not null;
        }

        /// <summary>
        /// TODO
        /// </summary>
        public class BatchPool
        {
            private static readonly TimeSpan IdleNodeCheck = TimeSpan.FromMinutes(5); // TODO: set this to an appropriate value
            private static readonly TimeSpan IdlePoolCheck = TimeSpan.FromMinutes(30); // TODO: set this to an appropriate value
            private static readonly TimeSpan ForcePoolRotationAge = TimeSpan.FromDays(60); // TODO: set this to an appropriate value

            /// <summary>
            /// TODO
            /// </summary>
            public enum ServiceKind
            {
                /// <summary>
                /// TODO
                /// </summary>
                Resize,

                /// <summary>
                /// TODO
                /// </summary>
                RemoveNodeIfIdle,

                /// <summary>
                /// TODO
                /// </summary>
                Rotate,

                /// <summary>
                /// TODO
                /// </summary>
                RemovePoolIfEmpty,
            }

            /// <summary>
            /// TODO
            /// </summary>
            public bool IsAvailable { get; private set; } = true;

            /// <summary>
            /// TODO
            /// </summary>
            public PoolInformation Pool { get; }

            /// <summary>
            /// TODO
            /// </summary>
            /// <param name="isLowPriority"></param>
            /// <param name="cancellationToken"></param>
            /// <returns></returns>
            public async Task<AffinityInformation> PrepareNodeAsync(bool isLowPriority, CancellationToken cancellationToken = default)
            {
                Task rebootTask = default;
                AffinityInformation result = default;
                await _azureProxy.ForEachComputeNodeAsync(Pool.PoolId, ConsiderNode, cancellationToken: cancellationToken);
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
                            await ServicePool(ServiceKind.Resize, cancellationToken);
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
            /// TODO
            /// </summary>
            /// <param name="affinity"></param>
            public void ReleaseNode(AffinityInformation affinity)
            {
                lock (lockObj)
                {
                    ReservedComuteNodes.Remove(affinity);
                }
            }

            /// <summary>
            /// TODO
            /// </summary>
            /// <param name="serviceKind"></param>
            /// <param name="cancellationToken"></param>
            /// <returns></returns>
            public async Task ServicePool(ServiceKind serviceKind, CancellationToken cancellationToken = default)
            {
                switch (serviceKind)
                {
                    case ServiceKind.Resize:
                        {
                            (bool dirty, int lowPri, int dedicated) values = default;
                            lock (lockObj)
                            {
                                values = (ResizeDirty, TargetLowPriority, TargetDedicated);
                            }

                            if (values.dirty && (await _azureProxy.GetAllocationStateAsync(Pool.PoolId, cancellationToken)) == AllocationState.Steady)
                            {
                                await _azureProxy.SetComputeNodeTargetsAsync(Pool.PoolId, values.lowPri, values.dedicated, cancellationToken);
                                lock (lockObj)
                                {
                                    ResizeDirty = TargetDedicated != values.dedicated || TargetLowPriority != values.lowPri;
                                }
                            }
                        }
                        break;

                    case ServiceKind.RemoveNodeIfIdle:
                        if ((await _azureProxy.GetAllocationStateAsync(Pool.PoolId, cancellationToken)) == AllocationState.Steady)
                        {
                            var lowPriDecrementTarget = 0;
                            var dedicatedDecrementTarget = 0;
                            var nodesToRemove = Enumerable.Empty<ComputeNode>();
                            await _azureProxy.ForEachComputeNodeAsync(Pool.PoolId, n =>
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

                    case ServiceKind.Rotate:
                        if (IsAvailable)
                        {
                            IsAvailable = Creation + ForcePoolRotationAge >= DateTime.UtcNow &&
                                !(Changed + IdlePoolCheck < DateTime.UtcNow && TargetDedicated == 0 && TargetLowPriority == 0);
                        }
                        break;

                    case ServiceKind.RemovePoolIfEmpty:
                        if (!IsAvailable)
                        {
                            var (lowPriorityNodes, dedicatedNodes) = await _azureProxy.GetCurrentComputeNodesAsync(Pool.PoolId, cancellationToken);
                            if ((lowPriorityNodes is null || lowPriorityNodes == 0) && (dedicatedNodes is null || dedicatedNodes == 0))
                            {
                                foreach (var queue in ManagedBatchPools.Values)
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
                                await _azureProxy.DeleteBatchPoolAsync(Pool.PoolId, cancellationToken);
                            }
                        }
                        break;
                }
            }

            /// <summary>
            /// TODO
            /// </summary>
            /// <returns></returns>
            public static BatchPool Create(PoolInformation poolInformation, IAzureProxy azureProxy)
                => new(poolInformation, azureProxy);

            private BatchPool(PoolInformation poolInformation, IAzureProxy azureProxy)
            {
                _azureProxy = azureProxy;
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

            private readonly IAzureProxy _azureProxy;
            private readonly object lockObj = new();
            private volatile int _resizeGuard = 0;

            private List<AffinityInformation> ReservedComuteNodes { get; } = new();
        }

        private static readonly object syncObject = new();
        private static ConcurrentDictionary<string, ConcurrentQueue<BatchPool>> ManagedBatchPools { get; } = new();

        /// <summary>
        /// TODO
        /// </summary>
        public class BatchPoolService : BackgroundService
        {
            private static readonly TimeSpan processInterval = TimeSpan.FromMinutes(1); // TODO: set this to an appropriate value

            private readonly ILogger<BatchPoolService> logger;

            /// <summary>
            /// Default constructor
            /// </summary>
            /// <param name="logger"><see cref="ILogger"/> instance</param>
            public BatchPoolService(ILogger<BatchPoolService> logger)
                => this.logger = logger;

            /// <inheritdoc/>
            public override Task StopAsync(CancellationToken cancellationToken)
            {
                logger.LogInformation("BatchPool task stopping...");
                return base.StopAsync(cancellationToken);
            }

            /// <inheritdoc/>
            protected override async Task ExecuteAsync(CancellationToken stoppingToken)
            {
                logger.LogInformation("BatchPool service started.");
                await Task.WhenAll(new[] { ResizeAsync(stoppingToken), RemoveNodeIfIdleAsync(stoppingToken), RotateAsync(stoppingToken), RemovePoolIfEmptyAsync(stoppingToken) }).ConfigureAwait(false);
                logger.LogInformation("BatchPool service gracefully stopped.");
            }

            private async Task ResizeAsync(CancellationToken stoppingToken)
            {
                logger.LogInformation("Resize BatchPool task started.");
                await ProcessPoolsAsync(BatchPool.ServiceKind.Resize, stoppingToken).ConfigureAwait(false);
                logger.LogInformation("Resize BatchPool task gracefully stopped.");
            }

            private async Task RemoveNodeIfIdleAsync(CancellationToken stoppingToken)
            {
                logger.LogInformation("RemoveNodeIfIdle BatchPool task started.");
                await ProcessPoolsAsync(BatchPool.ServiceKind.RemoveNodeIfIdle, stoppingToken).ConfigureAwait(false);
                logger.LogInformation("RemoveNodeIfIdle BatchPool task gracefully stopped.");
            }

            private async Task RotateAsync(CancellationToken stoppingToken)
            {
                logger.LogInformation("Rotate BatchPool task started.");
                await ProcessPoolsAsync(BatchPool.ServiceKind.Rotate, stoppingToken).ConfigureAwait(false);
                logger.LogInformation("Rotate BatchPool task gracefully stopped.");
            }

            private async Task RemovePoolIfEmptyAsync(CancellationToken stoppingToken)
            {
                logger.LogInformation("RemovePoolIfEmpty BatchPool task started.");
                await ProcessPoolsAsync(BatchPool.ServiceKind.RemovePoolIfEmpty, stoppingToken).ConfigureAwait(false);
                logger.LogInformation("RemovePoolIfEmpty BatchPool task gracefully stopped.");
            }

            private async Task ProcessPoolsAsync(BatchPool.ServiceKind service, CancellationToken stoppingToken)
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        await Task.WhenAll(ManagedBatchPools.Values.SelectMany(q => q).Select(p => p.ServicePool(service, stoppingToken)).ToArray());
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, ex.Message);
                    }

                    try
                    {
                        await Task.Delay(processInterval, stoppingToken);
                    }
                    catch (TaskCanceledException)
                    {
                        break;
                    }
                }
            }
        }
    }
}
