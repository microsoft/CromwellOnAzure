// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
                RemovePoolIfIdle,
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
            /// <returns></returns>
            public async Task<PoolInformation> ReserveNode(bool isLowPriority)
            {
                try
                {
                    lock (lockObj)
                    {
                        if (isLowPriority)
                        {
                            ++TargetLowPriority;
                        }
                        else
                        {
                            ++TargetDedicated;
                        }

                        ResizeDirty = true;
                    }

                    if (Interlocked.Increment(ref _resizeGuard) == 1)
                    {
                        await ServicePool(ServiceKind.Resize);
                    }
                }
                finally
                {
                    Interlocked.Decrement(ref _resizeGuard);
                }

                return Pool;
            }

            /// <summary>
            /// TODO
            /// </summary>
            /// <param name="isLowPriority"></param>
            /// <returns></returns>
            public async Task RemoveNode(bool isLowPriority)
            {
                try
                {
                    lock (lockObj)
                    {
                        if (isLowPriority)
                        {
                            --TargetLowPriority;
                        }
                        else
                        {
                            --TargetDedicated;
                        }
                    }

                    if (Interlocked.Increment(ref _resizeGuard) == 1)
                    {
                        await ServicePool(ServiceKind.Resize);
                    }
                }
                finally
                {
                    Interlocked.Decrement(ref _resizeGuard);
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
                        (bool dirty, int lowPri, int dedicated) values = default;
                        lock (lockObj)
                        {
                            values = (ResizeDirty, TargetLowPriority, TargetDedicated);
                        }

                        if (values.dirty)
                        {
                            var state = await _azureProxy.GetAllocationStateAsync(Pool.PoolId, cancellationToken);
                            if (state == AllocationState.Steady)
                            {
                                await _azureProxy.SetComputeNodeTargetsAsync(Pool.PoolId, values.lowPri, values.dedicated, cancellationToken);
                                lock (lockObj)
                                {
                                    ResizeDirty = TargetDedicated == values.dedicated && TargetLowPriority == values.lowPri;
                                }
                            }
                        }
                        break;

                    case ServiceKind.RemoveNodeIfIdle:
                        if (Changed + IdleNodeCheck > DateTime.UtcNow)
                        {
                            var state = await _azureProxy.GetAllocationStateAsync(Pool.PoolId, cancellationToken);
                            if (state == AllocationState.Steady)
                            {
                                var (lowPriorityNodes, dedicatedNodes) = await _azureProxy.GetCurrentComputeNodesAsync(Pool.PoolId, cancellationToken);
                                lock (lockObj)
                                {
                                    if (lowPriorityNodes != TargetLowPriority)
                                    {
                                        TargetLowPriority = lowPriorityNodes ?? 0;
                                    }

                                    if (dedicatedNodes != TargetDedicated)
                                    {
                                        TargetDedicated = dedicatedNodes ?? 0;
                                    }
                                }
                            }
                        }
                        break;

                    case ServiceKind.Rotate:
                        if (IsAvailable && Creation + IdlePoolCheck > DateTime.UtcNow)
                        {
                            IsAvailable = false;
                            // TODO: "duplicate" current pool
                        }
                        break;

                    case ServiceKind.RemovePoolIfIdle:
                        if (!IsAvailable)
                        {
                            var (lowPriorityNodes, dedicatedNodes) = await _azureProxy.GetCurrentComputeNodesAsync(Pool.PoolId, cancellationToken);
                            if ((lowPriorityNodes is null || lowPriorityNodes == 0) && (dedicatedNodes is null || dedicatedNodes == 0))
                            {
                                var queue = ManagedBatchPools.GetOrAdd(Pool.PoolId, k => new());
                                while (queue.TryDequeue(out var pool))
                                {
                                    if (ReferenceEquals(this, pool))
                                    {
                                        await _azureProxy.DeleteBatchPoolAsync(Pool.PoolId, cancellationToken);
                                        break;
                                    }
                                    queue.Enqueue(pool);
                                }
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
                logger.LogInformation("BatchPool task started.");

                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        await ProcessPools(BatchPool.ServiceKind.Resize, stoppingToken);
                        await ProcessPools(BatchPool.ServiceKind.RemoveNodeIfIdle, stoppingToken);
                        await ProcessPools(BatchPool.ServiceKind.Rotate, stoppingToken);
                        await ProcessPools(BatchPool.ServiceKind.RemovePoolIfIdle, stoppingToken);
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

                logger.LogInformation("BatchPool task gracefully stopped.");
            }

            private static async Task ProcessPools(BatchPool.ServiceKind serviceKind, CancellationToken stoppingToken)
                => await Task.WhenAll(ManagedBatchPools.Values.SelectMany(q => q).Select(p => p.ServicePool(serviceKind, stoppingToken)).ToArray());
        }
    }
}
