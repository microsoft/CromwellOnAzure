// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TesApi.Web
{
    /// <summary>
    /// A background service that montitors CloudPools in the batch system, orchestrates their lifecycle, and updates their state.
    /// This should only be used as a system-wide singleton service.  This class does not support scale-out on multiple machines,
    /// nor does it implement a leasing mechanism.  In the future, consider using the Lease Blob operation.
    /// </summary>
    public class BatchPoolService : BackgroundService
    {
        private readonly IBatchScheduler _batchScheduler;
        private readonly ILogger _logger;
        private readonly bool _isDisabled;

        /// <summary>
        /// Interval between each call to <see cref="IBatchPool.ServicePoolAsync(CancellationToken)"/>.
        /// </summary>
        public static readonly TimeSpan RunInterval = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="configuration"></param>
        /// <param name="batchScheduler"></param>
        /// <param name="logger"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public BatchPoolService(IConfiguration configuration, IBatchScheduler batchScheduler, ILogger<BatchPoolService> logger)
        {
            _batchScheduler = batchScheduler ?? throw new ArgumentNullException(nameof(batchScheduler));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _isDisabled = configuration.GetValue("UseLegacyBatchImplementationWithAutopools", false);
        }

        /// <inheritdoc />
        public override Task StartAsync(CancellationToken cancellationToken)
        {
            if (_isDisabled)
            {
                return Task.CompletedTask;
            }

            return base.StartAsync(cancellationToken);
        }

        /// <inheritdoc />
        public override Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Scheduler stopping...");
            return base.StopAsync(cancellationToken);
        }

        /// <inheritdoc />
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Scheduler started.");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await RemoveUnknownPools(stoppingToken);
                    await ServiceBatchPools(stoppingToken);
                    await Task.Delay(RunInterval, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception exc)
                {
                    _logger.LogError(exc, exc.Message);
                }
            }

            _logger.LogInformation("Scheduler gracefully stopped.");
        }

        /// <summary>
        /// Cleans up pools that are unknown/deleted.
        /// </summary>
        /// <param name="cancellationToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        private async ValueTask RemoveUnknownPools(CancellationToken cancellationToken)
        {
            var extraPools = _batchScheduler.GetPools().ToList();

            await foreach (var pool in _batchScheduler.GetCloudPools().WithCancellation(cancellationToken))
            {
                var batchPool = extraPools.Find(p => p.Pool.PoolId.Equals(pool.Id, StringComparison.OrdinalIgnoreCase));

                if (batchPool is not null)
                {
                    extraPools.Remove(batchPool);
                }
                else
                {
                    await pool.DeleteAsync(cancellationToken: cancellationToken);
                }
            }

            foreach (var pool in extraPools)
            {
                _ = _batchScheduler.RemovePoolFromList(pool);
            }
        }

        /// <summary>
        /// Retrieves all batch pools from the database and affords an opportunity to react to changes.
        /// </summary>
        /// <param name="cancellationToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        private async ValueTask ServiceBatchPools(CancellationToken cancellationToken)
        {
            var pools = _batchScheduler.GetPools().ToList();

            if (0 == pools.Count)
            {
                return;
            }

            var startTime = DateTime.UtcNow;

            foreach (var pool in pools)
            {
                try
                {
                    await pool.ServicePoolAsync(cancellationToken);
                }
                catch (Exception exc)
                {
                    _logger.LogError(exc, "Batch pool {PoolId} threw an exception in ServiceBatchPools.", pool.Pool?.PoolId);
                }
            }

            _logger.LogDebug($"ServiceBatchPools for {pools.Count} pools completed in {DateTime.UtcNow.Subtract(startTime).TotalSeconds} seconds.");
        }
    }
}
