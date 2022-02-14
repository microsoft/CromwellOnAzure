// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TesApi.Web
{
    /// <summary>
    /// 
    /// </summary>
    public class BatchPoolService : BackgroundService
    {
        private static readonly TimeSpan processInterval = TimeSpan.FromMinutes(1); // TODO: set this to an appropriate value

        private readonly ILogger logger;

        private readonly IBatchPoolsImpl batchPools;

        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="batchPools"></param>
        /// <param name="logger"><see cref="ILogger"/> instance</param>
        public BatchPoolService(IBatchPools batchPools, ILogger<BatchPoolService> logger)
        {
            this.batchPools = (IBatchPoolsImpl)batchPools;
            this.logger = logger;
        }

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
            await ProcessPoolsAsync(IBatchPool.ServiceKind.Resize, stoppingToken).ConfigureAwait(false);
            logger.LogInformation("Resize BatchPool task gracefully stopped.");
        }

        private async Task RemoveNodeIfIdleAsync(CancellationToken stoppingToken)
        {
            logger.LogInformation("RemoveNodeIfIdle BatchPool task started.");
            await ProcessPoolsAsync(IBatchPool.ServiceKind.RemoveNodeIfIdle, stoppingToken).ConfigureAwait(false);
            logger.LogInformation("RemoveNodeIfIdle BatchPool task gracefully stopped.");
        }

        private async Task RotateAsync(CancellationToken stoppingToken)
        {
            logger.LogInformation("Rotate BatchPool task started.");
            await ProcessPoolsAsync(IBatchPool.ServiceKind.Rotate, stoppingToken).ConfigureAwait(false);
            logger.LogInformation("Rotate BatchPool task gracefully stopped.");
        }

        private async Task RemovePoolIfEmptyAsync(CancellationToken stoppingToken)
        {
            logger.LogInformation("RemovePoolIfEmpty BatchPool task started.");
            await ProcessPoolsAsync(IBatchPool.ServiceKind.RemovePoolIfEmpty, stoppingToken).ConfigureAwait(false);
            logger.LogInformation("RemovePoolIfEmpty BatchPool task gracefully stopped.");
        }

        private async Task ProcessPoolsAsync(IBatchPool.ServiceKind service, CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await Task.WhenAll(batchPools.ManagedBatchPools.Values.SelectMany(q => q).Select(p => p.ServicePoolAsync(service, stoppingToken)).ToArray());
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
