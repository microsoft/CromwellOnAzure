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
    /// 
    /// </summary>
    public class BatchPoolService : BackgroundService
    {
        private static readonly TimeSpan resizeProcessInterval = TimeSpan.FromMinutes(1); // TODO: set this to an appropriate value
        private static readonly TimeSpan removeNodeIfIdleProcessInterval = TimeSpan.FromMinutes(2); // TODO: set this to an appropriate value
        private static readonly TimeSpan rotateProcessInterval = TimeSpan.FromMinutes(30); // TODO: set this to an appropriate value
        private static readonly TimeSpan removePoolIfEmptyProcessInterval = TimeSpan.FromMinutes(5); // TODO: set this to an appropriate value

        private readonly ILogger logger;
        private readonly IBatchPoolsImpl batchPools;
        private readonly bool isDisabled;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="configuration">The configuration instance settings</param>
        /// <param name="batchPools"></param>
        /// <param name="logger"><see cref="ILogger"/> instance</param>
        public BatchPoolService(IConfiguration configuration, IBatchPools batchPools, ILogger<BatchPoolService> logger)
        {
            this.batchPools = (IBatchPoolsImpl)batchPools;
            this.logger = logger;
            this.isDisabled = configuration.GetValue("BatchAutopool", false);
        }

        /// <inheritdoc />
        public override Task StartAsync(CancellationToken cancellationToken)
        {
            if (isDisabled)
            {
                return Task.CompletedTask;
            }

            return base.StartAsync(cancellationToken);
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
            await Task.WhenAll(new[]
            {
                ResizeAsync(stoppingToken),
                RemoveNodeIfIdleAsync(stoppingToken),
                RotateAsync(stoppingToken),
                RemovePoolIfEmptyAsync(stoppingToken)
            }).ConfigureAwait(false);
            logger.LogInformation("BatchPool service gracefully stopped.");
        }

        private async Task ResizeAsync(CancellationToken stoppingToken)
        {
            logger.LogInformation("Resize BatchPool task started.");
            await ProcessPoolsAsync(IBatchPool.ServiceKind.Resize, resizeProcessInterval, stoppingToken).ConfigureAwait(false);
            logger.LogInformation("Resize BatchPool task gracefully stopped.");
        }

        private async Task RemoveNodeIfIdleAsync(CancellationToken stoppingToken)
        {
            logger.LogInformation("RemoveNodeIfIdle BatchPool task started.");
            await ProcessPoolsAsync(IBatchPool.ServiceKind.RemoveNodeIfIdle, removeNodeIfIdleProcessInterval, stoppingToken).ConfigureAwait(false);
            logger.LogInformation("RemoveNodeIfIdle BatchPool task gracefully stopped.");
        }

        private async Task RotateAsync(CancellationToken stoppingToken)
        {
            logger.LogInformation("Rotate BatchPool task started.");
            await ProcessPoolsAsync(IBatchPool.ServiceKind.Rotate, rotateProcessInterval, stoppingToken).ConfigureAwait(false);
            logger.LogInformation("Rotate BatchPool task gracefully stopped.");
        }

        private async Task RemovePoolIfEmptyAsync(CancellationToken stoppingToken)
        {
            logger.LogInformation("RemovePoolIfEmpty BatchPool task started.");
            await ProcessPoolsAsync(IBatchPool.ServiceKind.RemovePoolIfEmpty, removePoolIfEmptyProcessInterval, stoppingToken).ConfigureAwait(false);
            logger.LogInformation("RemovePoolIfEmpty BatchPool task gracefully stopped.");
        }

        private async Task ProcessPoolsAsync(IBatchPool.ServiceKind service, TimeSpan processInterval, CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                logger.LogTrace("Calling {ServiceKind}", service);
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
                    logger.LogError(ex, @"Failure in {Task}: {Message}", service, ex.Message);
                }
                logger.LogTrace("Completed {ServiceKind}", service);

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
