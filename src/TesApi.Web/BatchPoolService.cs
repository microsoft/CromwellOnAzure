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
        /// <summary>
        /// Interval between calls to <seealso cref="IBatchPool.ServicePoolAsync(IBatchPool.ServiceKind, CancellationToken)"/>(<see cref="IBatchPool.ServiceKind.Resize"/>).
        /// </summary>
        public static TimeSpan ResizeInterval => resizeInterval;

        /// <summary>
        /// Interval between calls to <seealso cref="IBatchPool.ServicePoolAsync(IBatchPool.ServiceKind, CancellationToken)"/>(<see cref="IBatchPool.ServiceKind.RemoveNodeIfIdle"/>).
        /// </summary>
        public static TimeSpan RemoveNodeIfIdleInterval => removeNodeIfIdleInterval;

        /// <summary>
        /// Interval between calls to <seealso cref="IBatchPool.ServicePoolAsync(IBatchPool.ServiceKind, CancellationToken)"/>(<see cref="IBatchPool.ServiceKind.Rotate"/>).
        /// </summary>
        public static TimeSpan RotateInterval => rotateInterval;

        /// <summary>
        /// Interval between calls to <seealso cref="IBatchPool.ServicePoolAsync(IBatchPool.ServiceKind, CancellationToken)"/>(<see cref="IBatchPool.ServiceKind.RemovePoolIfEmpty"/>).
        /// </summary>
        public static TimeSpan RemovePoolIfEmptyInterval => removePoolIfEmptyInterval;

        // These initial values are for tests. As this service is expected to be a singleton, the constructor resets all these intervals to their production values.
        private static TimeSpan resizeInterval = TimeSpan.FromSeconds(1);
        private static TimeSpan removeNodeIfIdleInterval = TimeSpan.FromSeconds(2);
        private static TimeSpan rotateInterval = TimeSpan.FromSeconds(3);
        private static TimeSpan removePoolIfEmptyInterval = TimeSpan.FromSeconds(1);

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
            resizeInterval = TimeSpan.FromMinutes(configuration.GetValue<double>("BatchPoolResizeInterval", 0.5)); // TODO: set this to an appropriate value
            removeNodeIfIdleInterval = TimeSpan.FromMinutes(configuration.GetValue<double>("BatchPoolRemoveNodeIfIdleInterval", 2)); // TODO: set this to an appropriate value
            rotateInterval = TimeSpan.FromDays(configuration.GetValue<double>("BatchPoolRotateInterval", 0.25)); // TODO: set this to an appropriate value
            removePoolIfEmptyInterval = TimeSpan.FromMinutes(configuration.GetValue<double>("BatchPoolRemovePoolIfEmptyInterval", 60)); // TODO: set this to an appropriate value
        }

        /// <inheritdoc />
        public override Task StartAsync(CancellationToken cancellationToken)
            => isDisabled
                ? Task.CompletedTask
                : base.StartAsync(cancellationToken);

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
            logger.LogInformation("BatchPool Resize task started.");
            await ProcessPoolsAsync(IBatchPool.ServiceKind.Resize, ResizeInterval, stoppingToken).ConfigureAwait(false);
            logger.LogInformation("BatchPool Resize task gracefully stopped.");
        }

        private async Task RemoveNodeIfIdleAsync(CancellationToken stoppingToken)
        {
            logger.LogInformation("BatchPool RemoveNodeIfIdle task started.");
            await ProcessPoolsAsync(IBatchPool.ServiceKind.RemoveNodeIfIdle, RemoveNodeIfIdleInterval, stoppingToken).ConfigureAwait(false);
            logger.LogInformation("BatchPool RemoveNodeIfIdle task gracefully stopped.");
        }

        private async Task RotateAsync(CancellationToken stoppingToken)
        {
            logger.LogInformation("BatchPool Rotate task started.");
            await ProcessPoolsAsync(IBatchPool.ServiceKind.Rotate, RotateInterval, stoppingToken).ConfigureAwait(false);
            logger.LogInformation("BatchPool Rotate task gracefully stopped.");
        }

        private async Task RemovePoolIfEmptyAsync(CancellationToken stoppingToken)
        {
            logger.LogInformation("BatchPool RemovePoolIfEmpty task started.");
            await ProcessPoolsAsync(IBatchPool.ServiceKind.RemovePoolIfEmpty, RemovePoolIfEmptyInterval, stoppingToken).ConfigureAwait(false);
            logger.LogInformation("BatchPool RemovePoolIfEmpty task gracefully stopped.");
        }

        private async Task ProcessPoolsAsync(IBatchPool.ServiceKind service, TimeSpan processInterval, CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                logger.LogTrace("BatchPool Calling {ServiceKind}", service);
                try
                {
                    await Task.WhenAll(batchPools.ManagedBatchPools.Values.SelectMany(q => q).Select(p => p.ServicePoolAsync(service, stoppingToken)).ToArray());
                }
                catch (AggregateException ex)
                {
                    foreach (var e in ex.InnerExceptions)
                    {
                        if (e switch
                            {
                                var x when x is OperationCanceledException && stoppingToken.IsCancellationRequested => false,
                                _ => true,
                            })
                        {
                            LogError(e);
                        }
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    LogError(ex);
                }
                logger.LogTrace("BatchPool Finished {ServiceKind}", service);

                try
                {
                    await Task.Delay(processInterval, stoppingToken);
                }
                catch (TaskCanceledException)
                {
                    break;
                }
            }

            void LogError(Exception e)
                => logger.LogError(e, @"Failure in BatchPool {Task}: {Message}", service, e.Message);
        }
    }
}
