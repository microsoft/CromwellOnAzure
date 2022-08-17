// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading;
using System.Threading.Tasks;
using LazyCache;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TesApi.Web
{
    /// <summary>
    /// Background service to refresh VM list cache
    /// </summary>
    public class RefreshVMSizesAndPricesHostedService : BackgroundService
    {
        private static readonly TimeSpan listRefreshInterval = TimeSpan.FromDays(1);

        private readonly AzureProxy azureProxy;
        private readonly IAppCache cache;
        private readonly ILogger<RefreshVMSizesAndPricesHostedService> logger;

        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="azureProxy"><see cref="AzureProxy"/></param>
        /// <param name="cache">Lazy cache using <see cref="IAppCache"/></param>
        /// <param name="logger"><see cref="ILogger"/> instance</param>
        public RefreshVMSizesAndPricesHostedService(AzureProxy azureProxy, IAppCache cache, ILogger<RefreshVMSizesAndPricesHostedService> logger)
        {
            this.azureProxy = azureProxy;
            this.cache = cache;
            this.logger = logger;
        }

        /// <inheritdoc />
        public override Task StopAsync(CancellationToken cancellationToken)
        {
            logger.LogInformation("VM list refresh task stopping...");
            return base.StopAsync(cancellationToken);
        }

        /// <summary>
        /// The VM list cache refresh service that runs every day to get new sizes and prices
        /// </summary>
        /// <param name="stoppingToken">Triggered when Microsoft.Extensions.Hosting.IHostedService.StopAsync(System.Threading.CancellationToken) is called.</param>
        /// <returns>A System.Threading.Tasks.Task that represents the long running operations.</returns>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            logger.LogInformation("VM list cache refresh background service started.");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await RefreshVMSizesAndPricesAsync();
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, exc.Message);
                }

                try
                {
                    await Task.Delay(listRefreshInterval, stoppingToken);
                }
                catch (TaskCanceledException)
                {
                    break;
                }
            }

            logger.LogInformation("VM list refresh task gracefully stopped.");
        }

        private async Task RefreshVMSizesAndPricesAsync()
        {
            logger.LogInformation("VM list cache refresh call to Azure started.");
            var virtualMachineInfos = await azureProxy.GetVmSizesAndPricesAsync();
            cache.Add("vmSizesAndPrices", virtualMachineInfos, DateTimeOffset.MaxValue);
            logger.LogInformation("VM list cache refresh call to Azure completed.");
        }
    }
}
