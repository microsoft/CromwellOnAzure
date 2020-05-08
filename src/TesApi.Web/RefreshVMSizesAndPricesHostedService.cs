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
    public class RefreshVMSizesAndPricesHostedService : IHostedService
    {
        private static readonly TimeSpan listRefreshInterval = TimeSpan.FromDays(1);

        private readonly AzureProxy azureProxy;
        private readonly IAppCache cache;
        private readonly ILogger<RefreshVMSizesAndPricesHostedService> logger;
        private readonly CancellationTokenSource vmListRefreshService = new CancellationTokenSource();

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

        /// <summary>
        /// Start the service
        /// </summary>
        /// <param name="cancellationToken">Not used</param>
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            await Task.Factory.StartNew(() => RunAsync(), TaskCreationOptions.LongRunning);
        }

        /// <summary>
        /// Attempt to gracefully stop the service.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token to stop waiting for graceful exit</param>
        public async Task StopAsync(CancellationToken cancellationToken)
        {
            vmListRefreshService.Cancel();
            logger.LogInformation("VM list refresh task stopping...");

            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(100);
            }

            logger.LogInformation("VM list refresh task gracefully stopped.");
        }

        /// <summary>
        /// The VM list cache refresh service that runs every day to get new sizes and prices
        /// </summary>
        private async Task RunAsync()
        {
            logger.LogInformation("VM list cache refresh background service started.");

            while (!vmListRefreshService.IsCancellationRequested)
            {
                try
                {
                    await RefreshVMListAsync();
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, exc.Message);
                }

                await Task.Delay(listRefreshInterval);
            }
        }

        private async Task RefreshVMListAsync()
        {
            const string key = "vmSizesAndPrices";

            logger.LogInformation("VM list cache refresh call to Azure started.");
            var virtualMachineInfos = await azureProxy.GetVmSizesAndPricesAsync();
            cache.Add(key, virtualMachineInfos);
            logger.LogInformation("VM list cache refresh call to Azure completed.");
        }
    }
}
