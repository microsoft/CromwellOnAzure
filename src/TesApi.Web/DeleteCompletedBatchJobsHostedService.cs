using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TesApi.Models;

namespace TesApi.Web
{
    /// <summary>
    /// Background service to delete Batch jobs older than seven days for completed tasks
    /// </summary>
    public class DeleteCompletedBatchJobsHostedService : IHostedService
    {
        private static readonly TimeSpan oldestJobAge = TimeSpan.FromDays(7);
        private readonly IRepository<TesTask> repository;
        private readonly IAzureProxy azureProxy;
        private readonly ILogger<DeleteCompletedBatchJobsHostedService> logger;
        private readonly CancellationTokenSource jobCleanupService = new CancellationTokenSource();
        private bool isStopped;

        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="configuration">The configuration instance settings</param>
        /// <param name="azureProxy">Azure Proxy</param>
        /// <param name="repository">The main TES task database repository</param>
        /// <param name="logger">The logger instance</param>
        public DeleteCompletedBatchJobsHostedService(IConfiguration configuration, IAzureProxy azureProxy, IRepository<TesTask> repository, ILogger<DeleteCompletedBatchJobsHostedService> logger)
        {
            this.repository = repository;
            this.azureProxy = azureProxy;
            this.logger = logger;
            isStopped = configuration.GetValue("DisableBatchJobCleanup", false);
        }

        /// <summary>
        /// Start the service
        /// </summary>
        /// <param name="cancellationToken">Not used</param>
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            if (isStopped)
            {
                return;
            }

            await Task.Factory.StartNew(() => RunAsync(), TaskCreationOptions.LongRunning);
        }

        /// <summary>
        /// Attempt to gracefully stop the service.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token to stop waiting for graceful exit</param>
        public async Task StopAsync(CancellationToken cancellationToken)
        {
            jobCleanupService.Cancel();
            logger.LogInformation("Batch Job cleanup stopping...");

            while (!cancellationToken.IsCancellationRequested && !isStopped)
            {
                await Task.Delay(100);
            }

            logger.LogInformation("Batch Job cleanup gracefully stopped.");
        }

        /// <summary>
        /// The job clean up service that checks for old jobs on the Batch account that are safe to delete
        /// </summary>
        private async Task RunAsync()
        {
            var runInterval = TimeSpan.FromDays(1);
            logger.LogInformation("Batch Job cleanup started.");

            while (!jobCleanupService.IsCancellationRequested)
            {
                try
                {
                    await DeleteOldBatchJobs();
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, exc.Message);
                }

                await Task.Delay(runInterval);
            }

            isStopped = true;
        }

        private async Task DeleteOldBatchJobs()
        {
            var jobsToDelete = await azureProxy.ListOldJobsToDeleteAsync(oldestJobAge);

            foreach (var jobId in jobsToDelete)
            {
                logger.LogInformation($"Job Id to delete: {jobId}");

                var tesTaskId = jobId.Split(new[] { '-' })[0];
                logger.LogInformation($"TES task Id to delete: {tesTaskId}");

                TesTask tesTask = null;

                if (await repository.TryGetItemAsync(tesTaskId, item => tesTask = item))
                {
                    if (tesTask.State == TesState.COMPLETEEnum ||
                        tesTask.State == TesState.EXECUTORERROREnum ||
                        tesTask.State == TesState.SYSTEMERROREnum ||
                        tesTask.State == TesState.CANCELEDEnum ||
                        tesTask.State == TesState.UNKNOWNEnum)
                    {
                        await azureProxy.DeleteBatchJobAsync(tesTaskId);
                    }
                }
            }
        }
    }
}
