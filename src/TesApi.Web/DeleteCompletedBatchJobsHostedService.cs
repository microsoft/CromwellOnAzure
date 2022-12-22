// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Tes.Models;
using Tes.Repository;

namespace TesApi.Web
{
    /// <summary>
    /// Background service to delete Batch jobs older than seven days for completed tasks
    /// </summary>
    public class DeleteCompletedBatchJobsHostedService : BackgroundService
    {
        private static readonly TimeSpan oldestJobAge = TimeSpan.FromDays(7);
        private readonly IRepository<TesTask> repository;
        private readonly IAzureProxy azureProxy;
        private readonly ILogger<DeleteCompletedBatchJobsHostedService> logger;
        private readonly bool isDisabled;

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
            this.isDisabled = configuration.GetValue("DisableBatchJobCleanup", false);
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

        /// <inheritdoc />
        public override Task StopAsync(CancellationToken cancellationToken)
        {
            logger.LogInformation("Batch Job cleanup stopping...");
            return base.StopAsync(cancellationToken);
        }

        /// <summary>
        /// The job clean up service that checks for old jobs on the Batch account that are safe to delete
        /// </summary>
        /// <param name="cancellationToken">Triggered when Microsoft.Extensions.Hosting.IHostedService.StopAsync(System.Threading.CancellationToken) is called.</param>
        /// <returns>A System.Threading.Tasks.Task that represents the long running operations.</returns>
        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            var runInterval = TimeSpan.FromDays(1);
            logger.LogInformation("Batch Job cleanup started.");

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await DeleteOldBatchJobs(cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, exc.Message);
                }

                try
                {
                    await Task.Delay(runInterval, cancellationToken);
                }
                catch (TaskCanceledException)
                {
                    break;
                }
            }

            logger.LogInformation("Batch Job cleanup gracefully stopped.");
        }

        private async Task DeleteOldBatchJobs(CancellationToken cancellationToken) // TODO: implement
        {
            var jobsToDelete = await azureProxy.ListOldJobsToDeleteAsync(oldestJobAge);

            foreach (var jobId in jobsToDelete)
            {
                var tesTaskId = jobId.Split(new[] { '-' })[0];
                logger.LogInformation($"TES task: {tesTaskId} deleting Batch Job ID: {jobId}");

                TesTask tesTask = null;

                if (await repository.TryGetItemAsync(tesTaskId, item => tesTask = item))
                {
                    if (tesTask.State == TesState.COMPLETEEnum ||
                        tesTask.State == TesState.EXECUTORERROREnum ||
                        tesTask.State == TesState.SYSTEMERROREnum ||
                        tesTask.State == TesState.CANCELEDEnum ||
                        tesTask.State == TesState.UNKNOWNEnum)
                    {
                        await azureProxy.DeleteBatchJobAsync(tesTaskId, cancellationToken);

                        try
                        {
                            await azureProxy.DeleteBatchPoolIfExistsAsync(tesTaskId, cancellationToken);
                        }
                        catch (Exception exc)
                        {
                            logger.LogError(exc, $"TES task: {tesTaskId} Exception in DeleteOldBatchJobs when attempting to delete the manual batch pool");
                            // Do not rethrow
                        }
                    }
                }
            }
        }
    }
}
