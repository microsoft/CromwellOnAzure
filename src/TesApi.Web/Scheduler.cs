// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TesApi.Models;

namespace TesApi.Web
{
    /// <summary>
    /// A background service that schedules TES tasks in the batch system, orchestrates their lifecycle, and updates their state.
    /// This should only be used as a system-wide singleton service.  This class does not support scale-out on multiple machines,
    /// nor does it implement a leasing mechanism.  In the future, consider using the Lease Blob operation.
    /// </summary>
    public class Scheduler : IHostedService
    {
        private readonly IRepository<TesTask> repository;
        private readonly IBatchScheduler batchScheduler;
        private readonly ILogger<Scheduler> logger;
        private readonly CancellationTokenSource mainProcess = new CancellationTokenSource();
        private bool isStopped;
        private readonly TimeSpan runInterval = TimeSpan.FromSeconds(5);

        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="configuration">The configuration instance settings</param>
        /// <param name="repository">The main TES task database repository implementation</param>
        /// <param name="batchScheduler">The batch scheduler implementation</param>
        /// <param name="logger">The logger instance</param>
        public Scheduler(IConfiguration configuration, IRepository<TesTask> repository, IBatchScheduler batchScheduler, ILogger<Scheduler> logger)
        {
            this.repository = repository;
            this.batchScheduler = batchScheduler;
            this.logger = logger;
            isStopped = bool.TryParse(configuration["DisableBatchScheduling"], out var disableBatchScheduling) ? disableBatchScheduling : false;
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
        /// Attempt to gracefully stop the service
        /// </summary>
        /// <param name="cancellationToken">The cancellation token to stop waiting for graceful exit</param>
        public async Task StopAsync(CancellationToken cancellationToken)
        {
            mainProcess.Cancel();
            logger.LogInformation("Scheduler stopping...");

            while (!cancellationToken.IsCancellationRequested && !isStopped)
            {
                await Task.Delay(100);
            }

            logger.LogInformation("Scheduler gracefully stopped.");
        }

        /// <summary>
        /// The main thread that continuously schedules TES tasks in the batch system
        /// </summary>
        private async Task RunAsync()
        {
            logger.LogInformation("Scheduler started.");

            while (!mainProcess.IsCancellationRequested)
            {
                try
                {
                    await OrchestrateTesTasksOnBatch();
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, exc.Message);
                }

                await Task.Delay(this.runInterval);
            }

            isStopped = true;
        }

        /// <summary>
        /// Retrieves all actionable TES tasks from the database, performs an action in the batch system, and updates the resultant state
        /// </summary>
        /// <returns></returns>
        private async Task OrchestrateTesTasksOnBatch()
        {
            var tesTasks = await repository.GetItemsAsync(
                    predicate: t => t.State == TesState.QUEUEDEnum
                        || t.State == TesState.INITIALIZINGEnum
                        || t.State == TesState.RUNNINGEnum
                        || (t.State == TesState.CANCELEDEnum && t.IsCancelRequested));

            foreach (var tesTask in tesTasks)
            {
                try
                {
                    var isModified = await batchScheduler.ProcessTesTaskAsync(tesTask.Value);

                    if (isModified)
                    {
                        //task has transitioned
                        if (tesTask.Value.State == TesState.CANCELEDEnum
                           || tesTask.Value.State == TesState.COMPLETEEnum
                           || tesTask.Value.State == TesState.EXECUTORERROREnum
                           || tesTask.Value.State == TesState.SYSTEMERROREnum)
                        {
                            tesTask.Value.EndTime = DateTime.UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss.fffzzz", DateTimeFormatInfo.InvariantInfo);

                            if (tesTask.Value.State == TesState.EXECUTORERROREnum || tesTask.Value.State == TesState.SYSTEMERROREnum)
                            {
                                logger.LogDebug($"{tesTask.Value.Id} is in a terminal state: {tesTask.Value.State}");
                            }
                        }

                        await repository.UpdateItemAsync(tesTask.Value.Id, new RepositoryItem<TesTask> { ETag = tesTask.ETag, Value = tesTask.Value });
                    }
                }
                catch (Exception exc)
                {
                    if (++tesTask.Value.ErrorCount > 3) // TODO: Should we increment this for exceptions here (current behaviour) or the attempted executions on the batch?
                    {
                        tesTask.Value.State = TesState.SYSTEMERROREnum;
                        tesTask.Value.EndTime = DateTime.UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss.fffzzz", DateTimeFormatInfo.InvariantInfo);
                        tesTask.Value.WriteToSystemLog(exc.Message, exc.StackTrace);
                    }

                    logger.LogError(exc, $"TES Task '{tesTask.Value.Id}' threw an exception.");
                    await repository.UpdateItemAsync(tesTask.Value.Id, tesTask);
                }
            }
        }
    }
}
