// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Tes.Extensions;
using Tes.Models;
using Tes.Repository;

namespace TesApi.Web
{
    /// <summary>
    /// A background service that schedules TES tasks in the batch system, orchestrates their lifecycle, and updates their state.
    /// This should only be used as a system-wide singleton service.  This class does not support scale-out on multiple machines,
    /// nor does it implement a leasing mechanism.  In the future, consider using the Lease Blob operation.
    /// </summary>
    public class Scheduler : BackgroundService
    {
        private readonly IRepository<TesTask> repository;
        private readonly IBatchScheduler batchScheduler;
        private readonly ILogger<Scheduler> logger;
        private readonly bool isDisabled;
        private readonly bool usingBatchPools;
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
            isDisabled = configuration.GetValue("DisableBatchScheduling", false);
            usingBatchPools = !configuration.GetValue("BatchAutopool", false);
        }

        /// <summary>
        /// Start the service
        /// </summary>
        /// <param name="cancellationToken">Not used</param>
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
            logger.LogInformation("Scheduler stopping...");
            return base.StopAsync(cancellationToken);
        }

        /// <summary>
        /// The main thread that continuously schedules TES tasks in the batch system
        /// </summary>
        /// <param name="stoppingToken">Triggered when Microsoft.Extensions.Hosting.IHostedService.StopAsync(System.Threading.CancellationToken) is called.</param>
        /// <returns>A System.Threading.Tasks.Task that represents the long running operations.</returns>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            logger.LogInformation("Scheduler started.");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    if (usingBatchPools)
                    {
                        await UpdateBatchPools(stoppingToken);
                    }

                    await OrchestrateTesTasksOnBatch(stoppingToken);

                    if (usingBatchPools)
                    {
                        await ServiceBatchPools(stoppingToken);
                        await UpdateBatchPools(stoppingToken);
                    }

                    await Task.Delay(runInterval, stoppingToken);
                }
                catch (TaskCanceledException)
                {
                    break;
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, exc.Message);
                }
            }

            logger.LogInformation("Scheduler gracefully stopped.");
        }

        /// <summary>
        /// Retrieves all actionable TES tasks from the database, performs an action in the batch system, and updates the resultant state
        /// </summary>
        /// <returns></returns>
        private async Task OrchestrateTesTasksOnBatch(CancellationToken _1)
        {
            var tesTasks = (await repository.GetItemsAsync(
                    predicate: t => t.State == TesState.QUEUEDEnum
                        || t.State == TesState.INITIALIZINGEnum
                        || t.State == TesState.RUNNINGEnum
                        || (t.State == TesState.CANCELEDEnum && t.IsCancelRequested)))
                .ToList();

            if (!tesTasks.Any())
            {
                return;
            }

            var startTime = DateTime.UtcNow;

            foreach (var tesTask in tesTasks)
            {
                try
                {
                    var isModified = false;
                    try
                    {
                        isModified = await batchScheduler.ProcessTesTaskAsync(tesTask);
                    }
                    catch (Exception exc)
                    {
                        if (++tesTask.ErrorCount > 3) // TODO: Should we increment this for exceptions here (current behaviour) or the attempted executions on the batch?
                        {
                            tesTask.State = TesState.SYSTEMERROREnum;
                            tesTask.EndTime = DateTimeOffset.UtcNow;
                            tesTask.SetFailureReason("UnknownError", exc.Message, exc.StackTrace);
                        }

                        logger.LogError(exc, $"TES Task '{tesTask.Id}' threw an exception.");
                        await repository.UpdateItemAsync(tesTask);
                    }

                    if (isModified)
                    {
                        //task has transitioned
                        if (tesTask.State == TesState.CANCELEDEnum
                           || tesTask.State == TesState.COMPLETEEnum
                           || tesTask.State == TesState.EXECUTORERROREnum
                           || tesTask.State == TesState.SYSTEMERROREnum)
                        {
                            tesTask.EndTime = DateTimeOffset.UtcNow;

                            if (tesTask.State == TesState.EXECUTORERROREnum || tesTask.State == TesState.SYSTEMERROREnum)
                            {
                                logger.LogDebug($"{tesTask.Id} failed, state: {tesTask.State}, reason: {tesTask.FailureReason}");
                            }
                        }

                        await repository.UpdateItemAsync(tesTask);
                    }
                }
                catch (Microsoft.Azure.Cosmos.CosmosException exc)
                {
                    var currentTesTask = await repository.GetItemOrDefaultAsync(tesTask.Id);
                    if (exc.StatusCode == System.Net.HttpStatusCode.PreconditionFailed)
                    {
                        logger.LogError(exc, $"Updating TES Task '{tesTask.Id}' threw an exception attempting to set state: {tesTask.State}. Another actor had set current state: {currentTesTask?.State}");
                        currentTesTask?.SetWarning("ConcurrencyWriteFailure", tesTask.State.ToString(), exc.Message, exc.StackTrace);
                    }
                    else
                    {
                        logger.LogError(exc, $"Updating TES Task '{tesTask.Id}' threw {exc.GetType().FullName}: '{exc.Message}'. Stack trace: {exc.StackTrace}");
                        currentTesTask?.SetWarning("UnknownError", exc.Message, exc.StackTrace);
                    }

                    if (currentTesTask is not null)
                    {
                        await repository.UpdateItemAsync(currentTesTask);
                    }
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, $"Updating TES Task '{tesTask.Id}' threw {exc.GetType().FullName}: '{exc.Message}'. Stack trace: {exc.StackTrace}");
                }
            }

            logger.LogDebug($"OrchestrateTesTasksOnBatch for {tesTasks.Count} tasks completed in {DateTime.UtcNow.Subtract(startTime).TotalSeconds} seconds.");
        }

        /// <summary>
        /// Retrieves all batch pools from the database and prepares <see cref="BatchScheduler"/>'s metadata for each pool.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        private async Task UpdateBatchPools(CancellationToken cancellationToken)
        {
            var startTime = DateTime.UtcNow;

            try
            {
                if (!await batchScheduler.UpdateBatchPools(cancellationToken))
                {
                    return;
                }
            }
            catch (Exception exc)
            {
                logger.LogError(exc, "UpdateBatchPools threw an exception.");
            }

            logger.LogDebug($"UpdateBatchPools completed in {DateTime.UtcNow.Subtract(startTime).TotalSeconds} seconds.");
        }

        /// <summary>
        /// Retrieves all batch pools from the database and affords an opportunity to react to changes.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        private async Task ServiceBatchPools(CancellationToken cancellationToken)
        {
            var pools = await batchScheduler.GetPoolsAsync().ToListAsync(cancellationToken);

            if (0 == pools.Count)
            {
                logger.LogTrace("No pools found.");
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
                    logger.LogError(exc, "Batch pool {PoolId} threw an exception.", pool.Pool?.PoolId);
                }
            }

            logger.LogDebug($"ServiceBatchPools for {pools.Count} pools completed in {DateTime.UtcNow.Subtract(startTime).TotalSeconds} seconds.");
        }
    }
}
