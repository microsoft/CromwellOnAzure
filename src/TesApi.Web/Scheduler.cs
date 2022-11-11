// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
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
        private readonly bool usingBatchAutopools;
        private IEnumerable<Task> shutdownCandidates = Enumerable.Empty<Task>();
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
            usingBatchAutopools = configuration.GetValue("UseLegacyBatchImplementationWithAutopools", false);
        }

        /// <summary>
        /// Start the service
        /// </summary>
        /// <param name="cancellationToken"></param>
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
                shutdownCandidates = Enumerable.Empty<Task>();

                try
                {
                    await OrchestrateTesTasksOnBatch(stoppingToken);

                    if (!usingBatchAutopools)
                    {
                        shutdownCandidates = await batchScheduler.GetShutdownCandidatePools(stoppingToken);
                    }
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
                    await Task.Delay(runInterval, stoppingToken);
                }
                catch (TaskCanceledException)
                {
                    break;
                }
            }

            try
            {
                // Quickly delete pools without jobs on a best-effort basis since TES is shutting down. This was first
                // implemented for integration tests sharing a batch account that is not created anew for testing.
                //
                // We don't generate the pool list here because of possible delays in communicating with the Batch API
                // coupled with the fact that very little change of state is likely to have occured since the end of
                // the last call to OrchestrateTesTasksOnBatch().
                // The trade-off is that some newly job-emptied pools may not be included in this pool clean-out.
                await Task.WhenAll(shutdownCandidates);
            }
            catch (AggregateException exc)
            {
                logger.LogError(exc, exc.Message);
            }

            logger.LogInformation("Scheduler gracefully stopped.");
        }

        /// <summary>
        /// Retrieves all actionable TES tasks from the database, performs an action in the batch system, and updates the resultant state
        /// </summary>
        /// <returns></returns>
        private async ValueTask OrchestrateTesTasksOnBatch(CancellationToken stoppingToken)
        {
            var pools = new HashSet<string>();

            var tesTasks = (await repository.GetItemsAsync(
                    predicate: t => t.State == TesState.QUEUEDEnum
                        || t.State == TesState.INITIALIZINGEnum
                        || t.State == TesState.RUNNINGEnum
                        || (t.State == TesState.CANCELEDEnum && t.IsCancelRequested)))
                .OrderBy(t => t.CreationTime)
                .ToList();

            if (0 == tesTasks.Count)
            {
                batchScheduler.ClearBatchLogState();
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

                        logger.LogError(exc, $"TES task: {tesTask.Id} threw an exception in OrchestrateTesTasksOnBatch().");
                        await repository.UpdateItemAsync(tesTask);
                    }

                    if (isModified)
                    {
                        var hasErrored = false;
                        var hasEnded = false;

                        switch (tesTask.State)
                        {
                            case TesState.CANCELEDEnum:
                            case TesState.COMPLETEEnum:
                                hasEnded = true;
                                break;

                            case TesState.EXECUTORERROREnum:
                            case TesState.SYSTEMERROREnum:
                                hasErrored = true;
                                hasEnded = true;
                                break;

                            default:
                                break;
                        }

                        if (hasEnded)
                        {
                            tesTask.EndTime = DateTimeOffset.UtcNow;
                        }

                        if (hasErrored)
                        {
                            logger.LogDebug($"{tesTask.Id} failed, state: {tesTask.State}, reason: {tesTask.FailureReason}");
                        }

                        await repository.UpdateItemAsync(tesTask);
                    }
                }
                catch (Microsoft.Azure.Cosmos.CosmosException exc)
                {
                    TesTask currentTesTask = default;
                    _ = await repository.TryGetItemAsync(tesTask.Id, t => currentTesTask = t);

                    if (exc.StatusCode == System.Net.HttpStatusCode.PreconditionFailed)
                    {
                        logger.LogError(exc, $"Updating TES Task '{tesTask.Id}' threw an exception attempting to set state: {tesTask.State}. Another actor set state: {currentTesTask?.State}");
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

                if (!string.IsNullOrWhiteSpace(tesTask.PoolId) && (TesState.QUEUEDEnum == tesTask.State || TesState.RUNNINGEnum == tesTask.State))
                {
                    pools.Add(tesTask.PoolId);
                }
            }

            if (batchScheduler.NeedPoolFlush)
            {
                await batchScheduler.FlushPoolsAsync(pools, stoppingToken);
            }

            logger.LogDebug($"OrchestrateTesTasksOnBatch for {tesTasks.Count} tasks completed in {DateTime.UtcNow.Subtract(startTime).TotalSeconds} seconds.");
        }
    }
}
