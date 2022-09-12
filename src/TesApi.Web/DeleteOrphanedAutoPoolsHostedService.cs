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
    /// Background service to delete Batch auto pools that do not have the corresponding Batch job.
    /// This happens in rare cases when auto pool job is rapidly created and deleted but the pool continues with creation,
    /// resulting in an active pool with a single node that is not attached to a job.
    /// </summary>
    public class DeleteOrphanedAutoPoolsHostedService : BackgroundService
    {
        private static readonly TimeSpan runInterval = TimeSpan.FromHours(1);
        private static readonly TimeSpan minPoolAge = TimeSpan.FromMinutes(30);
        private static readonly string autoPoolIdPrefix = "TES_";
        private readonly IAzureProxy azureProxy;
        private readonly ILogger<DeleteOrphanedAutoPoolsHostedService> logger;
        private readonly bool isDisabled;

        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="configuration">The configuration instance settings</param>
        /// <param name="azureProxy">Azure Proxy</param>
        /// <param name="logger">The logger instance</param>
        public DeleteOrphanedAutoPoolsHostedService(IConfiguration configuration, IAzureProxy azureProxy, ILogger<DeleteOrphanedAutoPoolsHostedService> logger)
        {
            this.azureProxy = azureProxy;
            this.logger = logger;
            this.isDisabled = !configuration.GetValue("UseLegacyBatchImplementationWithAutopools", false);
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
        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            logger.LogInformation("Orphaned pool cleanup service started");

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await DeleteOrphanedAutoPoolsAsync(cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex) when (!(ex is OperationCanceledException && cancellationToken.IsCancellationRequested))
                {
                    logger.LogError(ex, ex.Message);
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
        }

        private async Task DeleteOrphanedAutoPoolsAsync(CancellationToken cancellationToken)
        {
            var activePoolIds = (await azureProxy.GetActivePoolIdsAsync(autoPoolIdPrefix, minPoolAge, cancellationToken)).ToList();

            if (activePoolIds.Any())
            {
                var poolIdsReferencedByJobs = (await azureProxy.GetPoolIdsReferencedByJobsAsync(cancellationToken)).ToList();

                var orphanedPoolIds = activePoolIds.Except(poolIdsReferencedByJobs);

                foreach (var orphanedPoolId in orphanedPoolIds)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    logger.LogInformation($"Deleting orphanded pool {orphanedPoolId}, since no jobs reference it.");
                    await azureProxy.DeleteBatchPoolAsync(orphanedPoolId, cancellationToken);
                }
            }
        }
    }
}
