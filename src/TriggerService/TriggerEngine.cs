// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading.Tasks;
using Common;
using Microsoft.Extensions.Logging;

namespace TriggerService
{
    public class TriggerEngine
    {
        private static readonly TimeSpan availabilityWaitTime = TimeSpan.FromSeconds(30);
        private readonly ILogger logger;
        private readonly CromwellOnAzureEnvironment environment;

        public TriggerEngine(ILoggerFactory loggerFactory, CromwellOnAzureEnvironment environment)
        {
            logger = loggerFactory.CreateLogger<TriggerEngine>();
            this.environment = environment;
        }

        public async Task RunAsync()
        {
            logger.LogInformation("Trigger Service successfully started.");

            await Task.WhenAll(
                UpdateExistingWorkflowsContinuouslyAsync(),
                ProcessAndAbortWorkflowsContinuouslyAsync());
        }

        public async Task UpdateExistingWorkflowsContinuouslyAsync()
        {
            while (true)
            {
                try
                {
                    await Task.WhenAll(
                        WaitForCromwellToBecomeAvailableAsync(),
                        WaitForAzureStorageToBecomeAvailableAsync());

                    await environment.UpdateExistingWorkflowsAsync();

                    await Task.Delay(TimeSpan.FromSeconds(20));
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, "UpdateExistingWorkflowsContinuouslyAsync exception");
                    await Task.Delay(TimeSpan.FromSeconds(1));
                }
            }
        }

        public async Task ProcessAndAbortWorkflowsContinuouslyAsync()
        {
            while (true)
            {
                try
                {
                    await Task.WhenAll(
                        WaitForCromwellToBecomeAvailableAsync(),
                        WaitForAzureStorageToBecomeAvailableAsync());

                    await environment.ProcessAndAbortWorkflowsAsync();

                    await Task.Delay(TimeSpan.FromSeconds(20));
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, "ProcessAndAbortWorkflowsContinuouslyAsync exception");
                    await Task.Delay(TimeSpan.FromSeconds(1));
                }
            }
        }

        private async Task WaitForCromwellToBecomeAvailableAsync()
        {
            var haveLoggedOnce = false;

            while (!await environment.IsCromwellAvailableAsync())
            {
                if (!haveLoggedOnce)
                {
                    logger.LogInformation($"Waiting {availabilityWaitTime.TotalSeconds:n0}s for Cromwell to become available...");
                    haveLoggedOnce = true;
                }

                await Task.Delay(availabilityWaitTime);
            }

            logger.LogInformation(Constants.CromwellIsAvailableMessage);
        }

        private async Task WaitForAzureStorageToBecomeAvailableAsync()
        {
            var isAzureStorageAvailable = await environment.IsAzureStorageAvailableAsync();

            if (isAzureStorageAvailable)
            {
                return;
            }

            var haveLoggedOnce = false;

            while (!isAzureStorageAvailable)
            {
                if (!haveLoggedOnce)
                {
                    logger.LogInformation($"Waiting {availabilityWaitTime.TotalSeconds:n0}s for Azure Storage to become available...");
                    haveLoggedOnce = true;
                }

                await Task.Delay(availabilityWaitTime);
                isAzureStorageAvailable = await environment.IsAzureStorageAvailableAsync();
            }

            logger.LogInformation($"Azure Storage is available.");
        }
    }
}

