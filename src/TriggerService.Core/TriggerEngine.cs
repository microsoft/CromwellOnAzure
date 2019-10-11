// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace TriggerService.Core
{
    public class TriggerEngine
    {
        private static readonly TimeSpan availabilityWaitTime = TimeSpan.FromSeconds(30);
        private readonly ILogger logger;
        private readonly Lab lab;

        public TriggerEngine(ILoggerFactory loggerFactory, Lab lab)
        {
            logger = loggerFactory.CreateLogger<TriggerEngine>();
            this.lab = lab;
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

                    await lab.UpdateExistingWorkflowsAsync();

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

                    await lab.ProcessAndAbortWorkflowsAsync();

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
            var isCromwellAvailable = await lab.IsCromwellAvailableAsync();

            if (isCromwellAvailable)
            {
                return;
            }

            var haveLoggedOnce = false;

            while (!isCromwellAvailable)
            {
                if (!haveLoggedOnce)
                {
                    logger.LogInformation($"Waiting {availabilityWaitTime.TotalSeconds:n0}s for Cromwell to become available...");
                    haveLoggedOnce = true;
                }

                await Task.Delay(availabilityWaitTime);
                isCromwellAvailable = await lab.IsCromwellAvailableAsync();
            }

            logger.LogInformation($"Cromwell is available.");
        }

        private async Task WaitForAzureStorageToBecomeAvailableAsync()
        {
            var isAzureStorageAvailable = await lab.IsAzureStorageAvailableAsync();

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
                isAzureStorageAvailable = await lab.IsAzureStorageAvailableAsync();
            }

            logger.LogInformation($"Azure Storage is available.");
        }
    }
}

