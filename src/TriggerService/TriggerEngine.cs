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
        private readonly ILogger logger;
        private readonly ICromwellOnAzureEnvironment environment;
        private readonly TimeSpan mainInterval;
        private readonly TimeSpan availabilityCheckInterval;
        private readonly AvailabilityTracker cromwellAvailability = new();
        private readonly AvailabilityTracker azStorageAvailability = new();

        public TriggerEngine(ILoggerFactory loggerFactory, ICromwellOnAzureEnvironment environment, TimeSpan mainInterval, TimeSpan availabilityCheckInterval)
        {
            logger = loggerFactory.CreateLogger<TriggerEngine>();
            this.environment = environment;
            this.mainInterval = mainInterval;
            this.availabilityCheckInterval = availabilityCheckInterval;
        }

        public async Task RunAsync()
        {
            logger.LogInformation("Trigger Service successfully started.");

            await Task.WhenAll(
                RunContinuouslyAsync(() => environment.UpdateExistingWorkflowsAsync(), nameof(environment.UpdateExistingWorkflowsAsync)),
                RunContinuouslyAsync(() => environment.ProcessAndAbortWorkflowsAsync(), nameof(environment.ProcessAndAbortWorkflowsAsync)));
        }

        private async Task RunContinuouslyAsync(Func<Task> task, string description)
        {
            while (true)
            {
                try
                {
                    await Task.WhenAll(
                        cromwellAvailability.WaitForAsync(
                            () => environment.IsCromwellAvailableAsync(),
                            availabilityCheckInterval,
                            Constants.CromwellSystemName,
                            msg => logger.LogInformation(msg)),
                        azStorageAvailability.WaitForAsync(
                            () => environment.IsAzureStorageAvailableAsync(),
                            availabilityCheckInterval,
                            "Azure Storage",
                            msg => logger.LogInformation(msg)));

                    await task.Invoke();
                    await Task.Delay(mainInterval);
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, $"RunContinuously exception for {description}");
                    await Task.Delay(TimeSpan.FromSeconds(1));
                }
            }
        }
    }
}

