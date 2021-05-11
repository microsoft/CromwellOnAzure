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
        private AvailabilityTracker cromwellAvailability = new AvailabilityTracker();
        private AvailabilityTracker azStorageAvailability = new AvailabilityTracker();

        public TriggerEngine(ILoggerFactory loggerFactory, ICromwellOnAzureEnvironment environment)
        {
            logger = loggerFactory.CreateLogger<TriggerEngine>();
            this.environment = environment;
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
                            TimeSpan.FromSeconds(30),
                            Constants.CromwellSystemName,
                            msg => logger.LogInformation(msg)),
                        azStorageAvailability.WaitForAsync(
                            () => environment.IsAzureStorageAvailableAsync(),
                            TimeSpan.FromSeconds(30),
                            "Azure Storage",
                            msg => logger.LogInformation(msg)));

                    await task.Invoke();
                    await Task.Delay(TimeSpan.FromSeconds(20));
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

