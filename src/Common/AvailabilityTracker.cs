// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Common
{
    /// <summary>
    /// Thread-safe class to keep track of availability state
    /// </summary>
    public class AvailabilityTracker
    {
        private bool hasBeenAvailable = false;
        private readonly SemaphoreSlim semaphoreSlim = new(1, 1);

        public static string GetAvailabilityMessage(string systemName) => $"{systemName} is available.";

        public async Task WaitForAsync(Func<Task<bool>> availabilityCondition, TimeSpan waitTime, string systemName, Action<string> informationLogger)
        {
            await semaphoreSlim.WaitAsync();

            try
            {
                while (!(await availabilityCondition.Invoke()))
                {
                    hasBeenAvailable = false;
                    informationLogger.Invoke($"Waiting {waitTime.TotalSeconds:n0}s for {systemName} to become available...");
                    await Task.Delay(waitTime);
                }

                if (!hasBeenAvailable)
                {
                    informationLogger.Invoke(GetAvailabilityMessage(systemName));
                }

                hasBeenAvailable = true;
            }
            finally
            {
                semaphoreSlim.Release();
            }
        }
    }
}
