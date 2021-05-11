using System;
using System.Threading.Tasks;

namespace Common
{
    public class AvailabilityTracker
    {
        private bool hasBeenAvailable = false;

        public static string GetAvailabilityMessage(string systemName) => $"{systemName} is available.";

        public async Task WaitForAsync(Func<Task<bool>> availabilityCondition, TimeSpan waitTime, string systemName, Action<string> informationLogger)
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
    }
}
