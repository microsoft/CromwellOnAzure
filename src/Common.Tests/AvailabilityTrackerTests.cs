// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Common.Tests
{
    [TestClass]
    public class AvailabilityTrackerTests
    {
        public AvailabilityTrackerTests()
            => Common.NewtonsoftJsonSafeInit.SetDefaultSettings();

        [TestMethod]
        public void CommonAvailabilityMessageMatches()
        {
            var availabilityMsg = AvailabilityTracker.GetAvailabilityMessage(Constants.CromwellSystemName);
            Assert.AreEqual("Cromwell is available.", availabilityMsg);
        }

        [TestMethod]
        public async Task WaitUntilSystemAvailable()
        {
            var availabilityTracker = new AvailabilityTracker();
            var attempts = 0;
            var stdOut = new List<string>();

            async Task<bool> availableAfter3Tries()
            {
                await Task.Yield();
                if (++attempts == 3)
                {
                    return true;
                }

                return false;
            }

            await availabilityTracker.WaitForAsync(availableAfter3Tries, TimeSpan.FromMilliseconds(1), "Test", msg => stdOut.Add(msg));

            Assert.IsTrue(stdOut.Count == attempts);
        }

        [TestMethod]
        public async Task NoLogsWhenSystemIsAlreadyAvailable()
        {
            var availabilityTracker = new AvailabilityTracker();
            var attempts = 0;
            var stdOut = new List<string>();

            async Task<bool> availableAfter3Tries()
            {
                await Task.Yield();
                if (++attempts == 3)
                {
                    return true;
                }

                return false;
            }

            Task<bool> cromwellIsAvailable()
                => Task.FromResult(true);

            await availabilityTracker.WaitForAsync(availableAfter3Tries, TimeSpan.FromMilliseconds(1), "Test", msg => stdOut.Add(msg));

            Assert.IsTrue(stdOut.Count == attempts);

            await availabilityTracker.WaitForAsync(cromwellIsAvailable, TimeSpan.FromMilliseconds(1), "Test", msg => stdOut.Add(msg));

            // Verify it did not log since it was already available
            Assert.IsTrue(stdOut.Count == attempts);
        }
    }
}
