// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;

namespace TriggerService.Tests
{
    [TestClass]
    public class TriggerEngineTests
    {
        [TestMethod]
        public async Task TriggerEngineRunsAndOnlyLogsAvailabilityOncePerSystemUponAvailableStateAsync()
        {
            var loggerFactory = new TestLoggerFake();
            var environment = new Mock<ICromwellOnAzureEnvironment>();
            var logger = loggerFactory.CreateLogger<TriggerEngineTests>();
            bool isStorageAvailable = false;
            bool isCromwellAvailable = false;

            environment.Setup(x => x.ProcessAndAbortWorkflowsAsync()).Returns(() =>
            {
                logger.LogInformation("ProcessAndAbortWorkflowsAsync");
                return Task.CompletedTask;
            });

            environment.Setup(x => x.UpdateExistingWorkflowsAsync()).Returns(() =>
            {
                logger.LogInformation("UpdateExistingWorkflowsAsync");
                return Task.CompletedTask;
            });

            environment.Setup(x => x.IsAzureStorageAvailableAsync()).Returns(() =>
            {
                logger.LogInformation("IsAzureStorageAvailableAsync");
                return Task.FromResult(isStorageAvailable);
            });

            environment.Setup(x => x.IsCromwellAvailableAsync()).Returns(() =>
            {
                logger.LogInformation("IsCromwellAvailableAsync");
                return Task.FromResult(isCromwellAvailable);
            });

            var engine = new TriggerEngine(loggerFactory, environment.Object, TimeSpan.FromMilliseconds(25), TimeSpan.FromMilliseconds(25));
            var task = Task.Run(() => engine.RunAsync());
            await Task.Delay(TimeSpan.FromSeconds(2));

            isStorageAvailable = true;
            isCromwellAvailable = true;
            await Task.Delay(TimeSpan.FromSeconds(2));

            isStorageAvailable = false;
            isCromwellAvailable = false;
            await Task.Delay(TimeSpan.FromSeconds(2));

            isStorageAvailable = true;
            isCromwellAvailable = true;
            await Task.Delay(TimeSpan.FromSeconds(2));

            var lines = loggerFactory.TestLogger.LogLines;
            var availableLines = lines.Where(line => line.Contains("is available", StringComparison.OrdinalIgnoreCase)).ToList();

            Console.WriteLine($"availableLines.Count: {availableLines.Count}");

            foreach (var line in availableLines)
            {
                Console.WriteLine(line);
            }

            Assert.IsTrue(availableLines.Count == 4);
        }

        /// <summary>
        /// Do not use this except for testing.  It is not actually a factory and only creates a single instance of a logger to facilitate verifying log messages
        /// </summary>
        public class TestLoggerFake : ILoggerFactory
        {
            public TestLogger TestLogger { get; set; } = new TestLogger();
            public void AddProvider(ILoggerProvider provider)
            {
                throw new NotImplementedException();
            }

            public ILogger CreateLogger(string categoryName)
            {
                return TestLogger;
            }

            public void Dispose()
            {
                
            }
        }

        public class TestLogger : ILogger, IDisposable
        {
            public List<string> LogLines { get; set; } = new List<string>();
            public IDisposable BeginScope<TState>(TState state)
            {
                return null;
            }

            public void Dispose()
            {

            }

            public bool IsEnabled(LogLevel logLevel)
            {
                return true;
            }

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
            {
                var now = DateTime.UtcNow;
                LogLines.Add($"{now.Second}:{now.Millisecond} {logLevel} {eventId} {state?.ToString()} {exception?.ToString()}");
            }
        }
    }
}
