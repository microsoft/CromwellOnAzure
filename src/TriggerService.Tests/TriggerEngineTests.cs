// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CommonUtilities.AzureCloud;
using CromwellApiClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Tes.Models;
using Tes.Repository;

namespace TriggerService.Tests
{
    [TestClass]
    public class TriggerEngineTests
    {
        public TriggerEngineTests()
            => Common.NewtonsoftJsonSafeInit.SetDefaultSettings();

        private volatile bool isStorageAvailable = false;
        private volatile bool isCromwellAvailable = false;

        [Ignore]
        [TestMethod]
        public async Task TriggerEngineRunsAndOnlyLogsAvailabilityOncePerSystemUponAvailableStateAsync()
        {
            // TODO - this test still occasionally fails on this: Assert.IsTrue(availableLines.Count == 4);
            // and results in availableLines.Count = 3
            var loggerFactory = new TestLoggerFake();
            var environment = new Mock<ITriggerHostedService>();
            var logger = loggerFactory.CreateLogger<TriggerHostedService>();

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

            var triggerServiceOptions = new Mock<IOptions<TriggerServiceOptions>>();

            triggerServiceOptions.Setup(o => o.Value).Returns(new TriggerServiceOptions()
            {
                DefaultStorageAccountName = "fakestorage",
                ApplicationInsightsAccountName = "fakeappinsights",
                AvailabilityCheckIntervalMilliseconds = 25,
                MainRunIntervalMilliseconds = 25
            });

            var postgreSqlOptions = new Mock<IOptions<PostgreSqlOptions>>().Object;
            var cromwellApiClient = new Mock<ICromwellApiClient>().Object;
            var tesTaskRepository = new Mock<IRepository<TesTask>>().Object;
            var azureStorage = new Mock<IAzureStorage>();

            azureStorage
                .Setup(az => az.GetWorkflowsByStateAsync(WorkflowState.New, It.IsAny<System.Threading.CancellationToken>()))
                .Returns(AsyncEnumerable.Repeat(
                    new TriggerFile
                    {
                        Uri = $"http://tempuri.org/workflows/new/Sample.json",
                        ContainerName = "workflows",
                        Name = $"new/Sample.json",
                        LastModified = DateTimeOffset.UtcNow
                    }, 1));

            azureStorage.Setup(x => x.IsAvailableAsync(It.IsAny<System.Threading.CancellationToken>()))
                .Returns(() =>
                {
                    logger.LogInformation("IsAzureStorageAvailableAsync");
                    var localBool = isStorageAvailable;
                    return Task.FromResult(localBool);
                });

            var storageUtility = new Mock<IAzureStorageUtility>();

            storageUtility
                .Setup(x => x.GetStorageAccountsUsingMsiAsync(It.IsAny<string>()))
                .Returns(Task.FromResult((new List<IAzureStorage>(), azureStorage.Object)));

            var triggerHostedService = new TriggerHostedService(logger, triggerServiceOptions.Object, cromwellApiClient, tesTaskRepository, storageUtility.Object, AzureCloudConfig.ForUnitTesting());

            //var engine = new TriggerHostedService(loggerFactory, environment.Object, TimeSpan.FromMilliseconds(25), TimeSpan.FromMilliseconds(25));
            _ = Task.Run(() => triggerHostedService.StartAsync(new System.Threading.CancellationToken()));


            await Task.Delay(TimeSpan.FromSeconds(2));
            var availableLines = loggerFactory.TestLogger.LogLines.Where(line => line.Contains("is available", StringComparison.OrdinalIgnoreCase)).ToList();
            Console.WriteLine($"1: availableLines.Count: {availableLines.Count}");

            isStorageAvailable = true;
            isCromwellAvailable = true;
            await Task.Delay(TimeSpan.FromSeconds(2));

            availableLines = loggerFactory.TestLogger.LogLines.Where(line => line.Contains("is available", StringComparison.OrdinalIgnoreCase)).ToList();
            Console.WriteLine($"2: availableLines.Count: {availableLines.Count}");

            isStorageAvailable = false;
            isCromwellAvailable = false;
            await Task.Delay(TimeSpan.FromSeconds(2));

            availableLines = loggerFactory.TestLogger.LogLines.Where(line => line.Contains("is available", StringComparison.OrdinalIgnoreCase)).ToList();
            Console.WriteLine($"3: availableLines.Count: {availableLines.Count}");

            isStorageAvailable = true;
            isCromwellAvailable = true;
            await Task.Delay(TimeSpan.FromSeconds(2));

            availableLines = loggerFactory.TestLogger.LogLines.Where(line => line.Contains("is available", StringComparison.OrdinalIgnoreCase)).ToList();
            Console.WriteLine($"4: availableLines.Count: {availableLines.Count}");

            foreach (var line in availableLines)
            {
                Console.WriteLine(line);
            }

            Assert.IsTrue(availableLines.Count == 4);
        }

        /// <summary>
        /// Do not use this except for testing.  It is not actually a factory and only creates a single instance of a logger to facilitate verifying log messages
        /// </summary>
        public sealed class TestLoggerFake : ILoggerFactory
        {
            public TestLogger TestLogger { get; set; } = new TestLogger();
            public void AddProvider(ILoggerProvider provider)
                => throw new NotImplementedException();

            public ILogger CreateLogger(string categoryName)
                => TestLogger;

            public void Dispose()
            { }
        }

        public sealed class TestLogger : ILogger, IDisposable
        {
            private readonly object _lock = new();
            public List<string> LogLines { get; set; } = new List<string>();
            public IDisposable BeginScope<TState>(TState state)
                => null;

            public void Dispose()
            { }

            public bool IsEnabled(LogLevel logLevel)
                => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
            {
                var now = DateTime.UtcNow;
                lock (_lock)
                {
                    LogLines.Add($"{now.Second}:{now.Millisecond} {logLevel} {eventId} {state?.ToString()} {exception?.ToString()}");
                }
            }
        }
    }
}
