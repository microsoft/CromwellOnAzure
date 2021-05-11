using System;
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
        public async Task TriggerEngineRunsAsync()
        {
            var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
            var environment = new Mock<ICromwellOnAzureEnvironment>();

            environment.Setup(x => x.ProcessAndAbortWorkflowsAsync()).Returns(() =>
            {
                Console.WriteLine("ProcessAndAbortWorkflowsAsync()");
                return Task.CompletedTask;
            });
            environment.Setup(x => x.UpdateExistingWorkflowsAsync()).Returns(() =>
            {
                Console.WriteLine("UpdateExistingWorkflowsAsync()");
                return Task.CompletedTask;
            });
            environment.Setup(x => x.IsAzureStorageAvailableAsync()).Returns(() =>
            {
                Console.WriteLine("IsAzureStorageAvailableAsync()");
                return Task.FromResult(true);
            });
            environment.Setup(x => x.IsCromwellAvailableAsync()).Returns(() =>
            {
                Console.WriteLine("IsCromwellAvailableAsync()");
                return Task.FromResult(true);
            });

            var engine = new TriggerEngine(loggerFactory, environment.Object);
            var task = Task.Run(() => engine.RunAsync());
            await Task.Delay(TimeSpan.FromSeconds(30));
        }
    }
}
