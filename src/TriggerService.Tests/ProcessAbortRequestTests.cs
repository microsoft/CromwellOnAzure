// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Threading.Tasks;
using Common;
using CromwellApiClient;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Newtonsoft.Json;
using Tes.Models;
using Tes.Repository;

namespace TriggerService.Tests
{
    [TestClass]
    public class ProcessAbortRequestTests
    {
        public ProcessAbortRequestTests()
        {
            Common.NewtonsoftJsonSafeInit.SetDefaultSettings();
        }

        [TestMethod]
        public async Task SuccessfulAbortRequestFileGetsMovedToFailedSubdirectory()
        {
            var workflowId = Guid.NewGuid();
            var cromwellApiClient = new Mock<ICromwellApiClient>();

            var (newTriggerName, newTriggerContent) = await ProcessAbortRequestAsync(workflowId, cromwellApiClient.Object);

            Assert.IsTrue(newTriggerName.StartsWith("failed/"));
            Assert.AreEqual("AbortRequested", newTriggerContent?.WorkflowFailureInfo?.WorkflowFailureReason);
            cromwellApiClient.Verify(mock => mock.PostAbortAsync(workflowId), Times.Once());
        }

        [TestMethod]
        public async Task FailedAbortRequestFileGetsMovedToFailedSubdirectory()
        {
            var workflowId = Guid.NewGuid();
            var cromwellApiClient = new Mock<ICromwellApiClient>();
            cromwellApiClient.Setup(ac => ac.PostAbortAsync(It.IsAny<Guid>())).Throws(new Exception("Workflow not found"));

            var (newTriggerName, newTriggerContent) = await ProcessAbortRequestAsync(workflowId, cromwellApiClient.Object);

            Assert.IsTrue(newTriggerName.StartsWith("failed/"));
            Assert.AreEqual("ErrorOccuredWhileAbortingWorkflow", newTriggerContent?.WorkflowFailureInfo?.WorkflowFailureReason);
            Assert.AreEqual("Workflow not found", newTriggerContent?.WorkflowFailureInfo?.WorkflowFailureReasonDetail);
            cromwellApiClient.Verify(mock => mock.PostAbortAsync(workflowId), Times.Once());
        }

        private static async Task<(string newTriggerName, Workflow newTriggerContent)> ProcessAbortRequestAsync(Guid workflowId, ICromwellApiClient cromwellApiClient)
        {
            string newTriggerName = null;
            Workflow newTriggerContent = null;

            var loggerFactory = new Mock<ILoggerFactory>();
            var azureStorage = new Mock<IAzureStorage>();
            var repository = new Mock<IRepository<TesTask>>();

            loggerFactory
                .Setup(f => f.CreateLogger(It.IsAny<string>()))
                .Returns(new Mock<ILogger>().Object);

            azureStorage
                .Setup(az => az.GetWorkflowsByStateAsync(WorkflowState.Abort))
                .Returns(Task.FromResult(new[] { 
                    new TriggerFile { 
                        Uri = $"http://tempuri.org/workflows/abort/{workflowId}.json", 
                        ContainerName = "workflows", 
                        Name = $"abort/{workflowId}.json", 
                        LastModified = DateTimeOffset.UtcNow } }.AsEnumerable()));

            azureStorage
                .Setup(az => az.DownloadBlobTextAsync(It.IsAny<string>(), $"abort/{workflowId}.json"))
                .Returns(Task.FromResult(string.Empty));

            azureStorage
                .Setup(az => az.UploadFileTextAsync(It.IsAny<string>(), "workflows", It.IsAny<string>()))
                .Callback((string content, string container, string blobName) => {
                    newTriggerName = blobName;
                    newTriggerContent = content is not null ? JsonConvert.DeserializeObject<Workflow>(content) : null; });

            var cromwellOnAzureEnvironment = new CromwellOnAzureEnvironment(loggerFactory.Object, azureStorage.Object, cromwellApiClient, repository.Object, Enumerable.Repeat(azureStorage.Object, 1));

            await cromwellOnAzureEnvironment.ProcessAndAbortWorkflowsAsync();

            return (newTriggerName, newTriggerContent);
        }
    }
}
