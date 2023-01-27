// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common;
using CromwellApiClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Newtonsoft.Json;
using Tes.Models;
using Tes.Repository;

namespace TriggerService.Tests
{
    [TestClass]
    public class ProcessNewWorkflowTests
    {
        public ProcessNewWorkflowTests()
            => Common.NewtonsoftJsonSafeInit.SetDefaultSettings();

        [TestMethod]
        public async Task NewWorkflowsAreMovedToInProgressSubdirectory()
        {
            var workflowId = Guid.NewGuid();
            var cromwellApiClient = new Mock<ICromwellApiClient>();

            cromwellApiClient
                .Setup(ac => ac.PostWorkflowAsync(It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<List<string>>(), It.IsAny<List<byte[]>>(), It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<string>(), It.IsAny<byte[]>()))
                .Returns(Task.FromResult(new PostWorkflowResponse { Id = workflowId }));

            var (newTriggerName, newTriggerContent) = await ProcessNewWorkflowAsync(cromwellApiClient.Object);

            Assert.AreEqual($"inprogress/Sample.{workflowId}.json", newTriggerName);
        }

        [TestMethod]
        public async Task NewWorkflowsThatFailToPostToCromwellAreMovedToFailedSubdirectory()
        {
            var workflowId = Guid.NewGuid();
            var cromwellApiClient = new Mock<ICromwellApiClient>();

            cromwellApiClient
                .Setup(ac => ac.PostWorkflowAsync(It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<List<string>>(), It.IsAny<List<byte[]>>(), It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<string>(), It.IsAny<byte[]>()))
                .Throws(new Exception("Error submitting new workflow"));

            var (newTriggerName, newTriggerContent) = await ProcessNewWorkflowAsync(cromwellApiClient.Object);

            Assert.IsTrue(newTriggerName.StartsWith("failed/"));
            Assert.AreEqual("ErrorSubmittingWorkflowToCromwell", newTriggerContent?.WorkflowFailureInfo?.WorkflowFailureReason);
            Assert.AreEqual("Error submitting new workflow", newTriggerContent?.WorkflowFailureInfo?.WorkflowFailureReasonDetail);
        }

        private static async Task<(string newTriggerName, Workflow newTriggerContent)> ProcessNewWorkflowAsync(ICromwellApiClient cromwellApiClient)
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
                .Setup(az => az.GetWorkflowsByStateAsync(WorkflowState.New))
                .Returns(Task.FromResult(new[] {
                    new TriggerFile {
                        Uri = $"http://tempuri.org/workflows/new/Sample.json",
                        ContainerName = "workflows",
                        Name = $"new/Sample.json",
                        LastModified = DateTimeOffset.UtcNow } }.AsEnumerable()));

            azureStorage
                .Setup(az => az.DownloadBlobTextAsync(It.IsAny<string>(), $"new/Sample.json"))
                .Returns(Task.FromResult(@"{'WorkflowUrl': 'https://tempuri.org/inputs/bam-to-unmapped-bams.wdl','WorkflowInputsUrl': 'https://tempuri.org/inputs/bam-to-unmapped-bams.inputs.json'}"));

            azureStorage
                .Setup(az => az.UploadFileTextAsync(It.IsAny<string>(), "workflows", It.IsAny<string>()))
                .Callback((string content, string container, string blobName) =>
                {
                    newTriggerName = blobName;
                    newTriggerContent = content is not null ? JsonConvert.DeserializeObject<Workflow>(content) : null;
                });

            var logger = new NullLogger<TriggerHostedService>();
            var optionsMock = new Mock<IOptions<TriggerServiceOptions>>();

            optionsMock.Setup(o => o.Value).Returns(new TriggerServiceOptions()
            {
                DefaultStorageAccountName = "fakestorage",
                ApplicationInsightsAccountName = "fakeappinsights"
            });

            var postgreSqlOptions = new Mock<IOptions<PostgreSqlOptions>>();

            postgreSqlOptions.Setup(o => o.Value).Returns(new PostgreSqlOptions
            {
                PostgreSqlServerName = "fakeserver",
                PostgreSqlDatabaseUserLogin = "fakeuser",
                PostgreSqlDatabaseUserPassword = "fake987",
            });
            var tesTaskRepository = new Mock<IRepository<TesTask>>().Object;
            var storageUtility = new Mock<IAzureStorageUtility>();

            storageUtility
                .Setup(x => x.GetStorageAccountsUsingMsiAsync(It.IsAny<string>()))
                .Returns(Task.FromResult((new List<IAzureStorage>(), azureStorage.Object)));

            var cromwellOnAzureEnvironment = new TriggerHostedService(
                logger,
                optionsMock.Object,
                cromwellApiClient,
                tesTaskRepository,
                storageUtility.Object);

            await cromwellOnAzureEnvironment.ProcessAndAbortWorkflowsAsync();

            return (newTriggerName, newTriggerContent);
        }

        [TestMethod]
        public async Task NewWorkflowsThatFailToParseAsJsonAreAnotatedAndMovedToFailedSubdirectory()
        {
            const string badJason = @"{'WorkflowUrl': 'https://tempuri.org/inputs/bam-to-unmapped-bams.wdl' 'WorkflowInputsUrl': 'https://tempuri.org/inputs/bam-to-unmapped-bams.inputs.json'}";
            const string errPrefix = "\nError(s): ";

            var cromwellApiClient = new Mock<ICromwellApiClient>();

            var triesToPost = false;
            cromwellApiClient
                .Setup(ac => ac.PostWorkflowAsync(It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<List<string>>(), It.IsAny<List<byte[]>>(), It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<string>(), It.IsAny<byte[]>()))
                .Callback(() => triesToPost = true)
                .Throws(new Exception("Should never get here."));

            string newTriggerName = null;
            string newTriggerContent = null;

            var loggerFactory = new Mock<ILoggerFactory>();
            var azureStorage = new Mock<IAzureStorage>();
            var repository = new Mock<IRepository<TesTask>>();

            loggerFactory
                .Setup(f => f.CreateLogger(It.IsAny<string>()))
                .Returns(new Mock<ILogger>().Object);

            azureStorage
                .Setup(az => az.GetWorkflowsByStateAsync(WorkflowState.New))
                .Returns(Task.FromResult(new[] {
                    new TriggerFile {
                        Uri = $"http://tempuri.org/workflows/new/Sample.json",
                        ContainerName = "workflows",
                        Name = $"new/Sample.json",
                        LastModified = DateTimeOffset.UtcNow } }.AsEnumerable()));

            azureStorage
                .Setup(az => az.DownloadBlobTextAsync(It.IsAny<string>(), $"new/Sample.json"))
                .Returns(Task.FromResult(badJason));

            azureStorage
                .Setup(az => az.UploadFileTextAsync(It.IsAny<string>(), "workflows", It.IsAny<string>()))
                .Callback((string content, string container, string blobName) =>
                {
                    newTriggerName = blobName;
                    newTriggerContent = content;
                });

            var deleted = false;
            azureStorage
                .Setup(az => az.DeleteBlobIfExistsAsync(It.IsAny<string>(), $"new/Sample.json"))
                .Callback(() => deleted = true);

            var logger = new Mock<ILogger<TriggerHostedService>>().Object;
            var triggerServiceOptions = new Mock<IOptions<TriggerServiceOptions>>();

            triggerServiceOptions.Setup(o => o.Value).Returns(new TriggerServiceOptions()
            {
                DefaultStorageAccountName = "fakestorage",
                ApplicationInsightsAccountName = "fakeappinsights"
            });
            var postgreSqlOptions = new Mock<IOptions<PostgreSqlOptions>>().Object;
            var cromwellApiClient2 = new Mock<ICromwellApiClient>().Object;
            var tesTaskRepository = new Mock<IRepository<TesTask>>().Object;
            var storageUtility = new Mock<IAzureStorageUtility>();

            storageUtility
                .Setup(x => x.GetStorageAccountsUsingMsiAsync(It.IsAny<string>()))
                .Returns(Task.FromResult((new List<IAzureStorage>(), azureStorage.Object)));

            var cromwellOnAzureEnvironment = new TriggerHostedService(
                logger,
                triggerServiceOptions.Object, 
                cromwellApiClient2, 
                tesTaskRepository, 
                storageUtility.Object);


            //var cromwellOnAzureEnvironment = new TriggerHostedService(loggerFactory.Object, azureStorage.Object, cromwellApiClient.Object, repository.Object, Enumerable.Repeat(azureStorage.Object, 1));

            await cromwellOnAzureEnvironment.ProcessAndAbortWorkflowsAsync();

            Assert.IsTrue(newTriggerName.StartsWith("failed/"));
            Assert.IsTrue(deleted);
            Assert.IsFalse(triesToPost);
            Assert.IsTrue((badJason + errPrefix).Length < newTriggerContent.Length);
            Assert.IsTrue(newTriggerContent.StartsWith(badJason + errPrefix));
        }
    }
}
