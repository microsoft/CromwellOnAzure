// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using System.Linq.Expressions;
using CromwellApiClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage.Blob;
using Moq;
using Tes.Repository;
using Tes.Models;

namespace TriggerService.Tests
{
    [TestClass]
    public class AbortedWorkflows_SurfaceFailuresToTriggerTests
    {
        [TestMethod]
        public async Task SurfaceWorkflowFailure_From_FailingTesTasksAsync()
        {
            var serviceCollection = new ServiceCollection()
                .AddLogging(loggingBuilder => loggingBuilder.AddConsole());

            var serviceProvider = serviceCollection.BuildServiceProvider();

            var azStorageMock = new Mock<IAzureStorage>();

            var cosmosdbRepositoryMock = new Mock<IRepository<TesTask>>();

            var mockWorkflow_id = Guid.NewGuid();

            azStorageMock.Setup(az => az
                .GetWorkflowBlobsToAbortAsync())
                .Returns(Task.FromResult(new List<CloudBlockBlob> {
                    new CloudBlockBlob(new Uri($@"http://tempuri.org/workflow/abort/abort.Sample.{mockWorkflow_id}.json"))}
                    .AsEnumerable()));

            azStorageMock.Setup(az => az
            .DownloadBlobTextAsync(It.IsAny<string>(), $"abort/abort.Sample.{ mockWorkflow_id}.json"))
            .Returns(Task.FromResult(@"{'WorkflowUrl': 'https://bam-to-unmapped-bams.wdl','WorkflowInputsUrl': 'https://bam-to-unmapped-bams.inputs.json'}"));

            azStorageMock.Setup(az => az
            .DownloadBlobTextAsync(It.IsAny<string>(), $"failed/abort.Sample.{ mockWorkflow_id}.json"))
            .Returns(Task.FromResult(@"{'WorkflowUrl': 'https://bam-to-unmapped-bams.wdl','WorkflowInputsUrl': 'https://bam-to-unmapped-bams.inputs.json','WorkflowFailureDetails': {'WorkflowFailureReason': 'Aborted'}}"));

            azStorageMock.Setup(az => az
            .DeleteBlobIfExistsAsync(It.IsAny<string>(), It.IsAny<string>()));

            azStorageMock.Setup(az => az
            .UploadFileTextAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
                .Returns(Task.FromResult(It.IsAny<string>()));
                
            var cromwellApiClient = new Mock<ICromwellApiClient>();
            cromwellApiClient.Setup(ac => ac
                .GetStatusAsync(It.IsAny<Guid>()))
                .Returns(Task.FromResult(new GetStatusResponse {
                 Id = Guid.Parse($"{mockWorkflow_id}"),
                 Status=WorkflowStatus.Failed}));

            cromwellApiClient.Setup(ac => ac
                .GetOutputsAsync(It.IsAny<Guid>()))
                .Returns(Task.FromResult(new GetOutputsResponse {}));

            cromwellApiClient.Setup(ac => ac
            .GetMetadataAsync(It.IsAny<Guid>()))
                .Returns(Task.FromResult(new GetMetadataResponse()));

            cromwellApiClient.Setup(ac => ac
            .GetTimingAsync(It.IsAny<Guid>()))
                .Returns(Task.FromResult(new GetTimingResponse()));

            cosmosdbRepositoryMock.Setup(r => r
            .GetItemsAsync(It.IsAny<Expression<Func<TesTask, bool>>>()))
            .Returns(Task.FromResult(new List<TesTask> { new TesTask {
             WorkflowId = $"{mockWorkflow_id}",
             Description = "BackendJobDescriptorKey_CommandCallNode_BamToUnmappedBams.SortSam:-1:1",
             Executors = new List<TesExecutor> { 
             new TesExecutor{
              Stdout="Stdout",
              Stderr="Stderr"}},
             Logs = new List<TesTaskLog> { 
                 new TesTaskLog { 
                     Logs = new List<TesExecutorLog>{
                     new TesExecutorLog {
                         ExitCode = 0,
                         Stderr = "Stderr.txt", 
                         Stdout = "Stdout.txt" }},
              FailureReason = "JobNotFound"}}}}.AsEnumerable()));               
            
            var environment = new CromwellOnAzureEnvironment(
                serviceProvider.GetRequiredService<ILoggerFactory>(),
                azStorageMock.Object,
                cromwellApiClient.Object,
                cosmosdbRepositoryMock.Object);

            var abortedworkflows = await environment.AbortWorkflowsAsync();

            Assert.IsTrue(abortedworkflows.FirstOrDefault()?.WorkflowFailureDetails.WorkflowFailureReason == "Aborted");
            Assert.IsTrue(abortedworkflows.FirstOrDefault()?.WorkflowFailureDetails.WorkflowFailureReasonDetail == null);
            Assert.IsTrue(abortedworkflows.FirstOrDefault()?.WorkflowFailureDetails.FailedTaskDetails == null);
        }

        [TestMethod]
        public async Task SurfaceWorkflowFailure_From_CromwellApiException()
        {
            var serviceCollection = new ServiceCollection()
                .AddLogging(loggingBuilder => loggingBuilder.AddConsole());

            var serviceProvider = serviceCollection.BuildServiceProvider();

            var azStorageMock = new Mock<IAzureStorage>();

            var cosmosdbRepositoryMock = new Mock<IRepository<TesTask>>();

            var mockWorkflow_id = Guid.NewGuid();

            azStorageMock.Setup(az => az
                .GetWorkflowBlobsToAbortAsync())
                .Returns(Task.FromResult(new List<CloudBlockBlob> {
                    new CloudBlockBlob(new Uri($@"http://tempuri.org/workflow/abort/abort.Sample.{mockWorkflow_id}.json"))}
                    .AsEnumerable()));

            azStorageMock.Setup(az => az
            .DownloadBlobTextAsync(It.IsAny<string>(), $"abort/abort.Sample.{ mockWorkflow_id}.json"))
            .Returns(Task.FromResult(@"{'WorkflowUrl': 'https://bam-to-unmapped-bams.wdl','WorkflowInputsUrl': 'https://bam-to-unmapped-bams.inputs.json'}"));

            azStorageMock.Setup(az => az
            .DownloadBlobTextAsync(It.IsAny<string>(), $"failed/abort.Sample.{ mockWorkflow_id}.json"))
            .Returns(Task.FromResult(@"{'WorkflowUrl': 'https://bam-to-unmapped-bams.wdl','WorkflowInputsUrl': 'https://bam-to-unmapped-bams.inputs.json','WorkflowFailureDetails': {'WorkflowFailureReason': 'Aborted'}}"));

            azStorageMock.Setup(az => az
            .DeleteBlobIfExistsAsync(It.IsAny<string>(), It.IsAny<string>()));

            azStorageMock.Setup(az => az
            .UploadFileTextAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
                .Returns(Task.FromResult(It.IsAny<string>()));

            var cromwellApiClient = new Mock<ICromwellApiClient>();
            cromwellApiClient.Setup(ac => ac
                .PostAbortAsync(It.IsAny<Guid>()))
                .Throws(new CromwellApiException("", null, System.Net.HttpStatusCode.NotFound));

            cromwellApiClient.Setup(ac => ac
                .GetOutputsAsync(It.IsAny<Guid>()))
                .Returns(Task.FromResult(new GetOutputsResponse { }));

            cromwellApiClient.Setup(ac => ac
            .GetMetadataAsync(It.IsAny<Guid>()))
                .Returns(Task.FromResult(new GetMetadataResponse()));

            cromwellApiClient.Setup(ac => ac
            .GetTimingAsync(It.IsAny<Guid>()))
                .Returns(Task.FromResult(new GetTimingResponse()));

            cosmosdbRepositoryMock.Setup(r => r
            .GetItemsAsync(It.IsAny<Expression<Func<TesTask, bool>>>()))
            .Returns(Task.FromResult(new List<TesTask> { new TesTask {
             WorkflowId = $"{mockWorkflow_id}",
             Description = "BackendJobDescriptorKey_CommandCallNode_BamToUnmappedBams.SortSam:-1:1",
             Executors = new List<TesExecutor> {
             new TesExecutor{
              Stdout = null,
              Stderr = null}},
             Logs = new List<TesTaskLog> {
                 new TesTaskLog {
                     Logs = new List<TesExecutorLog>{
                     new TesExecutorLog {
                         ExitCode = 0,
                         Stderr = null,
                         Stdout = null }}
              }}}}.AsEnumerable()));

            var environment = new CromwellOnAzureEnvironment(
                serviceProvider.GetRequiredService<ILoggerFactory>(),
                azStorageMock.Object,
                cromwellApiClient.Object,
                cosmosdbRepositoryMock.Object);

            var abortedworkflows = await environment.AbortWorkflowsAsync();           

            Assert.IsTrue(abortedworkflows.FirstOrDefault()?.WorkflowFailureDetails.WorkflowFailureReason == "Aborted");
            Assert.IsTrue(abortedworkflows.FirstOrDefault()?.WorkflowFailureDetails.FailedTaskDetails == null);
            Assert.IsTrue(abortedworkflows.FirstOrDefault()?.WorkflowFailureDetails.WorkflowFailureReasonDetail == null);
        }

        [TestMethod]
        public async Task SurfaceWorkflowFailure_From_FailingBatchAsync()
        {
            var serviceCollection = new ServiceCollection()
                .AddLogging(loggingBuilder => loggingBuilder.AddConsole());

            var serviceProvider = serviceCollection.BuildServiceProvider();

            var azStorageMock = new Mock<IAzureStorage>();

            var cosmosdbRepositoryMock = new Mock<IRepository<TesTask>>();

            var mockWorkflow_id = Guid.NewGuid();

            azStorageMock.Setup(az => az
                .GetWorkflowBlobsToAbortAsync())
                .Returns(Task.FromResult(new List<CloudBlockBlob> {
                    new CloudBlockBlob(new Uri($@"http://tempuri.org/workflow/abort/abort.Sample.{mockWorkflow_id}.json"))}
                    .AsEnumerable()));

            azStorageMock.Setup(az => az
            .DownloadBlobTextAsync(It.IsAny<string>(), $"abort/abort.Sample.{ mockWorkflow_id}.json"))
            .Returns(Task.FromResult(@"{'WorkflowUrl': 'https://bam-to-unmapped-bams.wdl','WorkflowInputsUrl': 'https://bam-to-unmapped-bams.inputs.json'}"));

            azStorageMock.Setup(az => az
            .DownloadBlobTextAsync(It.IsAny<string>(), $"failed/abort.Sample.{ mockWorkflow_id}.json"))
            .Returns(Task.FromResult(@"{'WorkflowUrl': 'https://bam-to-unmapped-bams.wdl','WorkflowInputsUrl': 'https://bam-to-unmapped-bams.inputs.json','WorkflowFailureDetails': {'WorkflowFailureReason': 'Aborted'}}"));

            azStorageMock.Setup(az => az
            .DeleteBlobIfExistsAsync(It.IsAny<string>(), It.IsAny<string>()));

            azStorageMock.Setup(az => az
            .UploadFileTextAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
                .Returns(Task.FromResult(It.IsAny<string>()));

            var cromwellApiClient = new Mock<ICromwellApiClient>();
            cromwellApiClient.Setup(ac => ac
                .GetStatusAsync(It.IsAny<Guid>()))
                .Returns(Task.FromResult(new GetStatusResponse
                {
                    Id = Guid.Parse($"{mockWorkflow_id}"),
                    Status = WorkflowStatus.Failed
                }));

            cromwellApiClient.Setup(ac => ac
                .GetOutputsAsync(It.IsAny<Guid>()))
                .Returns(Task.FromResult(new GetOutputsResponse { }));

            cromwellApiClient.Setup(ac => ac
            .GetMetadataAsync(It.IsAny<Guid>()))
                .Returns(Task.FromResult(new GetMetadataResponse()));

            cromwellApiClient.Setup(ac => ac
            .GetTimingAsync(It.IsAny<Guid>()))
                .Returns(Task.FromResult(new GetTimingResponse()));

            var failedTesTasks = new List<TesTask> { new TesTask {
             WorkflowId = $"{mockWorkflow_id}",
             Description = "BackendJobDescriptorKey_CommandCallNode_DummySample.ingest_outputs:-1:1",
             Executors = new List<TesExecutor> {
             new TesExecutor{
              Stdout="Stdout",
              Stderr="Stderr"}},
             Logs = new List<TesTaskLog> {
                 new TesTaskLog {
                     Logs = new List<TesExecutorLog>{
                     new TesExecutorLog {
                         ExitCode = 1,
                         Stderr = null,
                         Stdout = null }},
              FailureReason = "UnknownError",
                     SystemLogs = new List<string>{ "FailureExitCode", "The task process exited with an unexpected exit code" }},}} }.AsEnumerable(); 

            cosmosdbRepositoryMock.Setup(r => r
            .GetItemsAsync(It.IsAny<Expression<Func<TesTask, bool>>>()))
            .Returns(Task.FromResult(failedTesTasks));

            var environment = new CromwellOnAzureEnvironment(
                serviceProvider.GetRequiredService<ILoggerFactory>(),
                azStorageMock.Object,
                cromwellApiClient.Object,
                cosmosdbRepositoryMock.Object);

            var abortedworkflows = await environment.AbortWorkflowsAsync();

            Assert.IsTrue(abortedworkflows.FirstOrDefault()?.WorkflowFailureDetails.WorkflowFailureReason == "Aborted");
            Assert.IsTrue(abortedworkflows.FirstOrDefault()?.WorkflowFailureDetails.WorkflowFailureReasonDetail == null);
            Assert.IsTrue(abortedworkflows.FirstOrDefault()?.WorkflowFailureDetails.FailedTaskDetails == null);           
        }

        [TestMethod]
        public async Task SurfaceWorkflowFailure_From_FailingCromwellAsync()
        {
            var serviceCollection = new ServiceCollection()
                .AddLogging(loggingBuilder => loggingBuilder.AddConsole());

            var serviceProvider = serviceCollection.BuildServiceProvider();

            var azStorageMock = new Mock<IAzureStorage>();

            var cosmosdbRepositoryMock = new Mock<IRepository<TesTask>>();

            var mockWorkflow_id = Guid.NewGuid();

            azStorageMock.Setup(az => az
                .GetWorkflowBlobsToAbortAsync())
                .Returns(Task.FromResult(new List<CloudBlockBlob> {
                    new CloudBlockBlob(new Uri($@"http://tempuri.org/workflow/abort/abort.Sample.{mockWorkflow_id}.json"))}
                    .AsEnumerable()));

            azStorageMock.Setup(az => az
            .DownloadBlobTextAsync(It.IsAny<string>(), $"abort/abort.Sample.{ mockWorkflow_id}.json"))
            .Returns(Task.FromResult(@"{'WorkflowUrl': 'https://bam-to-unmapped-bams.wdl','WorkflowInputsUrl': 'https://bam-to-unmapped-bams.inputs.json'}"));

            azStorageMock.Setup(az => az
            .DownloadBlobTextAsync(It.IsAny<string>(), $"failed/abort.Sample.{ mockWorkflow_id}.json"))
            .Returns(Task.FromResult(@"{'WorkflowUrl': 'https://bam-to-unmapped-bams.wdl','WorkflowInputsUrl': 'https://bam-to-unmapped-bams.inputs.json','WorkflowFailureDetails': {'WorkflowFailureReason': 'Aborted'}}"));

            azStorageMock.Setup(az => az
            .DeleteBlobIfExistsAsync(It.IsAny<string>(), It.IsAny<string>()));

            azStorageMock.Setup(az => az
            .UploadFileTextAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
                .Returns(Task.FromResult(It.IsAny<string>()));

            var cromwellApiClient = new Mock<ICromwellApiClient>();
            cromwellApiClient.Setup(ac => ac
                .GetStatusAsync(It.IsAny<Guid>()))
                .Returns(Task.FromResult(new GetStatusResponse
                {
                    Id = Guid.Parse($"{mockWorkflow_id}"),
                    Status = WorkflowStatus.Failed
                }));

            cromwellApiClient.Setup(ac => ac
                .GetOutputsAsync(It.IsAny<Guid>()))
                .Returns(Task.FromResult(new GetOutputsResponse { }));

            cromwellApiClient.Setup(ac => ac
            .GetMetadataAsync(It.IsAny<Guid>()))
                .Returns(Task.FromResult(new GetMetadataResponse()));

            cromwellApiClient.Setup(ac => ac
            .GetTimingAsync(It.IsAny<Guid>()))
                .Returns(Task.FromResult(new GetTimingResponse()));

            var failedTesTasks = new List<TesTask> { }.AsEnumerable();

            cosmosdbRepositoryMock.Setup(r => r
            .GetItemsAsync(It.IsAny<Expression<Func<TesTask, bool>>>()))
            .Returns(Task.FromResult(failedTesTasks));

            var environment = new CromwellOnAzureEnvironment(
                serviceProvider.GetRequiredService<ILoggerFactory>(),
                azStorageMock.Object,
                cromwellApiClient.Object,
                cosmosdbRepositoryMock.Object);

            var abortedworkflows = await environment.AbortWorkflowsAsync();

            Assert.IsTrue(abortedworkflows.FirstOrDefault()?.WorkflowFailureDetails.WorkflowFailureReason == "Aborted");
            Assert.IsTrue(abortedworkflows.FirstOrDefault()?.WorkflowFailureDetails.WorkflowFailureReasonDetail == null);
            Assert.IsTrue(abortedworkflows.FirstOrDefault()?.WorkflowFailureDetails.FailedTaskDetails == null);
        }
    }
}
