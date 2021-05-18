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
    public class SurfaceWorkflowFailureDetailsToTriggerTests
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
                .GetRecentlyUpdatedBlobsAsync(AzureStorage.WorkflowState.InProgress))
                .Returns(Task.FromResult(new List<CloudBlockBlob> {
                    new CloudBlockBlob(new Uri($@"http://tempuri.org/workflow/inprogress/inprogress.Sample.{mockWorkflow_id}.json"))}
                    .AsEnumerable()));

            azStorageMock.Setup(az => az
            .DownloadBlobTextAsync(It.IsAny<string>(), $"inprogress/inprogress.Sample.{ mockWorkflow_id}.json"))
            .Returns(Task.FromResult(@"{'WorkflowUrl': 'https://bam-to-unmapped-bams.wdl','WorkflowInputsUrl': 'https://bam-to-unmapped-bams.inputs.json'}"));

            azStorageMock.Setup(az => az
            .DownloadBlobTextAsync(It.IsAny<string>(), $"failed/inprogress.Sample.{ mockWorkflow_id}.json"))
            .Returns(Task.FromResult(@"{'WorkflowUrl': 'https://bam-to-unmapped-bams.wdl','WorkflowInputsUrl': 'https://bam-to-unmapped-bams.inputs.json','WorkflowFailureDetails': {'WorkflowFailureReason': 'OneOrMoreTasksFailed', 'FailedTaskDetails': [{'TaskId': '4980dfbb_a1887d20c2c344c2a0a33212afbdb04f','FailureReason': 'JobNotFound','SystemLogs': [],'StdOut': 'stdout','StdErr': 'stderr'}]}}"));

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

            var updatedworkflows = await environment.UpdateWorkflowStatusesAsync();

            Assert.IsTrue(updatedworkflows.FirstOrDefault()?.WorkflowFailureDetails.WorkflowFailureReason == "OneOrMoreTasksFailed");
            Assert.IsTrue(updatedworkflows.FirstOrDefault()?.WorkflowFailureDetails.FailedTaskDetails.Count > 0);
            Assert.IsTrue(updatedworkflows.FirstOrDefault()?.WorkflowFailureDetails.FailedTaskDetails[0].FailureReason == "JobNotFound");
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
                .GetRecentlyUpdatedBlobsAsync(AzureStorage.WorkflowState.InProgress))
                .Returns(Task.FromResult(new List<CloudBlockBlob> {
                    new CloudBlockBlob(new Uri($@"http://tempuri.org/workflow/inprogress/inprogress.Sample.{mockWorkflow_id}.json"))}
                    .AsEnumerable()));

            azStorageMock.Setup(az => az
                .GetRecentlyUpdatedBlobsAsync(AzureStorage.WorkflowState.InProgress))
                .Returns(Task.FromResult(new List<CloudBlockBlob> {
                    new CloudBlockBlob(new Uri($@"http://tempuri.org/workflow/inprogress/inprogress.Sample.{mockWorkflow_id}.json"))}
                    .AsEnumerable()));

            azStorageMock.Setup(az => az
            .DownloadBlobTextAsync(It.IsAny<string>(), $"inprogress/inprogress.Sample.{ mockWorkflow_id}.json"))
            .Returns(Task.FromResult(@"{'WorkflowUrl': 'https://bam-to-unmapped-bams.wdl','WorkflowInputsUrl': 'https://bam-to-unmapped-bams.inputs.json'}"));

            azStorageMock.Setup(az => az
            .DownloadBlobTextAsync(It.IsAny<string>(), $"failed/inprogress.Sample.{ mockWorkflow_id}.json"))
            .Returns(Task.FromResult(@"{'WorkflowUrl': 'https://bam-to-unmapped-bams.wdl','WorkflowInputsUrl': 'https://bam-to-unmapped-bams.inputs.json','WorkflowFailureDetails': {'WorkflowFailureReason': 'CromwellApiException', 'WorkflowFailureReasonDetail': 'Cromwell reported workflow as NotFound (404)'}}"));

            azStorageMock.Setup(az => az
            .DeleteBlobIfExistsAsync(It.IsAny<string>(), It.IsAny<string>()));

            azStorageMock.Setup(az => az
            .UploadFileTextAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
                .Returns(Task.FromResult(It.IsAny<string>()));

            var cromwellApiClient = new Mock<ICromwellApiClient>();
            cromwellApiClient.Setup(ac => ac
                .GetStatusAsync(It.IsAny<Guid>()))
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

            var updatedworkflows = await environment.UpdateWorkflowStatusesAsync();

            Assert.IsTrue(updatedworkflows.Any(w =>
            w.WorkflowFailureDetails.WorkflowFailureReason == "CromwellApiException"
            && w.WorkflowFailureDetails.WorkflowFailureReasonDetail != null
            && w.WorkflowFailureDetails.FailedTaskDetails == null));

            Assert.IsTrue(updatedworkflows.FirstOrDefault()?.WorkflowFailureDetails.WorkflowFailureReason == "CromwellApiException");
            Assert.IsTrue(updatedworkflows.FirstOrDefault()?.WorkflowFailureDetails.FailedTaskDetails == null);
            Assert.IsTrue(updatedworkflows.FirstOrDefault()?.WorkflowFailureDetails.WorkflowFailureReasonDetail == "Cromwell reported workflow as NotFound (404)");
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
                .GetRecentlyUpdatedBlobsAsync(AzureStorage.WorkflowState.InProgress))
                .Returns(Task.FromResult(new List<CloudBlockBlob> {
                    new CloudBlockBlob(new Uri($@"http://tempuri.org/workflow/inprogress/inprogress.Sample.{mockWorkflow_id}.json"))}
                    .AsEnumerable()));

            azStorageMock.Setup(az => az
            .DownloadBlobTextAsync(It.IsAny<string>(), $"inprogress/inprogress.Sample.{ mockWorkflow_id}.json"))
            .Returns(Task.FromResult(@"{'WorkflowUrl': 'https://bam-to-unmapped-bams.wdl','WorkflowInputsUrl': 'https://bam-to-unmapped-bams.inputs.json'}"));

            azStorageMock.Setup(az => az
            .DownloadBlobTextAsync(It.IsAny<string>(), $"failed/inprogress.Sample.{ mockWorkflow_id}.json"))
            .Returns(Task.FromResult(@"{'WorkflowUrl': 'https://bam-to-unmapped-bams.wdl','WorkflowInputsUrl': 'https://bam-to-unmapped-bams.inputs.json','WorkflowFailureDetails': {'WorkflowFailureReason': 'BatchFailed', 'FailedTaskDetails': [{'TaskId': '4980dfbb_a1887d20c2c344c2a0a33212afbdb04f','FailureReason': 'DummyFailureReason','SystemLogs': ['ActiveJobAndScheduleQuotaReached'],'StdOut': '/__batch/stderr','StdErr': '/__batch/stderr'}]}}"));

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
                     SystemLogs = new List<string>{ "ActiveJobAndScheduleQuotaReached" }},}} }.AsEnumerable(); 

            cosmosdbRepositoryMock.Setup(r => r
            .GetItemsAsync(It.IsAny<Expression<Func<TesTask, bool>>>()))
            .Returns(Task.FromResult(failedTesTasks));

            var environment = new CromwellOnAzureEnvironment(
                serviceProvider.GetRequiredService<ILoggerFactory>(),
                azStorageMock.Object,
                cromwellApiClient.Object,
                cosmosdbRepositoryMock.Object);

            var updatedworkflows = await environment.UpdateWorkflowStatusesAsync();

            Assert.IsTrue(updatedworkflows.FirstOrDefault()?.WorkflowFailureDetails.WorkflowFailureReason == (failedTesTasks.Any(t =>
                    (t.Logs?.LastOrDefault()?.Logs?.LastOrDefault()?.ExitCode).GetValueOrDefault() != 0) ?
                    "BatchFailed" : "OneOrMoreTasksFailed"));
            Assert.IsTrue(updatedworkflows.FirstOrDefault()?.WorkflowFailureDetails.FailedTaskDetails.Count > 0);
            
        }
    }
}
