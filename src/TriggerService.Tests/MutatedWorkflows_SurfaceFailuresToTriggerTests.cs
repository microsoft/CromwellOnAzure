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
    public class MutatedWorkflows_SurfaceFailuresToTriggerTests
    {
        private IServiceCollection serviceCollection;

        private ServiceProvider serviceProvider;

        private readonly Mock<IAzureStorage> azStorageMock;

        private readonly Mock<IRepository<TesTask>> cosmosdbRepositoryMock;

        private readonly Mock<ICromwellApiClient> cromwellApiClient;

        private CromwellOnAzureEnvironment environment;

        private Guid mockWorkflow_id;

        public MutatedWorkflows_SurfaceFailuresToTriggerTests()
        {
            this.serviceCollection = (ServiceCollection)new ServiceCollection()
                .AddLogging(loggingBuilder => loggingBuilder.AddConsole());

            this.serviceProvider = serviceCollection.BuildServiceProvider();

            this.azStorageMock = new Mock<IAzureStorage>();

            this.cosmosdbRepositoryMock = new Mock<IRepository<TesTask>>();

            this.cromwellApiClient = new Mock<ICromwellApiClient>();

            this.azStorageMock.Setup(az => az
            .DeleteBlobIfExistsAsync(It.IsAny<string>(), It.IsAny<string>()));

            this.azStorageMock.Setup(az => az
            .UploadFileTextAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
                .Returns(Task.FromResult(It.IsAny<string>()));

            this.cromwellApiClient.Setup(ac => ac
                .GetOutputsAsync(It.IsAny<Guid>()))
                .Returns(Task.FromResult(new GetOutputsResponse { }));

            this.cromwellApiClient.Setup(ac => ac
            .GetMetadataAsync(It.IsAny<Guid>()))
                .Returns(Task.FromResult(new GetMetadataResponse()));

            this.cromwellApiClient.Setup(ac => ac
            .GetTimingAsync(It.IsAny<Guid>()))
                .Returns(Task.FromResult(new GetTimingResponse()));
        }

        [TestInitialize]
        public void TestInitializer()
        {
            this.mockWorkflow_id = Guid.NewGuid();

            this.azStorageMock.Setup(az => az
            .DownloadBlobTextAsync(It.IsAny<string>(), $"inprogress/inprogress.Sample.{ this.mockWorkflow_id}.json"))
            .Returns(Task.FromResult(@"{'WorkflowUrl': 'https://bam-to-unmapped-bams.wdl','WorkflowInputsUrl': 'https://bam-to-unmapped-bams.inputs.json'}"));

            this.azStorageMock.Setup(az => az
                .GetRecentlyUpdatedInProgressWorkflowBlobsAsync())
                .Returns(Task.FromResult(new List<CloudBlockBlob> {
                    new CloudBlockBlob(new Uri($@"http://tempuri.org/workflow/inprogress/inprogress.Sample.{this.mockWorkflow_id}.json"))}
                    .AsEnumerable()));

            this.cromwellApiClient.Setup(ac => ac
                .GetStatusAsync(It.IsAny<Guid>()))
                .Returns(Task.FromResult(new GetStatusResponse
                {
                    Id = Guid.Parse($"{this.mockWorkflow_id}"),
                    Status = WorkflowStatus.Failed
                }));
        }
        [TestMethod]
        public async Task SurfaceWorkflowFailure_From_FailingTesTasksAsync()
        {
            this.azStorageMock.Setup(az => az
            .DownloadBlobTextAsync(It.IsAny<string>(), $"failed/inprogress.Sample.{ this.mockWorkflow_id}.json"))
            .Returns(Task.FromResult(@"{'WorkflowUrl': 'https://bam-to-unmapped-bams.wdl','WorkflowInputsUrl': 'https://bam-to-unmapped-bams.inputs.json','WorkflowFailureDetails': {'WorkflowFailureReason': 'OneOrMoreTasksFailed', 'FailedTaskDetails': [{'TaskId': 'a1887d20c2c344c2a0a33212afbdb04f','FailureReason': 'JobNotFound','SystemLogs': [],'StdOut': 'stdout','StdErr': 'stderr'}]}}"));

            var failedTesTasks = new List<TesTask> { new TesTask {
             WorkflowId = $"{this.mockWorkflow_id}",
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
              FailureReason = "JobNotFound"}}}}.AsEnumerable();

            this.cosmosdbRepositoryMock.Setup(r => r
            .GetItemsAsync(It.IsAny<Expression<Func<TesTask, bool>>>()))
            .Returns(Task.FromResult(failedTesTasks));

            this.environment = new CromwellOnAzureEnvironment(
                this.serviceProvider.GetRequiredService<ILoggerFactory>(),
                this.azStorageMock.Object,
                this.cromwellApiClient.Object,
                this.cosmosdbRepositoryMock.Object);

            var updatedworkflows = await this.environment.UpdateWorkflowStatusesAsync();

            Assert.IsTrue(updatedworkflows.FirstOrDefault()?.WorkflowFailureDetails.WorkflowFailureReason == "OneOrMoreTasksFailed");
            Assert.IsTrue(updatedworkflows.FirstOrDefault()?.WorkflowFailureDetails.FailedTaskDetails.Count > 0);
            Assert.IsTrue(updatedworkflows.FirstOrDefault()?.WorkflowFailureDetails.FailedTaskDetails[0].FailureReason == "JobNotFound");
        }

        [TestMethod]
        public async Task SurfaceWorkflowFailure_From_CromwellApiException()
        {
            this.azStorageMock.Setup(az => az
            .DownloadBlobTextAsync(It.IsAny<string>(), $"failed/inprogress.Sample.{ mockWorkflow_id}.json"))
            .Returns(Task.FromResult(@"{'WorkflowUrl': 'https://bam-to-unmapped-bams.wdl','WorkflowInputsUrl': 'https://bam-to-unmapped-bams.inputs.json','WorkflowFailureDetails': {'WorkflowFailureReason': 'CromwellApiException', 'WorkflowFailureReasonDetail': 'Cromwell reported workflow as NotFound (404)'}}"));

            this.cromwellApiClient.Setup(ac => ac
                .GetStatusAsync(It.IsAny<Guid>()))
                .Throws(new CromwellApiException("", null, System.Net.HttpStatusCode.NotFound));

            var failedTesTasks = new List<TesTask> { new TesTask {
             WorkflowId = $"{this.mockWorkflow_id}",
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
              }}}}.AsEnumerable();

            this.cosmosdbRepositoryMock.Setup(r => r
            .GetItemsAsync(It.IsAny<Expression<Func<TesTask, bool>>>()))
            .Returns(Task.FromResult(failedTesTasks));

            this.environment = new CromwellOnAzureEnvironment(
                this.serviceProvider.GetRequiredService<ILoggerFactory>(),
                this.azStorageMock.Object,
                this.cromwellApiClient.Object,
                this.cosmosdbRepositoryMock.Object);

            var updatedworkflows = await this.environment.UpdateWorkflowStatusesAsync();

            Assert.IsTrue(updatedworkflows.FirstOrDefault()?.WorkflowFailureDetails.WorkflowFailureReason == "CromwellApiException");
            Assert.IsTrue(updatedworkflows.FirstOrDefault()?.WorkflowFailureDetails.FailedTaskDetails == null);
            Assert.IsTrue(updatedworkflows.FirstOrDefault()?.WorkflowFailureDetails.WorkflowFailureReasonDetail == "Cromwell reported workflow as NotFound (404)");
        }

        [TestMethod]
        public async Task SurfaceWorkflowFailure_From_FailingBatchAsync()
        {
            this.azStorageMock.Setup(az => az
            .DownloadBlobTextAsync(It.IsAny<string>(), $"failed/inprogress.Sample.{ this.mockWorkflow_id}.json"))
            .Returns(Task.FromResult(@"{'WorkflowUrl': 'https://bam-to-unmapped-bams.wdl','WorkflowInputsUrl': 'https://bam-to-unmapped-bams.inputs.json','WorkflowFailureDetails': {'WorkflowFailureReason': 'BatchFailed', 'FailedTaskDetails': [{'TaskId': '4980dfbb_a1887d20c2c344c2a0a33212afbdb04f','FailureReason': 'DummyFailureReason','SystemLogs': ['ActiveJobAndScheduleQuotaReached'],'StdOut': '/__batch/stderr','StdErr': '/__batch/stderr'}]}}"));

            var failedTesTasks = new List<TesTask> { new TesTask {
             WorkflowId = $"{this.mockWorkflow_id}",
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

            this.cosmosdbRepositoryMock.Setup(r => r
            .GetItemsAsync(It.IsAny<Expression<Func<TesTask, bool>>>()))
            .Returns(Task.FromResult(failedTesTasks));

            this.environment = new CromwellOnAzureEnvironment(
                this.serviceProvider.GetRequiredService<ILoggerFactory>(),
                this.azStorageMock.Object,
                this.cromwellApiClient.Object,
                this.cosmosdbRepositoryMock.Object);

            var updatedworkflows = await this.environment.UpdateWorkflowStatusesAsync();

            Assert.IsTrue(updatedworkflows.FirstOrDefault()?.WorkflowFailureDetails.WorkflowFailureReason == "BatchFailed");
            Assert.IsTrue(updatedworkflows.FirstOrDefault()?.WorkflowFailureDetails.FailedTaskDetails.Count > 0);
        }

        [TestMethod]
        public async Task SurfaceWorkflowFailure_From_FailingCromwellAsync()
        {
            this.azStorageMock.Setup(az => az
            .DownloadBlobTextAsync(It.IsAny<string>(), $"failed/inprogress.Sample.{ this.mockWorkflow_id}.json"))
            .Returns(Task.FromResult(@"{'WorkflowUrl': 'https://bam-to-unmapped-bams.wdl','WorkflowInputsUrl': 'https://bam-to-unmapped-bams.inputs.json','WorkflowFailureDetails': {'WorkflowFailureReason': 'CromwellFailed', 'FailedTaskDetails': []}}"));

            var failedTesTasks = new List<TesTask> { }.AsEnumerable();

            this.cosmosdbRepositoryMock.Setup(r => r
            .GetItemsAsync(It.IsAny<Expression<Func<TesTask, bool>>>()))
            .Returns(Task.FromResult(failedTesTasks));

            this.environment = new CromwellOnAzureEnvironment(
                this.serviceProvider.GetRequiredService<ILoggerFactory>(),
                this.azStorageMock.Object,
                this.cromwellApiClient.Object,
                this.cosmosdbRepositoryMock.Object);

            var updatedworkflows = await this.environment.UpdateWorkflowStatusesAsync();

            Assert.IsTrue(updatedworkflows.FirstOrDefault()?.WorkflowFailureDetails.WorkflowFailureReason == "CromwellFailed");
            Assert.IsTrue(updatedworkflows.FirstOrDefault()?.WorkflowFailureDetails.FailedTaskDetails.Count == 0);
        }
    }
}
