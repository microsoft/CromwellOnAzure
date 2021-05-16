// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using System.Linq.Expressions;
using Common;
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
        public async Task WorkflowFailureCausedByTesTaskFailureAsync()
        {
            var serviceCollection = new ServiceCollection()
                .AddLogging(loggingBuilder => loggingBuilder.AddConsole());

            var serviceProvider = serviceCollection.BuildServiceProvider();

            var azStorageMock = new Mock<IAzureStorage>();

            var cosmosdbRepositoryMock = new Mock<IRepository<TesTask>>();

            var mockWorkflow_id = Guid.NewGuid();

            azStorageMock.Setup(az => az
                .GetWorkflowsReadyForStateUpdateAsync(AzureStorage.WorkflowState.InProgress))
                .Returns(Task.FromResult(new List<CloudBlockBlob> {
                    new CloudBlockBlob(new Uri($@"http://tempuri.org/InProgress/InProgress.Sample.{mockWorkflow_id}.json"))}
                    .AsEnumerable()));

            azStorageMock.Setup(az => az
               .MutateStateAsync(It.IsAny<string>(),
               It.IsAny<string>(),
               AzureStorage.WorkflowState.Failed,
               It.IsAny<Action<Workflow>>()))
               .Returns(Task.FromResult(new Workflow {  
                   WorkflowFailureDetails = new WorkflowFailureInfo { 
                    WorkflowFailureReason="OneOrMoreTasksFailed"}}));

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
              Stdout="ExecutorStdout",
              Stderr="ExecutorStdout"}},
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

            Assert.IsTrue(updatedworkflows.Any(w => 
            w.WorkflowFailureDetails.WorkflowFailureReason == "OneOrMoreTasksFailed"));
        }
    }
}
