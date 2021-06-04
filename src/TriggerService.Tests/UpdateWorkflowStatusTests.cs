// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
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
    public class UpdateWorkflowStatusTests
    {
        [TestMethod]
        public async Task SurfaceWorkflowFailure_BatchNodeAllocationFailed()
        {
            var workflowId = Guid.NewGuid().ToString();

            var tesTasks = new[] { GetTesTask(workflowId, shard: -1, attempt: 1, TesTaskLogForBatchNodeAllocationFailure) };

            var (newTriggerName, newTriggerContent) = await UpdateWorkflowStatusAsync(workflowId, tesTasks, WorkflowStatus.Failed);

            var workflowFailureInfo = newTriggerContent?.WorkflowFailureInfo;
            var failedTask = workflowFailureInfo?.FailedTasks?.FirstOrDefault();

            Assert.IsTrue(newTriggerName.StartsWith("failed/"));
            Assert.AreEqual("OneOrMoreTasksFailed", workflowFailureInfo?.WorkflowFailureReason);
            Assert.AreEqual(1, workflowFailureInfo?.FailedTasks.Count);
            Assert.AreEqual("NodeAllocationFailed", failedTask?.FailureReason);
        }

        [TestMethod]
        public async Task SurfaceWorkflowFailure_BatchTaskFailed()
        {
            var workflowId = Guid.NewGuid().ToString();

            var tesTasks = new[] { GetTesTask(workflowId, shard: -1, attempt: 1, TesTaskLogForBatchTaskFailure) };

            var (newTriggerName, newTriggerContent) = await UpdateWorkflowStatusAsync(workflowId, tesTasks, WorkflowStatus.Failed);

            var workflowFailureInfo = newTriggerContent?.WorkflowFailureInfo;
            var failedTask = workflowFailureInfo?.FailedTasks?.FirstOrDefault();

            Assert.IsTrue(newTriggerName.StartsWith("failed/"));
            Assert.AreEqual("OneOrMoreTasksFailed", workflowFailureInfo?.WorkflowFailureReason);
            Assert.AreEqual(1, workflowFailureInfo?.FailedTasks.Count);
            Assert.AreEqual("FailureExitCode", failedTask?.FailureReason);
            Assert.AreEqual(1, failedTask?.SystemLogs?.Count);
            Assert.AreEqual("The task process exited with an unexpected exit code", failedTask?.SystemLogs.First());
            Assert.AreEqual("execution/__batch/stdout.txt", failedTask.StdOut);
            Assert.AreEqual("execution/__batch/stderr.txt", failedTask.StdErr);
        }

        [TestMethod]
        public async Task SurfaceWorkflowFailure_CromwellScriptFailed()
        {
            var workflowId = Guid.NewGuid().ToString();
            var tesTasks = new[] { GetTesTask(workflowId, shard: -1, attempt: 1, TesTaskLogForCromwellScriptFailure) };

            var (newTriggerName, newTriggerContent) = await UpdateWorkflowStatusAsync(workflowId, tesTasks, WorkflowStatus.Failed);

            var workflowFailureInfo = newTriggerContent?.WorkflowFailureInfo;
            var failedTask = workflowFailureInfo?.FailedTasks?.FirstOrDefault();

            Assert.IsTrue(newTriggerName.StartsWith("failed/"));
            Assert.AreEqual("OneOrMoreTasksFailed", workflowFailureInfo?.WorkflowFailureReason);
            Assert.AreEqual(1, workflowFailureInfo?.FailedTasks.Count);
            Assert.AreEqual("CromwellScriptFailed", failedTask?.FailureReason);
            Assert.AreEqual(1, failedTask?.CromwellResultCode);
            Assert.AreEqual("execution/stdout", failedTask.StdOut);
            Assert.AreEqual("execution/stderr", failedTask.StdErr);
        }

        [TestMethod]
        public async Task SurfaceWorkflowFailure_CromwellFailed()
        {
            var workflowId = Guid.NewGuid().ToString();

            var tesTasks = new[] { GetTesTask(workflowId, shard: -1, attempt: 1, TesTaskLogForSuccessfulTask) };

            var (newTriggerName, newTriggerContent) = await UpdateWorkflowStatusAsync(workflowId, tesTasks, WorkflowStatus.Failed);

            var workflowFailureInfo = newTriggerContent?.WorkflowFailureInfo;

            Assert.IsTrue(newTriggerName.StartsWith("failed/"));
            Assert.AreEqual("CromwellFailed", workflowFailureInfo?.WorkflowFailureReason);
            Assert.IsNull(workflowFailureInfo.FailedTasks);
        }

        [TestMethod]
        public async Task SurfaceWorkflowFailure_MultipleFailedTasks()
        {
            var workflowId = Guid.NewGuid().ToString();

            var tesTasks = new[] {
                GetTesTask(workflowId, shard: 1, attempt: 1, TesTaskLogForBatchNodeAllocationFailure),
                GetTesTask(workflowId, shard: 2, attempt: 1, TesTaskLogForBatchTaskFailure),
                GetTesTask(workflowId, shard: 3, attempt: 1, TesTaskLogForSuccessfulTask) };

            var (newTriggerName, newTriggerContent) = await UpdateWorkflowStatusAsync(workflowId, tesTasks, WorkflowStatus.Failed);

            var workflowFailureInfo = newTriggerContent?.WorkflowFailureInfo;

            Assert.IsTrue(newTriggerName.StartsWith("failed/"));
            Assert.AreEqual("OneOrMoreTasksFailed", workflowFailureInfo?.WorkflowFailureReason);
            Assert.AreEqual(2, workflowFailureInfo?.FailedTasks.Count);
            Assert.AreEqual("NodeAllocationFailed", workflowFailureInfo.FailedTasks[0].FailureReason);
            Assert.AreEqual("FailureExitCode", workflowFailureInfo.FailedTasks[1].FailureReason);
        }

        [TestMethod]
        public async Task SurfaceWorkflowFailure_MultipleCromwellAtttempts()
        {
            var workflowId = Guid.NewGuid().ToString();

            var tesTasks = new[] {
                GetTesTask(workflowId, shard: -1, attempt: 1, TesTaskLogForBatchNodeAllocationFailure),
                GetTesTask(workflowId, shard: -1, attempt: 2, TesTaskLogForBatchTaskFailure) };

            var (newTriggerName, newTriggerContent) = await UpdateWorkflowStatusAsync(workflowId, tesTasks, WorkflowStatus.Failed);

            var workflowFailureInfo = newTriggerContent?.WorkflowFailureInfo;
            var failedTask = workflowFailureInfo?.FailedTasks?.FirstOrDefault();

            Assert.IsTrue(newTriggerName.StartsWith("failed/"));
            Assert.AreEqual("OneOrMoreTasksFailed", workflowFailureInfo?.WorkflowFailureReason);
            Assert.AreEqual(1, workflowFailureInfo?.FailedTasks.Count);
            Assert.AreEqual("FailureExitCode", failedTask?.FailureReason);
        }

        [TestMethod]
        public async Task SurfaceWorkflowFailure_MultipleTesAtttempts()
        {
            var workflowId = Guid.NewGuid().ToString();

            var tesTasks = new[] { GetTesTask(workflowId, shard: -1, attempt: 1, TesTaskLogForBatchNodeAllocationFailure, TesTaskLogForBatchTaskFailure) };

            var (newTriggerName, newTriggerContent) = await UpdateWorkflowStatusAsync(workflowId, tesTasks, WorkflowStatus.Failed);

            var workflowFailureInfo = newTriggerContent?.WorkflowFailureInfo;
            var failedTask = workflowFailureInfo?.FailedTasks?.FirstOrDefault();

            Assert.IsTrue(newTriggerName.StartsWith("failed/"));
            Assert.AreEqual("OneOrMoreTasksFailed", workflowFailureInfo?.WorkflowFailureReason);
            Assert.AreEqual(1, workflowFailureInfo?.FailedTasks.Count);
            Assert.AreEqual("FailureExitCode", failedTask?.FailureReason);
        }

        [TestMethod]
        public async Task SurfaceWorkflowFailure_MultipleCromwellAtttemptsResultingInSuccessAreIgnored()
        {
            var workflowId = Guid.NewGuid().ToString();

            var tesTasks = new[] {
                GetTesTask(workflowId, shard: 1, attempt: 1, TesTaskLogForBatchNodeAllocationFailure),
                GetTesTask(workflowId, shard: 1, attempt: 2, TesTaskLogForSuccessfulTask),
                GetTesTask(workflowId, shard: 2, attempt: 1, TesTaskLogForBatchTaskFailure) };

            var (newTriggerName, newTriggerContent) = await UpdateWorkflowStatusAsync(workflowId, tesTasks, WorkflowStatus.Failed);

            var workflowFailureInfo = newTriggerContent?.WorkflowFailureInfo;
            var failedTask = workflowFailureInfo?.FailedTasks?.FirstOrDefault();

            Assert.IsTrue(newTriggerName.StartsWith("failed/"));
            Assert.AreEqual("OneOrMoreTasksFailed", workflowFailureInfo?.WorkflowFailureReason);
            Assert.AreEqual(1, workflowFailureInfo?.FailedTasks.Count);
            Assert.AreEqual("FailureExitCode", failedTask?.FailureReason);
        }

        [TestMethod]
        public async Task SurfaceWorkflowFailure_MultipleTesAtttemptsResultingInSuccessAreIgnored()
        {
            var workflowId = Guid.NewGuid().ToString();

            var tesTasks = new[] {
                GetTesTask(workflowId, shard: 1, attempt: 1, TesTaskLogForBatchNodeAllocationFailure, TesTaskLogForSuccessfulTask),
                GetTesTask(workflowId, shard: 2, attempt: 1, TesTaskLogForBatchTaskFailure) };

            var (newTriggerName, newTriggerContent) = await UpdateWorkflowStatusAsync(workflowId, tesTasks, WorkflowStatus.Failed);

            var workflowFailureInfo = newTriggerContent?.WorkflowFailureInfo;
            var failedTask = workflowFailureInfo?.FailedTasks?.FirstOrDefault();

            Assert.IsTrue(newTriggerName.StartsWith("failed/"));
            Assert.AreEqual("OneOrMoreTasksFailed", workflowFailureInfo?.WorkflowFailureReason);
            Assert.AreEqual(1, workflowFailureInfo?.FailedTasks.Count);
            Assert.AreEqual("FailureExitCode", failedTask?.FailureReason);
        }

        [TestMethod]
        public async Task SurfaceWorkflowFailure_WorkflowAborted()
        {
            var workflowId = Guid.NewGuid().ToString();

            var tesTasks = new[] { GetTesTask(workflowId, shard: 1, attempt: 1, TesTaskLogForSuccessfulTask) };
 
            var (newTriggerName, newTriggerContent) = await UpdateWorkflowStatusAsync(workflowId, tesTasks, WorkflowStatus.Aborted);

            var workflowFailureInfo = newTriggerContent?.WorkflowFailureInfo;

            Assert.IsTrue(newTriggerName.StartsWith("failed/"));
            Assert.AreEqual("Aborted", workflowFailureInfo?.WorkflowFailureReason);
            Assert.IsNull(workflowFailureInfo.FailedTasks);
        }

        [TestMethod]
        public async Task SuccessfulWorkflowHasNoWorkflowFailureInfo()
        {
            var workflowId = Guid.NewGuid().ToString();

            var tesTasks = new[] { GetTesTask(workflowId, shard: 1, attempt: 1, TesTaskLogForSuccessfulTask) };

            var (newTriggerName, newTriggerContent) = await UpdateWorkflowStatusAsync(workflowId, tesTasks, WorkflowStatus.Succeeded);

            Assert.IsTrue(newTriggerName.StartsWith("succeeded/"));
            Assert.IsNull(newTriggerContent.WorkflowFailureInfo);
        }

        [TestMethod]
        public async Task WorkflowNotFoundInCromwellResultsInFailedWorkflow()
        {
            var workflowId = Guid.NewGuid().ToString();

            var tesTasks = new[] { GetTesTask(workflowId, shard: 1, attempt: 1, TesTaskLogForSuccessfulTask) };

            var (newTriggerName, newTriggerContent) = await UpdateWorkflowStatusAsync(workflowId, tesTasks, new CromwellApiException(null, null, System.Net.HttpStatusCode.NotFound));

            Assert.IsTrue(newTriggerName.StartsWith("failed/"));
            Assert.AreEqual("WorkflowNotFoundInCromwell", newTriggerContent?.WorkflowFailureInfo?.WorkflowFailureReason);
        }

        private static TesTask GetTesTask(string workflowId, int shard, int attempt, params TesTaskLog[] tesTaskLogs)
        {
            return new TesTask
            {
                WorkflowId = $"{workflowId}",
                Description = $"BackendJobDescriptorKey_CommandCallNode_BamToUnmappedBams.SortSam:{shard}:{attempt}",
                Executors = new List<TesExecutor> { new TesExecutor { Stdout = "execution/stdout", Stderr = "execution/stderr" } },
                Logs = tesTaskLogs.ToList()
            };
        }

        private static TesTaskLog TesTaskLogForBatchTaskFailure => new TesTaskLog
        {
            Logs = new List<TesExecutorLog> { new TesExecutorLog { ExitCode = 1 } },
            SystemLogs = new List<string> { "FailureExitCode", "The task process exited with an unexpected exit code" },
            FailureReason = "FailureExitCode"
        };

        private static TesTaskLog TesTaskLogForBatchNodeAllocationFailure => new TesTaskLog
        {
            Logs = new List<TesExecutorLog> { new TesExecutorLog { ExitCode = null } },
            FailureReason = "NodeAllocationFailed"
        };

        private static TesTaskLog TesTaskLogForCromwellScriptFailure => new TesTaskLog
        {
            Logs = new List<TesExecutorLog> { new TesExecutorLog { ExitCode = 0 } },
            FailureReason = null,
            CromwellResultCode = 1
        };

        private static TesTaskLog TesTaskLogForSuccessfulTask => new TesTaskLog
        {
            Logs = new List<TesExecutorLog> { new TesExecutorLog { ExitCode = 0 } },
            FailureReason = null,
            CromwellResultCode = 0
        };

        private Task<(string newTriggerName, Workflow newTriggerContent)> UpdateWorkflowStatusAsync(string workflowId, IEnumerable<TesTask> tesTasks, WorkflowStatus cromwellWorkflowStatus)
        {
            return UpdateWorkflowStatusAsync(
                workflowId, 
                tesTasks, 
                cromwellApiClient => cromwellApiClient
                    .Setup(ac => ac.GetStatusAsync(It.IsAny<Guid>()))
                    .Returns(Task.FromResult(new GetStatusResponse { Id = Guid.Parse(workflowId), Status = cromwellWorkflowStatus })));
        }

        private Task<(string newTriggerName, Workflow newTriggerContent)> UpdateWorkflowStatusAsync(string workflowId, IEnumerable<TesTask> tesTasks, Exception cromwellWorkflowStatusException)
        {
            return UpdateWorkflowStatusAsync(
                workflowId,
                tesTasks,
                cromwellApiClient => cromwellApiClient
                    .Setup(ac => ac.GetStatusAsync(It.IsAny<Guid>()))
                    .Throws(cromwellWorkflowStatusException) );
        }

        private async Task<(string newTriggerName, Workflow newTriggerContent)> UpdateWorkflowStatusAsync(string workflowId, IEnumerable<TesTask> tesTasks, Action<Mock<ICromwellApiClient>> cromwellApiClientSetup)
        {
            string newTriggerName = null;
            Workflow newTriggerContent = null;

            var loggerFactory = new Mock<ILoggerFactory>();
            var azureStorage = new Mock<IAzureStorage>();
            var repository = new Mock<IRepository<TesTask>>();
            var cromwellApiClient = new Mock<ICromwellApiClient>();

            loggerFactory
                .Setup(f => f.CreateLogger(It.IsAny<string>()))
                .Returns(new Mock<ILogger>().Object);

            azureStorage
                .Setup(az => az.DownloadBlobTextAsync(It.IsAny<string>(), $"inprogress/inprogress.Sample.{workflowId}.json"))
                .Returns(Task.FromResult(@"{'WorkflowUrl': 'https://bam-to-unmapped-bams.wdl','WorkflowInputsUrl': 'https://bam-to-unmapped-bams.inputs.json'}"));

            azureStorage
                .Setup(az => az.GetWorkflowsByStateAsync(WorkflowState.InProgress))
                .Returns(Task.FromResult(new[] {
                    new TriggerFile {
                        Uri = $"http://tempuri.org/workflows/inprogress/inprogress.Sample.{workflowId}.json",
                        ContainerName = "workflows",
                        Name = $"inprogress/inprogress.Sample.{workflowId}.json",
                        LastModified = DateTimeOffset.UtcNow.AddMinutes(-5) } }.AsEnumerable()));

            azureStorage
                .Setup(az => az.UploadFileTextAsync(It.IsAny<string>(), "workflows", It.IsAny<string>()))
                .Callback((string content, string container, string blobName) => {
                    newTriggerName = blobName;
                    newTriggerContent = content != null ? JsonConvert.DeserializeObject<Workflow>(content) : null; });

            cromwellApiClientSetup(cromwellApiClient);

            cromwellApiClient
                .Setup(ac => ac.GetOutputsAsync(It.IsAny<Guid>()))
                .Returns(Task.FromResult(new GetOutputsResponse()));

            cromwellApiClient
                .Setup(ac => ac.GetMetadataAsync(It.IsAny<Guid>()))
                .Returns(Task.FromResult(new GetMetadataResponse()));

            cromwellApiClient
                .Setup(ac => ac.GetTimingAsync(It.IsAny<Guid>()))
                .Returns(Task.FromResult(new GetTimingResponse()));

            repository
                .Setup(r => r.GetItemsAsync(It.IsAny<Expression<Func<TesTask, bool>>>()))
                .Returns(Task.FromResult(tesTasks));

            var cromwellOnAzureEnvironment = new CromwellOnAzureEnvironment(loggerFactory.Object, azureStorage.Object, cromwellApiClient.Object, repository.Object);

            await cromwellOnAzureEnvironment.UpdateWorkflowStatusesAsync();

            return (newTriggerName, newTriggerContent);
        }
    }
}
