// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Common;
using CommonUtilities.AzureCloud;
using CromwellApiClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Newtonsoft.Json;
using Tes.Models;
using Tes.Repository;
using Tes.TaskSubmitters;

namespace TriggerService.Tests
{
    [TestClass]
    public class UpdateWorkflowStatusTests
    {
        public UpdateWorkflowStatusTests()
            => Common.NewtonsoftJsonSafeInit.SetDefaultSettings();

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
            Assert.AreEqual($"/tes-internal/{tesTasks.First().Id}/stdout.txt", failedTask.StdOut);
            Assert.AreEqual($"/tes-internal/{tesTasks.First().Id}/stderr.txt", failedTask.StdErr);
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

        [TestMethod]
        public async Task TaskWarningsAreAddedToSuccessfulWorkflow()
        {
            var workflowId = Guid.NewGuid().ToString();

            var tesTasks = new[] { GetTesTask(workflowId, shard: 1, attempt: 1, TesTaskLogForSuccessfulTaskWithWarning) };

            var (newTriggerName, newTriggerContent) = await UpdateWorkflowStatusAsync(workflowId, tesTasks, WorkflowStatus.Succeeded);

            Assert.IsTrue(newTriggerName.StartsWith("succeeded/"));
            Assert.AreEqual(1, newTriggerContent?.TaskWarnings?.Count);

            var taskWarning = newTriggerContent?.TaskWarnings?.FirstOrDefault();
            Assert.AreEqual("Warning1", taskWarning?.Warning);
            Assert.AreEqual(1, taskWarning?.WarningDetails.Count);
            Assert.AreEqual("Warning1Details", taskWarning?.WarningDetails.FirstOrDefault());
        }

        [TestMethod]
        public async Task TaskWarningsAreAddedToFailedWorkflow()
        {
            var workflowId = Guid.NewGuid().ToString();

            var tesTasks = new[] {
                GetTesTask(workflowId, shard: 1, attempt: 1, TesTaskLogForSuccessfulTaskWithWarning),
                GetTesTask(workflowId, shard: 2, attempt: 1, TesTaskLogForBatchTaskFailure) };

            var (newTriggerName, newTriggerContent) = await UpdateWorkflowStatusAsync(workflowId, tesTasks, WorkflowStatus.Failed);

            Assert.IsTrue(newTriggerName.StartsWith("failed/"));
            Assert.AreEqual(1, newTriggerContent?.TaskWarnings?.Count);

            var taskWarning = newTriggerContent?.TaskWarnings?.FirstOrDefault();
            Assert.AreEqual("Warning1", taskWarning?.Warning);
            Assert.AreEqual(1, taskWarning?.WarningDetails.Count);
            Assert.AreEqual("Warning1Details", taskWarning?.WarningDetails.FirstOrDefault());

            var workflowFailureInfo = newTriggerContent?.WorkflowFailureInfo;
            Assert.AreEqual("OneOrMoreTasksFailed", workflowFailureInfo?.WorkflowFailureReason);
            Assert.AreEqual(1, workflowFailureInfo?.FailedTasks.Count);
        }

        private static TesTask GetTesTask(string workflowId, int shard, int attempt, params TesTaskLog[] tesTaskLogs)
        {
            TesTask task = new()
            {
                Description = $"{workflowId}:BackendJobDescriptorKey_CommandCallNode_BamToUnmappedBams.SortSam:{shard}:{attempt}",
                Executors = [new() { Stdout = "execution/stdout", Stderr = "execution/stderr" }],
                Inputs = [new() { Name = "commandScript", Description = "workflow1.Task1.commandScript", Path = $"/cromwell-executions/test/{workflowId}/call-hello/{ShardString(shard)}execution/script", Type = TesFileType.FILE }],
                Outputs =
                [
                    new() { Name = "commandScript", Path = $"/cromwell-executions/test/{workflowId}/call-hello/{ShardString(shard)}execution/script" },
                    new() { Name = "stderr", Path = $"/cromwell-executions/test/{workflowId}/call-hello/{ShardString(shard)}execution/stderr" },
                    new() { Name = "stdout", Path = $"/cromwell-executions/test/{workflowId}/call-hello/{ShardString(shard)}execution/stdout" },
                    new() { Name = "rc", Path = $"/cromwell-executions/test/{workflowId}/call-hello/{ShardString(shard)}execution/rc" },
                ],
                Logs = [.. tesTaskLogs]
            };

            task.TaskSubmitter = TaskSubmitter.Parse(task);
            return task;

            static string ShardString(int shard) =>
                shard == -1 ? string.Empty : $"shard-{shard}/";
        }

        private static TesTaskLog TesTaskLogForBatchTaskFailure => new()
        {
            Logs = [new() { ExitCode = 1 }],
            SystemLogs = ["FailureExitCode", "The task process exited with an unexpected exit code"],
            FailureReason = "FailureExitCode"
        };

        private static TesTaskLog TesTaskLogForBatchNodeAllocationFailure => new()
        {
            Logs = [new() { ExitCode = null }],
            FailureReason = "NodeAllocationFailed"
        };

        private static TesTaskLog TesTaskLogForCromwellScriptFailure => new()
        {
            Logs = [new() { ExitCode = 0 }],
            FailureReason = null,
            CromwellResultCode = 1
        };

        private static TesTaskLog TesTaskLogForSuccessfulTask => new()
        {
            Logs = [new() { ExitCode = 0 }],
            FailureReason = null,
            CromwellResultCode = 0
        };

        private static TesTaskLog TesTaskLogForSuccessfulTaskWithWarning => new()
        {
            Logs = [new() { ExitCode = 0 }],
            SystemLogs = ["Warning1", "Warning1Details"],
            Warning = "Warning1"
        };

        private static Task<(string newTriggerName, Workflow newTriggerContent)> UpdateWorkflowStatusAsync(string workflowId, IEnumerable<TesTask> tesTasks, WorkflowStatus cromwellWorkflowStatus)
            => UpdateWorkflowStatusAsync(
                workflowId,
                tesTasks,
                cromwellApiClient => cromwellApiClient
                    .Setup(ac => ac.GetStatusAsync(It.IsAny<Guid>()))
                    .Returns(Task.FromResult(new GetStatusResponse { Id = Guid.Parse(workflowId), Status = cromwellWorkflowStatus })));

        private static Task<(string newTriggerName, Workflow newTriggerContent)> UpdateWorkflowStatusAsync(string workflowId, IEnumerable<TesTask> tesTasks, Exception cromwellWorkflowStatusException)
            => UpdateWorkflowStatusAsync(
                workflowId,
                tesTasks,
                cromwellApiClient => cromwellApiClient
                    .Setup(ac => ac.GetStatusAsync(It.IsAny<Guid>()))
                    .Throws(cromwellWorkflowStatusException));

        private static async Task<(string newTriggerName, Workflow newTriggerContent)> UpdateWorkflowStatusAsync(string workflowId, IEnumerable<TesTask> tesTasks, Action<Mock<ICromwellApiClient>> cromwellApiClientSetup)
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
                .Returns(AsyncEnumerable.Repeat(
                    new TriggerFile
                    {
                        Uri = $"http://tempuri.org/workflows/inprogress/inprogress.Sample.{workflowId}.json",
                        ContainerName = "workflows",
                        Name = $"inprogress/inprogress.Sample.{workflowId}.json",
                        LastModified = DateTimeOffset.UtcNow.AddMinutes(-5)
                    }, 1));

            azureStorage
                .Setup(az => az.UploadFileTextAsync(It.IsAny<string>(), "workflows", It.IsAny<string>()))
                .Callback((string content, string container, string blobName) =>
                {
                    newTriggerName = blobName;
                    newTriggerContent = content is not null ? JsonConvert.DeserializeObject<Workflow>(content) : null;
                });

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
                .Setup(r => r.GetItemsAsync(It.IsAny<Expression<Func<TesTask, bool>>>(), It.IsAny<System.Threading.CancellationToken>()))
                .Returns(Task.FromResult(tesTasks));

            var logger = new Mock<ILogger<TriggerHostedService>>().Object;
            var triggerServiceOptions = new Mock<IOptions<TriggerServiceOptions>>();

            triggerServiceOptions.Setup(o => o.Value).Returns(new TriggerServiceOptions()
            {
                DefaultStorageAccountName = "fakestorage",
                ApplicationInsightsAccountName = "fakeappinsights"
            });
            var postgreSqlOptions = new Mock<IOptions<PostgreSqlOptions>>().Object;
            var storageUtility = new Mock<IAzureStorageUtility>();

            storageUtility
                .Setup(x => x.GetStorageAccountsUsingMsiAsync(It.IsAny<string>()))
                .Returns(Task.FromResult((new List<IAzureStorage>(), azureStorage.Object)));

            var azureCloudConfig = AzureCloudConfig.CreateAsync().Result;
            var cromwellOnAzureEnvironment = new TriggerHostedService(logger, triggerServiceOptions.Object, cromwellApiClient.Object, repository.Object, storageUtility.Object, azureCloudConfig);

            await cromwellOnAzureEnvironment.UpdateWorkflowStatusesAsync();

            return (newTriggerName, newTriggerContent);
        }
    }
}
