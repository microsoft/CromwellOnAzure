﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Common;
using CommonUtilities.AzureCloud;
using CromwellApiClient;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Tes.Models;
using Tes.Repository;

[assembly: InternalsVisibleTo("TriggerService.Tests")]
namespace TriggerService
{
    public partial class TriggerHostedService : ITriggerHostedService, IHostedService
    {
        private const string OutputsContainerName = "outputs";
        private const int RetryLimit = 3;
        private readonly CancellationTokenSource hostCancellationTokenSource = new();
        private static readonly TimeSpan inProgressWorkflowInvisibilityPeriod = TimeSpan.FromMinutes(1);

        [GeneratedRegex("^[^/]*")]
        private static partial Regex WorkflowStateRegex();
        private static readonly Regex workflowStateRegex = WorkflowStateRegex();

        [GeneratedRegex("^(?:https?://|/)?[^/]+/[^/]+/([^?.]+)")]
        private static partial Regex BlobNameRegex();
        private static readonly Regex blobNameRegex = BlobNameRegex();  // Supporting "http://account.blob.core.windows.net/container/blob", "/account/container/blob" and "account/container/blob" URLs in the trigger file.

        private IAzureStorage storage { get; set; }
        internal ICromwellApiClient cromwellApiClient { get; set; }
        private readonly List<IAzureStorage> storageAccounts;
        private readonly ConcurrentDictionary<Guid, int> workflowRetries = [];
        private readonly ILogger<TriggerHostedService> logger;
        private readonly IRepository<TesTask> tesTaskRepository;
        private readonly AzureCloudConfig azureCloudConfig;
        private readonly AvailabilityTracker cromwellAvailability = new();
        private readonly AvailabilityTracker azStorageAvailability = new();
        private readonly TimeSpan mainInterval;
        private readonly TimeSpan availabilityCheckInterval;

        public TriggerHostedService(
            ILogger<TriggerHostedService> logger,
            IOptions<TriggerServiceOptions> triggerServiceOptions,
            ICromwellApiClient cromwellApiClient,
            IRepository<TesTask> tesTaskRepository,
            IAzureStorageUtility azureStorageUtility,
            AzureCloudConfig azureCloudConfig)
        {
            this.azureCloudConfig = azureCloudConfig;
            mainInterval = TimeSpan.FromMilliseconds(triggerServiceOptions.Value.MainRunIntervalMilliseconds);
            availabilityCheckInterval = TimeSpan.FromMilliseconds(triggerServiceOptions.Value.AvailabilityCheckIntervalMilliseconds);
            this.cromwellApiClient = cromwellApiClient;
            this.logger = logger;
            logger.LogInformation("Cromwell URL: {CromwellUrl}", cromwellApiClient.GetUrl());

            (storageAccounts, storage) = azureStorageUtility.GetStorageAccountsUsingMsiAsync(triggerServiceOptions.Value.DefaultStorageAccountName).Result;

            this.tesTaskRepository = tesTaskRepository;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            return Task.Run(RunAsync, cancellationToken);
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            hostCancellationTokenSource.Cancel();
            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        public async Task<bool> IsCromwellAvailableAsync()
        {
            try
            {
                _ = await cromwellApiClient.PostQueryAsync("[{\"pageSize\": \"1\"}]", hostCancellationTokenSource.Token);
                return true;
            }
            catch (CromwellApiException exc)
            {
                logger.LogWarning("Cromwell is unavailable.  Reason: {Message}", exc.Message);
                return false;
            }
        }

        /// <inheritdoc/>
        public async Task<bool> IsAzureStorageAvailableAsync()
            => await storage.IsAvailableAsync(hostCancellationTokenSource.Token);

        /// <inheritdoc/>
        public async Task UpdateExistingWorkflowsAsync()
        {
            try
            {
                await UpdateWorkflowStatusesAsync();
            }
            catch (Exception exc) when (exc is not OperationCanceledException)
            {
                logger.LogError(exc, "UpdateExistingWorkflowsAsync()");
                await Task.Delay(TimeSpan.FromSeconds(2), hostCancellationTokenSource.Token);
            }
        }

        /// <inheritdoc/>
        public async Task ProcessAndAbortWorkflowsAsync()
        {
            try
            {
                await AbortWorkflowsAsync();
                await ExecuteNewWorkflowsAsync();
            }
            catch (Exception exc) when (exc is not OperationCanceledException)
            {
                logger.LogError(exc, "ProcessAndAbortWorkflowsAsync()");
                await Task.Delay(TimeSpan.FromSeconds(2), hostCancellationTokenSource.Token);
            }
        }

        /// <inheritdoc/>
        public async Task ExecuteNewWorkflowsAsync()
        {
            await foreach (var blobTrigger in storage.GetWorkflowsByStateAsync(WorkflowState.New, hostCancellationTokenSource.Token))
            {
                PostWorkflowResponse response = default;

                try
                {
                    logger.LogInformation("Processing new workflow trigger: {BlobTrigger}", blobTrigger.Uri);
                    var blobTriggerJson = await storage.DownloadBlobTextAsync(blobTrigger.ContainerName, blobTrigger.Name, hostCancellationTokenSource.Token);
                    var processedTriggerInfo = await ProcessBlobTrigger(blobTriggerJson);

                    response = await cromwellApiClient.PostWorkflowAsync(
                        processedTriggerInfo.WorkflowSource.Filename, processedTriggerInfo.WorkflowSource.Data,
                        [.. processedTriggerInfo.WorkflowInputs.Select(a => a.Filename)],
                        [.. processedTriggerInfo.WorkflowInputs.Select(a => a.Data)],
                        hostCancellationTokenSource.Token,
                        processedTriggerInfo.WorkflowOptions.Filename, processedTriggerInfo.WorkflowOptions.Data,
                        processedTriggerInfo.WorkflowDependencies.Filename, processedTriggerInfo.WorkflowDependencies.Data);

                    await SetStateToInProgressAsync(blobTrigger.ContainerName, blobTrigger.Name, response.Id.ToString());
                }
                catch (Exception e) when (e is not OperationCanceledException)
                {
                    logger.LogError(e, "Exception in ExecuteNewWorkflowsAsync for {BlobTrigger}", blobTrigger.Uri);

                    if (response is not null)
                    {
                        _ = await cromwellApiClient.PostAbortAsync(response.Id, hostCancellationTokenSource.Token);
                    }

                    await MutateStateAsync(
                        blobTrigger.ContainerName,
                        blobTrigger.Name,
                        WorkflowState.Failed,
                        wf => wf.WorkflowFailureInfo = new()
                        {
                            WorkflowFailureReason = "ErrorSubmittingWorkflowToCromwell",
                            WorkflowFailureReasonDetail = $"Error processing trigger file {blobTrigger.Name}. Error: {e.Message}"
                        });
                }
            }
        }

        internal async Task<ProcessedTriggerInfo> ProcessBlobTrigger(string blobTriggerJson)
        {
            var triggerInfo = JsonConvert.DeserializeObject<Workflow>(blobTriggerJson)
                ?? throw new ArgumentNullException(nameof(blobTriggerJson), "must have data in the Trigger File");

            var workflowInputs = new List<ProcessedWorkflowItem>();

            if (string.IsNullOrWhiteSpace(triggerInfo.WorkflowUrl))
            {
                throw new ArgumentNullException(nameof(Workflow.WorkflowUrl), "must specify a WorkflowUrl in the Trigger File");
            }

            var workflowSource = await GetBlobFileNameAndData(triggerInfo.WorkflowUrl);

            if (triggerInfo.WorkflowInputsUrl is not null)
            {
                workflowInputs.Add(await GetBlobFileNameAndData(triggerInfo.WorkflowInputsUrl));
            }

            if (triggerInfo.WorkflowInputsUrls is not null)
            {
                foreach (var workflowInputsUrl in triggerInfo.WorkflowInputsUrls)
                {
                    workflowInputs.Add(await GetBlobFileNameAndData(workflowInputsUrl));
                }
            }

            var workflowOptions = await GetBlobFileNameAndData(triggerInfo.WorkflowOptionsUrl);
            var workflowDependencies = await GetBlobFileNameAndData(triggerInfo.WorkflowDependenciesUrl);
            var workflowLabels = await GetBlobFileNameAndData(triggerInfo.WorkflowLabelsUrl);

            return new(workflowSource, workflowInputs, workflowOptions, workflowDependencies, workflowLabels);
        }

        /// <inheritdoc/>
        public async Task UpdateWorkflowStatusesAsync()
        {
            await foreach (var blobTrigger in storage.GetWorkflowsByStateAsync(WorkflowState.InProgress, hostCancellationTokenSource.Token)
                .Where(blob => DateTimeOffset.UtcNow.Subtract(blob.LastModified) > inProgressWorkflowInvisibilityPeriod))
            {
                var id = Guid.Empty;

                try
                {
                    id = ExtractWorkflowId(blobTrigger.Name, WorkflowState.InProgress);
                    var sampleName = ExtractSampleName(blobTrigger.Name, WorkflowState.InProgress);
                    var statusResponse = await cromwellApiClient.GetStatusAsync(id, hostCancellationTokenSource.Token);

                    switch (statusResponse.Status)
                    {
                        case WorkflowStatus.Aborted:
                            {
                                logger.LogInformation("Setting to failed (aborted) Id: {Workflow}", id);

                                await UploadOutputsAsync(id, sampleName);
                                _ = await UploadMetadataAsync(id, sampleName);
                                await UploadTimingAsync(id, sampleName);

                                await MutateStateAsync(
                                    blobTrigger.ContainerName,
                                    blobTrigger.Name,
                                    WorkflowState.Failed,
                                    wf => wf.WorkflowFailureInfo = new() { WorkflowFailureReason = "Aborted" });

                                break;
                            }
                        case WorkflowStatus.Failed:
                            {
                                logger.LogInformation("Setting to failed Id: {Workflow}", id);

                                await UploadOutputsAsync(id, sampleName);
                                var metadata = await UploadMetadataAsync(id, sampleName);
                                await UploadTimingAsync(id, sampleName);

                                var taskWarnings = await GetWorkflowTaskWarningsAsync(id);
                                var workflowFailureInfo = await GetWorkflowFailureInfoAsync(id, metadata);

                                await MutateStateAsync(
                                    blobTrigger.ContainerName,
                                    blobTrigger.Name,
                                    WorkflowState.Failed,
                                    wf =>
                                    {
                                        wf.TaskWarnings = taskWarnings;
                                        wf.WorkflowFailureInfo = workflowFailureInfo;
                                    });

                                break;
                            }
                        case WorkflowStatus.Succeeded:
                            {
                                await UploadOutputsAsync(id, sampleName);
                                _ = await UploadMetadataAsync(id, sampleName);
                                await UploadTimingAsync(id, sampleName);

                                var taskWarnings = await GetWorkflowTaskWarningsAsync(id);

                                await MutateStateAsync(
                                    blobTrigger.ContainerName,
                                    blobTrigger.Name,
                                    WorkflowState.Succeeded,
                                    wf => wf.TaskWarnings = taskWarnings);

                                break;
                            }
                        default:
                            break;
                    }
                }
                catch (CromwellApiException cromwellException) when (cromwellException?.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    logger.LogError(cromwellException, "Exception in UpdateWorkflowStatusesAsync for {BlobTrigger}.  Id: {Workflow}  Cromwell reported workflow as NotFound (404).  Mutating state to Failed.", blobTrigger.Uri, id);

                    await MutateStateAsync(
                        blobTrigger.ContainerName,
                        blobTrigger.Name,
                        WorkflowState.Failed,
                        wf => wf.WorkflowFailureInfo = new() { WorkflowFailureReason = "WorkflowNotFoundInCromwell" });
                }
                catch (Exception exception) when (exception is not OperationCanceledException)
                {
                    logger.LogError(exception, "Exception in UpdateWorkflowStatusesAsync for {BlobTrigger}.  Id: {Workflow}", blobTrigger.Uri, id);

                    if (RetryLimit <= workflowRetries.AddOrUpdate(id, 1, (_, retries) => retries + 1))
                    {
                        logger.LogError("Exceeded retry count  UpdateWorkflowStatusesAsync for {BlobTrigger}.  Id: {Workflow}  Cromwell reported workflow as NotFound (404).  Mutating state to Failed.", blobTrigger.Uri, id);

                        await MutateStateAsync(
                            blobTrigger.ContainerName,
                            blobTrigger.Name,
                            WorkflowState.Failed,
                            wf => wf.WorkflowFailureInfo = new() { WorkflowFailureReason = "CromwellApiExceededRetryCount" });
                    }
                    else
                    {
                        return; // Keep retryable error count
                    }
                }

                // Remove retryable error count if one exists
                if (workflowRetries.TryGetValue(id, out var retries))
                {
                    while (!workflowRetries.TryRemove(new(id, retries)))
                    {
                        if (!workflowRetries.TryGetValue(id, out retries))
                        {
                            return;
                        }
                    }
                }
            }
        }

        /// <inheritdoc/>
        public async Task AbortWorkflowsAsync()
        {
            await foreach (var blobTrigger in storage.GetWorkflowsByStateAsync(WorkflowState.Abort, hostCancellationTokenSource.Token))
            {
                var id = Guid.Empty;

                try
                {
                    id = ExtractWorkflowId(blobTrigger.Name, WorkflowState.Abort);
                    logger.LogInformation("Aborting workflow ID: {WorkFlow} Url: {BlobTrigger}", id, blobTrigger.Uri);
                    _ = await cromwellApiClient.PostAbortAsync(id, hostCancellationTokenSource.Token);

                    await MutateStateAsync(
                        blobTrigger.ContainerName,
                        blobTrigger.Name,
                        WorkflowState.Failed,
                        wf => wf.WorkflowFailureInfo = new() { WorkflowFailureReason = "AbortRequested" });
                }
                catch (Exception e) when (e is not OperationCanceledException)
                {
                    logger.LogError(e, "Exception in AbortWorkflowsAsync for {BlobTrigger}.  Id: {WorkFlow}", blobTrigger.Uri, id);

                    await MutateStateAsync(
                        blobTrigger.ContainerName,
                        blobTrigger.Name,
                        WorkflowState.Failed,
                        wf => wf.WorkflowFailureInfo = new WorkflowFailureInfo { WorkflowFailureReason = "ErrorOccuredWhileAbortingWorkflow", WorkflowFailureReasonDetail = e.Message });
                }
            }
        }

        /// <inheritdoc/>
        public async Task<ProcessedWorkflowItem> GetBlobFileNameAndData(string url)
        {
            if (string.IsNullOrWhiteSpace(url))
            {
                return new(null, null);
            }

            var blobName = GetBlobName(url);

            if (string.IsNullOrEmpty(blobName))
            {
                throw new ArgumentException($"Invalid URL: {url}");
            }

            byte[] data;

            if (GetBlockBlobStorage(url, storageAccounts) is IAzureStorage aStorage)
            {
                try
                {
                    data = await aStorage.DownloadBlockBlobAsync(url, hostCancellationTokenSource.Token);
                }
                catch (Exception e) when (e is not OperationCanceledException)
                {
                    throw new Exception($"Failed to retrieve {url} from account {aStorage.AccountName}. " +
                        $"Possible causes include improperly configured container permissions, or an incorrect file name or location." +
                        $"{e?.GetType().FullName}:{e.InnerException?.Message ?? e.Message}", e);
                }
            }
            else
            {
                try
                {
                    data = await storage.DownloadFileUsingHttpClientAsync(url, hostCancellationTokenSource.Token);
                }
                catch (Exception e) when (e is not OperationCanceledException)
                {
                    throw new Exception($"Failed to download file from Http URL {url}. {e?.GetType().FullName}:{e.InnerException?.Message ?? e.Message}", e);
                }
            }

            return new(blobName, data);

            static IAzureStorage GetBlockBlobStorage(string url, IEnumerable<IAzureStorage> storages)
            {
                // If a URL is specified, and it uses a known storage account, and it doesn't use a SAS
                // OR, if it's specified as a local path to a known storage account
                return storages.FirstOrDefault(IsBlockBlobUrl);

                bool IsBlockBlobUrl(IAzureStorage storage)
                    => (Uri.TryCreate(url, UriKind.Absolute, out var uri)
                    && uri.Authority.Equals(storage.AccountAuthority, StringComparison.OrdinalIgnoreCase)
                    && uri.ParseQueryString().Get("sig") is null)
                    || url.TrimStart('/').StartsWith(storage.AccountName + "/", StringComparison.OrdinalIgnoreCase);
            }
        }

        public async Task MutateStateAsync(
            string container,
            string blobName,
            WorkflowState newState,
            Action<Workflow> workflowContentAction = null,
            Func<string, string> workflowNameAction = null)
        {
            var oldStateText = workflowStateRegex.Match(blobName).Value;
            var newStateText = newState.ToString().ToLowerInvariant();
            var newBlobName = workflowStateRegex.Replace(blobName, newStateText);

            if (workflowNameAction is not null)
            {
                newBlobName = workflowNameAction(newBlobName);
            }

            logger.LogInformation("Mutating state from '{OldState}' to '{NewState}' for blob {AccountName}/{Container}/{BlobName}", oldStateText, newStateText, storage.AccountName, container, blobName);

            Exception error = default;
            var newBlobText = await storage.DownloadBlobTextAsync(container, blobName, hostCancellationTokenSource.Token);
            var workflow = JsonConvert.DeserializeObject<Workflow>(newBlobText, new JsonSerializerSettings()
            {
                Error = (o, a) =>
            {
                error = error switch
                {
                    AggregateException ex => new AggregateException(ex.InnerExceptions.Append(a.ErrorContext.Error)),
                    Exception ex => new AggregateException(Enumerable.Empty<Exception>().Append(ex).Append(a.ErrorContext.Error)),
                    _ => a.ErrorContext.Error ?? new InvalidOperationException("Unknown error."),
                };
                a.ErrorContext.Handled = true;
            }
            });

            if (error is not null)
            {
                newBlobText += "\nError(s): " + error switch
                {
                    AggregateException ex => string.Join(", ", Enumerable.Empty<Exception>().Append(ex).Concat(ex.InnerExceptions).Select(e => e.Message)),
                    Exception ex => ex.Message,
                    _ => "Unknown error.",
                };
            }
            else
            {
                var jsonSerializerSettings = workflow is null ? new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore } : null;
                workflow ??= new();
                workflowContentAction?.Invoke(workflow);
                newBlobText = JsonConvert.SerializeObject(workflow, Formatting.Indented, jsonSerializerSettings);
            }

            _ = await storage.UploadFileTextAsync(newBlobText, container, newBlobName, hostCancellationTokenSource.Token);
            await storage.DeleteBlobIfExistsAsync(container, blobName, hostCancellationTokenSource.Token);
        }

        public Task SetStateToInProgressAsync(string container, string blobName, string workflowId)
            => MutateStateAsync(
                container,
                blobName,
                WorkflowState.InProgress,
                workflowNameAction: name => name.Replace(".json", $".{workflowId}.json"));

        private async Task RunAsync()
        {
            logger.LogInformation("Trigger Service successfully started.");
            logger.LogInformation("Running in AzureEnvironment: {AzureCloudConfigName}", azureCloudConfig.Name);

            await Task.WhenAll(
                RunContinuouslyAsync(UpdateExistingWorkflowsAsync, nameof(UpdateExistingWorkflowsAsync)),
                RunContinuouslyAsync(ProcessAndAbortWorkflowsAsync, nameof(ProcessAndAbortWorkflowsAsync)));
        }

        private async Task RunContinuouslyAsync(Func<Task> task, string description)
        {
            while (!hostCancellationTokenSource.IsCancellationRequested)
            {
                try
                {
                    await Task.WhenAll(
                        cromwellAvailability.WaitForAsync(
                            IsCromwellAvailableAsync,
                            availabilityCheckInterval,
                            Constants.CromwellSystemName,
                            msg => logger.LogInformation("{AvailabilityMessage}", msg),
                            hostCancellationTokenSource.Token),
                        azStorageAvailability.WaitForAsync(
                            IsAzureStorageAvailableAsync,
                            availabilityCheckInterval,
                            "Azure Storage",
                            msg => logger.LogInformation("{AvailabilityMessage}", msg),
                            hostCancellationTokenSource.Token));

                    await task.Invoke();
                    await Task.Delay(mainInterval, hostCancellationTokenSource.Token);
                }
                catch (Exception exc) when (exc is not OperationCanceledException)
                {
                    logger.LogError(exc, "RunContinuously exception for {Description}", description);
                    await Task.Delay(TimeSpan.FromSeconds(1), hostCancellationTokenSource.Token);
                }
            }
        }

        private async Task UploadOutputsAsync(Guid id, string sampleName)
        {
            try
            {
                var outputsResponse = await cromwellApiClient.GetOutputsAsync(id, hostCancellationTokenSource.Token);

                _ = await storage.UploadFileTextAsync(
                    outputsResponse.Json,
                    OutputsContainerName,
                    $"{sampleName}.{id}.outputs.json",
                    hostCancellationTokenSource.Token);
            }
            catch (Exception exc) when (exc is not OperationCanceledException)
            {
                logger.LogWarning(exc, "Getting outputs threw an exception for Id: {WorkFlow}", id);
            }
        }

        private async Task<string> UploadMetadataAsync(Guid id, string sampleName)
        {
            try
            {
                var metadataResponse = await cromwellApiClient.GetMetadataAsync(id, hostCancellationTokenSource.Token);

                _ = await storage.UploadFileTextAsync(
                    metadataResponse.Json,
                    OutputsContainerName,
                    $"{sampleName}.{id}.metadata.json",
                    hostCancellationTokenSource.Token);

                return metadataResponse.Json;
            }
            catch (Exception exc) when (exc is not OperationCanceledException)
            {
                logger.LogWarning(exc, "Getting metadata threw an exception for Id: {WorkFlow}", id);
            }

            return default;
        }

        private async Task UploadTimingAsync(Guid id, string sampleName)
        {
            try
            {
                var timingResponse = await cromwellApiClient.GetTimingAsync(id, hostCancellationTokenSource.Token);

                _ = await storage.UploadFileTextAsync(
                    timingResponse.Html,
                    OutputsContainerName,
                    $"{sampleName}.{id}.timing.html",
                    hostCancellationTokenSource.Token);
            }
            catch (Exception exc) when (exc is not OperationCanceledException)
            {
                logger.LogWarning(exc, "Getting timing threw an exception for Id: {WorkFlow}", id);
            }
        }

        private async Task<WorkflowFailureInfo> GetWorkflowFailureInfoAsync(Guid workflowId, string metadata)
        {
            const int maxFailureMessageLength = 4096;

            var metadataFailures = string.IsNullOrWhiteSpace(metadata)
                ? default
                : JsonConvert.DeserializeObject<CromwellMetadata>(metadata)?.Failures;

            if (metadataFailures?.Count > 0)
            {
                // Truncate failure messages; some can be 56k+ when it's an HTML message
                for (int i = 0; i < metadataFailures.Count; i++)
                {
                    if (metadataFailures[i].Message?.Length > maxFailureMessageLength)
                    {
                        metadataFailures[i].Message = $"{metadataFailures[i].Message[..maxFailureMessageLength]} [TRUNCATED by Cromwell On Azure's Trigger Service]";
                    }
                }
            }

            var tesTasks = await tesTaskRepository.GetItemsAsync(t => t.WorkflowId == workflowId.ToString(), hostCancellationTokenSource.Token);

            // Select the last attempt of each Cromwell task, and then select only the failed ones
            // If CromwellResultCode is > 0, point to Cromwell stderr/out. Otherwise, if batch exit code > 0, point to Batch stderr/out
            var failedTesTasks = tesTasks
                .GroupBy(t => new { t.CromwellTaskInstanceName, t.CromwellShard })
                .Select(grp => grp.OrderBy(t => t.CromwellAttempt).Last())
                .Where(t => t.FailureReason is not null || t.CromwellResultCode.GetValueOrDefault() != 0)
                .Select(t =>
                {
                    var cromwellScriptFailed = t.CromwellResultCode.GetValueOrDefault() != 0;
                    var batchTaskFailed = (t.Logs?.LastOrDefault()?.Logs?.LastOrDefault()?.ExitCode).GetValueOrDefault() != 0;
                    var executor = t.Executors?.LastOrDefault();
                    var batchExecutionDirectoryPath = t.Resources?.BackendParameters?.ContainsKey(nameof(TesResources.SupportedBackendParameters.internal_path_prefix)) ?? false
                        ? $"/{t.Resources.BackendParameters[nameof(TesResources.SupportedBackendParameters.internal_path_prefix)].Trim('/')}"
                        : $"/tes-internal/{t.Id}";
                    var batchStdOut = batchExecutionDirectoryPath is not null ? $"{batchExecutionDirectoryPath}/stdout.txt" : null;
                    var batchStdErr = batchExecutionDirectoryPath is not null ? $"{batchExecutionDirectoryPath}/stderr.txt" : null;

                    return new FailedTaskInfo
                    {
                        TaskId = t.Id,
                        TaskName = t.Name,
                        FailureReason = cromwellScriptFailed ? "CromwellScriptFailed" : t.FailureReason,
                        SystemLogs = t.Logs?.LastOrDefault()?.SystemLogs?.Where(log => !log.Equals(t.FailureReason)).ToList(),
                        StdOut = cromwellScriptFailed ? executor?.Stdout : batchTaskFailed ? batchStdOut : null,
                        StdErr = cromwellScriptFailed ? executor?.Stderr : batchTaskFailed ? batchStdErr : null,
                        CromwellResultCode = t.CromwellResultCode
                    };
                })
                .ToList();

            logger.LogInformation("Adding {FailedTesTasksCount} failed task details to trigger file for workflow {Workflow}", failedTesTasks.Count, workflowId);

            return new WorkflowFailureInfo
            {
                FailedTasks = failedTesTasks,
                CromwellFailureLogs = metadataFailures,
                WorkflowFailureReason = 0 == failedTesTasks.Count ? "CromwellFailed" : "OneOrMoreTasksFailed"
            };
        }

        private async Task<List<TaskWarning>> GetWorkflowTaskWarningsAsync(Guid workflowId)
        {
            var tesTasks = await tesTaskRepository.GetItemsAsync(t => t.WorkflowId == workflowId.ToString(), hostCancellationTokenSource.Token);

            // Select the last attempt of each Cromwell task, and then select only those that have a warning
            var taskWarnings = tesTasks
                .GroupBy(t => new { t.CromwellTaskInstanceName, t.CromwellShard })
                .Select(grp => grp.OrderBy(t => t.CromwellAttempt).Last())
                .Where(t => t.Warning is not null)
                .Select(t => new TaskWarning
                {
                    TaskId = t.Id,
                    TaskName = t.Name,
                    Warning = t.Warning,
                    WarningDetails = t.Logs?.LastOrDefault()?.SystemLogs?.Where(log => !log.Equals(t.Warning)).ToList()
                })
                .ToList();

            logger.LogInformation("Adding {TaskWarningsCount} task warnings to trigger file for workflow {Workflow}", taskWarnings.Count, workflowId);

            return taskWarnings;
        }

        private static Guid ExtractWorkflowId(string blobTriggerName, WorkflowState currentState)
        {
            var blobName = blobTriggerName[(currentState.ToString().Length + 1)..];
            var withoutExtension = Path.GetFileNameWithoutExtension(blobName);
            var textId = withoutExtension[(withoutExtension.LastIndexOf('.') + 1)..];
            return Guid.Parse(textId);
        }

        private static string ExtractSampleName(string blobTriggerName, WorkflowState currentState)
        {
            var blobName = blobTriggerName[(currentState.ToString().Length + 1)..];
            var withoutExtension = Path.GetFileNameWithoutExtension(blobName);
            return withoutExtension[..withoutExtension.LastIndexOf('.')];
        }

        private static string GetBlobName(string url)
            => blobNameRegex.Match(url)?.Groups[1].Value.Replace("/", "_");

        private static string GetParentPath(string path)
        {
            if (string.IsNullOrEmpty(path))
            {
                return null;
            }

            var pathComponents = path.TrimEnd('/').Split('/');

            return string.Join('/', pathComponents.Take(pathComponents.Length - 1));
        }
    }
}
