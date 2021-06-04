﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Common;
using CromwellApiClient;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Tes.Models;
using Tes.Repository;

[assembly: InternalsVisibleTo("TriggerService.Tests")]
namespace TriggerService
{
    public class CromwellOnAzureEnvironment : ICromwellOnAzureEnvironment
    {
        private static readonly TimeSpan inProgressWorkflowInvisibilityPeriod = TimeSpan.FromMinutes(1);
        private static readonly Regex workflowStateRegex = new Regex("^[^/]*");
        private static readonly Regex blobNameRegex = new Regex("^(?:https?://|/)?[^/]+/[^/]+/([^?.]+)");  // Supporting "http://account.blob.core.windows.net/container/blob", "/account/container/blob" and "account/container/blob" URLs in the trigger file.
        private IAzureStorage storage { get; set; }
        internal ICromwellApiClient cromwellApiClient { get; set; }
        private readonly ILogger<AzureStorage> logger;
        private readonly IRepository<TesTask> tesTaskRepository;

        public CromwellOnAzureEnvironment(ILoggerFactory loggerFactory, IAzureStorage storage, ICromwellApiClient cromwellApiClient, IRepository<TesTask> tesTaskRepository)
        {
            this.storage = storage;
            this.cromwellApiClient = cromwellApiClient;
            this.logger = loggerFactory.CreateLogger<AzureStorage>();
            this.tesTaskRepository = tesTaskRepository;
            logger.LogInformation($"Cromwell URL: {cromwellApiClient.GetUrl()}");
        }

        public async Task<bool> IsCromwellAvailableAsync()
        {
            try
            {
                await cromwellApiClient.PostQueryAsync("[{\"pageSize\": \"1\"}]");
                return true;
            }
            catch (CromwellApiException exc)
            {
                logger.LogWarning($"Cromwell is unavailable.  Reason: {exc.Message}");
                return false;
            }
        }

        public async Task<bool> IsAzureStorageAvailableAsync()
        {
            return await storage.IsAvailableAsync();
        }

        public async Task UpdateExistingWorkflowsAsync()
        {
            try
            {
                await UpdateWorkflowStatusesAsync();
            }
            catch (Exception exc)
            {
                logger.LogError(exc, $"UpdateExistingWorkflowsAsync()");
                await Task.Delay(TimeSpan.FromSeconds(2));
            }
        }

        public async Task ProcessAndAbortWorkflowsAsync()
        {
            try
            {
                await AbortWorkflowsAsync();
                await ExecuteNewWorkflowsAsync();
            }
            catch (Exception exc)
            {
                logger.LogError(exc, $"ProcessAndAbortWorkflowsAsync()");
                await Task.Delay(TimeSpan.FromSeconds(2));
            }
        }

        public async Task ExecuteNewWorkflowsAsync()
        {
            var blobTriggers = await storage.GetWorkflowsByStateAsync(WorkflowState.New);

            foreach (var blobTrigger in blobTriggers)
            {
                try
                {
                    logger.LogInformation($"Processing new workflow trigger: {blobTrigger.Uri}");
                    var blobTriggerJson = await storage.DownloadBlobTextAsync(blobTrigger.ContainerName, blobTrigger.Name);
                    var processedTriggerInfo = await ProcessBlobTrigger(blobTriggerJson);

                    var response = await cromwellApiClient.PostWorkflowAsync(
                        processedTriggerInfo.WorkflowSource.Filename, processedTriggerInfo.WorkflowSource.Data,
                        processedTriggerInfo.WorkflowInputs.Select(a => a.Filename).ToList(),
                        processedTriggerInfo.WorkflowInputs.Select(a => a.Data).ToList(),
                        processedTriggerInfo.WorkflowOptions.Filename, processedTriggerInfo.WorkflowOptions.Data,
                        processedTriggerInfo.WorkflowDependencies.Filename, processedTriggerInfo.WorkflowDependencies.Data);

                    await SetStateToInProgressAsync(blobTrigger.ContainerName, blobTrigger.Name, response.Id.ToString());
                }
                catch (Exception e)
                {
                    logger.LogError(e, $"Exception in ExecuteNewWorkflowsAsync for {blobTrigger.Uri}");
                    
                    await MutateStateAsync(
                        blobTrigger.ContainerName,
                        blobTrigger.Name,
                        WorkflowState.Failed,
                        wf => wf.WorkflowFailureInfo = new WorkflowFailureInfo { WorkflowFailureReason = "ErrorSubmittingWorkflowToCromwell", WorkflowFailureReasonDetail = e.Message });
                }
            }
        }

        internal async Task<ProcessedTriggerInfo> ProcessBlobTrigger(string blobTriggerJson)
        {
            var triggerInfo = JsonConvert.DeserializeObject<Workflow>(blobTriggerJson);
            if (triggerInfo == null)
            {
                throw new ArgumentNullException("must have data in the Trigger File");
            }

            var workflowInputs = new List<ProcessedWorkflowItem>();

            if (string.IsNullOrWhiteSpace(triggerInfo.WorkflowUrl))
            {
                throw new ArgumentNullException("must specify a WorkflowUrl in the Trigger File");
            }

            var workflowSource = await GetBlobFileNameAndData(triggerInfo.WorkflowUrl);

            if (triggerInfo.WorkflowInputsUrl != null)
            {
                workflowInputs.Add(await GetBlobFileNameAndData(triggerInfo.WorkflowInputsUrl));
            }

            if (triggerInfo.WorkflowInputsUrls != null)
            {
                foreach (var workflowInputsUrl in triggerInfo.WorkflowInputsUrls)
                {
                    workflowInputs.Add(await GetBlobFileNameAndData(workflowInputsUrl));
                }
            }

            var workflowOptions = await GetBlobFileNameAndData(triggerInfo.WorkflowOptionsUrl);
            var workflowDependencies = await GetBlobFileNameAndData(triggerInfo.WorkflowDependenciesUrl);

            return new ProcessedTriggerInfo(workflowSource, workflowInputs, workflowOptions, workflowDependencies);
        }

        public async Task UpdateWorkflowStatusesAsync()
        {
            var blobTriggers = (await storage.GetWorkflowsByStateAsync(WorkflowState.InProgress))
                .Where(blob => DateTimeOffset.UtcNow.Subtract(blob.LastModified) > inProgressWorkflowInvisibilityPeriod);
            
            foreach (var blobTrigger in blobTriggers)
            {
                var id = Guid.Empty;

                try
                {
                    id = ExtractWorkflowId(blobTrigger.Name, WorkflowState.InProgress);
                    var sampleName = ExtractSampleName(blobTrigger.Name, WorkflowState.InProgress);
                    var statusResponse = await cromwellApiClient.GetStatusAsync(id);                  
                    
                    switch (statusResponse.Status)
                    {
                        case WorkflowStatus.Running:
                            {
                                await UploadTimingAsync(id, sampleName);
                                break;
                            }
                        case WorkflowStatus.Aborted:
                            {
                                logger.LogInformation($"Setting to failed (aborted) Id: {id}");

                                await UploadOutputsAsync(id, sampleName);
                                await UploadTimingAsync(id, sampleName);

                                await MutateStateAsync(
                                    blobTrigger.ContainerName,
                                    blobTrigger.Name,
                                    WorkflowState.Failed,
                                    wf => wf.WorkflowFailureInfo = new WorkflowFailureInfo { WorkflowFailureReason = "Aborted" });

                                break;
                            }
                        case WorkflowStatus.Failed:
                            {                                
                                logger.LogInformation($"Setting to failed Id: {id}");                                
                                
                                await UploadOutputsAsync(id, sampleName);
                                await UploadTimingAsync(id, sampleName);
                                
                                var workflowFailureInfo = await GetWorkflowFailureInfoAsync(id);

                                await MutateStateAsync(
                                    blobTrigger.ContainerName, 
                                    blobTrigger.Name,
                                    WorkflowState.Failed,
                                    wf => wf.WorkflowFailureInfo = workflowFailureInfo);

                                break;
                            }
                        case WorkflowStatus.Succeeded:
                            {                                
                                await UploadOutputsAsync(id, sampleName);
                                await UploadTimingAsync(id, sampleName);
                                
                                await MutateStateAsync(
                                    blobTrigger.ContainerName, 
                                    blobTrigger.Name, 
                                    WorkflowState.Succeeded);
                                
                                break;
                            }
                    }
                }
                catch (CromwellApiException cromwellException) when (cromwellException?.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    logger.LogError(cromwellException, $"Exception in UpdateWorkflowStatusesAsync for {blobTrigger.Uri}.  Id: {id}  Cromwell reported workflow as NotFound (404).  Mutating state to Failed.");

                    await MutateStateAsync(
                        blobTrigger.ContainerName, 
                        blobTrigger.Name, 
                        WorkflowState.Failed, 
                        wf => wf.WorkflowFailureInfo = new WorkflowFailureInfo { WorkflowFailureReason = "WorkflowNotFoundInCromwell" });

                }
                catch (Exception exception)
                {
                    logger.LogError(exception, $"Exception in UpdateWorkflowStatusesAsync for {blobTrigger.Uri}.  Id: {id}");
                }
            }
        }

        public async Task AbortWorkflowsAsync()
        {
            var blobTriggers = await storage.GetWorkflowsByStateAsync(WorkflowState.Abort);
            
            foreach (var blobTrigger in blobTriggers)
            {
                var id = Guid.Empty;

                try
                {
                    id = ExtractWorkflowId(blobTrigger.Name, WorkflowState.Abort);
                    logger.LogInformation($"Aborting workflow ID: {id} Url: {blobTrigger.Uri}");
                    await cromwellApiClient.PostAbortAsync(id);

                    await MutateStateAsync(
                        blobTrigger.ContainerName, 
                        blobTrigger.Name, 
                        WorkflowState.Failed,
                        wf => wf.WorkflowFailureInfo = new WorkflowFailureInfo{ WorkflowFailureReason = "AbortRequested" });
                }
                catch (Exception e)
                {
                    logger.LogError(e, $"Exception in AbortWorkflowsAsync for {blobTrigger}.  Id: {id}");

                    await MutateStateAsync(
                        blobTrigger.ContainerName, 
                        blobTrigger.Name, 
                        WorkflowState.Failed,
                        wf => wf.WorkflowFailureInfo = new WorkflowFailureInfo { WorkflowFailureReason = "ErrorOccuredWhileAbortingWorkflow", WorkflowFailureReasonDetail = e.Message });
                }
            }
        }

        public async Task<ProcessedWorkflowItem> GetBlobFileNameAndData(string url)
        {
            if (string.IsNullOrWhiteSpace(url))
            {
                return new ProcessedWorkflowItem(null, null);
            }

            var blobName = GetBlobName(url);

            if (string.IsNullOrEmpty(blobName))
            {
                throw new ArgumentException($"url object submitted ({url}) is not valid URL");
            }

            byte[] data;

            if ((Uri.TryCreate(url, UriKind.Absolute, out var uri)
                && uri.Authority.Equals(storage.AccountAuthority, StringComparison.OrdinalIgnoreCase)
                && uri.ParseQueryString().Get("sig") == null)
                || url.TrimStart('/').StartsWith(storage.AccountName + "/", StringComparison.OrdinalIgnoreCase))
            {
                // If a URL is specified, and it uses the default storage account, and it doesn't use a SAS
                // OR if it's specified as a local path to the default storage account
                data = await storage.DownloadBlockBlobAsync(url);
            }
            else
            {
                data = await storage.DownloadFileUsingHttpClientAsync(url);
            }

            return new ProcessedWorkflowItem(blobName, data);
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

            if (workflowNameAction != null)
            {
                newBlobName = workflowNameAction(newBlobName);
            }

            logger.LogInformation($"Mutating state from '{oldStateText}' to '{newStateText}' for blob {storage.AccountName}/{container}/{blobName}");

            var workflow = JsonConvert.DeserializeObject<Workflow>(await storage.DownloadBlobTextAsync(container, blobName));
            var jsonSerializerSettings = workflow == null ? new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore } : null;
            workflow = workflow ?? new Workflow();
            workflowContentAction?.Invoke(workflow);
            await storage.UploadFileTextAsync(JsonConvert.SerializeObject(workflow, Formatting.Indented, jsonSerializerSettings), container, newBlobName);
            await storage.DeleteBlobIfExistsAsync(container, blobName);
        }

        public Task SetStateToInProgressAsync(string container, string blobName, string workflowId)
        {
            return MutateStateAsync(
                container, 
                blobName, 
                WorkflowState.InProgress, 
                workflowNameAction: name => name.Replace(".json", $".{workflowId}.json"));
        }

        private static string GetParentPath(string path)
        {
            if (string.IsNullOrEmpty(path))
            {
                return null;
            }

            var pathComponents = path.TrimEnd('/').Split('/');

            return string.Join('/', pathComponents.Take(pathComponents.Length - 1));
        }



        private async Task<WorkflowFailureInfo> GetWorkflowFailureInfoAsync(Guid workflowId)
        {
		    const string BatchExecutionDirectoryName = "__batch";
			
            var tesTasks = await tesTaskRepository.GetItemsAsync(t => t.WorkflowId == workflowId.ToString());

            // Select the last attempt of each Cromwell task, and then select only the failed ones
            // If CromwellResultCode is > 0, point to Cromwell stderr/out. Otherwise, if batch exit code > 0, point to Batch stderr/out
            var failedTesTasks = tesTasks
                .GroupBy(t => new { t.CromwellTaskInstanceName, t.CromwellShard })
                .Select(grp => grp.OrderBy(t => t.CromwellAttempt).Last())
                .Where(t => t.FailureReason != null || t.CromwellResultCode.GetValueOrDefault() != 0)
                .Select(t => {
                    var cromwellScriptFailed = t.CromwellResultCode.GetValueOrDefault() != 0;
                    var batchTaskFailed = (t.Logs?.LastOrDefault()?.Logs?.LastOrDefault()?.ExitCode).GetValueOrDefault() != 0;
                    var executor = t.Executors?.LastOrDefault();
                    var executionDirectoryPath = GetParentPath(executor?.Stdout);
                    var batchExecutionDirectoryPath = executionDirectoryPath != null ? $"{executionDirectoryPath}/{BatchExecutionDirectoryName}" : null;
                    var batchStdOut = batchExecutionDirectoryPath != null ? $"{batchExecutionDirectoryPath}/stdout.txt" : null;
                    var batchStdErr = batchExecutionDirectoryPath != null ? $"{batchExecutionDirectoryPath}/stderr.txt" : null;

                    return new FailedTaskInfo {
                        TaskId = t.Id,
                        FailureReason = cromwellScriptFailed ? "CromwellScriptFailed" : t.FailureReason,
                        SystemLogs = t.Logs?.LastOrDefault()?.SystemLogs?.Where(log => !log.Equals(t.FailureReason)).ToList(),
                        StdOut = cromwellScriptFailed ? executor?.Stdout : batchTaskFailed ? batchStdOut : null,
                        StdErr = cromwellScriptFailed ? executor?.Stderr : batchTaskFailed ? batchStdErr : null,
                        CromwellResultCode = t.CromwellResultCode }; })
                .ToList();

            logger.LogInformation($"Surfacing {failedTesTasks.Count} failed task details to trigger file for workflow {workflowId}");

            return new WorkflowFailureInfo {
                FailedTasks = failedTesTasks,
                WorkflowFailureReason = failedTesTasks.Any() ? "OneOrMoreTasksFailed": "CromwellFailed"
            };

        }
        private static Guid ExtractWorkflowId(string blobTriggerName, WorkflowState currentState)
        {
            var blobName = blobTriggerName.Substring(currentState.ToString().Length + 1);
            var withoutExtension = Path.GetFileNameWithoutExtension(blobName);
            var textId = withoutExtension.Substring(withoutExtension.LastIndexOf('.') + 1);
            return Guid.Parse(textId);
        }

        private static string ExtractSampleName(string blobTriggerName, WorkflowState currentState)
        {
            var blobName = blobTriggerName.Substring(currentState.ToString().Length + 1);
            var withoutExtension = Path.GetFileNameWithoutExtension(blobName);
            return withoutExtension.Substring(0, withoutExtension.LastIndexOf('.'));
        }

        private static string GetBlobName(string url)
        {
            return blobNameRegex.Match(url)?.Groups[1].Value.Replace("/", "_");
        }

        private async Task UploadOutputsAsync(Guid id, string sampleName)
        {
            const string outputsContainer = "outputs";

            try
            {
                var outputsResponse = await cromwellApiClient.GetOutputsAsync(id);

                await storage.UploadFileTextAsync(
                    outputsResponse.Json,
                    outputsContainer,
                    $"{sampleName}.{id}.outputs.json");

                var metadataResponse = await cromwellApiClient.GetMetadataAsync(id);

                await storage.UploadFileTextAsync(
                    metadataResponse.Json,
                    outputsContainer,
                    $"{sampleName}.{id}.metadata.json");
            }
            catch (Exception exc)
            {
                logger.LogWarning(exc, $"Getting outputs and/or timing threw an exception for Id: {id}");
            }
        }

        private async Task UploadTimingAsync(Guid id, string sampleName)
        {
            const string outputsContainer = "outputs";

            try
            {
                var timingResponse = await cromwellApiClient.GetTimingAsync(id);

                await storage.UploadFileTextAsync(
                    timingResponse.Html,
                    outputsContainer,
                    $"{sampleName}.{id}.timing.html");
            }
            catch (Exception exc)
            {
                logger.LogWarning(exc, $"Getting timing threw an exception for Id: {id}");
            }
        }
    }
}
