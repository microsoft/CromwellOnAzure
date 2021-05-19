// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
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
    public class CromwellOnAzureEnvironment
    {
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
            var blobTriggers = await storage.GetBlobsByStateAsync(AzureStorage.WorkflowState.New);

            foreach (var blobTrigger in blobTriggers)
            {
                try
                {
                    logger.LogInformation($"Processing new workflow trigger: {blobTrigger.Uri.AbsoluteUri}");
                    var blobTriggerJson = await storage.DownloadBlobTextAsync(blobTrigger.Container.Name, blobTrigger.Name);
                    var processedTriggerInfo = await ProcessBlobTrigger(blobTriggerJson);

                    var response = await cromwellApiClient.PostWorkflowAsync(
                                        processedTriggerInfo.WorkflowSource.Filename, processedTriggerInfo.WorkflowSource.Data,
                                        processedTriggerInfo.WorkflowInputs.Select(a => a.Filename).ToList(),
                                        processedTriggerInfo.WorkflowInputs.Select(a => a.Data).ToList(),
                                        processedTriggerInfo.WorkflowOptions.Filename, processedTriggerInfo.WorkflowOptions.Data,
                                        processedTriggerInfo.WorkflowDependencies.Filename, processedTriggerInfo.WorkflowDependencies.Data);

                    await SetStateToInProgressAsync(blobTrigger.Container.Name, blobTrigger.Name, response.Id.ToString());
                }
                catch (Exception e)
                {
                    logger.LogError(e, $"Exception in ExecuteNewWorkflowsAsync for {blobTrigger.Uri.AbsoluteUri}");
                    
                    await MutateStateAsync(
                        blobTrigger.Container.Name,
                        blobTrigger.Name,
                        AzureStorage.WorkflowState.Failed,
                        (w) => {
                            w.WorkflowFailureDetails = new WorkflowFailureInfo
                            {
                                WorkflowFailureReason = "ErrorSubmittingWorkflowToCromwell",
                                WorkflowFailureReasonDetail = e.Message
                            };});

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

        public async Task<ConcurrentBag<Workflow>> UpdateWorkflowStatusesAsync()
        {
            var wfWithUpdatedStatuses = new ConcurrentBag<Workflow>();
            var blobTriggers = await storage.GetRecentlyUpdatedInProgressWorkflowBlobsAsync();
            
            foreach (var blobTrigger in blobTriggers)
            {
                var id = Guid.Empty;
                try
                {
                    id = ExtractWorkflowId(blobTrigger.Name, AzureStorage.WorkflowState.InProgress);
                    var sampleName = ExtractSampleName(blobTrigger.Name, AzureStorage.WorkflowState.InProgress);
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
                                logger.LogInformation($"Setting to failed Id: {id}");

                                await UploadOutputsAsync(id, sampleName);
                                await UploadTimingAsync(id, sampleName);

                                var mutatedBlobName = await MutateStateAsync(
                                    blobTrigger.Container.Name,
                                    blobTrigger.Name,
                                    AzureStorage.WorkflowState.Failed,
                                    wf => wf.WorkflowFailureDetails = new WorkflowFailureInfo {
                                        WorkflowFailureReason = "Aborted"});

                                wfWithUpdatedStatuses.Add(JsonConvert
                                    .DeserializeObject<Workflow>(await storage
                                    .DownloadBlobTextAsync(blobTrigger.Container.Name,
                                    mutatedBlobName)));

                                break;
                            }
                        case WorkflowStatus.Failed:
                            {                                
                                logger.LogInformation($"Setting to failed Id: {id}");                                
                                
                                await UploadOutputsAsync(id, sampleName);
                                await UploadTimingAsync(id, sampleName);
                                
                                var workflowFailureInfo = await GetWorkflowFailureInfoAsync(id);

                                var mutatedBlobName = await MutateStateAsync(
                                blobTrigger.Container.Name, 
                                blobTrigger.Name,
                                AzureStorage.WorkflowState.Failed,
                                wf => wf.WorkflowFailureDetails = workflowFailureInfo);

                                logger.LogInformation($"Mutated Workflow to : {mutatedBlobName}");

                                wfWithUpdatedStatuses.Add(JsonConvert
                                    .DeserializeObject<Workflow>(await storage
                                    .DownloadBlobTextAsync(blobTrigger.Container.Name, 
                                    mutatedBlobName)));

                                break;
                            }
                        case WorkflowStatus.Succeeded:
                            {                                
                                await UploadOutputsAsync(id, sampleName);
                                await UploadTimingAsync(id, sampleName);
                                
                                await MutateStateAsync(
                                blobTrigger.Container.Name, 
                                blobTrigger.Name, 
                                AzureStorage.WorkflowState.Succeeded);
                                
                                break;
                            }
                    }
                }
                catch (CromwellApiException cromwellException) when (cromwellException?.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    logger.LogError(cromwellException, $"Exception in UpdateWorkflowStatusesAsync for {blobTrigger.StorageUri.PrimaryUri.AbsoluteUri}.  Id: {id}  Cromwell reported workflow as NotFound (404).  Mutating state to Failed.");

                    var mutatedBlobName = await MutateStateAsync(
                        blobTrigger.Container.Name, 
                        blobTrigger.Name, 
                        AzureStorage.WorkflowState.Failed, 
                        (w) => {
                            w.WorkflowFailureDetails = new WorkflowFailureInfo {
                                WorkflowFailureReason = "CromwellApiException",
                                WorkflowFailureReasonDetail = "Cromwell reported workflow as NotFound (404)"};});                    

                    wfWithUpdatedStatuses.Add(JsonConvert
                        .DeserializeObject<Workflow>(await storage
                        .DownloadBlobTextAsync(blobTrigger.Container.Name, 
                        mutatedBlobName)));

                }
                catch (Exception exception)
                {
                    logger.LogError(exception, $"Exception in UpdateWorkflowStatusesAsync for {blobTrigger.StorageUri.PrimaryUri.AbsoluteUri}.  Id: {id}");
                }

            }
            return wfWithUpdatedStatuses;
        }

        public async Task<ConcurrentBag<Workflow>> AbortWorkflowsAsync()
        {
            var wfWithUpdatedStatuses = new ConcurrentBag<Workflow>();
            var blobTriggers = await storage.GetWorkflowBlobsToAbortAsync();
            
            foreach (var blobTrigger in blobTriggers)
            {
                var id = Guid.Empty;

                try
                {
                    id = ExtractWorkflowId(blobTrigger.Name, AzureStorage.WorkflowState.Abort);
                    logger.LogInformation($"Aborting workflow ID: {id} Url: {blobTrigger.Uri.AbsoluteUri}");
                    await cromwellApiClient.PostAbortAsync(id);

                    var mutatedBlobName = await MutateStateAsync(
                        blobTrigger.Container.Name, 
                        blobTrigger.Name, 
                        AzureStorage.WorkflowState.Failed,
                        (w) => {
                            w.WorkflowFailureDetails = new WorkflowFailureInfo{
                                WorkflowFailureReason = "Aborted"};});

                    wfWithUpdatedStatuses.Add(JsonConvert
                                    .DeserializeObject<Workflow>(await storage
                                    .DownloadBlobTextAsync(blobTrigger.Container.Name,
                                    mutatedBlobName)));

                }
                catch (Exception e)
                {
                    logger.LogError(e, $"Exception in AbortWorkflowsAsync for {blobTrigger}.  Id: {id}");

                    var mutatedBlobName = await MutateStateAsync(
                        blobTrigger.Container.Name, 
                        blobTrigger.Name, 
                        AzureStorage.WorkflowState.Failed,
                        (w) => {
                            w.WorkflowFailureDetails = new WorkflowFailureInfo{
                                WorkflowFailureReason = "Aborted"};});

                    wfWithUpdatedStatuses.Add(JsonConvert
                                    .DeserializeObject<Workflow>(await storage
                                    .DownloadBlobTextAsync(blobTrigger.Container.Name,
                                    mutatedBlobName)));
                }
            }
            return wfWithUpdatedStatuses;
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
                throw new ArgumentException(@"url object submitted ({url}) is not valid URL");
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

        private static string GetWorkflowOldStateText(string blobNameOrAbsolutePath) {
            var blobNameDetailsArray = blobNameOrAbsolutePath.Split('/');
            return blobNameDetailsArray[blobNameDetailsArray.Length - 2].ToLowerInvariant();
        }

        private static string DeriveWorkflowBlobNameAfterMutation(string blobNameOrAbsolutePath, AzureStorage.WorkflowState newState)
        {
            var blobNameDetailsArray = blobNameOrAbsolutePath.Split('/');
            blobNameDetailsArray[(blobNameDetailsArray.Length - 2)] = newState.ToString().ToLowerInvariant();
            return string.Join('/', blobNameDetailsArray);
        }

        private static string DeriveWorkflowBlobName(string blobNameOrAbsolutePath)
        {
            var blobNameDetailsArray = blobNameOrAbsolutePath.Split('/');
            var detailsArrayLength = blobNameDetailsArray.Length;
            return $"{blobNameDetailsArray[detailsArrayLength - 2]}/{blobNameDetailsArray[detailsArrayLength - 1]}";
        }

        public async Task<string> MutateStateAsync(string container, string blobAbsolutePath, AzureStorage.WorkflowState newState, Action<Workflow> action = null)
        {
            var blobName = DeriveWorkflowBlobName(blobAbsolutePath);
            var newStateText = $"{newState.ToString().ToLowerInvariant()}";
            var oldStateText = GetWorkflowOldStateText(blobAbsolutePath);            
            var newBlobName = DeriveWorkflowBlobNameAfterMutation(blobAbsolutePath, newState);

            logger.LogInformation($"Mutating state from '{oldStateText}' to '{newStateText}' for {blobName}");

            var workflow = JsonConvert.DeserializeObject<Workflow>(await storage.DownloadBlobTextAsync(container, blobName));

            action?.Invoke(workflow);

            await storage.UploadFileTextAsync(
                JsonConvert.SerializeObject(workflow, Formatting.Indented),
                container,
                newBlobName);

            await storage.DeleteBlobIfExistsAsync(container, blobName);

            return newBlobName;
        }

        public async Task SetStateToInProgressAsync(string container, string blobNameOrAbsolutePath, string id)
        {
            //Test cases pass AbsolutePath where as the application passes blobName
            var blobName = DeriveWorkflowBlobName(blobNameOrAbsolutePath);
            var data = await storage.DownloadBlobTextAsync(container, blobName);
            var newBlobName = blobName.Replace("new/", $"{AzureStorage.WorkflowState.InProgress.ToString().ToLowerInvariant()}/");
            newBlobName = newBlobName.Replace(".json", $".{id}.json");
            await storage.UploadFileTextAsync(data, container, newBlobName);
            await storage.DeleteBlobIfExistsAsync(container, blobName);
        }

        private static Guid ExtractWorkflowId(string blobTriggerName, AzureStorage.WorkflowState currentState)
        {
            var blobName = blobTriggerName.Substring(currentState.ToString().Length + 1);
            var withoutExtension = Path.GetFileNameWithoutExtension(blobName);
            var textId = withoutExtension.Substring(withoutExtension.LastIndexOf('.') + 1);
            return Guid.Parse(textId);
        }

        private static string ExtractSampleName(string blobTriggerName, AzureStorage.WorkflowState currentState)
        {
            var blobName = blobTriggerName.Substring(currentState.ToString().Length + 1);
            var withoutExtension = Path.GetFileNameWithoutExtension(blobName);
            return withoutExtension.Substring(0, withoutExtension.LastIndexOf('.'));
        }

        private static string GetBlobName(string url)
        {
            return blobNameRegex.Match(url)?.Groups[1].Value.Replace("/", "_");
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

        private async Task<WorkflowFailureInfo> GetWorkflowFailureInfoAsync(Guid workflowid)
        {
            var tesTasks = await tesTaskRepository.GetItemsAsync(
                        predicate: t => t.WorkflowId == workflowid.ToString());

            var failedTesTasks = tesTasks
                .GroupBy(t => new { t.CromwellTaskInstanceName, t.CromwellShard })
                .Select(grp => grp.OrderBy(t => t.CromwellAttempt).Last())
                .Where(t => t.FailureReason != null);            

            logger.LogInformation($"Surfacing {failedTesTasks.Count()} failed task details to trigger file");

            return new WorkflowFailureInfo {
                FailedTaskDetails = failedTesTasks.Select(t => {

                    const string BatchExecutionDirectoryName = "__batch";

                    var cromwellFailed = t.CromwellResultCode.GetValueOrDefault() != 0;
                    var batchFailed = (t.Logs?.LastOrDefault()?.Logs?.LastOrDefault()?.ExitCode).GetValueOrDefault() != 0;

                    var executor = batchFailed ? t.Executors?.LastOrDefault() : t.Executors?.FirstOrDefault();
                    var executionDirectoryPath = batchFailed ? GetParentPath(executor?.Stdout) : null;
                    var batchExecutionDirectoryPath = batchFailed ? $"{executionDirectoryPath}/{BatchExecutionDirectoryName}" : null;

                    var batchStdout = batchExecutionDirectoryPath != null ? $"{batchExecutionDirectoryPath}/stdout.txt" : null;
                    var batchStderr = batchExecutionDirectoryPath != null ? $"{batchExecutionDirectoryPath}/stderr.txt" : null;
                    
                    return new FailedTaskInfo {
                        CromwellResultCode = t.CromwellResultCode,
                        FailureReason = t.FailureReason,
                        TaskId = t.Id,
                        StdErr = cromwellFailed ? executor?.Stderr : batchFailed ? batchStderr : null,
                        StdOut = cromwellFailed ? executor?.Stdout : batchFailed ? batchStdout : null,
                        SystemLogs = t.Logs?.LastOrDefault()?.SystemLogs?.Where(log => 
                        !log.Equals(t.FailureReason)).ToList() };}).ToList(),
                
                WorkflowFailureReason = failedTesTasks.Any(t =>
                    (t.Logs?.LastOrDefault()?.Logs?.LastOrDefault()?.ExitCode).GetValueOrDefault() != 0) ?
                    "BatchFailed": failedTesTasks.Any()? "OneOrMoreTasksFailed": "CromwellFailed"
            };

        }
    }
}
