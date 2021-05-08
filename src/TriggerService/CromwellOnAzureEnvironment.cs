// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Dynamic;
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
        private IRepository<TesTask> tesTaskRepository;
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
            var blobTriggers = await storage.GetWorkflowsByStateAsync(AzureStorage.WorkflowState.New);

            foreach (var blobTrigger in blobTriggers)
            {
                try
                {
                    logger.LogInformation($"Processing new workflow trigger: {blobTrigger.Uri.AbsoluteUri}");
                    var blobTriggerJson = await blobTrigger.DownloadTextAsync();
                    var processedTriggerInfo = await ProcessBlobTrigger(blobTriggerJson);

                    var response = await cromwellApiClient.PostWorkflowAsync(
                                        processedTriggerInfo.WorkflowSource.Filename, processedTriggerInfo.WorkflowSource.Data,
                                        processedTriggerInfo.WorkflowInputs.Select(a => a.Filename).ToList(),
                                        processedTriggerInfo.WorkflowInputs.Select(a => a.Data).ToList(),
                                        processedTriggerInfo.WorkflowOptions.Filename, processedTriggerInfo.WorkflowOptions.Data,
                                        processedTriggerInfo.WorkflowDependencies.Filename, processedTriggerInfo.WorkflowDependencies.Data);

                    await storage.SetStateToInProgressAsync(blobTrigger.Container.Name, blobTrigger.Name, response.Id.ToString());
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, $"Exception in ExecuteNewWorkflowsAsync for {blobTrigger.Uri.AbsoluteUri}");
                    await storage.MutateStateAsync(blobTrigger.Container.Name, blobTrigger.Name, AzureStorage.WorkflowState.Failed, exc.ToString());
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
            var blobTriggers = await storage.GetWorkflowsByStateAsync(AzureStorage.WorkflowState.InProgress);

            foreach (var blobTrigger in blobTriggers)
            {
                if ((DateTime.UtcNow - blobTrigger.Properties.LastModified.Value).TotalMinutes < 1)
                {
                    // Cromwell REST API is not transactional.  PostWorkflow then GetStatus immediately results in 404.
                    continue;
                }

                var id = Guid.Empty;
                dynamic wfInstance= JsonConvert.DeserializeObject("{}");
                try
                {
                    id = ExtractWorkflowId(blobTrigger, AzureStorage.WorkflowState.InProgress);
                    var sampleName = ExtractSampleName(blobTrigger);
                    var statusResponse = await cromwellApiClient.GetStatusAsync(id);
                    
                    wfInstance = JsonConvert.DeserializeObject(await storage.GetSerializedWorkflowTrigger(blobTrigger.Container.Name, blobTrigger.Name));
                    

                    switch (statusResponse.Status)
                    {
                        case WorkflowStatus.Running:
                            {
                                await UploadTimingAsync(blobTrigger, id, sampleName);
                                break;
                            }
                        case WorkflowStatus.Aborted:
                        case WorkflowStatus.Failed:
                            {
                                wfInstance.WorkflowFailureDetails = await GetWorkflowFailureReason(id, statusResponse.Status);                                
                                logger.LogInformation($"Setting to failed Id: {id}");
                                await storage.MutateStateAsync(blobTrigger.Container.Name, blobTrigger.Name, AzureStorage.WorkflowState.Failed, JsonConvert.SerializeObject(wfInstance));
                                await UploadOutputsAsync(blobTrigger, id, sampleName);
                                await UploadTimingAsync(blobTrigger, id, sampleName);
                                break;
                            }
                        case WorkflowStatus.Succeeded:
                            {                                
                                await UploadOutputsAsync(blobTrigger, id, sampleName);
                                await UploadTimingAsync(blobTrigger, id, sampleName);
                                await storage.MutateStateAsync(blobTrigger.Container.Name, blobTrigger.Name, AzureStorage.WorkflowState.Succeeded, JsonConvert.SerializeObject(wfInstance));
                                break;
                            }
                    }
                }
                catch (CromwellApiException cromwellException) when (cromwellException?.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    logger.LogError(cromwellException, $"Exception in UpdateWorkflowStatusesAsync for {blobTrigger.StorageUri.PrimaryUri.AbsoluteUri}.  Id: {id}  Cromwell reported workflow as NotFound (404).  Mutating state to Failed.");
                    wfInstance.WorkflowFailureDetails = cromwellException.ToString();
                    await storage.MutateStateAsync(blobTrigger.Container.Name, blobTrigger.Name, AzureStorage.WorkflowState.Failed, JsonConvert.SerializeObject(wfInstance));
                }
                catch (Exception exception)
                {
                    logger.LogError(exception, $"Exception in UpdateWorkflowStatusesAsync for {blobTrigger.StorageUri.PrimaryUri.AbsoluteUri}.  Id: {id}");
                }
            }
        }

        public async Task AbortWorkflowsAsync()
        {
            var blobTriggers = await storage.GetWorkflowsByStateAsync(AzureStorage.WorkflowState.Abort);
            
            foreach (var blobTrigger in blobTriggers)
            {
                var id = Guid.Empty;
                dynamic wfInstance = await storage.GetSerializedWorkflowTrigger(blobTrigger.Container.Name, blobTrigger.Name);

                try
                {
                    id = ExtractWorkflowId(blobTrigger, AzureStorage.WorkflowState.Abort);
                    wfInstance.WorkflowFailureDetails = await GetWorkflowFailureReason(id, WorkflowStatus.Aborted); wfInstance = JsonConvert.DeserializeObject(await storage.GetSerializedWorkflowTrigger(blobTrigger.Container.Name, blobTrigger.Name));
                    
                    logger.LogInformation($"Aborting workflow ID: {id} Url: {blobTrigger.Uri.AbsoluteUri}");
                    
                    await cromwellApiClient.PostAbortAsync(id);
                    wfInstance.WorkflowFailureDetails = await GetWorkflowFailureReason(id, WorkflowStatus.Aborted);
                    await storage.MutateStateAsync(blobTrigger.Container.Name, blobTrigger.Name, AzureStorage.WorkflowState.Abort, JsonConvert.SerializeObject(wfInstance));
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, $"Exception in UpdateWorkflowStatusesAsync for {blobTrigger}.  Id: {id}");
                    await storage.MutateStateAsync(blobTrigger.Container.Name, blobTrigger.Name, AzureStorage.WorkflowState.Abort);
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

        private static Guid ExtractWorkflowId(Microsoft.WindowsAzure.Storage.Blob.CloudBlockBlob blobTrigger, AzureStorage.WorkflowState currentState)
        {
            var blobName = blobTrigger.Name.Substring(currentState.ToString().Length + 1);
            var withoutExtension = Path.GetFileNameWithoutExtension(blobName);
            var textId = withoutExtension.Substring(withoutExtension.LastIndexOf('.') + 1);
            return Guid.Parse(textId);
        }

        private static string ExtractSampleName(Microsoft.WindowsAzure.Storage.Blob.CloudBlockBlob blobTrigger)
        {
            var blobName = blobTrigger.Name.Substring(AzureStorage.WorkflowState.InProgress.ToString().Length + 1);
            var withoutExtension = Path.GetFileNameWithoutExtension(blobName);
            return withoutExtension.Substring(0, withoutExtension.LastIndexOf('.'));
        }

        private static string GetBlobName(string url)
        {
            return blobNameRegex.Match(url)?.Groups[1].Value.Replace("/", "_");
        }

        private async Task UploadOutputsAsync(Microsoft.WindowsAzure.Storage.Blob.CloudBlockBlob blobTrigger, Guid id, string sampleName)
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

        private async Task UploadTimingAsync(Microsoft.WindowsAzure.Storage.Blob.CloudBlockBlob blobTrigger, Guid id, string sampleName)
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

        private async Task<string> GetWorkflowFailureReason(Guid workflowid, WorkflowStatus workFlowStatus)
        {
            switch (workFlowStatus)
            {
                case WorkflowStatus.Aborted:
                    return "{ Logs.FailureReason, 'Workflow Aborted' }";
                    
                case WorkflowStatus.Failed:
                    
                    var tesTasks = await tesTaskRepository.GetItemsAsync(
                        predicate: t => t.WorkflowId == workflowid.ToString());

                    //take all failed tasks 
                    var failedTesTasks = from testask in tesTasks 
                                         where testask.State == TesState.EXECUTORERROREnum ||
                                         testask.State == TesState.SYSTEMERROREnum ||
                                         (testask.State == TesState.COMPLETEEnum && testask.CromwellResultCode !=0)
                                         select testask;

                    //gather failed tasks which have passed in later attempts
                    var eliminateSuccessfulreattempts = from tasks in tesTasks
                                                        join fld in failedTesTasks on
                                                        new { tasks.CromwellTaskInstanceName, tasks.CromwellShard } equals new { fld.CromwellTaskInstanceName, fld.CromwellShard }
                                                        where (fld.State == TesState.EXECUTORERROREnum || fld.State == TesState.SYSTEMERROREnum) && (tasks.State == TesState.COMPLETEEnum && tasks.CromwellShard > 1)
                                                        select tasks;

                    //filter out failed tasks that succeeded in later attempts.
                    var filteredFailedTasks = from failedtask in failedTesTasks
                                              where !eliminateSuccessfulreattempts.Any(t => t.CromwellTaskInstanceName== failedtask.CromwellTaskInstanceName && t.CromwellShard == failedtask.CromwellShard)
                                              select failedtask;

                    var LatestfailedAttemptquery = filteredFailedTasks.GroupBy(r => new { r.CromwellTaskInstanceName, r.CromwellShard })
                        .Select(g => g.OrderByDescending(ge => ge.CromwellAttempt))
                        .First();
                        
                    return JsonConvert.SerializeObject(LatestfailedAttemptquery.Select(t =>
                   $@"{{Logs.FailureReason: '{t.Logs[t.Logs.Count -1].FailureReason}', Logs.FailureReason: '{t.Logs[t.Logs.Count -1].FailureReason}', Logs.SystemLogs: '{t.Logs[t.Logs.Count -1].SystemLogs}', Executor.StdErr: '{t.Executors[0].Stderr}', Executor.StdOut: '{t.Executors[0].Stdout}', CromwellResultCode: '{t.CromwellResultCode}'}}").ToList());                  
                
                default:                    
                    return string.Empty;
            }
        }
    }
}
