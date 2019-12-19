// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Common;
using CromwellApiClient;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace TriggerService
{
    public class CromwellOnAzureEnvironment
    {
        private static readonly Regex blobNameRegex = new Regex("^(?:https?://|/)?[^/]+/[^/]+/([^?.]+)");  // Supporting "http://account.blob.core.windows.net/container/blob", "/account/container/blob" and "account/container/blob" URLs in the trigger file.
        private IAzureStorage storage { get; set; }
        private ICromwellApiClient cromwellApiClient { get; set; }
        private readonly ILogger<AzureStorage> logger;

        public CromwellOnAzureEnvironment(ILoggerFactory loggerFactory, IAzureStorage storage, ICromwellApiClient cromwellApiClient)
        {
            this.storage = storage;
            this.cromwellApiClient = cromwellApiClient;
            this.logger = loggerFactory.CreateLogger<AzureStorage>();

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
                    var triggerInfo = JsonConvert.DeserializeObject<Workflow>(blobTriggerJson);
                    var tasks = new List<Task>();

                    (var workflowSourceFilename, var workflowSourceData) = await GetBlobFileNameAndData(triggerInfo.WorkflowUrl);
                    (var workflowInputsFilename, var workflowInputsData) = await GetBlobFileNameAndData(triggerInfo.WorkflowInputsUrl);
                    (var workflowOptionsFilename, var workflowOptionsData) = await GetBlobFileNameAndData(triggerInfo.WorkflowOptionsUrl);
                    (var workflowDependenciesFilename, var workflowDependenciesData) = await GetBlobFileNameAndData(triggerInfo.WorkflowDependenciesUrl);

                    var response = await cromwellApiClient.PostWorkflowAsync(
                        workflowSourceFilename,
                        workflowSourceData,
                        workflowInputsFilename,
                        workflowInputsData,
                        workflowOptionsFilename,
                        workflowOptionsData,
                        workflowDependenciesFilename,
                        workflowDependenciesData);

                    await storage.SetStateToInProgressAsync(blobTrigger.Container.Name, blobTrigger.Name, response.Id.ToString());
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, $"Exception in ExecuteNewWorkflowsAsync for {blobTrigger}");
                    await storage.MutateStateAsync(blobTrigger.Container.Name, blobTrigger.Name, AzureStorage.WorkflowState.Failed);
                }
            }
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

                try
                {
                    id = ExtractWorkflowId(blobTrigger, AzureStorage.WorkflowState.InProgress);
                    var sampleName = ExtractSampleName(blobTrigger);
                    var statusResponse = await cromwellApiClient.GetStatusAsync(id);

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
                                logger.LogInformation($"Setting to failed Id: {id}");
                                await storage.MutateStateAsync(blobTrigger.Container.Name, blobTrigger.Name, AzureStorage.WorkflowState.Failed);
                                await UploadOutputsAsync(blobTrigger, id, sampleName);
                                await UploadTimingAsync(blobTrigger, id, sampleName);
                                break;
                            }
                        case WorkflowStatus.Succeeded:
                            {
                                await UploadOutputsAsync(blobTrigger, id, sampleName);
                                await UploadTimingAsync(blobTrigger, id, sampleName);
                                await storage.MutateStateAsync(blobTrigger.Container.Name, blobTrigger.Name, AzureStorage.WorkflowState.Succeeded);
                                break;
                            }
                    }
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, $"Exception in UpdateWorkflowStatusesAsync for {blobTrigger}.  Id: {id}");
                    await storage.MutateStateAsync(blobTrigger.Container.Name, blobTrigger.Name, AzureStorage.WorkflowState.Failed);
                }
            }
        }

        public async Task AbortWorkflowsAsync()
        {
            var blobTriggers = await storage.GetWorkflowsByStateAsync(AzureStorage.WorkflowState.Abort);

            foreach (var blobTrigger in blobTriggers)
            {
                var id = Guid.Empty;

                try
                {
                    id = ExtractWorkflowId(blobTrigger, AzureStorage.WorkflowState.Abort);
                    logger.LogInformation($"Aborting workflow ID: {id} Url: {blobTrigger.Uri.AbsoluteUri}");
                    await cromwellApiClient.PostAbortAsync(id);
                    await storage.MutateStateAsync(blobTrigger.Container.Name, blobTrigger.Name, AzureStorage.WorkflowState.Failed);
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, $"Exception in UpdateWorkflowStatusesAsync for {blobTrigger}.  Id: {id}");
                    await storage.MutateStateAsync(blobTrigger.Container.Name, blobTrigger.Name, AzureStorage.WorkflowState.Failed);
                }
            }
        }

        public async Task<(string, byte[])> GetBlobFileNameAndData(string url)
        {
            if (string.IsNullOrWhiteSpace(url))
            {
                return (null, null);
            }

            var blobName = GetBlobName(url);

            byte[] data;

            if (((Uri.TryCreate(url, UriKind.Absolute, out var uri) && uri.Authority.Equals(storage.AccountAuthority, StringComparison.OrdinalIgnoreCase))
                || url.TrimStart('/').StartsWith(storage.AccountName + "/", StringComparison.OrdinalIgnoreCase))
                && uri.ParseQueryString().Get("sig") == null)
            {
                // use known credentials, unless the URL specifies a shared-access signature
                data = await storage.DownloadBlockBlobAsync(url);
            }
            else
            {
                data = await storage.DownloadFileUsingHttpClientAsync(url);
            }

            return (blobName, data);
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
    }
}
