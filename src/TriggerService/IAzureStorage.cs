// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;

namespace TriggerService
{
    public interface IAzureStorage
    {
        string AccountName { get; }
        string AccountAuthority { get; }
        string GetBlobSasUrl(string blobUrl, TimeSpan sasTokenDuration);
        Task<byte[]> DownloadFileAsync(string blobUrl);
        Task<string> UploadFileFromPathAsync(string path, string container, string blobName);
        Task<string> UploadFileTextAsync(string content, string container, string blobName);
        Task MutateStateAsync(string container, string blobName, AzureStorage.WorkflowState newState);
        Task<List<string>> GetByStateAsync(string container, AzureStorage.WorkflowState state);
        Task SetStateToInProgressAsync(string container, string blobName, string id);
        Task DeleteAllBlobsAsync(string container);
        Task DeleteContainerAsync(string container);
        Task<IEnumerable<CloudBlockBlob>> GetWorkflowsByStateAsync(AzureStorage.WorkflowState state);
        Task<bool> IsSingleBlobExistsFromPrefixAsync(string container, string blobPrefix);
        Task<bool> IsAvailableAsync();
    }
}
