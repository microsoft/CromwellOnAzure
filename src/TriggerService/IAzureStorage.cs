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
        Task<byte[]> DownloadBlockBlobAsync(string blobUrl);
        Task<string> UploadFileFromPathAsync(string path, string container, string blobName);
        Task<string> UploadFileTextAsync(string content, string container, string blobName);        
        Task DeleteAllBlobsAsync(string container);
        Task DeleteContainerAsync(string container);
        Task<string> DownloadBlobTextAsync(string container, string blobName);
        Task DeleteBlobIfExistsAsync(string container, string blobName);
        Task<IEnumerable<CloudBlockBlob>> GetWorkflowBlobsToAbortAsync();
        Task<IEnumerable<CloudBlockBlob>> GetBlobsByStateAsync(AzureStorage.WorkflowState state);
        Task<IEnumerable<CloudBlockBlob>> GetRecentlyUpdatedInProgressWorkflowBlobsAsync();
        Task<bool> IsSingleBlobExistsFromPrefixAsync(string container, string blobPrefix);
        Task<bool> IsAvailableAsync();
        Task<byte[]> DownloadFileUsingHttpClientAsync(string url);        
    }
}
