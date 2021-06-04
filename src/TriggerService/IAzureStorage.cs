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
        Task<byte[]> DownloadBlockBlobAsync(string blobUrl);
        Task<string> UploadFileTextAsync(string content, string container, string blobName);
        Task<string> DownloadBlobTextAsync(string container, string blobName);
        Task DeleteBlobIfExistsAsync(string container, string blobName);
        Task<IEnumerable<TriggerFile>> GetWorkflowsByStateAsync(WorkflowState state);
        Task<bool> IsAvailableAsync();
        Task<byte[]> DownloadFileUsingHttpClientAsync(string url);
    }
}
