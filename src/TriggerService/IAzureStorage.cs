// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace TriggerService
{
    public interface IAzureStorage
    {
        /// <summary>
        /// Storage account name
        /// </summary>
        string AccountName { get; }

        /// <summary>
        /// Blob service URI Authority
        /// </summary>
        string AccountAuthority { get; }

        /// <summary>
        /// Downloads blob
        /// </summary>
        /// <param name="blobUrl">URL of blob to download.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>Blob content.</returns>
        Task<byte[]> DownloadBlockBlobAsync(string blobUrl, CancellationToken cancellationToken);

        /// <summary>
        /// Uploads blob contents to storage account.
        /// </summary>
        /// <param name="content">Content of blob as text.</param>
        /// <param name="container">Name of container in the storage account.</param>
        /// <param name="blobName">Name of the blob inside of <paramref name="container"/>.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>URL of blob.</returns>
        /// <remarks>Creates container if it doesn't exist. Overwrites existing blob, otherwise creates new block blob.</remarks>
        Task<string> UploadFileTextAsync(string content, string container, string blobName, CancellationToken cancellationToken);

        /// <summary>
        /// Downloads blob contents as text
        /// </summary>
        /// <param name="container">Name of container in the storage account.</param>
        /// <param name="blobName">Name of the blob inside of <paramref name="container"/>.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>Contents of blob as <c>string</c>.</returns>
        Task<string> DownloadBlobTextAsync(string container, string blobName, CancellationToken cancellationToken);

        /// <summary>
        /// Deletes blob if it exists. Does not throw if blob does not exist.
        /// </summary>
        /// <param name="container">Name of container in the storage account.</param>
        /// <param name="blobName">Name of the blob inside of <paramref name="container"/>.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        Task DeleteBlobIfExistsAsync(string container, string blobName, CancellationToken cancellationToken);

        /// <summary>
        /// Enumerates all workflows last known to be in the specified state
        /// </summary>
        /// <param name="state">Prefix for the trigger blobs to enumerate.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>An enumeration of all trigger blobs in the <paramref name="state"/> state.</returns>
        IAsyncEnumerable<TriggerFile> GetWorkflowsByStateAsync(WorkflowState state, CancellationToken cancellationToken);

        /// <summary>
        /// Determines if the blob service is available
        /// </summary>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns><c>true</c> if storage account's blob service is available, <c>false</c> otherwise.</returns>
        Task<bool> IsAvailableAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Downloads file via protocol in <paramref name="url"/>
        /// </summary>
        /// <param name="url">URL of file to download.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>File content.</returns>
        Task<byte[]> DownloadFileUsingHttpClientAsync(string url, CancellationToken cancellationToken);
    }
}
