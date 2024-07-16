// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

namespace TriggerService
{
    public class AzureStorage : IAzureStorage
    {
        private const string WorkflowsContainerName = "workflows";
        private readonly BlobServiceClient blobClient;
        private readonly HttpClient httpClient;
        private readonly HashSet<string> createdContainers = [];

        public AzureStorage(BlobServiceClient account, HttpClient httpClient)
        {
            ServicePointManager.DefaultConnectionLimit = Environment.ProcessorCount * 8;
            ServicePointManager.Expect100Continue = false;

            this.httpClient = httpClient;

            blobClient = account;
            var host = account.Uri.Host;
            AccountName = host[..host.IndexOf('.')];
        }

        public string AccountName { get; }
        public string AccountAuthority => blobClient.Uri.Authority;

        public async Task<bool> IsAvailableAsync()
        {
            try
            {
                _ = await blobClient.GetBlobContainersAsync().ToListAsync();
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <inheritdoc />
        public IAsyncEnumerable<TriggerFile> GetWorkflowsByStateAsync(WorkflowState state)
        {
            var containerReference = blobClient.GetBlobContainerClient(WorkflowsContainerName);
            var lowercaseState = state.ToString().ToLowerInvariant();
            var blobs = containerReference.GetBlobsAsync(prefix: lowercaseState);
            var readmeBlobName = $"{lowercaseState}/readme.txt";

            return blobs
                .Where(blob => !blob.Name.Equals(lowercaseState, StringComparison.OrdinalIgnoreCase))
                .Where(blob => !blob.Name.Equals(readmeBlobName, StringComparison.OrdinalIgnoreCase))
                .Where(blob => blob.Properties.LastModified.HasValue)
                .Select(blob => new TriggerFile { Uri = containerReference.GetBlobClient(blob.Name).Uri.AbsoluteUri, ContainerName = WorkflowsContainerName, Name = blob.Name, LastModified = blob.Properties.LastModified.Value });
        }

        /// <inheritdoc />
        public async Task<string> UploadFileTextAsync(string content, string container, string blobName)
        {
            var containerReference = blobClient.GetBlobContainerClient(container);

            if (!createdContainers.Contains(container.ToLowerInvariant()))
            {
                // Only attempt to create the container once per lifetime of the process
                await containerReference.CreateIfNotExistsAsync();
                createdContainers.Add(container.ToLowerInvariant());
            }

            var blob = containerReference.GetBlockBlobClient(blobName);
            using var writer = new StreamWriter(new MemoryStream());
            writer.Write(content);
            writer.Flush();
            writer.BaseStream.Seek(0, SeekOrigin.Begin);
            await blob.UploadAsync(writer.BaseStream);
            return blob.Uri.AbsoluteUri;
        }

        /// <inheritdoc />
        public async Task<byte[]> DownloadBlockBlobAsync(string blobUrl)
        {
            // Supporting "http://account.blob.core.windows.net/container/blob", "/account/container/blob" and "account/container/blob" URLs
            if (!blobUrl.StartsWith("http", StringComparison.OrdinalIgnoreCase) && blobUrl.TrimStart('/').StartsWith(this.AccountName + "/", StringComparison.OrdinalIgnoreCase))
            {
                blobUrl = blobUrl.TrimStart('/').Replace(this.AccountName, $"https://{this.AccountAuthority}", StringComparison.OrdinalIgnoreCase);
            }

            BlobUriBuilder builder = new(new(blobUrl));

            var blob = blobClient.GetBlobContainerClient(builder.BlobContainerName).GetBlockBlobClient(builder.BlobName);

            using var memoryStream = new MemoryStream();
            _ = await blob.DownloadToAsync(memoryStream, new BlobDownloadToOptions { TransferValidation = new() { ChecksumAlgorithm = Azure.Storage.StorageChecksumAlgorithm.None } });
            return memoryStream.ToArray();
        }

        /// <inheritdoc />
        public async Task<byte[]> DownloadFileUsingHttpClientAsync(string url)
            => await httpClient.GetByteArrayAsync(url);

        /// <inheritdoc />
        public async Task<string> DownloadBlobTextAsync(string container, string blobName)
        {
            var blob = blobClient.GetBlobContainerClient(container).GetBlockBlobClient(blobName);

            using var reader = new StreamReader(new MemoryStream());
            _ = await blob.DownloadToAsync(reader.BaseStream);
            reader.BaseStream.Seek(0, SeekOrigin.Begin);
            return reader.ReadToEnd();
        }

        /// <inheritdoc />
        public Task DeleteBlobIfExistsAsync(string container, string blobName)
            => blobClient.GetBlobContainerClient(container).GetBlockBlobClient(blobName).DeleteIfExistsAsync();
    }
}
