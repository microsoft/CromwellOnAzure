﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.Management.ApplicationInsights.Management;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Rest;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using FluentAzure = Microsoft.Azure.Management.Fluent.Azure;

namespace TriggerService
{
    public class AzureStorage : IAzureStorage
    {
        private const string WorkflowsContainerName = "workflows";
        private readonly CloudStorageAccount account;
        private readonly CloudBlobClient blobClient;
        private readonly HttpClient httpClient;
        private readonly HashSet<string> createdContainers = new();

        public AzureStorage(CloudStorageAccount account, HttpClient httpClient)
        {
            ServicePointManager.DefaultConnectionLimit = Environment.ProcessorCount * 8;
            ServicePointManager.Expect100Continue = false;

            this.account = account;
            this.httpClient = httpClient;

            blobClient = account.CreateCloudBlobClient();
            var host = account.BlobStorageUri.PrimaryUri.Host;
            AccountName = host[..host.IndexOf(".")];
        }

        public string AccountName { get; }
        public string AccountAuthority => account.BlobStorageUri.PrimaryUri.Authority;

        public static async Task<string> GetAppInsightsConnectionStringAsync(string appInsightsApplicationId)
        {
            var azureClient = await GetAzureManagementClientAsync();
            var subscriptionIds = (await azureClient.Subscriptions.ListAsync()).Select(s => s.SubscriptionId);

            var credentials = new TokenCredentials(await GetAzureAccessTokenAsync());

            foreach (var subscriptionId in subscriptionIds)
            {
                try
                {
                    var app = (await new ApplicationInsightsManagementClient(credentials) { SubscriptionId = subscriptionId }.Components.ListAsync())
                        .FirstOrDefault(a => a.ApplicationId.Equals(appInsightsApplicationId, StringComparison.OrdinalIgnoreCase));

                    if (app is not null)
                    {
                        return app.ConnectionString;
                    }
                }
                catch
                {
                }
            }

            return null;
        }

        public async Task<bool> IsAvailableAsync()
        {
            try
            {
                BlobContinuationToken continuationToken = null;
                await blobClient.ListContainersSegmentedAsync(continuationToken);
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <inheritdoc />
        public async Task<IEnumerable<TriggerFile>> GetWorkflowsByStateAsync(WorkflowState state)
        {
            var containerReference = blobClient.GetContainerReference(WorkflowsContainerName);
            var lowercaseState = state.ToString().ToLowerInvariant();
            var blobs = await GetBlobsWithPrefixAsync(containerReference, lowercaseState);
            var readmeBlobName = $"{lowercaseState}/readme.txt";

            return blobs
                .Where(blob => !blob.Name.Equals(lowercaseState, StringComparison.OrdinalIgnoreCase))
                .Where(blob => !blob.Name.Equals(readmeBlobName, StringComparison.OrdinalIgnoreCase))
                .Where(blob => blob.Properties.LastModified.HasValue)
                .Select(blob => new TriggerFile { Uri = blob.Uri.AbsoluteUri, ContainerName = WorkflowsContainerName, Name = blob.Name, LastModified = blob.Properties.LastModified.Value });
        }

        /// <inheritdoc />
        public async Task<string> UploadFileTextAsync(string content, string container, string blobName)
        {
            var containerReference = blobClient.GetContainerReference(container);

            if (!createdContainers.Contains(container.ToLowerInvariant()))
            {
                // Only attempt to create the container once per lifetime of the process
                await containerReference.CreateIfNotExistsAsync();
                createdContainers.Add(container.ToLowerInvariant());
            }

            var blob = containerReference.GetBlockBlobReference(blobName);
            await blob.UploadTextAsync(content);
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

            var blob = new CloudBlockBlob(new Uri(blobUrl), account.Credentials);

            var options = new BlobRequestOptions()
            {
                DisableContentMD5Validation = true,
            };

            var context = new OperationContext();

            using var memoryStream = new MemoryStream();
            await blob.DownloadToStreamAsync(memoryStream, null, options, context);
            return memoryStream.ToArray();
        }

        /// <inheritdoc />
        public async Task<byte[]> DownloadFileUsingHttpClientAsync(string url)
            => await httpClient.GetByteArrayAsync(url);

        /// <inheritdoc />
        public Task<string> DownloadBlobTextAsync(string container, string blobName)
            => blobClient.GetContainerReference(container).GetBlockBlobReference(blobName).DownloadTextAsync();

        /// <inheritdoc />
        public Task DeleteBlobIfExistsAsync(string container, string blobName)
            => blobClient.GetContainerReference(container).GetBlockBlobReference(blobName).DeleteIfExistsAsync();



        /// <summary>
        /// Gets an authenticated Azure Client instance
        /// </summary>
        /// <returns>An authenticated Azure Client instance</returns>
        private static async Task<FluentAzure.IAuthenticated> GetAzureManagementClientAsync()
        {
            var accessToken = await GetAzureAccessTokenAsync();
            var azureCredentials = new AzureCredentials(new TokenCredentials(accessToken), null, null, AzureEnvironment.AzureGlobalCloud);
            var azureClient = FluentAzure.Authenticate(azureCredentials);

            return azureClient;
        }

        private static async Task<IEnumerable<CloudBlockBlob>> GetBlobsWithPrefixAsync(CloudBlobContainer blobContainer, string prefix)
        {
            var blobList = new List<CloudBlockBlob>();

            BlobContinuationToken continuationToken = null;

            do
            {
                var partialResult = await blobContainer.ListBlobsSegmentedAsync(
                    prefix: prefix,
                    useFlatBlobListing: true,
                    currentToken: continuationToken,
                    blobListingDetails: BlobListingDetails.None,
                    maxResults: null,
                    options: null,
                    operationContext: null);

                continuationToken = partialResult.ContinuationToken;

                blobList.AddRange(partialResult.Results.OfType<CloudBlockBlob>());
            }
            while (continuationToken is not null);

            return blobList;
        }




        private static Task<string> GetAzureAccessTokenAsync(string resource = "https://management.azure.com/")
            => new AzureServiceTokenProvider().GetAccessTokenAsync(resource);


    }
}
