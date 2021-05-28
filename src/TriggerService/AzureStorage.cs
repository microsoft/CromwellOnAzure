// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.Management.ApplicationInsights.Management;
using FluentAzure = Microsoft.Azure.Management.Fluent.Azure;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Extensions.Logging;
using Microsoft.Rest;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace TriggerService
{
    public class AzureStorage : IAzureStorage
    {
        private const string WorkflowsContainerName = "workflows";
        private readonly ILogger<AzureStorage> logger;
        private readonly CloudStorageAccount account;
        private readonly CloudBlobClient blobClient;
        private readonly HttpClient httpClient;

        public AzureStorage(ILogger<AzureStorage> logger, CloudStorageAccount account, HttpClient httpClient)
        {
            ServicePointManager.DefaultConnectionLimit = Environment.ProcessorCount * 8;
            ServicePointManager.Expect100Continue = false;

            this.logger = logger;
            this.account = account;
            this.httpClient = httpClient;

            blobClient = account.CreateCloudBlobClient();
            var host = account.BlobStorageUri.PrimaryUri.Host;
            AccountName = host.Substring(0, host.IndexOf("."));
        }

        public string AccountName { get; }
        public string AccountAuthority => account.BlobStorageUri.PrimaryUri.Authority;

        public static async Task<string> GetAppInsightsInstrumentationKeyAsync(string appInsightsApplicationId)
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

                    if (app != null)
                    {
                        return app.InstrumentationKey;
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

        /// <summary>
        /// Return all blobs for a given state, except readme files
        /// </summary>
        /// <param name="state">Workflow state to query for</param>
        /// <returns></returns>
        public async Task<IEnumerable<CloudBlockBlob>> GetWorkflowsByStateAsync(WorkflowState state)
        {
            var containerReference = blobClient.GetContainerReference(WorkflowsContainerName);
            var lowercaseState = state.ToString().ToLowerInvariant();
            var blobs = await GetBlobsWithPrefixAsync(containerReference, lowercaseState);
            var readmeBlobName = $"{lowercaseState}/readme.txt";
            return blobs.Where(blob => !blob.Name.Equals(readmeBlobName, StringComparison.OrdinalIgnoreCase));
        }

        public async Task<string> UploadFileFromPathAsync(string path, string container, string blobName)
        {
            var containerReference = blobClient.GetContainerReference(container);
            await containerReference.CreateIfNotExistsAsync();
            var blob = containerReference.GetBlockBlobReference(blobName);
            await blob.UploadFromFileAsync(path);
            return blob.Uri.AbsoluteUri;
        }

        public string GetBlobSasUrl(string blobUrl, TimeSpan sasTokenDuration)
        {
            var policy = new SharedAccessBlobPolicy() { Permissions = SharedAccessBlobPermissions.Read, SharedAccessExpiryTime = DateTime.Now.Add(sasTokenDuration) };
            var blob = new CloudBlob(new Uri(blobUrl), blobClient);
            return blobUrl + blob.GetSharedAccessSignature(policy, null, null, SharedAccessProtocol.HttpsOnly, null);
        }

        public async Task<string> UploadFileTextAsync(string content, string container, string blobName)
        {
            var containerReference = blobClient.GetContainerReference(container);
            await containerReference.CreateIfNotExistsAsync();
            var blob = containerReference.GetBlockBlobReference(blobName);
            await blob.UploadTextAsync(content);
            return blob.Uri.AbsoluteUri;
        }

        public async Task<bool> IsSingleBlobExistsFromPrefixAsync(string container, string blobPrefix)
        {
            var containerReference = blobClient.GetContainerReference(container);
            var blobs = await GetBlobsWithPrefixAsync(containerReference, blobPrefix);
            return blobs.Count() == 1;
        }

        public async Task DeleteAllBlobsAsync(string container)
        {
            var containerReference = blobClient.GetContainerReference(container);
            var blobs = await GetBlobsWithPrefixAsync(containerReference, null);

            foreach (var blob in blobs)
            {
                await blob.DeleteIfExistsAsync();
            }
        }

        public async Task DeleteContainerAsync(string container)
        {
            var containerReference = blobClient.GetContainerReference(container);
            await containerReference.DeleteIfExistsAsync();
        }

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

            using (var memoryStream = new MemoryStream())
            {
                await blob.DownloadToStreamAsync(memoryStream, null, options, context);
                return memoryStream.ToArray();
            }
        }

        public async Task<byte[]> DownloadFileUsingHttpClientAsync(string url)
        {
            return await httpClient.GetByteArrayAsync(url);
        }

        /// <summary>
        /// Return all blobs for a given state, except readme files
        /// </summary>
        /// <param name="state">Workflow state to query for</param>
        /// <returns></returns>
        public async Task<IEnumerable<CloudBlockBlob>> GetBlobsByStateAsync(WorkflowState state)
        {
            var containerReference = blobClient.GetContainerReference(WorkflowsContainerName);
            var lowercaseState = state.ToString().ToLowerInvariant();
            var blobs = await GetBlobsWithPrefixAsync(containerReference, lowercaseState);
            var readmeBlobName = $"{lowercaseState}/readme.txt";
            return blobs.Where(blob => !blob.Name.Equals(readmeBlobName, StringComparison.OrdinalIgnoreCase));
        }

        /// <summary>
        /// Return all blobs for a WorkflowState.Abort state.
        /// </summary>
        /// <returns></returns>
        public async Task<IEnumerable<CloudBlockBlob>> GetWorkflowBlobsToAbortAsync()
        {
            return (await GetBlobsByStateAsync(WorkflowState.Abort));
        }

        /// <summary>
        /// Return all blobs for a given state LastModified in the past 1 minute, except readme files 
        /// </summary>
        /// <param name="state">Workflow state to query for</param>
        /// <returns></returns>
        public async Task<IEnumerable<CloudBlockBlob>> GetRecentlyUpdatedInProgressWorkflowBlobsAsync()
        {
            return (await GetBlobsByStateAsync(WorkflowState.InProgress)).Where(blob => DateTime.UtcNow - blob.Properties.LastModified.Value < new TimeSpan(1, 0, 0));
        }

        /// <summary>
        /// Asynchronously download blob text 
        /// </summary>
        /// <param name="container"></param>
        /// <param name="blobName"></param>
        /// <returns></returns>
        public async Task<string> DownloadBlobTextAsync(string container, string blobName)
        {
            var containerReference = blobClient.GetContainerReference(container);
            var blob = containerReference.GetBlockBlobReference(blobName);
            return await blob.DownloadTextAsync();
        }

        /// <summary>
        /// Asynchronously deleted blob identified by parameters if it exists
        /// </summary>
        /// <param name="container"></param>
        /// <param name="blobName"></param>
        /// <returns></returns>
        public async Task DeleteBlobIfExistsAsync(string container, string blobName)
        {
            var containerReference = blobClient.GetContainerReference(container);
            var blob = containerReference.GetBlockBlobReference(blobName);
            await blob.DeleteIfExistsAsync();
        }

        public enum WorkflowState { New, InProgress, Succeeded, Failed, Abort };

        public class StorageAccountInfo
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string BlobEndpoint { get; set; }
            public string SubscriptionId { get; set; }
        }

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
                    operationContext: null).ConfigureAwait(false);

                continuationToken = partialResult.ContinuationToken;

                blobList.AddRange(partialResult.Results.OfType<CloudBlockBlob>());
            }
            while (continuationToken != null);

            return blobList;
        }

        public static async Task<CloudStorageAccount> GetCloudStorageAccountUsingMsiAsync(string accountName)
        {
            var accounts = await GetAccessibleStorageAccountsAsync();
            var account = accounts.FirstOrDefault(s => s.Name == accountName);

            if (account == null)
            {
                throw new Exception($"Azure Storage account with name: {accountName} not found in list of {accounts.Count} storage accounts.");
            }

            var key = await GetStorageAccountKeyAsync(account);
            var storageCredentials = new Microsoft.WindowsAzure.Storage.Auth.StorageCredentials(account.Name, key);
            return new CloudStorageAccount(storageCredentials, true);
        }

        private static Task<string> GetAzureAccessTokenAsync(string resource = "https://management.azure.com/")
        {
            return new AzureServiceTokenProvider().GetAccessTokenAsync(resource);
        }

        private static async Task<List<StorageAccountInfo>> GetAccessibleStorageAccountsAsync()
        {
            var azureClient = await GetAzureManagementClientAsync();

            var subscriptionIds = (await azureClient.Subscriptions.ListAsync()).Select(s => s.SubscriptionId);

            return (await Task.WhenAll(
                subscriptionIds.Select(async subId =>
                    (await azureClient.WithSubscription(subId).StorageAccounts.ListAsync())
                        .Select(a => new StorageAccountInfo { Id = a.Id, Name = a.Name, SubscriptionId = subId, BlobEndpoint = a.EndPoints.Primary.Blob }))))
                .SelectMany(a => a)
                .ToList();
        }

        private static async Task<string> GetStorageAccountKeyAsync(StorageAccountInfo storageAccountInfo)
        {
            var azureClient = await GetAzureManagementClientAsync();
            var storageAccount = await azureClient.WithSubscription(storageAccountInfo.SubscriptionId).StorageAccounts.GetByIdAsync(storageAccountInfo.Id);

            return (await storageAccount.GetKeysAsync()).First().Value;
        }
    }
}
