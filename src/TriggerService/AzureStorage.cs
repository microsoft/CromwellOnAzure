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
        private readonly HashSet<string> createdContainers = new HashSet<string>();

        public AzureStorage(CloudStorageAccount account, HttpClient httpClient)
        {
            ServicePointManager.DefaultConnectionLimit = Environment.ProcessorCount * 8;
            ServicePointManager.Expect100Continue = false;

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

                    if (app is not null)
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

        private class StorageAccountInfo
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string BlobEndpoint { get; set; }
            public string SubscriptionId { get; set; }
        }

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
                    operationContext: null).ConfigureAwait(false);

                continuationToken = partialResult.ContinuationToken;

                blobList.AddRange(partialResult.Results.OfType<CloudBlockBlob>());
            }
            while (continuationToken is not null);

            return blobList;
        }

        public static async Task<(IEnumerable<IAzureStorage>, IAzureStorage)> GetStorageAccountsUsingMsiAsync(string accountName)
        {
            IAzureStorage defaultAzureStorage = default;
            (var accounts, var defaultAccount) = await GetCloudStorageAccountsUsingMsiAsync(accountName);
            return (accounts.Select(GetAzureStorage).ToList(), defaultAzureStorage ?? throw new Exception($"Azure Storage account with name: {accountName} not found in list of {accounts.Count} storage accounts."));

            IAzureStorage GetAzureStorage(CloudStorageAccount cloudStorage)
            {
                var azureStorage = new AzureStorage(cloudStorage, new HttpClient());
                if (cloudStorage.Equals(defaultAccount))
                {
                    defaultAzureStorage = azureStorage;
                }
                return azureStorage;
            }
        }

        private static async Task<(IList<CloudStorageAccount>, CloudStorageAccount)> GetCloudStorageAccountsUsingMsiAsync(string accountName)
        {
            CloudStorageAccount defaultAccount = default;
            var accounts = await GetAccessibleStorageAccountsAsync();
            return (await Task.WhenAll(accounts.Select(GetCloudAccountFromStorageAccountInfo)), defaultAccount ?? throw new Exception($"Azure Storage account with name: {accountName} not found in list of {accounts.Count} storage accounts."));

            async Task<CloudStorageAccount> GetCloudAccountFromStorageAccountInfo(StorageAccountInfo account)
            {
                var key = await GetStorageAccountKeyAsync(account);
                var storageCredentials = new Microsoft.WindowsAzure.Storage.Auth.StorageCredentials(account.Name, key);
                var storageAccount = new CloudStorageAccount(storageCredentials, true);
                if (account.Name == accountName)
                {
                    defaultAccount = storageAccount;
                }
                return storageAccount;
            }
        }

        private static Task<string> GetAzureAccessTokenAsync(string resource = "https://management.azure.com/")
            => new AzureServiceTokenProvider().GetAccessTokenAsync(resource);

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

            return (await storageAccount.GetKeysAsync())[0].Value;
        }
    }
}
