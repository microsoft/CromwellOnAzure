// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using LazyCache;
using Microsoft.Azure.Batch;
using Polly;
using Polly.Retry;
using TesApi.Models;

namespace TesApi.Web
{
    ///<inheritdoc/>
    /// <summary>
    /// Implements caching and retries for <see cref="IAzureProxy"/>.
    /// </summary>
    public class CachingWithRetriesAzureProxy : IAzureProxy
    {
        private readonly IAzureProxy azureProxy;
        private readonly IAppCache cache;

        private readonly AsyncRetryPolicy asyncRetryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(3, attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)));

        private readonly RetryPolicy retryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetry(3, attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)));

        /// <summary>
        /// Contructor to create a cache of <see cref="IAzureProxy"/>
        /// </summary>
        /// <param name="azureProxy"><see cref="AzureProxy"/></param>
        /// <param name="cache">Lazy cache using <see cref="IAppCache"/></param>
        public CachingWithRetriesAzureProxy(IAzureProxy azureProxy, IAppCache cache)
        {
            this.azureProxy = azureProxy;
            this.cache = cache;
        }

        public Task CreateBatchJobAsync(string jobId, CloudTask cloudTask, PoolInformation poolInformation) => azureProxy.CreateBatchJobAsync(jobId, cloudTask, poolInformation);
        public Task DeleteBatchJobAsync(string taskId) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.DeleteBatchJobAsync(taskId));
        public Task DeleteBatchPoolAsync(string poolId, CancellationToken cancellationToken) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.DeleteBatchPoolAsync(poolId, cancellationToken));
        public Task<string> DownloadBlobAsync(Uri blobAbsoluteUri) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.DownloadBlobAsync(blobAbsoluteUri));
        public Task<IEnumerable<string>> GetActivePoolIdsAsync(string prefix, TimeSpan minAge, CancellationToken cancellationToken) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.GetActivePoolIdsAsync(prefix, minAge, cancellationToken));

        ///<inheritdoc/>
        public Task<AzureBatchAccountQuotas> GetBatchAccountQuotasAsync()
        {
            return cache.GetOrAddAsync("batchAccountQuotas", () =>
                asyncRetryPolicy.ExecuteAsync(() => azureProxy.GetBatchAccountQuotasAsync()), DateTimeOffset.Now.AddHours(1));
        }

        public int GetBatchActiveJobCount() => retryPolicy.Execute(() => azureProxy.GetBatchActiveJobCount());
        public IEnumerable<AzureBatchNodeCount> GetBatchActiveNodeCountByVmSize() => retryPolicy.Execute(() => azureProxy.GetBatchActiveNodeCountByVmSize());
        public int GetBatchActivePoolCount() => retryPolicy.Execute(() => azureProxy.GetBatchActivePoolCount());
        public Task<AzureBatchJobAndTaskState> GetBatchJobAndTaskStateAsync(string tesTaskId) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.GetBatchJobAndTaskStateAsync(tesTaskId));

        ///<inheritdoc/>
        public async Task<ContainerRegistryInfo> GetContainerRegistryInfoAsync(string imageName)
        {
            var containerRegistryInfo = cache.Get<ContainerRegistryInfo>(imageName);

            if (containerRegistryInfo == null)
            {
                containerRegistryInfo = await asyncRetryPolicy.ExecuteAsync(() => azureProxy.GetContainerRegistryInfoAsync(imageName));

                if (containerRegistryInfo != null)
                {
                    cache.Add(imageName, containerRegistryInfo, DateTimeOffset.Now.AddHours(1));
                }
            }

            return containerRegistryInfo;
        }

        public Task<string> GetNextBatchJobIdAsync(string tesTaskId) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.GetNextBatchJobIdAsync(tesTaskId));
        public Task<IEnumerable<string>> GetPoolIdsReferencedByJobsAsync(CancellationToken cancellationToken) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.GetPoolIdsReferencedByJobsAsync(cancellationToken));

        ///<inheritdoc/>
        public Task<string> GetStorageAccountKeyAsync(StorageAccountInfo storageAccountInfo)
        {
            return cache.GetOrAddAsync(storageAccountInfo.Id, () =>
                asyncRetryPolicy.ExecuteAsync(() => azureProxy.GetStorageAccountKeyAsync(storageAccountInfo)), DateTimeOffset.Now.AddHours(1));
        }

        ///<inheritdoc/>
        public async Task<StorageAccountInfo> GetStorageAccountInfoAsync(string storageAccountName)
        {
            var storageAccountInfo = cache.Get<StorageAccountInfo>(storageAccountName);

            if (storageAccountInfo == null)
            {
                storageAccountInfo = await asyncRetryPolicy.ExecuteAsync(() => azureProxy.GetStorageAccountInfoAsync(storageAccountName));

                if (storageAccountInfo != null)
                {
                    cache.Add(storageAccountName, storageAccountInfo, DateTimeOffset.MaxValue);
                }
            }

            return storageAccountInfo;
        }

        ///<inheritdoc/>
        public Task<List<VirtualMachineInfo>> GetVmSizesAndPricesAsync()
        {
            return cache.GetOrAddAsync("vmSizesAndPrices", () => azureProxy.GetVmSizesAndPricesAsync(), DateTimeOffset.MaxValue);
        }

        public Task<IEnumerable<string>> ListBlobsAsync(Uri directoryUri) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.ListBlobsAsync(directoryUri));
        public Task<IEnumerable<string>> ListOldJobsToDeleteAsync(TimeSpan oldestJobAge) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.ListOldJobsToDeleteAsync(oldestJobAge));
        public Task UploadBlobAsync(Uri blobAbsoluteUri, string content) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.UploadBlobAsync(blobAbsoluteUri, content));
        public Task<(Uri, string)> GetCosmosDbEndpointAndKeyAsync(string cosmosDbAccountName) =>  azureProxy.GetCosmosDbEndpointAndKeyAsync(cosmosDbAccountName);
        public Task UploadBlobFromFileAsync(Uri blobAbsoluteUri, string filePath) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.UploadBlobFromFileAsync(blobAbsoluteUri, filePath));
        public bool LocalFileExists(string path) => azureProxy.LocalFileExists(path);
        public bool TryReadCwlFile(string workflowId, out string content) => azureProxy.TryReadCwlFile(workflowId, out content);
    }
}
