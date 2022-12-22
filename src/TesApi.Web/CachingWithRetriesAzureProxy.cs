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
using Tes.Models;

namespace TesApi.Web
{
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
        /// <param name="azureProxy"><see cref="IAzureProxy"/></param>
        /// <param name="cache">Lazy cache using <see cref="IAppCache"/></param>
        public CachingWithRetriesAzureProxy(IAzureProxy azureProxy, IAppCache cache)
        {
            this.azureProxy = azureProxy;
            this.cache = cache;
        }

        /// <inheritdoc/>
        public Task CreateBatchJobAsync(string jobId, CloudTask cloudTask, PoolInformation poolInformation) => azureProxy.CreateBatchJobAsync(jobId, cloudTask, poolInformation);

        /// <inheritdoc/>
        public Task DeleteBatchJobAsync(string taskId, CancellationToken cancellationToken = default) => asyncRetryPolicy.ExecuteAsync(ct => azureProxy.DeleteBatchJobAsync(taskId, ct), cancellationToken);

        /// <inheritdoc/>
        public Task DeleteBatchPoolAsync(string poolId, CancellationToken cancellationToken = default) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.DeleteBatchPoolAsync(poolId, cancellationToken));

        /// <inheritdoc/>
        public Task<string> DownloadBlobAsync(Uri blobAbsoluteUri) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.DownloadBlobAsync(blobAbsoluteUri));

        /// <inheritdoc/>
        public Task<bool> BlobExistsAsync(Uri blobAbsoluteUri) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.BlobExistsAsync(blobAbsoluteUri));

        /// <inheritdoc/>
        public Task<IEnumerable<string>> GetActivePoolIdsAsync(string prefix, TimeSpan minAge, CancellationToken cancellationToken) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.GetActivePoolIdsAsync(prefix, minAge, cancellationToken));

        /// <inheritdoc/>
        public Task<AzureBatchAccountQuotas> GetBatchAccountQuotasAsync()
            => cache.GetOrAddAsync("batchAccountQuotas", () =>
                asyncRetryPolicy.ExecuteAsync(() => azureProxy.GetBatchAccountQuotasAsync()), DateTimeOffset.Now.AddHours(1));

        /// <inheritdoc/>
        public int GetBatchActiveJobCount() => retryPolicy.Execute(() => azureProxy.GetBatchActiveJobCount());

        /// <inheritdoc/>
        public IEnumerable<AzureBatchNodeCount> GetBatchActiveNodeCountByVmSize() => retryPolicy.Execute(() => azureProxy.GetBatchActiveNodeCountByVmSize());

        /// <inheritdoc/>
        public int GetBatchActivePoolCount() => retryPolicy.Execute(() => azureProxy.GetBatchActivePoolCount());

        /// <inheritdoc/>
        public Task<AzureBatchJobAndTaskState> GetBatchJobAndTaskStateAsync(string tesTaskId) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.GetBatchJobAndTaskStateAsync(tesTaskId));

        /// <inheritdoc/>
        public async Task<ContainerRegistryInfo> GetContainerRegistryInfoAsync(string imageName)
        {
            var containerRegistryInfo = cache.Get<ContainerRegistryInfo>(imageName);

            if (containerRegistryInfo is null)
            {
                containerRegistryInfo = await asyncRetryPolicy.ExecuteAsync(() => azureProxy.GetContainerRegistryInfoAsync(imageName));

                if (containerRegistryInfo is not null)
                {
                    cache.Add(imageName, containerRegistryInfo, DateTimeOffset.Now.AddHours(1));
                }
            }

            return containerRegistryInfo;
        }

        /// <inheritdoc/>
        public Task<string> GetNextBatchJobIdAsync(string tesTaskId) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.GetNextBatchJobIdAsync(tesTaskId));

        /// <inheritdoc/>
        public Task<IEnumerable<string>> GetPoolIdsReferencedByJobsAsync(CancellationToken cancellationToken) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.GetPoolIdsReferencedByJobsAsync(cancellationToken));

        /// <inheritdoc/>
        public Task<string> GetStorageAccountKeyAsync(StorageAccountInfo storageAccountInfo)
            => cache.GetOrAddAsync(storageAccountInfo.Id, () =>
                asyncRetryPolicy.ExecuteAsync(() => azureProxy.GetStorageAccountKeyAsync(storageAccountInfo)), DateTimeOffset.Now.AddHours(1));

        /// <inheritdoc/>
        public async Task<StorageAccountInfo> GetStorageAccountInfoAsync(string storageAccountName)
        {
            var storageAccountInfo = cache.Get<StorageAccountInfo>(storageAccountName);

            if (storageAccountInfo is null)
            {
                storageAccountInfo = await asyncRetryPolicy.ExecuteAsync(() => azureProxy.GetStorageAccountInfoAsync(storageAccountName));

                if (storageAccountInfo is not null)
                {
                    cache.Add(storageAccountName, storageAccountInfo, DateTimeOffset.MaxValue);
                }
            }

            return storageAccountInfo;
        }

        /// <inheritdoc/>
        public Task<List<VirtualMachineInformation>> GetVmSizesAndPricesAsync()
            => cache.GetOrAddAsync("vmSizesAndPrices", () => azureProxy.GetVmSizesAndPricesAsync(), DateTimeOffset.MaxValue);

        /// <inheritdoc/>
        public Task<IEnumerable<string>> ListBlobsAsync(Uri directoryUri) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.ListBlobsAsync(directoryUri));

        /// <inheritdoc/>
        public Task<IEnumerable<string>> ListOldJobsToDeleteAsync(TimeSpan oldestJobAge) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.ListOldJobsToDeleteAsync(oldestJobAge));

        /// <inheritdoc/>
        public Task<IEnumerable<string>> ListOrphanedJobsToDeleteAsync(TimeSpan minJobAge, CancellationToken cancellationToken = default) => asyncRetryPolicy.ExecuteAsync(ct => azureProxy.ListOrphanedJobsToDeleteAsync(minJobAge, ct), cancellationToken);

        /// <inheritdoc/>
        public Task UploadBlobAsync(Uri blobAbsoluteUri, string content) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.UploadBlobAsync(blobAbsoluteUri, content));

        /// <inheritdoc/>
        public Task<(string, string)> GetCosmosDbEndpointAndKeyAsync(string cosmosDbAccountName) => azureProxy.GetCosmosDbEndpointAndKeyAsync(cosmosDbAccountName);

        /// <inheritdoc/>
        public Task UploadBlobFromFileAsync(Uri blobAbsoluteUri, string filePath) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.UploadBlobFromFileAsync(blobAbsoluteUri, filePath));

        /// <inheritdoc/>
        public bool LocalFileExists(string path) => azureProxy.LocalFileExists(path);

        /// <inheritdoc/>
        public bool TryReadCwlFile(string workflowId, out string content) => azureProxy.TryReadCwlFile(workflowId, out content);

        /// <inheritdoc/>
        public Task<ManualBatchPoolCreationResult> CreateManualBatchPoolAsync(string poolName, string vmSize, bool isLowPriority, string executorImage, BatchNodeInfo nodeInfo, string dockerInDockerImageName, string blobxferImageName, IEnumerable<string> identityResourceIds, bool disableBatchNodesPublicIpAddress, string batchNodesSubnetId, string startTaskSasUrl, string startTaskPath)
            => azureProxy.CreateManualBatchPoolAsync(poolName, vmSize, isLowPriority, executorImage, nodeInfo, dockerInDockerImageName, blobxferImageName, identityResourceIds, disableBatchNodesPublicIpAddress, batchNodesSubnetId, startTaskSasUrl, startTaskPath);

        /// <inheritdoc/>
        public Task DeleteBatchPoolIfExistsAsync(string poolId, CancellationToken cancellationToken = default)
            => azureProxy.DeleteBatchPoolIfExistsAsync(poolId, cancellationToken);
    }
}
