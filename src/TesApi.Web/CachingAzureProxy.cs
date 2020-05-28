// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using LazyCache;
using Microsoft.Azure.Batch;
using Microsoft.Extensions.Logging;
using TesApi.Models;
using static TesApi.Web.AzureProxy;

namespace TesApi.Web
{
    ///<inheritdoc/>
    /// <summary>
    /// Implements caching for <see cref="IAzureProxy"/>.
    /// </summary>
    public class CachingAzureProxy : IAzureProxy
    {
        private readonly IAzureProxy azureProxy;
        private readonly IAppCache cache;
        private readonly ILogger<CachingAzureProxy> logger;

        /// <summary>
        /// Contructor to create a cache of <see cref="IAzureProxy"/>
        /// </summary>
        /// <param name="azureProxy"><see cref="AzureProxy"/></param>
        /// <param name="cache">Lazy cache using <see cref="IAppCache"/></param>
        /// <param name="logger"><see cref="ILogger"/> instance</param>
        public CachingAzureProxy(IAzureProxy azureProxy, IAppCache cache, ILogger<CachingAzureProxy> logger)
        {
            this.azureProxy = azureProxy;
            this.cache = cache;
            this.logger = logger;
        }

        public Task CreateBatchJobAsync(string jobId, CloudTask cloudTask, PoolInformation poolInformation) => azureProxy.CreateBatchJobAsync(jobId, cloudTask, poolInformation);
        public Task DeleteBatchJobAsync(string taskId) => azureProxy.DeleteBatchJobAsync(taskId);
        public Task DeleteBatchPoolAsync(string poolId, CancellationToken cancellationToken) => azureProxy.DeleteBatchPoolAsync(poolId, cancellationToken);
        public Task<string> DownloadBlobAsync(Uri blobAbsoluteUri) => azureProxy.DownloadBlobAsync(blobAbsoluteUri);
        public Task<IEnumerable<string>> GetActivePoolIdsAsync(string prefix, TimeSpan minAge, CancellationToken cancellationToken) => azureProxy.GetActivePoolIdsAsync(prefix, minAge, cancellationToken);

        ///<inheritdoc/>
        public Task<AzureBatchAccountQuotas> GetBatchAccountQuotasAsync()
        {
            return cache.GetOrAddAsync("batchAccountQuotas", () => azureProxy.GetBatchAccountQuotasAsync(), DateTimeOffset.Now.AddHours(1));
        }

        public int GetBatchActiveJobCount() => azureProxy.GetBatchActiveJobCount();
        public IEnumerable<AzureBatchNodeCount> GetBatchActiveNodeCountByVmSize() => azureProxy.GetBatchActiveNodeCountByVmSize();
        public int GetBatchActivePoolCount() => azureProxy.GetBatchActivePoolCount();
        public Task<AzureBatchJobAndTaskState> GetBatchJobAndTaskStateAsync(string tesTaskId) => azureProxy.GetBatchJobAndTaskStateAsync(tesTaskId);

        ///<inheritdoc/>
        public async Task<ContainerRegistryInfo> GetContainerRegistryInfoAsync(string imageName)
        {
            var containerRegistryInfo = cache.Get<ContainerRegistryInfo>(imageName);

            if (containerRegistryInfo == null)
            {
                containerRegistryInfo = await azureProxy.GetContainerRegistryInfoAsync(imageName);

                if (containerRegistryInfo != null)
                {
                    cache.Add(imageName, containerRegistryInfo, DateTimeOffset.Now.AddHours(1));
                }
            }

            return containerRegistryInfo;
        }

        public Task<string> GetNextBatchJobIdAsync(string tesTaskId) => azureProxy.GetNextBatchJobIdAsync(tesTaskId);
        public Task<IEnumerable<string>> GetPoolIdsReferencedByJobsAsync(CancellationToken cancellationToken) => azureProxy.GetPoolIdsReferencedByJobsAsync(cancellationToken);

        ///<inheritdoc/>
        public Task<string> GetStorageAccountKeyAsync(StorageAccountInfo storageAccountInfo)
        {
            return cache.GetOrAddAsync(storageAccountInfo.Id, () => azureProxy.GetStorageAccountKeyAsync(storageAccountInfo), DateTimeOffset.Now.AddHours(1));
        }

        ///<inheritdoc/>
        public async Task<StorageAccountInfo> GetStorageAccountInfoAsync(string storageAccountName)
        {
            var storageAccountInfo = cache.Get<StorageAccountInfo>(storageAccountName);

            if (storageAccountInfo == null)
            {
                storageAccountInfo = await azureProxy.GetStorageAccountInfoAsync(storageAccountName);

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

        public Task<IEnumerable<string>> ListBlobsAsync(Uri directoryUri) => azureProxy.ListBlobsAsync(directoryUri);
        public Task<IEnumerable<string>> ListOldJobsToDeleteAsync(TimeSpan oldestJobAge) => azureProxy.ListOldJobsToDeleteAsync(oldestJobAge);
        public Task UploadBlobAsync(Uri blobAbsoluteUri, string content) => azureProxy.UploadBlobAsync(blobAbsoluteUri, content);
        public Task<(Uri, string)> GetCosmosDbEndpointAndKeyAsync(string cosmosDbAccountName) => azureProxy.GetCosmosDbEndpointAndKeyAsync(cosmosDbAccountName);
        public Task UploadBlobFromFileAsync(Uri blobAbsoluteUri, string filePath) => azureProxy.UploadBlobFromFileAsync(blobAbsoluteUri, filePath);
        public bool LocalFileExists(string path) => azureProxy.LocalFileExists(path);
        public bool TryReadCwlFile(string workflowId, out string content) => azureProxy.TryReadCwlFile(workflowId, out content);
    }
}
