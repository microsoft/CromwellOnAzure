// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Extensions.Caching.Memory;
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
        private readonly ILogger<CachingAzureProxy> logger;

        private MemoryCache cache { get; set; } = new MemoryCache(new MemoryCacheOptions());

        /// <summary>
        /// Contructor to create a cache of <see cref="IAzureProxy"/>
        /// </summary>
        /// <param name="azureProxy"></param>
        /// <param name="logger"></param>
        public CachingAzureProxy(IAzureProxy azureProxy, ILogger<CachingAzureProxy> logger)
        {
            this.azureProxy = azureProxy;
            this.logger = logger;
            GetVmSizesAndPricesAsync().Wait();
        }

        public Task CreateBatchJobAsync(string jobId, CloudTask cloudTask, PoolInformation poolInformation) => azureProxy.CreateBatchJobAsync(jobId, cloudTask, poolInformation);
        public Task DeleteBatchJobAsync(string taskId) => azureProxy.DeleteBatchJobAsync(taskId);
        public Task DeleteBatchPoolAsync(string poolId, CancellationToken cancellationToken) => azureProxy.DeleteBatchPoolAsync(poolId, cancellationToken);
        public Task<string> DownloadBlobAsync(Uri blobAbsoluteUri) => azureProxy.DownloadBlobAsync(blobAbsoluteUri);
        public Task<IEnumerable<string>> GetActivePoolIdsAsync(string prefix, TimeSpan minAge, CancellationToken cancellationToken) => azureProxy.GetActivePoolIdsAsync(prefix, minAge, cancellationToken);

        ///<inheritdoc/>
        public async Task<AzureBatchAccountQuotas> GetBatchAccountQuotasAsync()
        {
            const string key = "batchAccountQuotas";

            if (!cache.TryGetValue(key, out AzureBatchAccountQuotas azureBatchAccountQuotas))
            {
                // retry
                var batchAccountQuotas = await azureProxy.GetBatchAccountQuotasAsync();
                return cache.Set(key, batchAccountQuotas, TimeSpan.FromHours(1));
            }

            return azureBatchAccountQuotas;
        }

        public int GetBatchActiveJobCount() => azureProxy.GetBatchActiveJobCount();
        public IEnumerable<AzureProxy.AzureBatchNodeCount> GetBatchActiveNodeCountByVmSize() => azureProxy.GetBatchActiveNodeCountByVmSize();
        public int GetBatchActivePoolCount() => azureProxy.GetBatchActivePoolCount();
        public Task<AzureProxy.AzureBatchJobAndTaskState> GetBatchJobAndTaskStateAsync(string tesTaskId) => azureProxy.GetBatchJobAndTaskStateAsync(tesTaskId);

        ///<inheritdoc/>
        public async Task<ContainerRegistryInfo> GetContainerRegistryInfoAsync(string imageName)
        {
            if (!cache.TryGetValue(imageName, out ContainerRegistryInfo containerRegistry))
            {
                // retry
                containerRegistry = await azureProxy.GetContainerRegistryInfoAsync(imageName);

                if (containerRegistry != null)
                {
                    cache.Set(imageName, containerRegistry, TimeSpan.FromHours(1));
                }
            }

            return containerRegistry;
        }

        public Task<string> GetNextBatchJobIdAsync(string tesTaskId) => azureProxy.GetNextBatchJobIdAsync(tesTaskId);
        public Task<IEnumerable<string>> GetPoolIdsReferencedByJobsAsync(CancellationToken cancellationToken) => azureProxy.GetPoolIdsReferencedByJobsAsync(cancellationToken);

        ///<inheritdoc/>
        public async Task<string> GetStorageAccountKeyAsync(StorageAccountInfo storageAccountInfo)
        {
            if (!cache.TryGetValue(storageAccountInfo.Id, out string accountKey))
            {
                // retry
                accountKey = await azureProxy.GetStorageAccountKeyAsync(storageAccountInfo);
                cache.Set(storageAccountInfo.Id, accountKey, TimeSpan.FromHours(1));
            }

            return accountKey;
        }

        ///<inheritdoc/>
        public async Task<StorageAccountInfo> GetStorageAccountInfoAsync(string storageAccountName)
        {
            if (!cache.TryGetValue(storageAccountName, out StorageAccountInfo storageAccountInfo))
            {
                // retry
                storageAccountInfo = await azureProxy.GetStorageAccountInfoAsync(storageAccountName);

                if (storageAccountInfo != null)
                {
                    cache.Set(storageAccountName, storageAccountInfo, DateTimeOffset.MaxValue);
                }
            }

            return storageAccountInfo;
        }

        ///<inheritdoc/>
        public async Task<List<VirtualMachineInfo>> GetVmSizesAndPricesAsync()
        {
            const string key = "vmSizesAndPrices";

            if (!cache.TryGetValue(key, out List<VirtualMachineInfo> cachedVmSizesAndPrices))
            {
                // retry
                var vmSizesAndPrices = await azureProxy.GetVmSizesAndPricesAsync();
                cache.Set(key, vmSizesAndPrices.ToList(), TimeSpan.FromDays(1));
            }

            return cachedVmSizesAndPrices;
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
