﻿// Copyright (c) Microsoft Corporation.
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
using BatchModels = Microsoft.Azure.Management.Batch.Models;

namespace TesApi.Web
{
    /// <summary>
    /// Implements caching and retries for <see cref="IAzureProxy"/>.
    /// </summary>
    public class CachingWithRetriesAzureProxy : IAzureProxy
    {
        private readonly IAzureProxy azureProxy;
        private readonly IAppCache cache;

        private static readonly int RetryCount = 3;
        private static TimeSpan SleepDurationProvider(int attempt)
            => TimeSpan.FromSeconds(Math.Pow(2, attempt));

        private readonly AsyncRetryPolicy asyncRetryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(RetryCount, SleepDurationProvider);

        private readonly RetryPolicy retryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetry(RetryCount, SleepDurationProvider);

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
        public Task CreateBatchJobAsync(string jobId, CloudTask cloudTask, PoolInformation poolInformation, JobPreparationTask jobPreparationTask, JobReleaseTask jobReleaseTask) => azureProxy.CreateBatchJobAsync(jobId, cloudTask, poolInformation, jobPreparationTask, jobReleaseTask);

        /// <inheritdoc/>
        public Task DeleteBatchJobAsync(string taskId, CancellationToken cancellationToken = default) => asyncRetryPolicy.ExecuteAsync(ct => azureProxy.DeleteBatchJobAsync(taskId, ct), cancellationToken);

        /// <inheritdoc/>
        public Task DeleteBatchPoolAsync(string poolId, CancellationToken cancellationToken = default) => asyncRetryPolicy.ExecuteAsync(ct => azureProxy.DeleteBatchPoolAsync(poolId, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<CloudPool> GetBatchPoolAsync(string poolId, DetailLevel detailLevel = default, CancellationToken cancellationToken = default) => asyncRetryPolicy.ExecuteAsync(ct => azureProxy.GetBatchPoolAsync(poolId, detailLevel, ct), cancellationToken);

        /// <inheritdoc/>
        public Task CommitBatchPoolChangesAsync(CloudPool pool, CancellationToken cancellationToken = default) => asyncRetryPolicy.ExecuteAsync(ct => azureProxy.CommitBatchPoolChangesAsync(pool, ct), cancellationToken);

        /// <inheritdoc/>
        public Task DeleteBatchComputeNodesAsync(string poolId, IEnumerable<ComputeNode> computeNodes, CancellationToken cancellationToken = default)
        {
            cache.Remove(poolId);
            return asyncRetryPolicy.ExecuteAsync(ct => azureProxy.DeleteBatchComputeNodesAsync(poolId, computeNodes, ct), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<string> DownloadBlobAsync(Uri blobAbsoluteUri) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.DownloadBlobAsync(blobAbsoluteUri));

        /// <inheritdoc/>
        public Task<IEnumerable<string>> GetActivePoolIdsAsync(string prefix, TimeSpan minAge, CancellationToken cancellationToken) => asyncRetryPolicy.ExecuteAsync(ct => azureProxy.GetActivePoolIdsAsync(prefix, minAge, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<IEnumerable<CloudPool>> GetActivePoolsAsync(string hostName, CancellationToken cancellationToken = default) => asyncRetryPolicy.ExecuteAsync(ct => azureProxy.GetActivePoolsAsync(hostName, ct), cancellationToken);

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
        public Task<List<VirtualMachineInformation>> GetVmSizesAndPricesAsync() => cache.GetOrAddAsync("vmSizesAndPrices", () => azureProxy.GetVmSizesAndPricesAsync(), DateTimeOffset.MaxValue);

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
        public Task<PoolInformation> CreateBatchPoolAsync(BatchModels.Pool poolInfo, bool isPreemptable) => azureProxy.CreateBatchPoolAsync(poolInfo, isPreemptable);

        /// <inheritdoc/>
        public Task DeleteBatchPoolIfExistsAsync(string poolId, CancellationToken cancellationToken = default)
            => azureProxy.DeleteBatchPoolIfExistsAsync(poolId, cancellationToken);

        /// <inheritdoc/>
        public Task<(int? lowPriorityNodes, int? dedicatedNodes)> GetCurrentComputeNodesAsync(string poolId, CancellationToken cancellationToken = default) => asyncRetryPolicy.ExecuteAsync(ct => azureProxy.GetCurrentComputeNodesAsync(poolId, ct), cancellationToken);

        /// <inheritdoc/>
        public async Task<(Microsoft.Azure.Batch.Common.AllocationState? AllocationState, int? TargetLowPriority, int? TargetDedicated)> GetComputeNodeAllocationStateAsync(string poolId, CancellationToken cancellationToken = default)
        {
            var allocationState = cache.Get<(Microsoft.Azure.Batch.Common.AllocationState? AllocationState, int? TargetLowPriority, int? TargetDedicated)>(poolId);

            if (default == allocationState)
            {
                allocationState = await asyncRetryPolicy.ExecuteAsync(ct => azureProxy.GetComputeNodeAllocationStateAsync(poolId, ct), cancellationToken);

                if (default != allocationState)
                {
                    cache.Add(poolId, allocationState, DateTimeOffset.Now.Add(BatchPoolService.RunInterval).Subtract(TimeSpan.FromSeconds(1)));
                }
            }

            return allocationState;
        }

        /// <inheritdoc/>
        public Task SetComputeNodeTargetsAsync(string poolId, int? targetLowPriorityComputeNodes, int? targetDedicatedComputeNodes, CancellationToken cancellationToken = default)
        {
            cache.Remove(poolId);
            return asyncRetryPolicy.ExecuteAsync(ct => azureProxy.SetComputeNodeTargetsAsync(poolId, targetLowPriorityComputeNodes, targetDedicatedComputeNodes, ct), cancellationToken);
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<ComputeNode> ListComputeNodesAsync(string poolId, DetailLevel detailLevel = null) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.ListComputeNodesAsync(poolId, detailLevel), retryPolicy);

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudJob> ListJobsAsync(DetailLevel detailLevel = null) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.ListJobsAsync(detailLevel), retryPolicy);
    }
}
