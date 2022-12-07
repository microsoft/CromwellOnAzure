// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Tes.Models;

namespace TesApi.Web
{
    /// <summary>
    /// Interface for the Azure API wrapper
    /// </summary>
    public interface IAzureProxy
    {
        /// <summary>
        /// Gets CosmosDB endpoint and key
        /// </summary>
        /// <param name="cosmosDbAccountName"></param>
        /// <returns>The CosmosDB endpoint and key of the specified account</returns>
        Task<(string, string)> GetCosmosDbEndpointAndKeyAsync(string cosmosDbAccountName);

        /// <summary>
        /// Gets a new Azure Batch job id to schedule another task
        /// </summary>
        /// <param name="tesTaskId">The unique TES task ID</param>
        /// <returns>The next logical, new Azure Batch job ID</returns>
        Task<string> GetNextBatchJobIdAsync(string tesTaskId);

        /// <summary>
        /// Creates a new Azure Batch job
        /// </summary>
        /// <param name="jobId"></param>
        /// <param name="cloudTask"></param>
        /// <param name="poolInformation"></param>
        Task CreateBatchJobAsync(string jobId, CloudTask cloudTask, PoolInformation poolInformation);

        /// <summary>
        /// Gets the <see cref="ContainerRegistryInfo"/> for the given image name
        /// </summary>
        /// <param name="imageName">Image name</param>
        /// <returns><see cref="ContainerRegistryInfo"/></returns>
        Task<ContainerRegistryInfo> GetContainerRegistryInfoAsync(string imageName);

        /// <summary>
        /// Gets the <see cref="StorageAccountInfo"/> for the given storage account name
        /// </summary>
        /// <param name="storageAccountName">Storage account name</param>
        /// <returns><see cref="StorageAccountInfo"/></returns>
        Task<StorageAccountInfo> GetStorageAccountInfoAsync(string storageAccountName);

        /// <summary>
        /// Creates a Pool in Azure Batch that is NOT an AutoPool
        /// </summary>
        Task<ManualBatchPoolCreationResult> CreateManualBatchPoolAsync(
            string poolName,
            string vmSize,
            bool isLowPriority,
            string executorImage,
            BatchNodeInfo nodeInfo,
            string dockerInDockerImageName,
            string blobxferImageName,
            IEnumerable<string> identityResourceId,
            bool disableBatchNodesPublicIpAddress,
            string batchNodesSubnetId,
            string startTaskSasUrl,
            string startTaskPath);


        /// <summary>
        /// Get the current states of the Azure Batch job and task corresponding to the given TES task
        /// </summary>
        /// <param name="tesTaskId">The unique ID of the TES task</param>
        /// <returns>A higher-level abstraction of the current state of the Azure Batch task</returns>
        Task<AzureBatchJobAndTaskState> GetBatchJobAndTaskStateAsync(string tesTaskId);

        /// <summary>
        /// Deletes an Azure Batch job
        /// </summary>
        /// <param name="taskId">The unique TES task ID</param>
        /// <param name="cancellationToken"></param>
        Task DeleteBatchJobAsync(string taskId, CancellationToken cancellationToken = default);

        /// <summary>
        /// Get Batch account quota
        /// </summary>
        /// <returns><see cref="AzureBatchAccountQuotas"/></returns>
        Task<AzureBatchAccountQuotas> GetBatchAccountQuotasAsync();

        /// <summary>
        /// Gets the counts of active batch nodes, grouped by VmSize
        /// </summary>
        /// <returns>Batch node counts</returns>
        IEnumerable<AzureBatchNodeCount> GetBatchActiveNodeCountByVmSize();

        /// <summary>
        /// Gets the count of active batch pools
        /// </summary>
        /// <returns>Count of active batch pools</returns>
        int GetBatchActivePoolCount();

        /// <summary>
        /// Gets the count of active batch jobs
        /// </summary>
        /// <returns>Count of active batch jobs</returns>
        int GetBatchActiveJobCount();

        /// <summary>
        /// Gets the price and resource summary of all available VMs in a region
        /// </summary>
        /// <returns><see cref="Tes.Models.VirtualMachineInformation"/> for available VMs in a region.</returns>
        Task<List<VirtualMachineInformation>> GetVmSizesAndPricesAsync();

        /// <summary>
        /// Gets the primary key of the given storage account.
        /// </summary>
        /// <param name="storageAccountInfo">Storage account info</param>
        /// <returns>The primary key</returns>
        Task<string> GetStorageAccountKeyAsync(StorageAccountInfo storageAccountInfo);

        /// <summary>
        /// Uploads the text content to a blob
        /// </summary>
        /// <param name="blobAbsoluteUri">Absolute Blob URI</param>
        /// <param name="content">Blob content</param>
        /// <returns>A task to await</returns>
        Task UploadBlobAsync(Uri blobAbsoluteUri, string content);

        /// <summary>
        /// Uploads the file content to a blob
        /// </summary>
        /// <param name="blobAbsoluteUri">Absolute Blob URI</param>
        /// <param name="filePath">File path</param>
        /// <returns>A task to await</returns>
        Task UploadBlobFromFileAsync(Uri blobAbsoluteUri, string filePath);

        /// <summary>
        /// Downloads a blob
        /// </summary>
        /// <param name="blobAbsoluteUri">Absolute Blob URI</param>
        /// <returns>Blob content</returns>
        Task<string> DownloadBlobAsync(Uri blobAbsoluteUri);

        /// <summary>
        /// Check if a blob exists.
        /// </summary>
        /// <param name="blobAbsoluteUri">Absolute Blob URI</param>
        /// <returns>Blob exists boolean.</returns>
        Task<bool> BlobExistsAsync(Uri blobAbsoluteUri);

        /// <summary>
        /// Gets the list of blobs in the given directory
        /// </summary>
        /// <param name="directoryUri">Directory Uri</param>
        /// <returns>List of blob paths</returns>
        Task<IEnumerable<string>> ListBlobsAsync(Uri directoryUri);

        /// <summary>
        /// Gets the ids of completed Batch jobs older than specified timespan
        /// </summary>
        /// <returns>List of Batch job ids</returns>
        Task<IEnumerable<string>> ListOldJobsToDeleteAsync(TimeSpan oldestJobAge);

        /// <summary>
        /// Gets the ids of orphaned Batch jobs older than specified timespan
        /// </summary>
        /// <returns>List of Batch job ids</returns>
        Task<IEnumerable<string>> ListOrphanedJobsToDeleteAsync(TimeSpan minJobAge, CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets the list of active pool ids matching the prefix and with creation time older than the minAge
        /// </summary>
        /// <returns>Active pool ids</returns>
        Task<IEnumerable<string>> GetActivePoolIdsAsync(string prefix, TimeSpan minAge, CancellationToken cancellationToken);

        /// <summary>
        /// Gets the list of pool ids referenced by the jobs
        /// </summary>
        /// <returns>Pool ids</returns>
        Task<IEnumerable<string>> GetPoolIdsReferencedByJobsAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Deletes the specified pool
        /// </summary>
        Task DeleteBatchPoolAsync(string poolId, CancellationToken cancellationToken = default);

        /// <summary>
        /// Deletes the specified pool if it exists
        /// </summary>
        Task DeleteBatchPoolIfExistsAsync(string poolId, CancellationToken cancellationToken = default);

        /// <summary>
        /// Checks if a local file exists
        /// </summary>
        bool LocalFileExists(string path);

        /// <summary>
        /// Reads the content of the Common Workflow Language (CWL) file associated with the parent workflow of the TES task
        /// </summary>
        /// <param name="workflowId">Parent workflow</param>
        /// <param name="content">Content of the file</param>
        /// <returns>True if file was found</returns>
        bool TryReadCwlFile(string workflowId, out string content);
    }
}
