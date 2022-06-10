// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using Microsoft.Azure.Management.ApplicationInsights.Management;
using Microsoft.Azure.Management.Batch;
using Microsoft.Azure.Management.Batch.Models;
using Microsoft.Azure.Management.ContainerRegistry.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Extensions.Logging;
using Microsoft.Rest;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Polly;
using Polly.Retry;
using Tes.Models;
using FluentAzure = Microsoft.Azure.Management.Fluent.Azure;

namespace TesApi.Web
{
    /// <summary>
    /// Wrapper for Azure APIs
    /// </summary>
    public class AzureProxy : IAzureProxy
    {
        private const char BatchJobAttemptSeparator = '-';
        private const string DefaultAzureBillingRegionName = "US West";

        private static readonly HttpClient httpClient = new HttpClient();

        private readonly ILogger logger;
        private readonly Func<Task<BatchAccount>> getBatchAccountFunc;
        private readonly BatchClient batchClient;
        private readonly string subscriptionId;
        private readonly string location;
        private readonly string billingRegionName;
        private readonly string azureOfferDurableId;
        private readonly string batchResourceGroupName;
        private readonly string batchAccountName;
        private readonly AsyncRetryPolicy batchRetryPolicy;

        /// <summary>
        /// The constructor
        /// </summary>
        /// <param name="batchAccountName">Batch account name</param>
        /// <param name="azureOfferDurableId">Azure offer id</param>
        /// <param name="logger">The logger</param>
        public AzureProxy(string batchAccountName, string azureOfferDurableId, ILogger logger)
        {
            this.logger = logger;
            this.batchAccountName = batchAccountName;
            var findBatchAccountResult = FindBatchAccountAsync(batchAccountName).Result;
            batchResourceGroupName = findBatchAccountResult.ResourceGroupName;
            subscriptionId = findBatchAccountResult.SubscriptionId;
            location = findBatchAccountResult.Location;
            batchClient = BatchClient.Open(new BatchTokenCredentials($"https://{findBatchAccountResult.BatchAccountEndpoint}", () => GetAzureAccessTokenAsync("https://batch.core.windows.net/")));

            getBatchAccountFunc = async () => 
                await new BatchManagementClient(new TokenCredentials(await GetAzureAccessTokenAsync())) { SubscriptionId = findBatchAccountResult.SubscriptionId }
                    .BatchAccount
                    .GetAsync(findBatchAccountResult.ResourceGroupName, batchAccountName);

            this.azureOfferDurableId = azureOfferDurableId;

            if (! AzureRegionUtils.TryGetBillingRegionName(location, out billingRegionName))
            {
                logger.LogWarning($"Azure ARM location '{location}' does not have a corresponding Azure Billing Region.  Prices from the fallback billing region '{DefaultAzureBillingRegionName}' will be used instead.");
                billingRegionName = DefaultAzureBillingRegionName;
            }

            batchRetryPolicy = Polly.Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(new[]
                {
                    TimeSpan.FromSeconds(1),
                    TimeSpan.FromSeconds(1),
                    TimeSpan.FromSeconds(2),
                    TimeSpan.FromSeconds(2),
                    TimeSpan.FromSeconds(4),
                }, (exception, timeSpan, retryCount, context) => {
                    logger.LogWarning(exception, $"Azure Batch Job exception during {context.OperationKey} for Job ID: {context["jobId"]} (potentially transient due to Batch cache race condition), waiting {timeSpan.TotalSeconds:n0}s...");
                });
        }

        // TODO: Static method because the instrumentation key is needed in both Program.cs and Startup.cs and we wanted to avoid intializing the batch client twice.
        // Can we skip initializing app insights with a instrumentation key in Program.cs? If yes, change this to an instance method.
        /// <summary>
        /// Gets the Application Insights instrumentation key
        /// </summary>
        /// <param name="appInsightsApplicationId">Application Insights application id</param>
        /// <returns>Application Insights instrumentation key</returns>
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

        /// <summary>
        /// Gets CosmosDB endpoint and key
        /// </summary>
        /// <param name="cosmosDbAccountName"></param>
        /// <returns>The CosmosDB endpoint and key of the specified account</returns>
        public async Task<(string, string)> GetCosmosDbEndpointAndKeyAsync(string cosmosDbAccountName)
        {
            var azureClient = await GetAzureManagementClientAsync();
            var subscriptionIds = (await azureClient.Subscriptions.ListAsync()).Select(s => s.SubscriptionId);

            var account = (await Task.WhenAll(subscriptionIds.Select(async subId => await azureClient.WithSubscription(subId).CosmosDBAccounts.ListAsync())))
                .SelectMany(a => a)
                .FirstOrDefault(a => a.Name.Equals(cosmosDbAccountName, StringComparison.OrdinalIgnoreCase));

            if (account == null)
            {
                throw new Exception($"CosmosDB account '{cosmosDbAccountName} does not exist or the TES app service does not have Account Reader role on the account.");
            }

            var key = (await azureClient.WithSubscription(account.Manager.SubscriptionId).CosmosDBAccounts.ListKeysAsync(account.ResourceGroupName, account.Name)).PrimaryMasterKey;

            return (account.DocumentEndpoint, key);
        }

        /// <summary>
        /// Gets a new Azure Batch job id to schedule another task
        /// </summary>
        /// <param name="tesTaskId">The unique TES task ID</param>
        /// <returns>The next logical, new Azure Batch job ID</returns>
        public async Task<string> GetNextBatchJobIdAsync(string tesTaskId)
        {
            var jobFilter = new ODATADetailLevel
            {
                FilterClause = $"startswith(id,'{tesTaskId}{BatchJobAttemptSeparator}')",
                SelectClause = "id"
            };

            var lastAttemptNumber = (await batchClient.JobOperations.ListJobs(jobFilter).ToListAsync())
                .Select(j => int.Parse(j.Id.Split(BatchJobAttemptSeparator)[1]))
                .OrderBy(a => a)
                .LastOrDefault();

            return $"{tesTaskId}{BatchJobAttemptSeparator}{lastAttemptNumber + 1}";
        }


        /// <summary>
        /// Gets the counts of active batch nodes, grouped by VmSize
        /// </summary>
        /// <returns>Batch node counts</returns>
        public IEnumerable<AzureBatchNodeCount> GetBatchActiveNodeCountByVmSize()
        {
            return batchClient.PoolOperations.ListPools()
                .Select(p => new
                {
                    p.VirtualMachineSize,
                    DedicatedNodeCount = Math.Max(p.TargetDedicatedComputeNodes ?? 0, p.CurrentDedicatedComputeNodes ?? 0),
                    LowPriorityNodeCount = Math.Max(p.TargetLowPriorityComputeNodes ?? 0, p.CurrentLowPriorityComputeNodes ?? 0)
                })
                .GroupBy(x => x.VirtualMachineSize)
                .Select(grp => new AzureBatchNodeCount { VirtualMachineSize = grp.Key, DedicatedNodeCount = grp.Sum(x => x.DedicatedNodeCount), LowPriorityNodeCount = grp.Sum(x => x.LowPriorityNodeCount) });
        }

        /// <summary>
        /// Gets the count of active batch pools
        /// </summary>
        /// <returns>Count of active batch pools</returns>
        public int GetBatchActivePoolCount()
        {
            var activePoolsFilter = new ODATADetailLevel
            {
                FilterClause = $"state eq 'active' or state eq 'deleting'",
                SelectClause = "id"
            };

            return batchClient.PoolOperations.ListPools(activePoolsFilter).Count();
        }

        /// <summary>
        /// Gets the count of active batch jobs
        /// </summary>
        /// <returns>Count of active batch jobs</returns>
        public int GetBatchActiveJobCount()
        {
            var activeJobsFilter = new ODATADetailLevel
            {
                FilterClause = $"state eq 'active' or state eq 'disabling' or state eq 'terminating' or state eq 'deleting'",
                SelectClause = "id"
            };

            return batchClient.JobOperations.ListJobs(activeJobsFilter).Count();
        }

        /// <summary>
        /// Gets the batch quotas
        /// </summary>
        /// <returns>Batch quotas</returns>
        public async Task<AzureBatchAccountQuotas> GetBatchAccountQuotasAsync()
        {
            try
            {
                var batchAccount = await getBatchAccountFunc();

                return new AzureBatchAccountQuotas
                {
                    ActiveJobAndJobScheduleQuota = batchAccount.ActiveJobAndJobScheduleQuota,
                    DedicatedCoreQuota = batchAccount.DedicatedCoreQuota.Value,
                    DedicatedCoreQuotaPerVMFamily = batchAccount.DedicatedCoreQuotaPerVMFamily,
                    DedicatedCoreQuotaPerVMFamilyEnforced = batchAccount.DedicatedCoreQuotaPerVMFamilyEnforced,
                    LowPriorityCoreQuota = batchAccount.LowPriorityCoreQuota.Value,
                    PoolQuota = batchAccount.PoolQuota,

                };
            }
            catch (Exception ex)
            {
                logger.LogError(ex, $"An exception occurred when getting the batch account.");
                throw;
            }
        }

        /// <summary>
        /// Creates a new Azure Batch job
        /// </summary>
        /// <param name="jobId"></param>
        /// <param name="cloudTask"></param>
        /// <param name="poolInformation"></param>
        /// <returns></returns>
        public async Task CreateBatchJobAsync(string jobId, CloudTask cloudTask, PoolInformation poolInformation)
        {
            var contextDictionary = new Dictionary<string, object>();
            contextDictionary.Add("jobId", jobId);

            var job = batchClient.JobOperations.CreateJob(jobId, poolInformation);
            await batchRetryPolicy.ExecuteAsync(async (context) => await job.CommitAsync(), new Context("CreateBatchJobAsync-CommitAsync-1", contextDictionary));

            try
            {
                await batchRetryPolicy.ExecuteAsync(async (context) => job = await batchClient.JobOperations.GetJobAsync(job.Id), new Context("CreateBatchJobAsync-GetJobAsync", contextDictionary));
                job.PoolInformation = poolInformation;  // Redoing this since the container registry password is not retrieved by GetJobAsync()
                job.OnAllTasksComplete = OnAllTasksComplete.TerminateJob;
                await batchRetryPolicy.ExecuteAsync(async (context) => await job.AddTaskAsync(cloudTask), new Context("CreateBatchJobAsync-AddTaskAsync", contextDictionary));
                await batchRetryPolicy.ExecuteAsync(async (context) => await job.CommitAsync(), new Context("CreateBatchJobAsync-CommitAsync-2", contextDictionary));
            }
            catch (Exception ex)
            {
                var batchError = JsonConvert.SerializeObject((ex as BatchException)?.RequestInformation?.BatchError);
                logger.LogError(ex, $"Deleting {job.Id} because adding task to it failed. Batch error: {batchError}");

                await batchRetryPolicy.ExecuteAsync(async (context) => await batchClient.JobOperations.DeleteJobAsync(job.Id), new Context("CreateBatchJobAsync-DeleteJobAsync", contextDictionary));

                if (!string.IsNullOrWhiteSpace(poolInformation?.PoolId))
                {
                    // With manual pools, the PoolId property is set
                    await DeleteBatchPoolIfExistsAsync(poolInformation.PoolId);
                }

                throw;
            }
        }

        /// <summary>
        /// Gets the combined state of Azure Batch job, task and pool that corresponds to the given TES task
        /// </summary>
        /// <param name="tesTaskId">The unique TES task ID</param>
        /// <returns>Job state information</returns>
        public async Task<AzureBatchJobAndTaskState> GetBatchJobAndTaskStateAsync(string tesTaskId)
        {
            try
            {
                var nodeAllocationFailed = false;
                string nodeErrorCode = null;
                IEnumerable<string> nodeErrorDetails = null;
                var activeJobWithMissingAutoPool = false;
                ComputeNodeState? nodeState = null;
                TaskState? taskState = null;
                TaskExecutionInformation taskExecutionInformation = null;

                var jobFilter = new ODATADetailLevel
                {
                    FilterClause = $"startswith(id,'{tesTaskId}{BatchJobAttemptSeparator}')",
                    SelectClause = "*"
                };

                var jobInfos = (await batchClient.JobOperations.ListJobs(jobFilter).ToListAsync())
                    .Select(j => new { Job = j, AttemptNumber = int.Parse(j.Id.Split(BatchJobAttemptSeparator)[1]) });

                if (!jobInfos.Any())
                {
                    return new AzureBatchJobAndTaskState { JobState = null };
                }

                if (jobInfos.Count(j => j.Job.State == JobState.Active) > 1)
                {
                    return new AzureBatchJobAndTaskState { MoreThanOneActiveJobFound = true };
                }

                var lastJobInfo = jobInfos.OrderBy(j => j.AttemptNumber).Last();

                var job = lastJobInfo.Job;
                var attemptNumber = lastJobInfo.AttemptNumber;

                if (job.State == JobState.Active && job.ExecutionInformation?.PoolId != null)
                {
                    var poolFilter = new ODATADetailLevel
                    {
                        FilterClause = $"id eq '{job.ExecutionInformation.PoolId}'",
                        SelectClause = "*"
                    };

                    var pool = (await batchClient.PoolOperations.ListPools(poolFilter).ToListAsync()).FirstOrDefault();

                    if (pool != null)
                    {
                        nodeAllocationFailed = pool.ResizeErrors?.Count > 0;

                        var node = (await pool.ListComputeNodes().ToListAsync()).FirstOrDefault();

                        if (node != null)
                        {
                            nodeState = node.State;
                            var nodeError = node.Errors?.FirstOrDefault();
                            nodeErrorCode = nodeError?.Code;
                            nodeErrorDetails = nodeError?.ErrorDetails?.Select(e => e.Value);
                        }
                    }
                    else
                    {
                        if (job.CreationTime.HasValue && DateTime.UtcNow.Subtract(job.CreationTime.Value) > TimeSpan.FromMinutes(30))
                        {
                            activeJobWithMissingAutoPool = true;
                        }
                    }
                }

                try
                {
                    var batchTask = await batchClient.JobOperations.GetTaskAsync(job.Id, tesTaskId);
                    taskState = batchTask.State;
                    taskExecutionInformation = batchTask.ExecutionInformation;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, $"Failed to get task for TesTask {tesTaskId}");
                }

                return new AzureBatchJobAndTaskState
                {
                    MoreThanOneActiveJobFound = false,
                    ActiveJobWithMissingAutoPool = activeJobWithMissingAutoPool,
                    AttemptNumber = attemptNumber,
                    NodeAllocationFailed = nodeAllocationFailed,
                    NodeErrorCode = nodeErrorCode,
                    NodeErrorDetails = nodeErrorDetails,
                    NodeState = nodeState,
                    JobState = job.State,
                    JobStartTime = job.ExecutionInformation?.StartTime,
                    JobEndTime = job.ExecutionInformation?.EndTime,
                    JobSchedulingError = job.ExecutionInformation?.SchedulingError,
                    TaskState = taskState,
                    TaskExecutionResult = taskExecutionInformation?.Result,
                    TaskStartTime = taskExecutionInformation?.StartTime,
                    TaskEndTime = taskExecutionInformation?.EndTime,
                    TaskExitCode = taskExecutionInformation?.ExitCode,
                    TaskFailureInformation = taskExecutionInformation?.FailureInformation,
                    TaskContainerState = taskExecutionInformation?.ContainerInformation?.State,
                    TaskContainerError = taskExecutionInformation?.ContainerInformation?.Error
                };
            }
            catch (Exception ex)
            {
                logger.LogError(ex, $"GetBatchJobAndTaskStateAsync failed for TesTask {tesTaskId}");
                throw;
            }
        }

        /// <summary>
        /// Deletes an Azure Batch job
        /// </summary>
        /// <param name="tesTaskId">The unique TES task ID</param>
        public async Task DeleteBatchJobAsync(string tesTaskId)
        {
            var jobFilter = new ODATADetailLevel
            {
                FilterClause = $"startswith(id,'{tesTaskId}{BatchJobAttemptSeparator}') and state ne 'deleting'",
                SelectClause = "id"
            };

            var batchJobsToDelete = await batchClient.JobOperations.ListJobs(jobFilter).ToListAsync();

            if (batchJobsToDelete.Count > 1)
            {
                logger.LogWarning($"Found more than one active job for TES task {tesTaskId}");
            }

            foreach (var job in batchJobsToDelete)
            {
                logger.LogInformation($"Deleting job {job.Id}");
                await batchClient.JobOperations.DeleteJobAsync(job.Id);
            }
        }

        /// <summary>
        /// Gets the ids of completed Batch jobs older than specified timespan
        /// </summary>
        /// <returns>List of Batch job ids</returns>
        public async Task<IEnumerable<string>> ListOldJobsToDeleteAsync(TimeSpan oldestJobAge)
        {
            var filter = new ODATADetailLevel
            {
                FilterClause = $"state eq 'completed' and executionInfo/endTime lt DateTime'{DateTime.Today.Subtract(oldestJobAge):yyyy-MM-ddTHH:mm:ssZ}'",
                SelectClause = "id"
            };

            return (await batchClient.JobOperations.ListJobs(filter).ToListAsync()).Select(c => c.Id);
        }

        /// <summary>
        /// Gets the ids of orphaned Batch jobs older than the specified timespan
        /// These jobs are active for prolonged period of time, have auto pool, NoAction termination option, and no tasks
        /// </summary>
        /// <returns>List of Batch job ids</returns>
        public async Task<IEnumerable<string>> ListOrphanedJobsToDeleteAsync(TimeSpan minJobAge)
        {
            var filter = new ODATADetailLevel
            {
                FilterClause = $"state eq 'active' and creationTime lt DateTime'{DateTime.UtcNow.Subtract(minJobAge):yyyy-MM-ddTHH:mm:ssZ}'",
                SelectClause = "id,poolInfo,onAllTasksComplete"
            };

            var noActionTesjobs = (await batchClient.JobOperations.ListJobs(filter).ToListAsync())
                .Where(j => j.PoolInformation?.AutoPoolSpecification?.AutoPoolIdPrefix == "TES" && j.OnAllTasksComplete == OnAllTasksComplete.NoAction);

            var noActionTesjobsWithNoTasks = await noActionTesjobs.ToAsyncEnumerable().WhereAwait(async j => !(await j.ListTasks().ToListAsync()).Any()).ToListAsync();

            return noActionTesjobsWithNoTasks.Select(j => j.Id);
        }

        /// <summary>
        /// Gets the list of active pool ids matching the prefix and with creation time older than the minAge
        /// </summary>
        /// <returns>Active pool ids</returns>
        public async Task<IEnumerable<string>> GetActivePoolIdsAsync(string prefix, TimeSpan minAge, CancellationToken cancellationToken = default)
        {
            var activePoolsFilter = new ODATADetailLevel
            {
                FilterClause = $"state eq 'active' and startswith(id, '{prefix}') and creationTime lt DateTime'{DateTime.UtcNow.Subtract(minAge):yyyy-MM-ddTHH:mm:ssZ}'",
                SelectClause = "id"
            };

            return (await batchClient.PoolOperations.ListPools(activePoolsFilter).ToListAsync(cancellationToken)).Select(p => p.Id);
        }

        /// <summary>
        /// Gets the list of pool ids referenced by the jobs
        /// </summary>
        /// <returns>Pool ids</returns>
        public async Task<IEnumerable<string>> GetPoolIdsReferencedByJobsAsync(CancellationToken cancellationToken = default)
        {
            return (await batchClient.JobOperations.ListJobs(new ODATADetailLevel(selectClause: "executionInfo")).ToListAsync(cancellationToken))
                .Where(j => !string.IsNullOrEmpty(j.ExecutionInformation?.PoolId))
                .Select(j => j.ExecutionInformation.PoolId);
        }

        /// <summary>
        /// Deletes the specified pool
        /// </summary>
        public Task DeleteBatchPoolAsync(string poolId, CancellationToken cancellationToken = default)
        {
            return batchClient.PoolOperations.DeletePoolAsync(poolId, cancellationToken: cancellationToken);
        }

        /// <summary>
        /// Deletes the specified pool
        /// </summary>
        public async Task DeleteBatchPoolIfExistsAsync(string poolId, CancellationToken cancellationToken = default)
        {
            try
            {
                var poolFilter = new ODATADetailLevel
                {
                    FilterClause = $"startswith(id,'{poolId}') and state ne 'deleting'",
                    SelectClause = "id"
                };

                var poolsToDelete = await batchClient.PoolOperations.ListPools(poolFilter).ToListAsync();

                foreach (var pool in poolsToDelete)
                {
                    logger.LogInformation($"Deleting pool {pool.Id}");
                    await batchClient.PoolOperations.DeletePoolAsync(pool.Id, cancellationToken: cancellationToken);
                }
            }
            catch (Exception exc)
            {
                logger.LogError(exc, $"Exception while attempting to delete pool starting with ID: {poolId}");
                throw;
            }
        }

        /// <summary>
        /// Gets the list of container registries that the TES server has access to
        /// </summary>
        /// <returns>List of container registries</returns>
        private async Task<IEnumerable<ContainerRegistryInfo>> GetAccessibleContainerRegistriesAsync()
        {
            var azureClient = await GetAzureManagementClientAsync();
            var subscriptionIds = (await azureClient.Subscriptions.ListAsync()).Select(s => s.SubscriptionId);
            var infos = new List<ContainerRegistryInfo>();

            foreach (var subId in subscriptionIds)
            {
                try
                {
                    var registries = (await azureClient.WithSubscription(subId).ContainerRegistries.ListAsync()).ToList();

                    foreach (var r in registries)
                    {
                        try
                        {
                            var server = await r.GetCredentialsAsync();
                            var info = new ContainerRegistryInfo { RegistryServer = r.LoginServerUrl, Username = server.Username, Password = server.AccessKeys[AccessKeyType.Primary] };
                            infos.Add(info);
                        }
                        catch (Exception ex)
                        {
                            logger.LogWarning($"TES service doesn't have permission to get credentials for registry {r.LoginServerUrl}.  Please verify that 'Admin user' is enabled in the 'Access Keys' area in the Azure Portal for this container registry.  Exception: {ex}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    logger.LogWarning($"TES service doesn't have permission to list container registries in subscription {subId}.  Exception: {ex}");
                }
            }

            return infos;
        }

        /// <summary>
        /// Gets the list of storage accounts that the TES server has access to
        /// </summary>
        /// <returns>List of storage accounts</returns>
        public async Task<IEnumerable<StorageAccountInfo>> GetAccessibleStorageAccountsAsync()
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

        /// <summary>
        /// Gets the primary key of the given storage account
        /// </summary>
        /// <param name="storageAccountInfo">Storage account info</param>
        /// <returns>The primary key</returns>
        public async Task<string> GetStorageAccountKeyAsync(StorageAccountInfo storageAccountInfo)
        {
            try
            {
                var azureClient = await GetAzureManagementClientAsync();
                var storageAccount = await azureClient.WithSubscription(storageAccountInfo.SubscriptionId).StorageAccounts.GetByIdAsync(storageAccountInfo.Id);

                return (await storageAccount.GetKeysAsync()).First().Value;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, $"An exception occurred when getting the storage account key for account {storageAccountInfo.Name}.");
                throw;
            }
        }

        /// <summary>
        /// Uploads the text content to a blob
        /// </summary>
        /// <param name="blobAbsoluteUri">Absolute Blob URI</param>
        /// <param name="content">Blob content</param>
        /// <returns>A task to await</returns>
        public Task UploadBlobAsync(Uri blobAbsoluteUri, string content)
        {
            return new CloudBlockBlob(blobAbsoluteUri).UploadTextAsync(content);
        }

        /// <summary>
        /// Uploads the file content to a blob
        /// </summary>
        /// <param name="blobAbsoluteUri">Absolute Blob URI</param>
        /// <param name="filePath">File path</param>
        /// <returns>A task to await</returns>
        public Task UploadBlobFromFileAsync(Uri blobAbsoluteUri, string filePath)
        {
            return new CloudBlockBlob(blobAbsoluteUri).UploadFromFileAsync(filePath);
        }

        /// <summary>
        /// Downloads a blob
        /// </summary>
        /// <param name="blobAbsoluteUri">Absolute Blob URI</param>
        /// <returns>Blob content</returns>
        public Task<string> DownloadBlobAsync(Uri blobAbsoluteUri)
        {
            return new CloudBlockBlob(blobAbsoluteUri).DownloadTextAsync();
        }

        /// <summary>
        /// Gets the list of blobs in the given directory
        /// </summary>
        /// <param name="directoryUri">Directory Uri</param>
        /// <returns>List of blob paths</returns>
        public async Task<IEnumerable<string>> ListBlobsAsync(Uri directoryUri)
        {
            var blob = new CloudBlockBlob(directoryUri);
            var directory = blob.Container.GetDirectoryReference(blob.Name);

            BlobContinuationToken continuationToken = null;
            var results = new List<string>();

            do
            {
                var response = await directory.ListBlobsSegmentedAsync(useFlatBlobListing: true, blobListingDetails: BlobListingDetails.None, maxResults: null, currentToken: continuationToken, options: null, operationContext: null);
                continuationToken = response.ContinuationToken;
                results.AddRange(response.Results.Cast<CloudBlob>().Select(b => b.Name));
            }
            while (continuationToken != null);

            return results;
        }

        /// <summary>
        /// Get/sets cached value for the price and resource summary of all available VMs in a region for the <see cref="BatchAccount"/>.
        /// </summary>
        /// <returns><see cref="VirtualMachineInfo"/> for available VMs in a region.</returns>
        public async Task<List<VirtualMachineInfo>> GetVmSizesAndPricesAsync()
        {
            var vmSizesAndPrices = await GetVmSizesAndPricesRawAsync();
            return vmSizesAndPrices.ToList();
        }
		
		/// <summary>
        /// Checks if a local file exists
        /// </summary>
        public bool LocalFileExists(string path)
        {
            return File.Exists(path);
        }

        /// <summary>
        /// Reads the content of the Common Workflow Language (CWL) file associated with the parent workflow of the TES task
        /// </summary>
        /// <param name="workflowId">Parent workflow</param>
        /// <param name="content">Content of the file</param>
        /// <returns>True if file was found</returns>
        public bool TryReadCwlFile(string workflowId, out string content)
        {
            var fileName = $"cwl_temp_file_{workflowId}.cwl";

            try
            {
                var filePath = Directory.GetFiles("/cromwell-tmp", fileName, SearchOption.AllDirectories).FirstOrDefault();

                if (filePath != null)
                {
                    content = File.ReadAllText(filePath);
                    return true;
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, $"Error looking up or retrieving contents of CWL file '{fileName}'");
            }

            content = null;
            return false;
        }

        private async Task<string> GetPricingContentJsonAsync()
        {
            var pricingUrl = $"https://management.azure.com/subscriptions/{subscriptionId}/providers/Microsoft.Commerce/RateCard?api-version=2016-08-31-preview&$filter=OfferDurableId eq '{azureOfferDurableId}' and Currency eq 'USD' and Locale eq 'en-US' and RegionInfo eq 'US'";

            try
            {
                var accessToken = await GetAzureAccessTokenAsync();
                var pricingRequest = new HttpRequestMessage(HttpMethod.Get, pricingUrl);
                pricingRequest.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
                var pricingResponse = await httpClient.SendAsync(pricingRequest);
                var content = await pricingResponse.Content.ReadAsByteArrayAsync();
                return Encoding.UTF8.GetString(content).TrimStart('\ufeff');
            }
            catch (Exception ex)
            {
                logger.LogInformation($"GetPricingContentJsonAsync URL: {pricingUrl}");
                logger.LogError(ex, $"Could not retrieve VM pricing info. Make sure that TES service principal has Billing Reader role on the subscription");
                throw;
            }
        }

        private IEnumerable<VmPrice> ExtractVmPricesFromRateCardResponse(List<(string VmSize, string FamilyName, string MeterName, string MeterSubCategory)> supportedVmSizes, string pricingContent)
        {
            var rateCardMeters = JObject.Parse(pricingContent)["Meters"]
                .Where(m => m["MeterCategory"].ToString() == "Virtual Machines" && m["MeterStatus"].ToString() == "Active" && m["MeterRegion"].ToString().Equals(billingRegionName, StringComparison.OrdinalIgnoreCase))
                .Select(m => new { MeterName = m["MeterName"].ToString(), MeterSubCategory = m["MeterSubCategory"].ToString(), MeterRate = m["MeterRates"]["0"].ToString() })
                .Where(m => !m.MeterSubCategory.Contains("Windows"))
                .Select(m => new { 
                    MeterName = m.MeterName.Replace(" Low Priority", "", StringComparison.OrdinalIgnoreCase),
                    m.MeterSubCategory,
                    MeterRate = decimal.Parse(m.MeterRate), 
                    IsLowPriority = m.MeterName.Contains(" Low Priority", StringComparison.OrdinalIgnoreCase) })
                .ToList();

            return supportedVmSizes
                .Select(v => new {
                    v.VmSize,
                    RateCardMeters = rateCardMeters.Where(m => m.MeterName.Equals(v.MeterName, StringComparison.OrdinalIgnoreCase) && m.MeterSubCategory.Equals(v.MeterSubCategory, StringComparison.OrdinalIgnoreCase)) })
                .Select(v => new VmPrice {
                    VmSize = v.VmSize,
                    PricePerHourDedicated = v.RateCardMeters.FirstOrDefault(m => !m.IsLowPriority)?.MeterRate,
                    PricePerHourLowPriority = v.RateCardMeters.FirstOrDefault(m => m.IsLowPriority)?.MeterRate })
                .Where(v => v.PricePerHourDedicated != null);
        }

        /// <summary>
        /// Get the price and resource summary of all available VMs in a region for the <see cref="BatchAccount"/>.
        /// </summary>
        /// <returns><see cref="VirtualMachineInfo"/> for available VMs in a region.</returns>
        private async Task<IEnumerable<VirtualMachineInfo>> GetVmSizesAndPricesRawAsync()
        {
            static double ConvertMiBToGiB(int value) => Math.Round(value / 1024.0, 2);

            var azureClient = await GetAzureManagementClientAsync();
            var vmSizesAvailableAtLocation = (await azureClient.WithSubscription(subscriptionId).VirtualMachines.Sizes.ListByRegionAsync(location)).ToList();

            IEnumerable<VmPrice> vmPrices;

            var supportedVmSizes = AzureBillingUtils.GetVmSizesSupportedByBatch().ToList();

            try
            {
                var pricingContent = await GetPricingContentJsonAsync();
                vmPrices = ExtractVmPricesFromRateCardResponse(supportedVmSizes, pricingContent);
            }
            catch
            {
                logger.LogWarning("Using default VM prices. Please see: https://github.com/microsoft/CromwellOnAzure/blob/master/docs/troubleshooting-guide.md#dynamic-cost-optimization-and-ratecard-api-access");
                vmPrices = JsonConvert.DeserializeObject<IEnumerable<VmPrice>>(File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "DefaultVmPrices.json")));
            }

            var vmInfos = new List<VirtualMachineInfo>();

            foreach (var supportedVmSize in supportedVmSizes)
            {
                var vmSpecification = vmSizesAvailableAtLocation.SingleOrDefault(x => x.Name.Equals(supportedVmSize.VmSize, StringComparison.OrdinalIgnoreCase));
                var vmPrice = vmPrices.SingleOrDefault(x => x.VmSize.Equals(supportedVmSize.VmSize, StringComparison.OrdinalIgnoreCase));

                if (vmSpecification != null && vmPrice != null)
                {
                    vmInfos.Add(new VirtualMachineInfo
                    {
                        VmSize = supportedVmSize.VmSize,
                        MemoryInGB = ConvertMiBToGiB(vmSpecification.MemoryInMB),
                        NumberOfCores = vmSpecification.NumberOfCores,
                        ResourceDiskSizeInGB = ConvertMiBToGiB(vmSpecification.ResourceDiskSizeInMB),
                        MaxDataDiskCount = vmSpecification.MaxDataDiskCount,
                        VmFamily = supportedVmSize.FamilyName,
                        LowPriority = false,
                        PricePerHour = vmPrice.PricePerHourDedicated
                    });

                    if(vmPrice.LowPriorityAvailable)
                    {
                        vmInfos.Add(new VirtualMachineInfo
                        {
                            VmSize = supportedVmSize.VmSize,
                            MemoryInGB = ConvertMiBToGiB(vmSpecification.MemoryInMB),
                            NumberOfCores = vmSpecification.NumberOfCores,
                            ResourceDiskSizeInGB = ConvertMiBToGiB(vmSpecification.ResourceDiskSizeInMB),
                            MaxDataDiskCount = vmSpecification.MaxDataDiskCount,
                            VmFamily = supportedVmSize.FamilyName,
                            LowPriority = true,
                            PricePerHour = vmPrice.PricePerHourLowPriority
                        });
                    }
                }
            }

            // TODO: Check if pricing API did not return the list and vmInfos is null
            return vmInfos;
        }

        private static Task<string> GetAzureAccessTokenAsync(string resource = "https://management.azure.com/")
        {
            return new AzureServiceTokenProvider().GetAccessTokenAsync(resource);
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

        /// <summary>
        /// Creates an Azure Batch pool that's lifecycle must be manually managed
        /// </summary>
        /// <param name="poolName">The name of the pool. This becomes the Pool.Id</param>
        /// <param name="vmSize">The Azure SKU for the VM of the pool</param>
        /// <param name="isLowPriority">True if a low-priority VM should be used; false for a dedicated</param>
        /// <param name="executorImage">The image required by the TesTask</param>
        /// <param name="nodeInfo">Information about the pool to be created</param>
        /// <param name="dockerInDockerImageName">Image that contains Docker to download private images</param>
        /// <param name="blobxferImageName">Image name for blobxfer, the Azure storage transfer tool</param>
        /// <param name="identityResourceId">The resource ID of a user-assigned managed identity to assign to the pool</param>
        /// <param name="disableBatchNodesPublicIpAddress">True to remove the public IP address of the Batch node</param>
        /// <param name="batchNodesSubnetId">The subnet ID of the Batch VM in the pool</param>
        /// <param name="startTaskSasUrl">SAS URL for the start task</param>
        /// <param name="startTaskPath">Local path on the Azure Batch node for the script</param>
        /// <returns></returns>
        public async Task CreateManualBatchPoolAsync(
            string poolName, 
            string vmSize, 
            bool isLowPriority, 
            string executorImage, 
            BatchNodeInfo nodeInfo,
            string dockerInDockerImageName, 
            string blobxferImageName, 
            string identityResourceId, 
            bool disableBatchNodesPublicIpAddress, 
            string batchNodesSubnetId,
            string startTaskSasUrl,
            string startTaskPath
            )
        {
            try
            {
                var tokenCredentials = new TokenCredentials(await GetAzureAccessTokenAsync());

                var vmConfigManagement = new Microsoft.Azure.Management.Batch.Models.VirtualMachineConfiguration(
                    new Microsoft.Azure.Management.Batch.Models.ImageReference(
                    nodeInfo.BatchImagePublisher,
                    nodeInfo.BatchImageOffer,
                    nodeInfo.BatchImageSku,
                    nodeInfo.BatchImageVersion),
                    nodeInfo.BatchNodeAgentSkuId);

                Microsoft.Azure.Management.Batch.Models.StartTask startTask = null;

                //if (useStartTask)
                //{
                //    startTask = new Microsoft.Azure.Management.Batch.Models.StartTask
                //    {
                //        // Pool StartTask: install Docker as start task if it's not already
                //        CommandLine = $"/bin/sh {startTaskPath}",
                //        UserIdentity = new Microsoft.Azure.Management.Batch.Models.UserIdentity(null, new Microsoft.Azure.Management.Batch.Models.AutoUserSpecification(elevationLevel: Microsoft.Azure.Management.Batch.Models.ElevationLevel.Admin, scope: Microsoft.Azure.Management.Batch.Models.AutoUserScope.Pool)),
                //        ResourceFiles = new List<Microsoft.Azure.Management.Batch.Models.ResourceFile> { new Microsoft.Azure.Management.Batch.Models.ResourceFile(null, null, startTaskSasUrl, null, startTaskPath) }
                //    };
                //}

                var containerRegistryInfo = await GetContainerRegistryInfoAsync(executorImage);

                if (containerRegistryInfo != null)
                {
                    var containerRegistryMgmt = new Microsoft.Azure.Management.Batch.Models.ContainerRegistry(
                        userName: containerRegistryInfo.Username,
                        registryServer: containerRegistryInfo.RegistryServer,
                        password: containerRegistryInfo.Password);

                    // Download private images at node startup, since those cannot be downloaded in the main task that runs multiple containers.
                    // Doing this also requires that the main task runs inside a container, hence downloading the "docker" image (contains docker client) as well.
                    vmConfigManagement.ContainerConfiguration = new Microsoft.Azure.Management.Batch.Models.ContainerConfiguration
                    {
                        ContainerImageNames = new List<string> { executorImage, dockerInDockerImageName, blobxferImageName },
                        ContainerRegistries = new List<Microsoft.Azure.Management.Batch.Models.ContainerRegistry> { containerRegistryMgmt }
                    };

                    var containerRegistryInfoForDockerInDocker = await GetContainerRegistryInfoAsync(dockerInDockerImageName);

                    if (containerRegistryInfoForDockerInDocker != null && containerRegistryInfoForDockerInDocker.RegistryServer != containerRegistryInfo.RegistryServer)
                    {
                        var containerRegistryForDockerInDockerMgmt = new Microsoft.Azure.Management.Batch.Models.ContainerRegistry(
                            userName: containerRegistryInfoForDockerInDocker.Username,
                            registryServer: containerRegistryInfoForDockerInDocker.RegistryServer,
                            password: containerRegistryInfoForDockerInDocker.Password);

                        vmConfigManagement.ContainerConfiguration.ContainerRegistries.Add(containerRegistryForDockerInDockerMgmt);
                    }

                    var containerRegistryInfoForBlobXfer = await GetContainerRegistryInfoAsync(blobxferImageName);

                    if (containerRegistryInfoForBlobXfer != null && containerRegistryInfoForBlobXfer.RegistryServer != containerRegistryInfo.RegistryServer && containerRegistryInfoForBlobXfer.RegistryServer != containerRegistryInfoForDockerInDocker.RegistryServer)
                    {
                        var containerRegistryForBlobXferMgmt = new Microsoft.Azure.Management.Batch.Models.ContainerRegistry(
                            userName: containerRegistryInfoForBlobXfer.Username,
                            registryServer: containerRegistryInfoForBlobXfer.RegistryServer,
                            password: containerRegistryInfoForBlobXfer.Password);

                        vmConfigManagement.ContainerConfiguration.ContainerRegistries.Add(containerRegistryForBlobXferMgmt);
                    }
                }

                var poolInfo = new Microsoft.Azure.Management.Batch.Models.Pool(name: poolName)
                {
                    VmSize = vmSize,
                    ScaleSettings = new Microsoft.Azure.Management.Batch.Models.ScaleSettings
                    {
                        FixedScale = new Microsoft.Azure.Management.Batch.Models.FixedScaleSettings
                        {
                            TargetDedicatedNodes = isLowPriority ? 0 : 1,
                            TargetLowPriorityNodes = isLowPriority ? 1 : 0,
                            ResizeTimeout = TimeSpan.FromMinutes(30), 
                            // TODO does this do anything with fixed scale settings?
                            NodeDeallocationOption = Microsoft.Azure.Management.Batch.Models.ComputeNodeDeallocationOption.TaskCompletion
                        }
                    },
                    DeploymentConfiguration = new Microsoft.Azure.Management.Batch.Models.DeploymentConfiguration
                    {
                        VirtualMachineConfiguration = vmConfigManagement
                    },
                    Identity = new Microsoft.Azure.Management.Batch.Models.BatchPoolIdentity
                    {
                        Type = Microsoft.Azure.Management.Batch.Models.PoolIdentityType.UserAssigned,
                        UserAssignedIdentities = new Dictionary<string, Microsoft.Azure.Management.Batch.Models.UserAssignedIdentities>
                        {
                            [identityResourceId] = new Microsoft.Azure.Management.Batch.Models.UserAssignedIdentities()
                        }
                    },
                    StartTask = startTask
                };

                if (!string.IsNullOrEmpty(batchNodesSubnetId))
                {
                    poolInfo.NetworkConfiguration = new Microsoft.Azure.Management.Batch.Models.NetworkConfiguration
                    {
                        PublicIPAddressConfiguration = new Microsoft.Azure.Management.Batch.Models.PublicIPAddressConfiguration(disableBatchNodesPublicIpAddress ? Microsoft.Azure.Management.Batch.Models.IPAddressProvisioningType.NoPublicIPAddresses : Microsoft.Azure.Management.Batch.Models.IPAddressProvisioningType.BatchManaged),
                        SubnetId = batchNodesSubnetId
                    };
                }

                var batchManagementClient = new BatchManagementClient(tokenCredentials) { SubscriptionId = subscriptionId };
                logger.LogInformation($"Creating manual batch pool named {poolName} with vmSize {vmSize} and low priority {isLowPriority}");
                var pool = await batchManagementClient.Pool.CreateAsync(batchResourceGroupName, batchAccountName, poolInfo.Name, poolInfo);
                logger.LogInformation($"Successfully created manual batch pool named {poolName} with vmSize {vmSize} and low priority {isLowPriority}");
            }
            catch (Exception exc)
            {
                logger.LogError(exc, $"Error trying to create manual batch pool named {poolName} with vmSize {vmSize} and low priority {isLowPriority}");
                throw;
            }
        }

        private static async Task<(string SubscriptionId, string ResourceGroupName, string Location, string BatchAccountEndpoint)> FindBatchAccountAsync(string batchAccountName)
        {
            var resourceGroupRegex = new Regex("/*/resourceGroups/([^/]*)/*");

            var tokenCredentials = new TokenCredentials(await GetAzureAccessTokenAsync());
            var azureClient = await GetAzureManagementClientAsync();

            var subscriptionIds = (await azureClient.Subscriptions.ListAsync()).Select(s => s.SubscriptionId);

            foreach (var subId in subscriptionIds)
            {
                var batchAccount = (await new BatchManagementClient(tokenCredentials) { SubscriptionId = subId }.BatchAccount.ListAsync())
                    .FirstOrDefault(a => a.Name.Equals(batchAccountName, StringComparison.OrdinalIgnoreCase));

                if (batchAccount != null)
                {
                    var resourceGroupName = resourceGroupRegex.Match(batchAccount.Id).Groups[1].Value;

                    return (subId, resourceGroupName, batchAccount.Location, batchAccount.AccountEndpoint);
                }
            }

            throw new Exception($"Batch account '{batchAccountName}' does not exist or the TES app service does not have Contributor role on the account.");
        }

        /// <inheritdoc/>
        public async Task<ContainerRegistryInfo> GetContainerRegistryInfoAsync(string imageName)
        {
            return (await GetAccessibleContainerRegistriesAsync())
                .FirstOrDefault(reg => reg.RegistryServer.Equals(imageName.Split('/').FirstOrDefault(), StringComparison.OrdinalIgnoreCase));
        }

        /// <inheritdoc/>
        public async Task<StorageAccountInfo> GetStorageAccountInfoAsync(string storageAccountName)
        {
            return (await GetAccessibleStorageAccountsAsync())
                .FirstOrDefault(storageAccount => storageAccount.Name.Equals(storageAccountName, StringComparison.OrdinalIgnoreCase));
        }

        private class VmPrice
        {
            public string VmSize { get; set; }
            public decimal? PricePerHourDedicated { get; set; }
            public decimal? PricePerHourLowPriority { get; set; }

            [JsonIgnore]
            public bool LowPriorityAvailable => PricePerHourLowPriority != null;
        }
    }
}
