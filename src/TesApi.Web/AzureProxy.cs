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
using Microsoft.Azure.Management.Compute.Fluent;
using Microsoft.Azure.Management.ContainerRegistry.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Extensions.Logging;
using Microsoft.Rest;
using Microsoft.WindowsAzure.Storage.Blob;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using Tes.Models;

using BatchModels = Microsoft.Azure.Management.Batch.Models;
using FluentAzure = Microsoft.Azure.Management.Fluent.Azure;
using Polly;
using Polly.Retry;

namespace TesApi.Web
{
    /// <summary>
    /// Wrapper for Azure APIs
    /// </summary>
    public class AzureProxy : IAzureProxy
    {
        private const char BatchJobAttemptSeparator = '-';
        private const string DefaultAzureBillingRegionName = "US West";

        private static readonly HttpClient httpClient = new();
        private static readonly AsyncRetryPolicy batchRaceConditionJobNotFoundRetryPolicy = Policy
            .Handle<BatchException>(ex => ex.RequestInformation.BatchError.Code == BatchErrorCodeStrings.JobNotFound)
            .WaitAndRetryAsync(10, retryAttempt => TimeSpan.FromSeconds(1));

        private readonly ILogger logger;
        private readonly Func<Task<BatchModels.BatchAccount>> getBatchAccountFunc;
        private readonly BatchClient batchClient;
        private readonly string subscriptionId;
        private readonly string location;
        private readonly string billingRegionName;
        private readonly string azureOfferDurableId;
        private readonly string batchResourceGroupName;
        private readonly string batchAccountName;


        /// <summary>
        /// The constructor
        /// </summary>
        /// <param name="batchAccountName">Batch account name</param>
        /// <param name="azureOfferDurableId">Azure offer id</param>
        /// <param name="logger">The logger</param>
        public AzureProxy(string batchAccountName, string azureOfferDurableId, ILogger<AzureProxy> logger)
        {
            this.logger = logger;
            this.batchAccountName = batchAccountName;
            var (SubscriptionId, ResourceGroupName, Location, BatchAccountEndpoint) = FindBatchAccountAsync(batchAccountName).Result;
            batchResourceGroupName = ResourceGroupName;
            subscriptionId = SubscriptionId;
            location = Location;
            batchClient = BatchClient.Open(new BatchTokenCredentials($"https://{BatchAccountEndpoint}", () => GetAzureAccessTokenAsync("https://batch.core.windows.net/")));

            getBatchAccountFunc = async () =>
                await new BatchManagementClient(new TokenCredentials(await GetAzureAccessTokenAsync())) { SubscriptionId = SubscriptionId }
                    .BatchAccount
                    .GetAsync(ResourceGroupName, batchAccountName);

            this.azureOfferDurableId = azureOfferDurableId;

            if (!AzureRegionUtils.TryGetBillingRegionName(location, out billingRegionName))
            {
                logger.LogWarning($"Azure ARM location '{location}' does not have a corresponding Azure Billing Region.  Prices from the fallback billing region '{DefaultAzureBillingRegionName}' will be used instead.");
                billingRegionName = DefaultAzureBillingRegionName;
            }
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

        /// <inheritdoc/>
        public async Task<(string, string)> GetCosmosDbEndpointAndKeyAsync(string cosmosDbAccountName)
        {
            var azureClient = await GetAzureManagementClientAsync();
            var subscriptionIds = (await azureClient.Subscriptions.ListAsync()).Select(s => s.SubscriptionId);

            var account = (await Task.WhenAll(subscriptionIds.Select(async subId => await azureClient.WithSubscription(subId).CosmosDBAccounts.ListAsync())))
                .SelectMany(a => a)
                .FirstOrDefault(a => a.Name.Equals(cosmosDbAccountName, StringComparison.OrdinalIgnoreCase));

            if (account is null)
            {
                throw new Exception($"CosmosDB account '{cosmosDbAccountName} does not exist or the TES app service does not have Account Reader role on the account.");
            }

            var key = (await azureClient.WithSubscription(account.Manager.SubscriptionId).CosmosDBAccounts.ListKeysAsync(account.ResourceGroupName, account.Name)).PrimaryMasterKey;

            return (account.DocumentEndpoint, key);
        }

        /// <inheritdoc/>
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


        /// <inheritdoc/>
        public IEnumerable<AzureBatchNodeCount> GetBatchActiveNodeCountByVmSize()
            => batchClient.PoolOperations.ListPools()
                .Select(p => new
                {
                    p.VirtualMachineSize,
                    DedicatedNodeCount = Math.Max(p.TargetDedicatedComputeNodes ?? 0, p.CurrentDedicatedComputeNodes ?? 0),
                    LowPriorityNodeCount = Math.Max(p.TargetLowPriorityComputeNodes ?? 0, p.CurrentLowPriorityComputeNodes ?? 0)
                })
                .GroupBy(x => x.VirtualMachineSize)
                .Select(grp => new AzureBatchNodeCount { VirtualMachineSize = grp.Key, DedicatedNodeCount = grp.Sum(x => x.DedicatedNodeCount), LowPriorityNodeCount = grp.Sum(x => x.LowPriorityNodeCount) });

        /// <inheritdoc/>
        public int GetBatchActivePoolCount()
        {
            var activePoolsFilter = new ODATADetailLevel
            {
                FilterClause = $"state eq 'active' or state eq 'deleting'",
                SelectClause = "id"
            };

            return batchClient.PoolOperations.ListPools(activePoolsFilter).Count();
        }

        /// <inheritdoc/>
        public int GetBatchActiveJobCount()
        {
            var activeJobsFilter = new ODATADetailLevel
            {
                FilterClause = $"state eq 'active' or state eq 'disabling' or state eq 'terminating' or state eq 'deleting'",
                SelectClause = "id"
            };

            return batchClient.JobOperations.ListJobs(activeJobsFilter).Count();
        }

        /// <inheritdoc/>
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

        /// <inheritdoc/>
        public async Task CreateBatchJobAsync(string jobId, CloudTask cloudTask, PoolInformation poolInformation)
        {
            logger.LogInformation($"TES task: {cloudTask.Id} - creating Batch job");
            var job = batchClient.JobOperations.CreateJob(jobId, poolInformation);
            job.OnAllTasksComplete = OnAllTasksComplete.TerminateJob;
            await job.CommitAsync();
            logger.LogInformation($"TES task: {cloudTask.Id} - Batch job committed successfully.");

            try
            {
                logger.LogInformation($"TES task: {cloudTask.Id} adding task to job.");
                job = await batchRaceConditionJobNotFoundRetryPolicy.ExecuteAsync(() => 
                    batchClient.JobOperations.GetJobAsync(job.Id));

                await job.AddTaskAsync(cloudTask);
                logger.LogInformation($"TES task: {cloudTask.Id} added task successfully.");
            }
            catch (Exception ex)
            {
                var batchError = JsonConvert.SerializeObject((ex as BatchException)?.RequestInformation?.BatchError);
                logger.LogError(ex, $"TES task: {cloudTask.Id} deleting {job.Id} because adding task to it failed. Batch error: {batchError}");

                await batchClient.JobOperations.DeleteJobAsync(job.Id);
                throw;
            }
        }

        /// <inheritdoc/>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1826:Do not use Enumerable methods on indexable collections", Justification = "FirstOrDefault() is straightforward, the alternative is less clear.")]
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
                var poolId = job.ExecutionInformation?.PoolId;

                if (job.State == JobState.Active && poolId is not null)
                {
                    var poolFilter = new ODATADetailLevel
                    {
                        FilterClause = $"id eq '{poolId}'",
                        SelectClause = "*"
                    };

                    var pool = await batchClient.PoolOperations.ListPools(poolFilter).ToAsyncEnumerable().FirstOrDefaultAsync();

                    if (pool is not null)
                    {
                        nodeAllocationFailed = pool.ResizeErrors?.Count > 0;

                        var node = await pool.ListComputeNodes().ToAsyncEnumerable().FirstOrDefaultAsync(n => (n.RecentTasks?.Select(t => t.JobId) ?? Enumerable.Empty<string>()).Contains(job.Id));

                        if (node is not null)
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

        /// <inheritdoc/>
        public async Task DeleteBatchJobAsync(string tesTaskId, CancellationToken cancellationToken = default)
        {
            var jobFilter = new ODATADetailLevel
            {
                FilterClause = $"startswith(id,'{tesTaskId}{BatchJobAttemptSeparator}') and state ne 'deleting'",
                SelectClause = "id"
            };

            var batchJobsToDelete = await batchClient.JobOperations.ListJobs(jobFilter).ToListAsync(cancellationToken);

            if (batchJobsToDelete.Count > 1)
            {
                logger.LogWarning($"Found more than one active job for TES task {tesTaskId}");
            }

            foreach (var job in batchJobsToDelete)
            {
                logger.LogInformation($"Deleting job {job.Id}");
                await batchClient.JobOperations.DeleteJobAsync(job.Id, cancellationToken: cancellationToken);
            }
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<string>> ListOldJobsToDeleteAsync(TimeSpan oldestJobAge)
        {
            var filter = new ODATADetailLevel
            {
                FilterClause = $"state eq 'completed' and executionInfo/endTime lt DateTime'{DateTime.Today.Subtract(oldestJobAge):yyyy-MM-ddTHH:mm:ssZ}'",
                SelectClause = "id"
            };

            return (await batchClient.JobOperations.ListJobs(filter).ToListAsync()).Select(c => c.Id);
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<string>> ListOrphanedJobsToDeleteAsync(TimeSpan minJobAge, CancellationToken cancellationToken = default)
        {
            var filter = new ODATADetailLevel
            {
                FilterClause = $"state eq 'active' and creationTime lt DateTime'{DateTime.UtcNow.Subtract(minJobAge):yyyy-MM-ddTHH:mm:ssZ}'",
                SelectClause = "id,poolInfo,onAllTasksComplete"
            };

            var noActionTesjobs = (await batchClient.JobOperations.ListJobs(filter).ToListAsync(cancellationToken))
                .Where(j => j.PoolInformation?.AutoPoolSpecification?.AutoPoolIdPrefix == "TES" && j.OnAllTasksComplete == OnAllTasksComplete.NoAction);

            var noActionTesjobsWithNoTasks = await noActionTesjobs.ToAsyncEnumerable().WhereAwait(async j => !(await j.ListTasks().ToListAsync(cancellationToken)).Any()).ToListAsync(cancellationToken);

            return noActionTesjobsWithNoTasks.Select(j => j.Id);
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<string>> GetActivePoolIdsAsync(string prefix, TimeSpan minAge, CancellationToken cancellationToken = default)
        {
            var activePoolsFilter = new ODATADetailLevel
            {
                FilterClause = $"state eq 'active' and startswith(id, '{prefix}') and creationTime lt DateTime'{DateTime.UtcNow.Subtract(minAge):yyyy-MM-ddTHH:mm:ssZ}'",
                SelectClause = "id"
            };

            return (await batchClient.PoolOperations.ListPools(activePoolsFilter).ToListAsync(cancellationToken)).Select(p => p.Id);
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<string>> GetPoolIdsReferencedByJobsAsync(CancellationToken cancellationToken = default)
            => (await batchClient.JobOperations.ListJobs(new ODATADetailLevel(selectClause: "executionInfo")).ToListAsync(cancellationToken))
                .Where(j => !string.IsNullOrEmpty(j.ExecutionInformation?.PoolId))
                .Select(j => j.ExecutionInformation.PoolId);

        /// <inheritdoc/>
        public Task DeleteBatchPoolAsync(string poolId, CancellationToken cancellationToken = default)
            => batchClient.PoolOperations.DeletePoolAsync(poolId, cancellationToken: cancellationToken);

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

                var poolsToDelete = await batchClient.PoolOperations.ListPools(poolFilter).ToListAsync(cancellationToken);

                foreach (var pool in poolsToDelete)
                {
                    logger.LogInformation($"Pool ID: {pool.Id} Pool State: {pool?.State} deleting...");
                    await batchClient.PoolOperations.DeletePoolAsync(pool.Id, cancellationToken: cancellationToken);
                }
            }
            catch (Exception exc)
            {
                var batchErrorCode = (exc as BatchException)?.RequestInformation?.BatchError?.Code;

                if (batchErrorCode?.Trim().Equals("PoolBeingDeleted", StringComparison.OrdinalIgnoreCase) == true)
                {
                    // Do not throw if it's a deletion race condition
                    // Docs: https://learn.microsoft.com/en-us/rest/api/batchservice/Pool/Delete?tabs=HTTP

                    return;
                }

                logger.LogError(exc, $"Pool ID: {poolId} exception while attempting to delete the pool.  Batch error code: {batchErrorCode}");
                throw;
            }
        }

        /// <summary>
        /// Gets the list of container registries that the TES server has access to
        /// </summary>
        private async Task<IEnumerable<ContainerRegistryInfo>> GetAccessibleContainerRegistriesAsync()
        {
            var azureClient = await GetAzureManagementClientAsync();
            var subscriptionIds = (await azureClient.Subscriptions.ListAsync()).Select(s => s.SubscriptionId);
            var infos = new List<ContainerRegistryInfo>();
            logger.LogInformation(@"GetAccessibleContainerRegistriesAsync() called.");

            foreach (var subId in subscriptionIds)
            {
                try
                {
                    var registries = (await azureClient.WithSubscription(subId).ContainerRegistries.ListAsync()).ToList();
                    logger.LogInformation(@$"Searching {subId} for container registries.");

                    foreach (var r in registries)
                    {
                        logger.LogInformation(@$"Found {r.Name}. AdminUserEnabled: {r.AdminUserEnabled}");

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

            logger.LogInformation(@"GetAccessibleContainerRegistriesAsync() returning {Count} registries.", infos.Count);
            return infos;
        }

        private static async Task<IEnumerable<StorageAccountInfo>> GetAccessibleStorageAccountsAsync()
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

        /// <inheritdoc/>
        public async Task<string> GetStorageAccountKeyAsync(StorageAccountInfo storageAccountInfo)
        {
            try
            {
                var azureClient = await GetAzureManagementClientAsync();
                var storageAccount = await azureClient.WithSubscription(storageAccountInfo.SubscriptionId).StorageAccounts.GetByIdAsync(storageAccountInfo.Id);

                return (await storageAccount.GetKeysAsync())[0].Value;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, $"An exception occurred when getting the storage account key for account {storageAccountInfo.Name}.");
                throw;
            }
        }

        /// <inheritdoc/>
        public Task UploadBlobAsync(Uri blobAbsoluteUri, string content)
            => new CloudBlockBlob(blobAbsoluteUri).UploadTextAsync(content);

        /// <inheritdoc/>
        public Task UploadBlobFromFileAsync(Uri blobAbsoluteUri, string filePath)
            => new CloudBlockBlob(blobAbsoluteUri).UploadFromFileAsync(filePath);

        /// <inheritdoc/>
        public Task<string> DownloadBlobAsync(Uri blobAbsoluteUri)
            => new CloudBlockBlob(blobAbsoluteUri).DownloadTextAsync();

        /// <inheritdoc/>
        public Task<bool> BlobExistsAsync(Uri blobAbsoluteUri)
            => new CloudBlockBlob(blobAbsoluteUri).ExistsAsync();

        /// <inheritdoc/>
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
            while (continuationToken is not null);

            return results;
        }

        /// <inheritdoc/>
        public async Task<List<VirtualMachineInformation>> GetVmSizesAndPricesAsync()
            => (await GetVmSizesAndPricesRawAsync()).ToList();

        /// <inheritdoc/>
        public bool LocalFileExists(string path)
            => File.Exists(path);

        /// <inheritdoc/>
        public bool TryReadCwlFile(string workflowId, out string content)
        {
            var fileName = $"cwl_temp_file_{workflowId}.cwl";

            try
            {
                var filePath = Directory.GetFiles("/cromwell-tmp", fileName, SearchOption.AllDirectories).FirstOrDefault();

                if (filePath is not null)
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

        private IEnumerable<VmPrice> ExtractVmPricesFromRateCardResponse(List<(string VmSize, string MeterName, string MeterSubCategory)> supportedVmSizes, string pricingContent)
        {
            var rateCardMeters = JObject.Parse(pricingContent)["Meters"]
                .Where(m => m["MeterCategory"].ToString() == "Virtual Machines" && m["MeterStatus"].ToString() == "Active" && m["MeterRegion"].ToString().Equals(billingRegionName, StringComparison.OrdinalIgnoreCase))
                .Select(m => new { MeterName = m["MeterName"].ToString(), MeterSubCategory = m["MeterSubCategory"].ToString(), MeterRate = m["MeterRates"]["0"].ToString() })
                .Where(m => !m.MeterSubCategory.Contains("Windows"))
                .Select(m => new
                {
                    MeterName = m.MeterName.Replace(" Low Priority", string.Empty, StringComparison.OrdinalIgnoreCase),
                    m.MeterSubCategory,
                    MeterRate = decimal.Parse(m.MeterRate),
                    IsLowPriority = m.MeterName.Contains(" Low Priority", StringComparison.OrdinalIgnoreCase)
                })
                .ToList();

            return supportedVmSizes
                .Select(v => new
                {
                    v.VmSize,
                    RateCardMeters = rateCardMeters.Where(m => m.MeterName.Equals(v.MeterName, StringComparison.OrdinalIgnoreCase) && m.MeterSubCategory.Equals(v.MeterSubCategory, StringComparison.OrdinalIgnoreCase))
                })
                .Select(v => new VmPrice
                {
                    VmSize = v.VmSize,
                    PricePerHourDedicated = v.RateCardMeters.FirstOrDefault(m => !m.IsLowPriority)?.MeterRate,
                    PricePerHourLowPriority = v.RateCardMeters.FirstOrDefault(m => m.IsLowPriority)?.MeterRate
                })
                .Where(v => v.PricePerHourDedicated is not null);
        }

        /// <summary>
        /// Get the price and resource summary of all available VMs in a region for the <see cref="BatchModels.BatchAccount"/>.
        /// </summary>
        /// <returns><see cref="VirtualMachineInformation"/> for available VMs in a region.</returns>
        private async Task<IEnumerable<VirtualMachineInformation>> GetVmSizesAndPricesRawAsync()
        {
            static double ConvertMiBToGiB(int value) => Math.Round(value / 1024.0, 2);

            var azureClient = await GetAzureManagementClientAsync();

            var vmSizesAvailableAtLocation = (await azureClient.WithSubscription(subscriptionId).ComputeSkus.ListbyRegionAndResourceTypeAsync(Region.Create(location), ComputeResourceType.VirtualMachines))
                .Select(vm => new { VmSize = vm.Name.Value, VmFamily = vm.Inner.Family, Capabilities = vm.Capabilities.ToDictionary(c => c.Name, c => c.Value) })
                .Select(vm => new {
                    VmSize = vm.VmSize,
                    VmFamily = vm.VmFamily,
                    NumberOfCores = int.Parse(vm.Capabilities.GetValueOrDefault("vCPUsAvailable", vm.Capabilities["vCPUs"])),
                    MemoryGiB = double.Parse(vm.Capabilities["MemoryGB"]),
                    DiskGiB = ConvertMiBToGiB(int.Parse(vm.Capabilities["MaxResourceVolumeMB"])),
                    MaxDataDiskCount = int.Parse(vm.Capabilities.GetValueOrDefault("MaxDataDiskCount", "0")) });

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

            var vmInfos = new List<VirtualMachineInformation>();

            foreach (var (vmSize, _, _) in supportedVmSizes)
            {
                var vmSpecification = vmSizesAvailableAtLocation.SingleOrDefault(vm => vm.VmSize.Equals(vmSize, StringComparison.OrdinalIgnoreCase));
                var vmPrice = vmPrices.SingleOrDefault(vm => vm.VmSize.Equals(vmSize, StringComparison.OrdinalIgnoreCase));

                if (vmSpecification is not null && vmPrice is not null)
                {
                    vmInfos.Add(new VirtualMachineInformation
                    {
                        VmSize = vmSize,
                        MemoryInGB = vmSpecification.MemoryGiB,
                        NumberOfCores = vmSpecification.NumberOfCores,
                        ResourceDiskSizeInGB = vmSpecification.DiskGiB,
                        MaxDataDiskCount = vmSpecification.MaxDataDiskCount,
                        VmFamily = vmSpecification.VmFamily,
                        LowPriority = false,
                        PricePerHour = vmPrice.PricePerHourDedicated
                    });

                    if (vmPrice.LowPriorityAvailable)
                    {
                        vmInfos.Add(new VirtualMachineInformation
                        {
                            VmSize = vmSize,
                            MemoryInGB = vmSpecification.MemoryGiB,
                            NumberOfCores = vmSpecification.NumberOfCores,
                            ResourceDiskSizeInGB = vmSpecification.DiskGiB,
                            MaxDataDiskCount = vmSpecification.MaxDataDiskCount,
                            VmFamily = vmSpecification.VmFamily,
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
            => new AzureServiceTokenProvider().GetAccessTokenAsync(resource);

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

        /// <inheritdoc/>
        public async Task<PoolInformation> CreateBatchPoolAsync(BatchModels.Pool poolInfo, bool isPreemptable)
        {
            try
            {
                var tokenCredentials = new TokenCredentials(await GetAzureAccessTokenAsync());
                var batchManagementClient = new BatchManagementClient(tokenCredentials) { SubscriptionId = subscriptionId };
                logger.LogInformation($"Creating manual batch pool named {poolInfo.Name} with vmSize {poolInfo.VmSize} and low priority {isPreemptable}");
                var pool = await batchManagementClient.Pool.CreateAsync(batchResourceGroupName, batchAccountName, poolInfo.Name, poolInfo);
                logger.LogInformation($"Successfully created manual batch pool named {poolInfo.Name} with vmSize {poolInfo.VmSize} and low priority {isPreemptable}");
                return new() { PoolId = pool.Name };
            }
            catch (Exception exc)
            {
                logger.LogError(exc, $"Error trying to create manual batch pool named {poolInfo.Name} with vmSize {poolInfo.VmSize} and low priority {isPreemptable}");
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

                if (batchAccount is not null)
                {
                    var resourceGroupName = resourceGroupRegex.Match(batchAccount.Id).Groups[1].Value;

                    return (subId, resourceGroupName, batchAccount.Location, batchAccount.AccountEndpoint);
                }
            }

            throw new Exception($"Batch account '{batchAccountName}' does not exist or the TES app service does not have Contributor role on the account.");
        }

        /// <inheritdoc/>
        public async Task<ContainerRegistryInfo> GetContainerRegistryInfoAsync(string imageName)
            => (await GetAccessibleContainerRegistriesAsync())
                .FirstOrDefault(reg => reg.RegistryServer.Equals(imageName.Split('/').FirstOrDefault(), StringComparison.OrdinalIgnoreCase));

        /// <inheritdoc/>
        public async Task<StorageAccountInfo> GetStorageAccountInfoAsync(string storageAccountName)
            => (await GetAccessibleStorageAccountsAsync())
                .FirstOrDefault(storageAccount => storageAccount.Name.Equals(storageAccountName, StringComparison.OrdinalIgnoreCase));

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudJob> ListJobsAsync(DetailLevel detailLevel = null)
            => batchClient.JobOperations.ListJobs(detailLevel: detailLevel).ToAsyncEnumerable();

        private class VmPrice
        {
            public string VmSize { get; set; }
            public decimal? PricePerHourDedicated { get; set; }
            public decimal? PricePerHourLowPriority { get; set; }

            [JsonIgnore]
            public bool LowPriorityAvailable => PricePerHourLowPriority is not null;
        }
    }
}
