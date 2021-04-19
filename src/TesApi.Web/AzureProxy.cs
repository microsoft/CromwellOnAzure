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
using Microsoft.Azure.Management.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Extensions.Logging;
using Microsoft.Rest;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Tes.Models;

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

        /// <summary>
        /// The constructor
        /// </summary>
        /// <param name="batchAccountName">Batch account name</param>
        /// <param name="azureOfferDurableId">Azure offer id</param>
        /// <param name="logger">The logger</param>
        public AzureProxy(string batchAccountName, string azureOfferDurableId, ILogger logger)
        {
            this.logger = logger;

            var findBatchAccountResult = FindBatchAccountAsync(batchAccountName).Result;

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
                    LowPriorityCoreQuota = batchAccount.LowPriorityCoreQuota.Value,
                    PoolQuota = batchAccount.PoolQuota
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
            var job = batchClient.JobOperations.CreateJob(jobId, poolInformation);

            await job.CommitAsync();

            try
            {
                job = await batchClient.JobOperations.GetJobAsync(job.Id); // Retrieve the "bound" version of the job
                job.PoolInformation = poolInformation;  // Redoing this since the container registry password is not retrieved by GetJobAsync()
                job.OnAllTasksComplete = OnAllTasksComplete.TerminateJob;

                await job.AddTaskAsync(cloudTask);
                await job.CommitAsync();
            }
            catch (Exception ex)
            {
                var batchError = JsonConvert.SerializeObject((ex as BatchException)?.RequestInformation?.BatchError);
                logger.LogError(ex, $"Deleting {job.Id} because adding task to it failed. Batch error: {batchError}");
                await batchClient.JobOperations.DeleteJobAsync(job.Id);
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
            var count = batchJobsToDelete.Count();

            if (count > 1)
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
                    var registries = await azureClient.WithSubscription(subId).ContainerRegistries.ListAsync();

                    foreach (var r in registries)
                    {
                        var server = await r.GetCredentialsAsync();
                        var info = new ContainerRegistryInfo { RegistryServer = r.LoginServerUrl, Username = server.Username, Password = server.AccessKeys[AccessKeyType.Primary] };
                        infos.Add(info);
                    }
                }
                catch (Exception)
                {
                    logger.LogWarning($"TES service has no permission to list container registries in subscription {subId}.");
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

        private IEnumerable<VmPrice> ExtractVmPricesFromRateCardResponse(string pricingContent)
        {
            return JObject.Parse(pricingContent)["Meters"]
                .Where(m => m["MeterCategory"].ToString() == "Virtual Machines" && m["MeterStatus"].ToString() == "Active" && m["MeterRegion"].ToString().Equals(billingRegionName, StringComparison.OrdinalIgnoreCase))
                .Select(m => new { MeterNames = m["MeterName"].ToString(), MeterSubCategories = m["MeterSubCategory"].ToString().Replace(" Series", ""), PricePerHour = decimal.Parse(m["MeterRates"]["0"].ToString()) })
                .Where(m => !m.MeterSubCategories.Contains("Windows"))
                .Select(m => new { MeterNames = m.MeterNames.Replace(" Low Priority", ""), m.MeterSubCategories, m.PricePerHour, LowPriority = m.MeterNames.Contains(" Low Priority") })
                .Select(m => new VmPrice
                {
                    VmSizes = m.MeterNames.Split(new char[] { '/' }).Select(x => ((m.MeterSubCategories.Contains("Basic") ? "Basic_" : "Standard_") + x).Replace(" ", "_") + (m.MeterSubCategories.Contains("Promo") ? "_Promo" : "")).ToArray(),
                    VmSeries = m.MeterSubCategories.Replace(" Promo", "").Replace(" Basic", "").Split(new char[] { '/' }),
                    PricePerHour = m.PricePerHour,
                    LowPriority = m.LowPriority
                });
        }

        /// <summary>
        /// Get the price and resource summary of all available VMs in a region for the <see cref="BatchAccount"/>.
        /// </summary>
        /// <returns><see cref="VirtualMachineInfo"/> for available VMs in a region.</returns>
        private async Task<IEnumerable<VirtualMachineInfo>> GetVmSizesAndPricesRawAsync()
        {
            const double mbToGbRatio = 0.001;
            const double mibToGbRatio = 0.001048576;

            static double ConvertMBOrMiBToGB(int value) =>  Math.Round(value * (value % 1024 == 0 ? mibToGbRatio : mbToGbRatio), 3);

            var azureClient = await GetAzureManagementClientAsync();
            var vmSizesAvailableAtLocation = (await azureClient.WithSubscription(subscriptionId).VirtualMachines.Sizes.ListByRegionAsync(location)).ToList();

            IEnumerable<VmPrice> vmPrices;

            try
            {
                var pricingContent = await GetPricingContentJsonAsync();
                vmPrices = ExtractVmPricesFromRateCardResponse(pricingContent);
            }
            catch
            {
                logger.LogWarning("Using default VM prices. Please see: https://github.com/microsoft/CromwellOnAzure/blob/master/docs/troubleshooting-guide.md#dynamic-cost-optimization-and-ratecard-api-access");
                vmPrices = JsonConvert.DeserializeObject<IEnumerable<VmPrice>>(File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "DefaultVmPrices.json")));
            }

            var vmInfos = new List<VirtualMachineInfo>();

            foreach (var vmPrice in vmPrices)
            {
                for (var i = 0; i < vmPrice.VmSizes.Length; i++)
                {
                    var vmSize = vmSizesAvailableAtLocation.SingleOrDefault(x => x.Name == vmPrice.VmSizes[i]);

                    if (vmSize != null)
                    {
                        vmInfos.Add(new VirtualMachineInfo
                        {
                            VmSize = vmPrice.VmSizes[i],
                            MemoryInGB = ConvertMBOrMiBToGB(vmSize.MemoryInMB),
                            NumberOfCores = vmSize.NumberOfCores,
                            ResourceDiskSizeInGB = ConvertMBOrMiBToGB(vmSize.ResourceDiskSizeInMB),
                            MaxDataDiskCount = vmSize.MaxDataDiskCount,
                            VmSeries = vmPrice.VmSeries[i],
                            LowPriority = vmPrice.LowPriority,
                            PricePerHour = vmPrice.PricePerHour
                        });
                    }
                }
            }

            // TODO: Check if pricing API did not return the list and vmInfos is null
            return vmInfos.Where(vm => GetVmSizesSupportedByBatch().Contains(vm.VmSize, StringComparer.OrdinalIgnoreCase));
        }

        private static Task<string> GetAzureAccessTokenAsync(string resource = "https://management.azure.com/")
        {
            return new AzureServiceTokenProvider().GetAccessTokenAsync(resource);
        }

        /// <summary>
        /// Gets an authenticated Azure Client instance
        /// </summary>
        /// <returns>An authenticated Azure Client instance</returns>
        private static async Task<Microsoft.Azure.Management.Fluent.Azure.IAuthenticated> GetAzureManagementClientAsync()
        {
            var accessToken = await GetAzureAccessTokenAsync();
            var azureCredentials = new AzureCredentials(new TokenCredentials(accessToken), null, null, AzureEnvironment.AzureGlobalCloud);
            var azureClient = Microsoft.Azure.Management.Fluent.Azure.Authenticate(azureCredentials);

            return azureClient;
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

        // TODO: Batch will provide an API for this in a future release of the client library
        private static IEnumerable<string> GetVmSizesSupportedByBatch()
        {
            return new List<string> {
                "Standard_A1",
                "Standard_A1_v2",
                "Standard_A10",
                "Standard_A11",
                "Standard_A2",
                "Standard_A2_v2",
                "Standard_A2m_v2",
                "Standard_A3",
                "Standard_A4",
                "Standard_A4_v2",
                "Standard_A4m_v2",
                "Standard_A5",
                "Standard_A6",
                "Standard_A7",
                "Standard_A8",
                "Standard_A8_v2",
                "Standard_A8m_v2",
                "Standard_A9",
                "Standard_D1",
                "Standard_D1_v2",
                "Standard_D11",
                "Standard_D11_v2",
                "Standard_D12",
                "Standard_D12_v2",
                "Standard_D13",
                "Standard_D13_v2",
                "Standard_D14",
                "Standard_D14_v2",
                "Standard_D15_v2",
                "Standard_D16_v3",
                "Standard_D16s_v3",
                "Standard_D2",
                "Standard_D2_v2",
                "Standard_D2_v3",
                "Standard_D2s_v3",
                "Standard_D3",
                "Standard_D3_v2",
                "Standard_D32_v3",
                "Standard_D32s_v3",
                "Standard_D4",
                "Standard_D4_v2",
                "Standard_D4_v3",
                "Standard_D4s_v3",
                "Standard_D5_v2",
                "Standard_D64_v3",
                "Standard_D64s_v3",
                "Standard_D8_v3",
                "Standard_D8s_v3",
                "Standard_DS1",
                "Standard_DS1_v2",
                "Standard_DS11",
                "Standard_DS11_v2",
                "Standard_DS12",
                "Standard_DS12_v2",
                "Standard_DS13",
                "Standard_DS13_v2",
                "Standard_DS14",
                "Standard_DS14_v2",
                "Standard_DS15_v2",
                "Standard_DS2",
                "Standard_DS2_v2",
                "Standard_DS3",
                "Standard_DS3_v2",
                "Standard_DS4",
                "Standard_DS4_v2",
                "Standard_DS5_v2",
                "Standard_E16_v3",
                "Standard_E16s_v3",
                "Standard_E2_v3",
                "Standard_E2s_v3",
                "Standard_E32_v3",
                "Standard_E32s_v3",
                "Standard_E4_v3",
                "Standard_E4s_v3",
                "Standard_E64_v3",
                "Standard_E64s_v3",
                "Standard_E8_v3",
                "Standard_E8s_v3",
                "Standard_F1",
                "Standard_F16",
                "Standard_F16s",
                "Standard_F16s_v2",
                "Standard_F1s",
                "Standard_F2",
                "Standard_F2s",
                "Standard_F2s_v2",
                "Standard_F32s_v2",
                "Standard_F4",
                "Standard_F4s",
                "Standard_F4s_v2",
                "Standard_F64s_v2",
                "Standard_F72s_v2",
                "Standard_F8",
                "Standard_F8s",
                "Standard_F8s_v2",
                "Standard_G1",
                "Standard_G2",
                "Standard_G3",
                "Standard_G4",
                "Standard_G5",
                "Standard_GS1",
                "Standard_GS2",
                "Standard_GS3",
                "Standard_GS4",
                "Standard_GS5",
                "Standard_H16",
                "Standard_H16m",
                "Standard_H16mr",
                "Standard_H16r",
                "Standard_H8",
                "Standard_H8m",
                "Standard_L16s",
                "Standard_L32s",
                "Standard_L4s",
                "Standard_L8s"
            };
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
            public string[] VmSizes { get; set; }
            public string[] VmSeries { get; set; }
            public decimal PricePerHour { get; set; }
            public bool LowPriority { get; set; }
        }
    }
}
