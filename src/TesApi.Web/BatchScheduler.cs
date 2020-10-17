// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Web;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using TesApi.Models;

namespace TesApi.Web
{
    /// <summary>
    /// Orchestrates <see cref="TesTask"/>s on Azure Batch
    /// </summary>
    public class BatchScheduler : IBatchScheduler
    {
        private const int DefaultCoreCount = 1;
        private const int DefaultMemoryGb = 2;
        private const int DefaultDiskGb = 10;
        private const string CromwellPathPrefix = "/cromwell-executions/";
        private const string CromwellScriptFileName = "script";
        private const string BatchExecutionDirectoryName = "__batch";
        private const string UploadFilesScriptFileName = "upload_files_script";
        private const string DownloadFilesScriptFileName = "download_files_script";
        private const string DockerInDockerImageName = "docker";
        private const string BlobxferImageName = "mcr.microsoft.com/blobxfer";
        private static readonly Regex queryStringRegex = new Regex(@"[^\?.]*(\?.*)");
        private static readonly TimeSpan sasTokenDuration = TimeSpan.FromDays(3);
        private readonly ILogger logger;
        private readonly IAzureProxy azureProxy;
        private readonly string defaultStorageAccountName;
        private readonly List<TesTaskStateTransition> tesTaskStateTransitions;
        private readonly bool usePreemptibleVmsOnly;
        private readonly List<ExternalStorageContainerInfo> externalStorageContainers;

        /// <summary>
        /// Default constructor invoked by ASP.NET Core DI
        /// </summary>
        /// <param name="logger">Logger instance provided by ASP.NET Core DI</param>
        /// <param name="configuration">Configuration</param>
        /// <param name="azureProxy">Azure proxy</param>
        public BatchScheduler(ILogger logger, IConfiguration configuration, IAzureProxy azureProxy)
        {
            this.logger = logger;
            this.azureProxy = azureProxy;

            defaultStorageAccountName = configuration["DefaultStorageAccountName"];    // This account contains the cromwell-executions container
            usePreemptibleVmsOnly = configuration.GetValue("UsePreemptibleVmsOnly", false);

            externalStorageContainers = configuration["ExternalStorageContainers"]?.Split(new[] { ',', ';', '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(uri => {
                    if (StorageAccountUrlSegments.TryCreate(uri, out var s))
                    { 
                        return new ExternalStorageContainerInfo { BlobEndpoint = s.BlobEndpoint, AccountName = s.AccountName, ContainerName = s.ContainerName, SasToken = s.SasToken };
                    }
                    else
                    {
                        logger.LogError($"Invalid value '{uri}' found in 'ExternalStorageContainers' configuration. Value must be a valid azure storage account or container URL.");
                        return null;
                    }})
                .Where(storageAccountInfo => storageAccountInfo != null)
                .ToList();

            logger.LogInformation($"DefaultStorageAccountName: {defaultStorageAccountName}");
            logger.LogInformation($"usePreemptibleVmsOnly: {usePreemptibleVmsOnly}");

            static bool tesTaskIsQueuedInitializingOrRunning(TesTask tesTask) => tesTask.State == TesState.QUEUEDEnum || tesTask.State == TesState.INITIALIZINGEnum || tesTask.State == TesState.RUNNINGEnum;
            static bool tesTaskIsInitializingOrRunning(TesTask tesTask) => tesTask.State == TesState.INITIALIZINGEnum || tesTask.State == TesState.RUNNINGEnum;
            static bool tesTaskIsQueuedOrInitializing(TesTask tesTask) => tesTask.State == TesState.QUEUEDEnum || tesTask.State == TesState.INITIALIZINGEnum;
            static bool tesTaskIsQueued(TesTask tesTask) => tesTask.State == TesState.QUEUEDEnum;
            static bool tesTaskCancellationRequested(TesTask tesTask) => tesTask.State == TesState.CANCELEDEnum && tesTask.IsCancelRequested;

            static void SetTaskStateAndLog(TesTask tesTask, TesState newTaskState, CombinedBatchTaskInfo batchInfo)
            {
                tesTask.State = newTaskState;

                var tesTaskLog = tesTask.GetOrAddTesTaskLog();
                var tesTaskExecutorLog = tesTaskLog.GetOrAddExecutorLog();

                tesTaskLog.BatchNodeMetrics = batchInfo.BatchNodeMetrics;
                tesTaskLog.CromwellResultCode = batchInfo.CromwellRcCode;
                tesTaskLog.EndTime = DateTime.UtcNow;
                tesTaskExecutorLog.StartTime = batchInfo.BatchTaskStartTime;
                tesTaskExecutorLog.EndTime = batchInfo.BatchTaskEndTime;
                tesTaskExecutorLog.ExitCode = batchInfo.BatchTaskExitCode;

                tesTask.SetFailureReason(batchInfo.FailureReason);

                if (batchInfo.SystemLogItems != null)
                {
                    tesTask.AddToSystemLog(batchInfo.SystemLogItems);
                }
            }

            static void SetTaskCompleted(TesTask tesTask, CombinedBatchTaskInfo batchInfo) => SetTaskStateAndLog(tesTask, TesState.COMPLETEEnum, batchInfo);
            static void SetTaskExecutorError(TesTask tesTask, CombinedBatchTaskInfo batchInfo) => SetTaskStateAndLog(tesTask, TesState.EXECUTORERROREnum, batchInfo);
            static void SetTaskSystemError(TesTask tesTask, CombinedBatchTaskInfo batchInfo) => SetTaskStateAndLog(tesTask, TesState.SYSTEMERROREnum, batchInfo);

            async Task DeleteBatchJobAndSetTaskStateAsync(TesTask tesTask, TesState newTaskState, CombinedBatchTaskInfo batchInfo) { await this.azureProxy.DeleteBatchJobAsync(tesTask.Id); SetTaskStateAndLog(tesTask, newTaskState, batchInfo); }
            Task DeleteBatchJobAndSetTaskExecutorErrorAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo) => DeleteBatchJobAndSetTaskStateAsync(tesTask, TesState.EXECUTORERROREnum, batchInfo);
            Task DeleteBatchJobAndSetTaskSystemErrorAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo) => DeleteBatchJobAndSetTaskStateAsync(tesTask, TesState.SYSTEMERROREnum, batchInfo);

            Task DeleteBatchJobAndRequeueTaskAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo) => ++tesTask.ErrorCount > 3
                ? DeleteBatchJobAndSetTaskExecutorErrorAsync(tesTask, batchInfo) 
                : DeleteBatchJobAndSetTaskStateAsync(tesTask, TesState.QUEUEDEnum, batchInfo);

            async Task CancelTaskAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo) { await this.azureProxy.DeleteBatchJobAsync(tesTask.Id); tesTask.IsCancelRequested = false; }

            tesTaskStateTransitions = new List<TesTaskStateTransition>()
            {
                // TODO TONY: Recognized error: FailureReason, one line to SystemLog
                // Unknown error: FailureReason=UnknownPoolError/UnknownNodeError etc., one line to System log with Node error code if it exists or other code-like message we can get from pool or node, plus dump of AzureBatchJobAndTaskState - just non null values serialized

                new TesTaskStateTransition(tesTaskCancellationRequested, batchTaskState: null, CancelTaskAsync),
                new TesTaskStateTransition(tesTaskIsQueued, BatchTaskState.JobNotFound, (tesTask, _) => AddBatchJobAsync(tesTask)),
                new TesTaskStateTransition(tesTaskIsQueued, BatchTaskState.MissingBatchTask, DeleteBatchJobAndRequeueTaskAsync),
                new TesTaskStateTransition(tesTaskIsQueued, BatchTaskState.Initializing, (tesTask, _) => tesTask.State = TesState.INITIALIZINGEnum),
                new TesTaskStateTransition(tesTaskIsQueuedOrInitializing, BatchTaskState.NodeAllocationFailed, DeleteBatchJobAndRequeueTaskAsync),
                new TesTaskStateTransition(tesTaskIsQueuedOrInitializing, BatchTaskState.Running, (tesTask, _) => tesTask.State = TesState.RUNNINGEnum),
                new TesTaskStateTransition(tesTaskIsQueuedInitializingOrRunning, BatchTaskState.MoreThanOneActiveJobFound, DeleteBatchJobAndSetTaskSystemErrorAsync),
                new TesTaskStateTransition(tesTaskIsQueuedInitializingOrRunning, BatchTaskState.CompletedSuccessfully, SetTaskCompleted),
                new TesTaskStateTransition(tesTaskIsQueuedInitializingOrRunning, BatchTaskState.CompletedWithErrors, SetTaskExecutorError),
                new TesTaskStateTransition(tesTaskIsQueuedInitializingOrRunning, BatchTaskState.ActiveJobWithMissingAutoPool, DeleteBatchJobAndRequeueTaskAsync),
                new TesTaskStateTransition(tesTaskIsQueuedInitializingOrRunning, BatchTaskState.NodeFailedDuringStartupOrExecution, DeleteBatchJobAndSetTaskExecutorErrorAsync),
                new TesTaskStateTransition(tesTaskIsQueuedInitializingOrRunning, BatchTaskState.NodeUnusable, DeleteBatchJobAndSetTaskExecutorErrorAsync),
                new TesTaskStateTransition(tesTaskIsInitializingOrRunning, BatchTaskState.JobNotFound, SetTaskSystemError),
                new TesTaskStateTransition(tesTaskIsInitializingOrRunning, BatchTaskState.MissingBatchTask, DeleteBatchJobAndSetTaskSystemErrorAsync),
                new TesTaskStateTransition(tesTaskIsInitializingOrRunning, BatchTaskState.NodePreempted, DeleteBatchJobAndRequeueTaskAsync) // TODO: Implement preemption detection
            };
        }

        /// <summary>
        /// Iteratively manages execution of a <see cref="TesTask"/> on Azure Batch until completion or failure
        /// </summary>
        /// <param name="tesTask">The <see cref="TesTask"/></param>
        /// <returns>True if the TES task needs to be persisted.</returns>
        public async Task<bool> ProcessTesTaskAsync(TesTask tesTask)
        {
            var combinedBatchTaskInfo = await GetBatchTaskStateAsync(tesTask);
            var tesTaskChanged = await HandleTesTaskTransitionAsync(tesTask, combinedBatchTaskInfo);
            return tesTaskChanged;
        }

        private static string GetCromwellExecutionDirectoryPath(TesTask task)
        {
            return GetParentPath(task.Inputs?.FirstOrDefault(IsCromwellCommandScript)?.Path);
        }

        private static string GetBatchExecutionDirectoryPath(TesTask task)
        {
            return $"{GetCromwellExecutionDirectoryPath(task)}/{BatchExecutionDirectoryName}";
        }

        /// <summary>
        /// Get the parent path of the given path
        /// </summary>
        /// <param name="path">The path</param>
        /// <returns>The parent path</returns>
        private static string GetParentPath(string path)
        {
            if (string.IsNullOrEmpty(path))
            {
                return null;
            }

            var pathComponents = path.TrimEnd('/').Split('/');

            return string.Join('/', pathComponents.Take(pathComponents.Length - 1));
        }

        /// <summary>
        /// Determines if the <see cref="TesInput"/> file is a Cromwell command script
        /// </summary>
        /// <param name="inputFile"><see cref="TesInput"/> file</param>
        /// <returns>True if the file is a Cromwell command script</returns>
        private static bool IsCromwellCommandScript(TesInput inputFile)
        {
            return inputFile.Name.Equals("commandScript");
        }

        /// <summary>
        /// Verifies existence and translates local file URLs to absolute paths (e.g. file:///tmp/cwl_temp_dir_8026387118450035757/args.py becomes /tmp/cwl_temp_dir_8026387118450035757/args.py)
        /// Only considering files in /cromwell-tmp because that is the only local directory mapped from Cromwell container
        /// </summary>
        /// <param name="fileUri">File URI</param>
        /// <param name="localPath">Local path</param>
        /// <returns></returns>
        private bool TryGetCromwellTmpFilePath(string fileUri, out string localPath)
        {
            localPath = Uri.TryCreate(fileUri, UriKind.Absolute, out var uri) && uri.IsFile && uri.AbsolutePath.StartsWith("/cromwell-tmp/") && this.azureProxy.LocalFileExists(uri.AbsolutePath) ? uri.AbsolutePath : null;

            return localPath != null;
        }

        /// <summary>
        /// Checks if the specified string represents a HTTP URL that is publicly accessible
        /// </summary>
        /// <param name="uriString">URI string</param>
        /// <returns>True if the URL can be used as is, without adding SAS token to it</returns>
        private async Task<bool> IsPublicHttpUrl(string uriString)
        {
            var isHttpUrl = Uri.TryCreate(uriString, UriKind.Absolute, out var uri) && (uri.Scheme.Equals(Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase) || uri.Scheme.Equals(Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase));

            if (!isHttpUrl)
            {
                return false;
            }
                        
            if (HttpUtility.ParseQueryString(uri.Query).Get("sig") != null)
            {
                return true;
            }

            if (StorageAccountUrlSegments.TryCreate(uriString, out var parts))
            {
                if (await TryGetStorageAccountInfoAsync(parts.AccountName))
                {
                    return false;
                }

                if (TryGetExternalStorageAccountInfo(parts.AccountName, parts.ContainerName, out _))
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Adds a new Azure Batch pool/job/task for the given <see cref="TesTask"/>
        /// </summary>
        /// <param name="tesTask">The <see cref="TesTask"/> to schedule on Azure Batch</param>
        /// <returns>A task to await</returns>
        private async Task AddBatchJobAsync(TesTask tesTask)
        {
            try
            {
                var jobId = await azureProxy.GetNextBatchJobIdAsync(tesTask.Id);
                var virtualMachineInfo = await GetVmSizeAsync(tesTask.Resources);
                var tesTaskLog = tesTask.AddTesTaskLog();

                await CheckBatchAccountQuotas((int)tesTask.Resources.CpuCores.GetValueOrDefault(DefaultCoreCount), virtualMachineInfo.LowPriority ?? false);

                // TODO?: Support for multiple executors. Cromwell has single executor per task.
                var dockerImage = tesTask.Executors.First().Image;
                var cloudTask = await ConvertTesTaskToBatchTaskAsync(tesTask);
                var poolInformation = await CreatePoolInformation(dockerImage, virtualMachineInfo.VmSize, virtualMachineInfo.LowPriority ?? false);
                tesTaskLog.VirtualMachineInfo = virtualMachineInfo;

                logger.LogInformation($"Creating batch job for TES task {tesTask.Id}. Using VM size {virtualMachineInfo.VmSize}.");
                await azureProxy.CreateBatchJobAsync(jobId, cloudTask, poolInformation);

                tesTaskLog.StartTime = DateTimeOffset.UtcNow;

                tesTask.State = TesState.INITIALIZINGEnum;
            }
            catch (AzureBatchQuotaMaxedOutException exception)
            {
                logger.LogInformation($"Not enough quota available for task Id {tesTask.Id}. Reason: {exception.Message}. Task will remain in queue.");
            }
            catch (TesException exc)
            {
                tesTask.State = TesState.SYSTEMERROREnum;
                tesTask.SetFailureReason(exc);
                logger.LogError(exc, exc.Message);
            }
            catch (Exception exc)
            {
                tesTask.State = TesState.SYSTEMERROREnum;
                tesTask.SetFailureReason("UnknownError", exc.Message, exc.StackTrace);
                logger.LogError(exc, exc.Message);
            }
        }

        private async Task<bool> TryGetStorageAccountInfoAsync(string accountName, Action<StorageAccountInfo> onSuccess = null)
        {
            try
            {
                var storageAccountInfo = await azureProxy.GetStorageAccountInfoAsync(accountName);

                if (storageAccountInfo != null)
                {
                    onSuccess?.Invoke(storageAccountInfo);
                    return true;
                }
                else
                {
                    logger.LogError($"Could not find storage account '{accountName}'. Either the account does not exist or the TES app service does not have permission to it.");
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, $"Exception while getting storage account '{accountName}'");
            }

            return false;
        }

        private bool TryGetExternalStorageAccountInfo(string accountName, string containerName, out ExternalStorageContainerInfo result)
        {
            result = externalStorageContainers?.FirstOrDefault(c =>
                c.AccountName.Equals(accountName, StringComparison.OrdinalIgnoreCase)
                && (string.IsNullOrEmpty(c.ContainerName) || c.ContainerName.Equals(containerName, StringComparison.OrdinalIgnoreCase)));

            return result != null;
        }

        /// <summary>
        /// Returns an Azure Storage Blob or Container URL with SAS token given a path that uses one of the following formats: 
        /// - /accountName/containerName
        /// - /accountName/containerName/blobName
        /// - /cromwell-executions/blobName
        /// - https://accountName.blob.core.windows.net/containerName
        /// - https://accountName.blob.core.windows.net/containerName/blobName
        /// </summary>
        /// <param name="path">The file path to convert. Two-part path is treated as container path. Paths with three or more parts are treated as blobs.</param>
        /// <param name="getContainerSas">Get the container SAS even if path is longer than two parts</param>
        /// <returns>An Azure Block Blob or Container URL with SAS token</returns>
        private async Task<string> MapLocalPathToSasUrlAsync(string path, bool getContainerSas = false)
        {
            // TODO: Optional: If path is /container/... where container matches the name of the container in the default storage account, prepend the account name to the path.
            // This would allow the user to omit the account name for files stored in the default storage account

            // /cromwell-executions/... URLs become /defaultStorageAccountName/cromwell-executions/... to unify how URLs starting with /acct/container/... pattern are handled.
            if (path.StartsWith(CromwellPathPrefix, StringComparison.OrdinalIgnoreCase))
            {
                path = $"/{defaultStorageAccountName}{path}";
            }

            if(!StorageAccountUrlSegments.TryCreate(path, out var pathSegments))
            {
                logger.LogError($"Could not parse path '{path}'.");
                return null;
            }

            if (TryGetExternalStorageAccountInfo(pathSegments.AccountName, pathSegments.ContainerName, out var externalStorageAccountInfo))
            {
                return new StorageAccountUrlSegments(externalStorageAccountInfo.BlobEndpoint, pathSegments.ContainerName, pathSegments.BlobName, externalStorageAccountInfo.SasToken).ToUriString();
            }
            else
            {
                StorageAccountInfo storageAccountInfo = null;
                
                if(!await TryGetStorageAccountInfoAsync(pathSegments.AccountName, info => storageAccountInfo = info))
                { 
                    logger.LogError($"Could not find storage account '{pathSegments.AccountName}' corresponding to path '{path}'. Either the account does not exist or the TES app service does not have permission to it.");
                    return null;
                }

                try
                {
                    var accountKey = await azureProxy.GetStorageAccountKeyAsync(storageAccountInfo);
                    var resultPathSegments = new StorageAccountUrlSegments(storageAccountInfo.BlobEndpoint, pathSegments.ContainerName, pathSegments.BlobName);
                        
                    if (pathSegments.IsContainer || getContainerSas)
                    {
                        var policy = new SharedAccessBlobPolicy()
                        {
                            Permissions = SharedAccessBlobPermissions.Add | SharedAccessBlobPermissions.Create | SharedAccessBlobPermissions.List | SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Write,
                            SharedAccessExpiryTime = DateTime.Now.Add(sasTokenDuration)
                        };

                        var containerUri = new StorageAccountUrlSegments(storageAccountInfo.BlobEndpoint, pathSegments.ContainerName).ToUri();
                        resultPathSegments.SasToken = new CloudBlobContainer(containerUri, new StorageCredentials(storageAccountInfo.Name, accountKey)).GetSharedAccessSignature(policy, null, SharedAccessProtocol.HttpsOnly, null);
                    }
                    else
                    {
                        var policy = new SharedAccessBlobPolicy() { Permissions = SharedAccessBlobPermissions.Read, SharedAccessExpiryTime = DateTime.Now.Add(sasTokenDuration) };
                        resultPathSegments.SasToken = new CloudBlob(resultPathSegments.ToUri(), new StorageCredentials(storageAccountInfo.Name, accountKey)).GetSharedAccessSignature(policy, null, null, SharedAccessProtocol.HttpsOnly, null);
                    }

                    return resultPathSegments.ToUriString();
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, $"Could not get the key of storage account '{pathSegments.AccountName}'. Make sure that the TES app service has Contributor access to it.");
                    return null;
                }
            }
        }

        // TODO: Detect batch node preemption and return BatchTaskState.NodePreempted
        /// <summary>
        /// Gets the current state of the Azure Batch task
        /// </summary>
        /// <param name="tesTaskId">The unique ID of the <see cref="TesTask"/></param>
        /// <returns>A higher-level abstraction of the current state of the Azure Batch task</returns>
        private async Task<CombinedBatchTaskInfo> GetBatchTaskStateAsync(TesTask tesTask)
        {
            var azureBatchJobAndTaskState = await azureProxy.GetBatchJobAndTaskStateAsync(tesTask.Id);

            if (azureBatchJobAndTaskState.ActiveJobWithMissingAutoPool)
            {
                var batchJobInfo = JsonConvert.SerializeObject(azureBatchJobAndTaskState);
                logger.LogWarning($"Found active job without auto pool for TES task {tesTask.Id}. Deleting the job and requeuing the task. BatchJobInfo: {batchJobInfo}");
                return new CombinedBatchTaskInfo { BatchTaskState = BatchTaskState.ActiveJobWithMissingAutoPool, FailureReason = BatchTaskState.ActiveJobWithMissingAutoPool.ToString() };
            }

            if (azureBatchJobAndTaskState.MoreThanOneActiveJobFound)
            {
                return new CombinedBatchTaskInfo { BatchTaskState = BatchTaskState.MoreThanOneActiveJobFound, FailureReason = BatchTaskState.MoreThanOneActiveJobFound.ToString() };
            }

            switch (azureBatchJobAndTaskState.JobState)
            {
                case null:
                case JobState.Deleting:
                    // TODO TONY JobNotFound is both good and bad, depending on the task state, but that decision needs to be made outside
                    return new CombinedBatchTaskInfo { BatchTaskState = BatchTaskState.JobNotFound, FailureReason = BatchTaskState.JobNotFound.ToString() };
                    // return (BatchTaskState.JobNotFound, null, null); // Treating the deleting job as if it doesn't exist, // TODO TONY: Is there an error code here? SHoud transition interpret BatchTaskState and then pull error code if needed? As oppoised to handle doing it?
                case JobState.Active:
                    {
                        if (azureBatchJobAndTaskState.NodeAllocationFailed)
                        {
                            return new CombinedBatchTaskInfo { BatchTaskState = BatchTaskState.NodeAllocationFailed, FailureReason = BatchTaskState.NodeAllocationFailed.ToString() }; // TODO TONY Need to get additional error code here
                        }

                        if (azureBatchJobAndTaskState.NodeErrorCode != null)
                        {
                            if(azureBatchJobAndTaskState.NodeErrorCode == "DiskFull")
                            {
                                return new CombinedBatchTaskInfo { BatchTaskState = BatchTaskState.NodeFailedDuringStartupOrExecution, FailureReason = azureBatchJobAndTaskState.NodeErrorCode };
                            }
                            else
                            {
                                return new CombinedBatchTaskInfo { BatchTaskState = BatchTaskState.NodeFailedDuringStartupOrExecution, FailureReason = BatchTaskState.NodeFailedDuringStartupOrExecution.ToString(), SystemLogItems = new [] { azureBatchJobAndTaskState.NodeErrorCode } };
                            }
                            // TODO TONY handle elsewhere: var message = $"{azureBatchJobAndTaskState.NodeErrorCode}: {(azureBatchJobAndTaskState.NodeErrorDetails != null ? string.Join(", ", azureBatchJobAndTaskState.NodeErrorDetails) : null)}";
                              //return (BatchTaskState.NodeFailedDuringStartupOrExecution, azureBatchJobAndTaskState.NodeErrorCode, azureBatchJobAndTaskState); // Do we extract relevant part of azureBatchJobAndTaskState into a string? Or string[]
                        }

                        if (azureBatchJobAndTaskState.NodeState == ComputeNodeState.Unusable) // TODO TONY Need to get additional error code here
                        {
                            return new CombinedBatchTaskInfo { BatchTaskState = BatchTaskState.NodeUnusable, FailureReason = BatchTaskState.NodeUnusable.ToString() };
                        }

                        break;
                    }
                case JobState.Terminating:
                case JobState.Completed:
                    break;
                default:
                    throw new Exception($"Found batch job {tesTask.Id} in unexpected state: {azureBatchJobAndTaskState.JobState}");
            }

            switch (azureBatchJobAndTaskState.TaskState)
            {
                case null:
                    return new CombinedBatchTaskInfo { BatchTaskState = BatchTaskState.MissingBatchTask, FailureReason = BatchTaskState.MissingBatchTask.ToString() };
                case TaskState.Active:
                case TaskState.Preparing:
                    return new CombinedBatchTaskInfo { BatchTaskState = BatchTaskState.Initializing };
                case TaskState.Running:
                    return new CombinedBatchTaskInfo { BatchTaskState = BatchTaskState.Running };
                case TaskState.Completed:
                    var batchJobInfo = JsonConvert.SerializeObject(azureBatchJobAndTaskState);

                    if (azureBatchJobAndTaskState.TaskExitCode == 0 && azureBatchJobAndTaskState.TaskFailureInformation == null)
                    {
                        var metrics = await GetBatchNodeMetricsAndCromwellResultCodeAsync(tesTask);

                        return new CombinedBatchTaskInfo
                        {
                            BatchTaskState = BatchTaskState.CompletedSuccessfully,
                            BatchTaskExitCode = azureBatchJobAndTaskState.TaskExitCode,
                            BatchTaskStartTime = metrics.TaskStartTime ?? azureBatchJobAndTaskState.TaskStartTime,
                            BatchTaskEndTime = metrics.TaskEndTime ?? azureBatchJobAndTaskState.TaskEndTime,
                            BatchNodeMetrics = metrics.BatchNodeMetrics,
                            CromwellRcCode = metrics.CromwellRcCode
                        };
                    }
                    else
                    {
                        logger.LogError($"Task {tesTask.Id} failed. ExitCode: {azureBatchJobAndTaskState.TaskExitCode}, BatchJobInfo: {batchJobInfo}");

                        return new CombinedBatchTaskInfo
                        {
                            BatchTaskState = BatchTaskState.CompletedWithErrors,
                            FailureReason = azureBatchJobAndTaskState.TaskFailureInformation?.Code,
                            BatchTaskExitCode = azureBatchJobAndTaskState.TaskExitCode,
                            BatchTaskStartTime = azureBatchJobAndTaskState.TaskStartTime,
                            BatchTaskEndTime = azureBatchJobAndTaskState.TaskEndTime,
                            SystemLogItems = new[] { azureBatchJobAndTaskState.TaskFailureInformation?.Details?.FirstOrDefault()?.Value }
                        };
                    }
                default:
                    throw new Exception($"Found batch task {tesTask.Id} in unexpected state: {azureBatchJobAndTaskState.TaskState}");
            }
        }

        /// <summary>
        /// Transitions the <see cref="TesTask"/> to the new state, based on the rules defined in the tesTaskStateTransitions list.
        /// </summary>
        /// <param name="tesTask">TES task</param>
        /// <param name="combinedBatchTaskInfo">Current Azure Batch task info</param>
        /// <returns>True if the TES task was changed.</returns>
        private async Task<bool> HandleTesTaskTransitionAsync(TesTask tesTask, CombinedBatchTaskInfo combinedBatchTaskInfo)
        {
            // TODO: Here we need just need to apply actions
            // When task is executed the following may be touched:
            // tesTask.Log[].SystemLog
            // tesTask.Log[].FailureReason
            // tesTask.Log[].CromwellResultCode
            // tesTask.Log[].BatchExecutionMetrics
            // tesTask.Log[].EndTime
            // tesTask.Log[].Log[].StdErr
            // tesTask.Log[].Log[].ExitCode
            // tesTask.Log[].Log[].StartTime
            // tesTask.Log[].Log[].EndTime
            var tesTaskChanged = false;

            var mapItem = tesTaskStateTransitions
                .FirstOrDefault(m => (m.Condition == null || m.Condition(tesTask)) && (m.CurrentBatchTaskState == null || m.CurrentBatchTaskState == combinedBatchTaskInfo.BatchTaskState));

            if (mapItem != null)
            {
                if (mapItem.AsyncAction != null)
                {
                    await mapItem.AsyncAction(tesTask, combinedBatchTaskInfo);
                    tesTaskChanged = true;
                }

                if (mapItem.Action != null)
                {
                    mapItem.Action(tesTask, combinedBatchTaskInfo);
                    tesTaskChanged = true;
                }
            }

            return tesTaskChanged;
        }

        /// <summary>
        /// Returns job preparation and main Batch tasks that represents the given <see cref="TesTask"/>
        /// </summary>
        /// <param name="task">The <see cref="TesTask"/></param>
        /// <returns>Job preparation and main Batch tasks</returns>
        private async Task<CloudTask> ConvertTesTaskToBatchTaskAsync(TesTask task)
        {
            var cromwellPathPrefixWithoutEndSlash = CromwellPathPrefix.TrimEnd('/');
            var taskId = task.Id;

            var queryStringsToRemoveFromLocalFilePaths = task.Inputs
                .Select(i => i.Path)
                .Concat(task.Outputs.Select(o => o.Path))
                .Where(p => p != null)
                .Select(p => queryStringRegex.Match(p).Groups[1].Value)
                .Where(qs => !string.IsNullOrEmpty(qs))
                .ToList();

            var inputFiles = task.Inputs.Distinct();
            var cromwellExecutionDirectoryPath = GetCromwellExecutionDirectoryPath(task);

            if (cromwellExecutionDirectoryPath == null)
            {
                throw new TesException("NoCromwellExecutionDirectory", $"Could not identify Cromwell execution directory path for task {task.Id}. This TES instance supports Cromwell tasks only.");
            }

            foreach (var output in task.Outputs)
            {
                if (!output.Path.StartsWith(CromwellPathPrefix, StringComparison.OrdinalIgnoreCase))
                {
                    throw new TesException("InvalidOutputPath", $"Unsupported output path '{output.Path}' for task Id {task.Id}. Must start with {CromwellPathPrefix}");
                }
            }

            var batchExecutionDirectoryPath = GetBatchExecutionDirectoryPath(task);

            // TODO: Cromwell bug: Cromwell command write_tsv() generates a file in the execution directory, for example execution/write_tsv_3922310b441805fc43d52f293623efbc.tmp. These are not passed on to TES inputs.
            // WORKAROUND: Get the list of files in the execution directory and add them to task inputs.
            var executionDirectoryUri = new Uri(await MapLocalPathToSasUrlAsync(cromwellExecutionDirectoryPath, getContainerSas: true));
            var blobsInExecutionDirectory = (await azureProxy.ListBlobsAsync(executionDirectoryUri)).Where(b => !b.EndsWith($"/{CromwellScriptFileName}")).Where(b => !b.Contains($"/{BatchExecutionDirectoryName}/"));
            var additionalInputFiles = blobsInExecutionDirectory.Select(b => $"{CromwellPathPrefix}{b}").Select(b => new TesInput { Content = null, Path = b, Url = b, Name = Path.GetFileName(b), Type = TesFileType.FILEEnum });
            var filesToDownload = await Task.WhenAll(inputFiles.Union(additionalInputFiles).Select(async f => await GetTesInputFileUrl(f, task.Id, queryStringsToRemoveFromLocalFilePaths)));

            // Using --include and not using --no-recursive as a workaround for https://github.com/Azure/blobxfer/issues/123
            var downloadFilesScriptContent = string.Join(" && ", filesToDownload.Select(f =>
            {
                var downloadSingleFile = f.Url.Contains(".blob.core.")
                    ? $"blobxfer download --storage-url '{f.Url}' --local-path '{f.Path}' --chunk-size-bytes 104857600 --rename --include '{StorageAccountUrlSegments.Create(f.Url).BlobName}'"
                    : $"mkdir -p {GetParentPath(f.Path)} && wget -O '{f.Path}' '{f.Url}'";

                var exitIfDownloadedFileIsNotFound = $"{{ [ -f '{f.Path}' ] && : || {{ echo 'Failed to download file {f.Url}' 1>&2 && exit 1; }} }}";

                return $"{downloadSingleFile} && {exitIfDownloadedFileIsNotFound}";
            }));

            var downloadFilesScriptPath = $"{batchExecutionDirectoryPath}/{DownloadFilesScriptFileName}";
            var writableDownloadFilesScriptUrl = new Uri(await MapLocalPathToSasUrlAsync(downloadFilesScriptPath, getContainerSas: true));
            var downloadFilesScriptUrl = await MapLocalPathToSasUrlAsync(downloadFilesScriptPath);
            await azureProxy.UploadBlobAsync(writableDownloadFilesScriptUrl, downloadFilesScriptContent);

            var filesToUpload = await Task.WhenAll(
                task.Outputs.Select(async f =>
                    new TesOutput { Path = f.Path, Url = await MapLocalPathToSasUrlAsync(f.Path, getContainerSas: true), Name = f.Name, Type = f.Type }));

            var uploadFilesScriptContent = string.Join(" && ", filesToUpload.Select(f =>
            {
                // Ignore missing stdout/stderr files. CWL workflows have an issue where if the stdout/stderr are redirected, they are still listed in the TES outputs
                // Ignore any other missing files and directories. WDL tasks can have optional output files.
                // Syntax is: If file or directory doesn't exist, run a noop (":") operator, otherwise run the upload command:
                // { if not exists do nothing else upload; } && { ... }

                return $"{{ [ ! -e '{f.Path}' ] && : || blobxfer upload --storage-url '{f.Url}' --local-path '{f.Path}' --one-shot-bytes 104857600 {(f.Type == TesFileType.FILEEnum ? "--rename --no-recursive" : "")}; }}";
            }));

            var uploadFilesScriptPath = $"{batchExecutionDirectoryPath}/{UploadFilesScriptFileName}";
            var writableUploadFilesScriptUrl = new Uri(await MapLocalPathToSasUrlAsync(uploadFilesScriptPath, getContainerSas: true));
            var uploadFilesScriptUrl = await MapLocalPathToSasUrlAsync(uploadFilesScriptPath);
            await azureProxy.UploadBlobAsync(writableUploadFilesScriptUrl, uploadFilesScriptContent);

            var executor = task.Executors.First();

            var volumeMountsOption = $"-v /mnt{cromwellPathPrefixWithoutEndSlash}:{cromwellPathPrefixWithoutEndSlash}";

            var executorImageIsPublic = (await azureProxy.GetContainerRegistryInfoAsync(executor.Image)) == null;

            var metricsPath = $"{batchExecutionDirectoryPath}/metrics.txt";
            var metricsUrl = new Uri(await MapLocalPathToSasUrlAsync(metricsPath, getContainerSas: true));

            var taskCommand = $@"
                write_ts() {{ echo ""$1=$(date -Iseconds)"" >> /mnt{metricsPath}; }} && \
                mkdir -p /mnt{batchExecutionDirectoryPath} && \
                (grep -q alpine /etc/os-release && apk add bash || :) && \
                write_ts BlobXferPullStart && \
                docker pull --quiet {BlobxferImageName} && \
                write_ts BlobXferPullEnd && \
                {(executorImageIsPublic ? $"write_ts ExecutorPullStart && docker pull --quiet {executor.Image} && write_ts ExecutorPullEnd && \\" : "")}
                write_ts DownloadStart && \
                docker run --rm {volumeMountsOption} --entrypoint=/bin/sh {BlobxferImageName} {downloadFilesScriptPath} && \
                write_ts DownloadEnd && \
                chmod -R o+rwx /mnt{cromwellPathPrefixWithoutEndSlash} && \
                write_ts ExecutorStart && \
                docker run --rm {volumeMountsOption} --entrypoint= --workdir / {executor.Image} {executor.Command[0]} -c ""{ string.Join(" && ", executor.Command.Skip(1))}"" && \
                write_ts ExecutorEnd && \
                write_ts UploadStart && \
                docker run --rm {volumeMountsOption} --entrypoint=/bin/sh {BlobxferImageName} {uploadFilesScriptPath} && \
                write_ts UploadEnd && \
                /bin/bash -c 'df -k /mnt > disk.txt && read -a diskusage <<< $(tail -n 1 disk.txt) && echo DiskSizeInKB=${{diskusage[1]}} >> /mnt{metricsPath} && echo DiskUsageInKB=${{diskusage[2]}} >> /mnt{metricsPath}' && \
                docker run --rm {volumeMountsOption} {BlobxferImageName} upload --storage-url ""{metricsUrl}"" --local-path ""{metricsPath}"" --rename --no-recursive
            ";

            var batchExecutionDirectoryUrl = await MapLocalPathToSasUrlAsync($"{batchExecutionDirectoryPath}", getContainerSas: true);

            var cloudTask = new CloudTask(taskId, $"/bin/sh -c '{taskCommand.Trim()}'")
            {
                UserIdentity = new UserIdentity(new AutoUserSpecification(elevationLevel: ElevationLevel.Admin, scope: AutoUserScope.Pool)),
                ResourceFiles = new List<ResourceFile> { ResourceFile.FromUrl(downloadFilesScriptUrl, $"/mnt{downloadFilesScriptPath}"), ResourceFile.FromUrl(uploadFilesScriptUrl, $"/mnt{uploadFilesScriptPath}") },
                OutputFiles = new List<OutputFile> {
                    new OutputFile(
                        "../std*.txt",
                        new OutputFileDestination(new OutputFileBlobContainerDestination(batchExecutionDirectoryUrl)),
                        new OutputFileUploadOptions(OutputFileUploadCondition.TaskFailure))
                }
            };

            if (!executorImageIsPublic)
            {
                // If the executor image is private, and in order to run multiple containers in the main task, the image has to be downloaded via pool ContainerConfiguration.
                // This also requires that the main task runs inside a container. So we run the "docker" container that in turn runs other containers.
                // If the executor image is public, there is no need for pool ContainerConfiguration and task can run normally, without being wrapped in a docker container.
                // Volume mapping for docker.sock below allows the docker client in the container to access host's docker daemon.
                var containerRunOptions = $"--rm -v /var/run/docker.sock:/var/run/docker.sock -v /mnt{cromwellPathPrefixWithoutEndSlash}:/mnt{cromwellPathPrefixWithoutEndSlash} ";
                cloudTask.ContainerSettings = new TaskContainerSettings(DockerInDockerImageName, containerRunOptions);
            }

            return cloudTask;
        }

        /// <summary>
        /// Converts the input file URL into proper http URL with SAS token, ready for batch to download.
        /// Removes the query strings from the input file path and the command script content.
        /// Uploads the file if content is provided.
        /// </summary>
        /// <param name="inputFile"><see cref="TesInput"/> file</param>
        /// <param name="taskId">TES task Id</param>
        /// <param name="queryStringsToRemoveFromLocalFilePaths">Query strings to remove from local file paths</param>
        /// <returns>List of modified <see cref="TesInput"/> files</returns>
        private async Task<TesInput> GetTesInputFileUrl(TesInput inputFile, string taskId, List<string> queryStringsToRemoveFromLocalFilePaths)
        {
            if (inputFile.Path != null && !inputFile.Path.StartsWith(CromwellPathPrefix, StringComparison.OrdinalIgnoreCase))
            {
                throw new TesException("InvalidInputFilePath", $"Unsupported input path '{inputFile.Path}' for task Id {taskId}. Must start with '{CromwellPathPrefix}'.");
            }

            if (inputFile.Url != null && inputFile.Content != null)
            {
                throw new TesException("InvalidInputFilePath", "Input Url and Content cannot be both set");
            }

            if (inputFile.Url == null && inputFile.Content == null)
            {
                throw new TesException("InvalidInputFilePath", "One of Input Url or Content must be set");
            }

            if (inputFile.Type == TesFileType.DIRECTORYEnum)
            {
                throw new TesException("InvalidInputFilePath", "Directory input is not supported.");
            }

            string inputFileUrl;

            if (inputFile.Content != null || IsCromwellCommandScript(inputFile))
            {
                inputFileUrl = await MapLocalPathToSasUrlAsync(inputFile.Path);
                var writableUrl = new Uri(await MapLocalPathToSasUrlAsync(inputFile.Path, getContainerSas: true));

                var content = inputFile.Content ?? await azureProxy.DownloadBlobAsync(new Uri(inputFileUrl));
                content = IsCromwellCommandScript(inputFile) ? RemoveQueryStringsFromLocalFilePaths(content, queryStringsToRemoveFromLocalFilePaths) : content;

                await azureProxy.UploadBlobAsync(writableUrl, content);
            }
            else if (TryGetCromwellTmpFilePath(inputFile.Url, out var localPath))
            {
                inputFileUrl = await MapLocalPathToSasUrlAsync(inputFile.Path);
                var writableUrl = new Uri(await MapLocalPathToSasUrlAsync(inputFile.Path, getContainerSas: true));
                await azureProxy.UploadBlobFromFileAsync(writableUrl, localPath);
            }
            else if (await this.IsPublicHttpUrl(inputFile.Url))
            {
                inputFileUrl = inputFile.Url;
            }
            else
            {
                // Convert file:///account/container/blob paths to /account/container/blob
                var url = Uri.TryCreate(inputFile.Url, UriKind.Absolute, out var tempUrl) && tempUrl.IsFile ? tempUrl.AbsolutePath : inputFile.Url;

                var mappedUrl = await MapLocalPathToSasUrlAsync(url);

                if (mappedUrl != null)
                {
                    inputFileUrl = mappedUrl;
                }
                else
                {
                    throw new TesException("InvalidInputFilePath", $"Unsupported input URL '{inputFile.Url}' for task Id {taskId}. Must start with 'http', '{CromwellPathPrefix}' or use '/accountName/containerName/blobName' pattern where TES service has Contributor access to the storage account.");
                }
            }

            var path = RemoveQueryStringsFromLocalFilePaths(inputFile.Path, queryStringsToRemoveFromLocalFilePaths);
            return new TesInput { Url = inputFileUrl, Path = path };
        }

        /// <summary>
        /// Constructs an Azure Batch PoolInformation instance
        /// </summary>
        /// <param name="image">The image name for the current <see cref="TesTask"/></param>
        /// <param name="vmSize">The Azure VM sku</param>
        /// <param name="preemptible">True if preemptible machine should be used</param>
        /// <returns></returns>
        private async Task<PoolInformation> CreatePoolInformation(string image, string vmSize, bool preemptible)
        {
            var vmConfig = new VirtualMachineConfiguration(
                imageReference: new ImageReference("ubuntu-server-container", "microsoft-azure-batch", "16-04-lts", "latest"),
                nodeAgentSkuId: "batch.node.ubuntu 16.04");

            var containerRegistryInfo = await azureProxy.GetContainerRegistryInfoAsync(image);

            if (containerRegistryInfo != null)
            {
                var containerRegistry = new ContainerRegistry(
                    userName: containerRegistryInfo.Username,
                    registryServer: containerRegistryInfo.RegistryServer,
                    password: containerRegistryInfo.Password);

                // Download private images at node startup, since those cannot be downloaded in the main task that runs multiple containers.
                // Doing this also requires that the main task runs inside a container, hence downloading the "docker" image (contains docker client) as well.
                vmConfig.ContainerConfiguration = new ContainerConfiguration
                {
                    ContainerImageNames = new List<string> { image, DockerInDockerImageName },
                    ContainerRegistries = new List<ContainerRegistry> { containerRegistry }
                };
            }

            var poolSpecification = new PoolSpecification
            {
                VirtualMachineConfiguration = vmConfig,
                VirtualMachineSize = vmSize,
                ResizeTimeout = TimeSpan.FromMinutes(30),
                TargetLowPriorityComputeNodes = preemptible ? 1 : 0,
                TargetDedicatedComputeNodes = preemptible ? 0 : 1
            };

            var poolInformation = new PoolInformation
            {
                AutoPoolSpecification = new AutoPoolSpecification
                {
                    AutoPoolIdPrefix = "TES",
                    PoolLifetimeOption = PoolLifetimeOption.Job,
                    PoolSpecification = poolSpecification,
                    KeepAlive = false
                }
            };

            return poolInformation;
        }

        /// <summary>
        /// Removes a set of strings from the given string
        /// </summary>
        /// <param name="stringsToRemove">Strings to remove</param>
        /// <param name="originalString">The original string</param>
        /// <returns>The modified string</returns>
        private static string RemoveQueryStringsFromLocalFilePaths(string originalString, IEnumerable<string> stringsToRemove)
        {
            if (!stringsToRemove.Any(s => originalString.Contains(s, StringComparison.OrdinalIgnoreCase)))
            {
                return originalString;
            }

            var modifiedString = originalString;

            foreach (var stringToRemove in stringsToRemove)
            {
                modifiedString = modifiedString.Replace(stringToRemove, "", StringComparison.OrdinalIgnoreCase);
            }

            return modifiedString;
        }

        /// <summary>
        /// Check quotas for available active jobs, pool and CPU cores.
        /// </summary>
        /// <param name="workflowCoresRequirement">The core requirements of the current <see cref="TesTask"/>.</param>
        /// <param name="preemptible">True if preemptible cores are required.</param>
        private async Task CheckBatchAccountQuotas(int workflowCoresRequirement, bool preemptible)
        {
            var batchQuotas = await azureProxy.GetBatchAccountQuotasAsync();
            var coreQuota = preemptible ? batchQuotas.LowPriorityCoreQuota : batchQuotas.DedicatedCoreQuota;
            var poolQuota = batchQuotas.PoolQuota;
            var activeJobAndJobScheduleQuota = batchQuotas.ActiveJobAndJobScheduleQuota;

            var activeJobsCount = azureProxy.GetBatchActiveJobCount();
            var activePoolsCount = azureProxy.GetBatchActivePoolCount();
            var activeNodeCountByVmSize = azureProxy.GetBatchActiveNodeCountByVmSize();
            var virtualMachineInfoList = await azureProxy.GetVmSizesAndPricesAsync();

            var totalCoresInUse = activeNodeCountByVmSize
                .Sum(x => virtualMachineInfoList.FirstOrDefault(vm => vm.VmSize.Equals(x.VirtualMachineSize, StringComparison.OrdinalIgnoreCase)).NumberOfCores * (preemptible ? x.LowPriorityNodeCount : x.DedicatedNodeCount));

            if (workflowCoresRequirement > coreQuota)
            {
                // Here, the workflow task requires more cores than the total Batch account's cores quota - FAIL
                const string azureSupportUrl = "https://portal.azure.com/#blade/Microsoft_Azure_Support/HelpAndSupportBlade/newsupportrequest";
                throw new AzureBatchLowQuotaException($"Azure Batch Account does not have enough {(preemptible ? "low priority" : "dedicated")} cores quota to run a workflow with cpu core requirement of {workflowCoresRequirement}. Please submit an Azure Support request to increase your quota: {azureSupportUrl}");
            }

            if (activeJobsCount + 1 > activeJobAndJobScheduleQuota)
            {
                throw new AzureBatchQuotaMaxedOutException($"No remaining active jobs quota available. There are {activeJobsCount} active jobs out of {activeJobAndJobScheduleQuota}.");
            }

            if (activePoolsCount + 1 > poolQuota)
            {
                throw new AzureBatchQuotaMaxedOutException($"No remaining pool quota available. There are {activePoolsCount} pools in use out of {poolQuota}.");
            }

            if ((totalCoresInUse + workflowCoresRequirement) > coreQuota)
            {
                throw new AzureBatchQuotaMaxedOutException($"Not enough core quota remaining to schedule task requiring {workflowCoresRequirement} {(preemptible ? "low priority" : "dedicated")} cores. There are {totalCoresInUse} cores in use out of {coreQuota}.");
            }
        }

        /// <summary>
        /// Gets the cheapest available VM size that satisfies the <see cref="TesTask"/> execution requirements
        /// </summary>
        /// <param name="tesResources"><see cref="TesResources"/></param>
        /// <returns>The virtual machine info</returns>
        private async Task<VirtualMachineInfo> GetVmSizeAsync(TesResources tesResources)
        {
            var requiredNumberOfCores = tesResources.CpuCores.GetValueOrDefault(DefaultCoreCount);
            var requiredMemoryInGB = tesResources.RamGb.GetValueOrDefault(DefaultMemoryGb);
            var requiredDiskSizeInGB = tesResources.DiskGb.GetValueOrDefault(DefaultDiskGb);
            var preemptible = usePreemptibleVmsOnly || tesResources.Preemptible.GetValueOrDefault(true);

            var virtualMachineInfoList = await azureProxy.GetVmSizesAndPricesAsync();

            var selectedVm = virtualMachineInfoList
                .Where(x =>
                    x.LowPriority == preemptible
                    && x.NumberOfCores >= requiredNumberOfCores
                    && x.MemoryInGB >= requiredMemoryInGB
                    && x.ResourceDiskSizeInGB >= requiredDiskSizeInGB)
                .OrderBy(x => x.PricePerHour)
                .FirstOrDefault();

            if (selectedVm == null)
            {
                var alternateVm = virtualMachineInfoList
                    .Where(x =>
                        x.LowPriority == !preemptible
                        && x.NumberOfCores >= requiredNumberOfCores
                        && x.MemoryInGB >= requiredMemoryInGB
                        && x.ResourceDiskSizeInGB >= requiredDiskSizeInGB)
                    .OrderBy(x => x.PricePerHour)
                    .FirstOrDefault();

                var noVmFoundMessage = $"No VM (out of {virtualMachineInfoList.Count}) available with the required resources (cores: {requiredNumberOfCores}, memory: {requiredMemoryInGB} GB, disk: {requiredDiskSizeInGB} GB, preemptible: {preemptible})";

                if (alternateVm != null)
                {
                    noVmFoundMessage += $" Please note that a VM with LowPriority={!preemptible} WAS found however.";
                }

                logger.LogError(noVmFoundMessage);
                throw new AzureBatchVirtualMachineAvailabilityException(noVmFoundMessage);
            }

            return selectedVm;
        }

        private async Task<(BatchNodeMetrics BatchNodeMetrics, DateTimeOffset? TaskStartTime, DateTimeOffset? TaskEndTime, int? CromwellRcCode)> GetBatchNodeMetricsAndCromwellResultCodeAsync(TesTask tesTask)
        {
            static double? GetDurationInSeconds(Dictionary<string, string> dict, string startKey, string endKey)
            {
                return TryGetValueAsDateTimeOffset(dict, startKey, out var startTime) && TryGetValueAsDateTimeOffset(dict, endKey, out var endTime)
                    ? endTime.Subtract(startTime).TotalSeconds
                    : (double?)null;
            }

            static bool TryGetValueAsDateTimeOffset(Dictionary<string, string> dict, string key, out DateTimeOffset result)
            {
                result = default;
                return dict.TryGetValue(key, out var valueAsString) && DateTimeOffset.TryParse(valueAsString, out result);
            }

            static bool TryGetValueAsDouble(Dictionary<string, string> dict, string key, out double result)
            {
                result = default;
                return dict.TryGetValue(key, out var valueAsString) && double.TryParse(valueAsString, out result);
            }

            BatchNodeMetrics batchNodeMetrics = null;
            DateTimeOffset? taskStartTime = null;
            DateTimeOffset? taskEndTime = null;
            int? cromwellRcCode = null;

            try
            {
                var cromwellRcContent = await GetFileContentAsync($"{GetCromwellExecutionDirectoryPath(tesTask)}/rc");

                if (cromwellRcContent != null && int.TryParse(cromwellRcContent, out var temp))
                {
                    cromwellRcCode = temp;
                }

                var metricsContent = await GetFileContentAsync($"{GetBatchExecutionDirectoryPath(tesTask)}/metrics.txt");

                if (metricsContent != null)
                {
                    try
                    {
                        var metrics = DelimitedTextToDictionary(metricsContent.Trim());

                        var diskSizeInGB = TryGetValueAsDouble(metrics, "DiskSizeInKB", out var diskSizeKB)  ? diskSizeKB / 1024 / 1024 : (double?)null;
                        var diskUsageInGB = TryGetValueAsDouble(metrics, "DiskUsageInKB", out var diskUsageKB) ? diskUsageKB / 1024 / 1024 : (double?)null;

                        batchNodeMetrics = new BatchNodeMetrics
                        {
                            BlobXferImagePullDurationInSeconds = GetDurationInSeconds(metrics, "BlobXferPullStart", "BlobXferPullEnd"),
                            ExecutorImagePullDurationInSeconds = GetDurationInSeconds(metrics, "ExecutorPullStart", "ExecutorPullEnd"),
                            FileDownloadDurationInSeconds = GetDurationInSeconds(metrics, "DownloadStart", "DownloadEnd"),
                            ExecutorDurationInSeconds = GetDurationInSeconds(metrics, "ExecutorStart", "ExecutorEnd"),
                            FileUploadDurationInSeconds = GetDurationInSeconds(metrics, "UploadStart", "UploadEnd"),
                            MaxDiskUsageInGB = diskUsageInGB,
                            MaxDiskUsagePercent = diskUsageInGB.HasValue && diskSizeInGB.HasValue && diskSizeInGB > 0 ? (float?)(diskUsageInGB / diskSizeInGB) : null
                        };

                        taskStartTime = TryGetValueAsDateTimeOffset(metrics, "BlobXferPullStart", out var startTime) ? (DateTimeOffset?)startTime : null;
                        taskEndTime = TryGetValueAsDateTimeOffset(metrics, "UploadEnd", out var endTime) ? (DateTimeOffset?)endTime: null;
                    }
                    catch (Exception ex)
                    {
                        logger.LogError($"Failed to parse metrics for task {tesTask.Id}. Error: {ex.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogError($"Failed to get batch node metrics for task {tesTask.Id}. Error: {ex.Message}");
            }

            return (batchNodeMetrics, taskStartTime, taskEndTime, cromwellRcCode);


            // TODO TONY:
            //batchNodeMetrics.ExecutorImageSizeInGB = null;
            //batchNodeMetrics.FileDownloadSizeInGB = null;
            //batchNodeMetrics.FileUploadSizeInGB = null;
            //batchNodeMetrics.MaxDiskUsageInGB = null;
            //batchNodeMetrics.MaxDiskUsagePercent = null;
            //batchNodeMetrics.MaxResidentMemoryUsageInGB = null;
            //batchNodeMetrics.MaxResidentMemoryUsagePercent = null;
            // TODO:
            //executorLog.ExitCode = null; // Batch task exit code

            // Interating time points:
            // Trigger file created - creation time of the blob => needs to be picked up by Trigger service and written back to the trigger file
            // Trigger file sent to Cromwell => needs to be written by trigger service
            // Task added to TES by Cromwell = tesTask.CreatedTime - OK
            // Task sent to batch for execution = tesTask.TesTaskLog.BatchNodeMetrics.TimeScheduledForExecution - could be tesTask.Logs[].StartTime
            // Task actually started running on the node (before downloads started) = tesTask.TesTaskLog.BatchNodeMetrics.TimeExecutionStartedOnNode, could be tesTask.Logs[].Logs[].StartTime
            // Downloads size and duration (images and files) = tesTask.TesTaskLog.BatchNodeMetrics.FileDownloadSizeInGB / FileDownloadDurationInSeconds
            // Executor script started running on the node (after all downloads)
            // Executor script finished running on the node (or just capture duration) = tesTask.TesTaskLog.BatchNodeMetrics.ExecutorDurationInSeconds
            // Uploads size and duration = tesTask.TesTaskLog.BatchNodeMetricsFileUploadSizeInGB / FileUploadDurationInSeconds
            // Task ended running on the node = tesTask.TesTaskLog.BatchNodeMetrics.TimeExecutionEndedOnNode, could be tesTask.Logs[].Logs[].EndTime
            // Batch/TES realized the task is done and set the task state to completed = tesTask.EndTime - OK, although the value could be obtained from the tesTask.Logs[].EndTime of the last log (usually one)

            // Interesting resource usage:
            // Max resident memory consumed on the node = MaxResidentMemoryUsageInGB
            // Max resident memory consumed on the node as percentage of total = MaxResidentMemoryUsagePercent
            // Max disk consumed on the node = MaxDiskUsageInGB
            // Max disk consumed on the node as percentage of total = MaxDiskUsagePercent

            // Start/end time provided by the original TES:
            // tesTask.StartTime - Date + time the task was created
            // tesTask.Logs[].StartTime - When the task started (there is a Logs item for each execution attempt - There might be multiple? When do we add a new one? ). Could be sent to batch.
            // tesTask.Logs[].EndTime - When the task ended (there is a Logs item for each execution attempt - same q as above). Could be TES realized batch is done.
            // tesTask.Logs[].Logs[].StartTime - Time the executor started (there is a Logs item for each executor - there is only one). Could be actual code started running on the node.
            // tesTask.Logs[].Logs[].EndTime - Time the executor ended (there is a Logs item for each executor - there is only one). Could be actual code ended running on the node.
            // Added by us:
            // tesTask.EndTime = date time when task went to final state. Could be equal to tesTask.Logs[].EndTime of the last Log item (usually one).

            // Other:
            // tesTask.Logs[].Logs[].ExitCode - exit code of the executor - as reported by batch, not Cromwell RC
            // tesTask.Logs[].SystemLogs[]: We could use this to record final_error_code, instead of or in addition to tesTask.Logs[].FinalErrorCode
            //   - System logs are any logs the system decides are relevant, which are not tied directly to an Executor process. 
            //   - Content is implementation specific: format, size, etc.  System logs may be collected here to provide convenient access.  
            //   - For example, the system may include the name of the host where the task is executing, an error message that caused a SYSTEM_ERROR state (e.g. disk is full), etc.  
            //   - System logs are only included in the FULL task view.


            // TesTask log was conventient place to add new properties as it has the Metadata dictionary intended for this purpose.
            // When the task is done, it should be easy to get the following:
            // Overall state = tesTask.State
            // For failed tasks, the reason for failure = tesTask.Logs[LAST].FailureReason - may want to bring this to tesTask.FailureReason, but not serialize it. May want to also write to tesTask.Logs[].SystemLogs[] array as item 0, so it is exposed via TES APIs
            // For completed tasks, Cromwell RC code = tesTask.Logs[LAST].CromwellResultCode - may want to bring this to tesTask.CromwellResultCode, but not serialize it.

            // Trigger service needs to write to Trigger file:
            // 1. Why did my workflow end up in failed folder?
            // 2. How can I reduce the cost and duration of the workflow? Which steps need more/less computing resources?
            // Count of: total number of tasks, running, completed successfully, failed. overall duration, total vm time, total est cost
            // List of failed tasks (state is EXECUTOR_ERROR/SYSTEM_ERROR or have CromwellRC != 0), along with FailureReason for each task.
            // FailedTasks: TaskNameWithShard, FailureReason, CromwellRC
            // Should FailureReason be set to "NonZeroCromwellResultCode" when task is COMPLETED but has non-zero Cromwell RC, or should it be left empty in CosmosDb, since it didn't actually fail in TES/Batch? Should it be set it in the report only?
            // Should we consider missing RC code (but that should fail the task)?
            // Should we include the tail of the stderr from the execution if RC != 0 and tail from __batch stderr if executor/system error? These could go to TaskLog.SystemLog or TaskLog.ExecutorLog, and include this in trigger file just for the first failed task?
            // Approximate cost of the execution, summarized by task name (excluding the shard number), plus total VM time, total elapsed time and cost
            // Memory and disk usage percentage for each task in the workflow, Or grouped by (task name w/o shard + VM mem/disk requested and allocated) (with max GB and % for mem/disk, average/min/max duration of execution on the node incl. up/down)
            // TaskName, DiskRequestd, MemoryRequested, DiskAllocated, MemoryAllocated, MaxDiskGB, MaxDiskPct, MaxMemGB, MaxMemPct, AvgDurationSec, MaxDurationSec, MinDurationSec, InstanceCount, TotalVmTime, TotalVmCost, only consider reporting successful tasks here.


            // May want to separate TES model from TesApi.Web, so it can be referenced by Trigger service
        }

        private async Task<string> GetFileContentAsync(string path)
        {
            try
            {
                return await azureProxy.DownloadBlobAsync(new Uri(await MapLocalPathToSasUrlAsync(path)));
            }
            catch
            {
                return null;
            }
        }

        private static Dictionary<string, string> DelimitedTextToDictionary(string text, string fieldDelimiter = "=", string rowDelimiter = "\n")
        {
            return text.Split(rowDelimiter)
                .Select(line => { var parts = line.Split(fieldDelimiter); return new KeyValuePair<string, string>(parts[0], parts[1]); })
                .ToDictionary(kv => kv.Key, kv => kv.Value);
        }

        /// <summary>
        /// Class that captures how <see cref="TesTask"/> transitions from current state to the new state, given the current Batch task state and optional condition. 
        /// Transitions typically include an action that needs to run in order for the task to move to the new state.
        /// </summary>
        private class TesTaskStateTransition
        {
            public TesTaskStateTransition(Func<TesTask, bool> condition, BatchTaskState? batchTaskState, Func<TesTask, CombinedBatchTaskInfo, Task> asyncAction)
                : this(condition, batchTaskState, asyncAction, null)
            {
            }

            public TesTaskStateTransition(Func<TesTask, bool> condition, BatchTaskState? batchTaskState, Action<TesTask, CombinedBatchTaskInfo> action)
                : this(condition, batchTaskState, null, action)
            {
            }

            private TesTaskStateTransition(Func<TesTask, bool> condition, BatchTaskState? batchTaskState, Func<TesTask, CombinedBatchTaskInfo, Task> asyncAction, Action<TesTask, CombinedBatchTaskInfo> action)
            {
                Condition = condition;
                CurrentBatchTaskState = batchTaskState;
                AsyncAction = asyncAction;
                Action = action;
            }

            public Func<TesTask, bool> Condition { get; set; }
            public BatchTaskState? CurrentBatchTaskState { get; set; }
            public Func<TesTask, CombinedBatchTaskInfo, Task> AsyncAction { get; set; }
            public Action<TesTask, CombinedBatchTaskInfo> Action { get; set; }
        }

        private class ExternalStorageContainerInfo
        {
            public string AccountName { get; set; }
            public string ContainerName { get; set; }
            public string BlobEndpoint { get; set; }
            public string SasToken { get; set; }
        }

        private class CombinedBatchTaskInfo
        {
            public BatchTaskState BatchTaskState { get; set; }
            public BatchNodeMetrics BatchNodeMetrics { get; set; }
            public string FailureReason { get; set; }
            public DateTimeOffset? BatchTaskStartTime { get; set; }
            public DateTimeOffset? BatchTaskEndTime { get; set; }
            public int? BatchTaskExitCode { get; set; }
            public int? CromwellRcCode { get; set; }
            public IEnumerable<string> SystemLogItems { get; set; }
        }
    }
}
