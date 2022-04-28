﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

using Azure;

using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using Newtonsoft.Json;

using Tes.Extensions;
using Tes.Models;
using Tes.Repository;

using BatchModels = Microsoft.Azure.Management.Batch.Models;

namespace TesApi.Web
{
    /// <summary>
    /// Orchestrates <see cref="TesTask"/>s on Azure Batch
    /// </summary>
    public class BatchScheduler : IBatchScheduler, IBatchPoolsImpl
    {
        /// <summary>
        /// CosmosDB container id for storing pool metadata that needs to survive reboots/etc.
        /// </summary>
        public const string CosmosDbContainerId = "Pools";

        private static readonly Guid PoolNameNamespace = new("719FCE51-66D8-4822-AC04-746E56B95893");
        private const string AzureSupportUrl = "https://portal.azure.com/#blade/Microsoft_Azure_Support/HelpAndSupportBlade/newsupportrequest";
        private const int DefaultCoreCount = 1;
        private const int DefaultMemoryGb = 2;
        private const int DefaultDiskGb = 10;
        private const string CromwellPathPrefix = "/cromwell-executions/";
        private const string CromwellScriptFileName = "script";
        private const string BatchExecutionDirectoryName = "__batch";
        private const string BatchScriptFileName = "batch_script";
        private const string UploadFilesScriptFileName = "upload_files_script";
        private const string DownloadFilesScriptFileName = "download_files_script";
        private const string startTaskScriptFilename = "start-task.sh";
        private static readonly string batchStartTaskLocalPathOnBatchNode = $"/mnt/batch/tasks/startup/wd/{startTaskScriptFilename}";
        private static readonly Regex queryStringRegex = new(@"[^\?.]*(\?.*)");
        private readonly string dockerInDockerImageName;
        private readonly string blobxferImageName;
        private readonly string cromwellDrsLocalizerImageName;
        private readonly ILogger logger;
        private readonly IAzureProxy azureProxy;
        private readonly IStorageAccessProvider storageAccessProvider;
        private readonly IEnumerable<string> allowedVmSizes;
        private readonly List<TesTaskStateTransition> tesTaskStateTransitions;
        private readonly bool usePreemptibleVmsOnly;
        private readonly string batchNodesSubnetId;
        private readonly bool disableBatchNodesPublicIpAddress;
        private readonly bool enableBatchAutopool;
        private readonly BatchNodeInfo batchNodeInfo;
        private readonly string marthaUrl;
        private readonly string marthaKeyVaultName;
        private readonly string marthaSecretName;
        //private readonly string defaultStorageAccountName;
        private readonly BatchPoolFactory _poolFactory;
        private readonly IRepository<BatchPool.PoolData> _poolDataRepository;
        private readonly Random random = new();

        /// <summary>
        /// Orchestrates <see cref="TesTask"/>s on Azure Batch
        /// </summary>
        /// <param name="logger">Logger <see cref="ILogger"/></param>
        /// <param name="configuration">Configuration <see cref="IConfiguration"/></param>
        /// <param name="azureProxy">Azure proxy <see cref="IAzureProxy"/></param>
        /// <param name="storageAccessProvider">Storage access provider <see cref="IStorageAccessProvider"/></param>
        /// <param name="poolDataRepository"></param>
        /// <param name="poolFactory"></param>
        public BatchScheduler(ILogger<BatchScheduler> logger, IConfiguration configuration, IAzureProxy azureProxy, IStorageAccessProvider storageAccessProvider, IRepository<BatchPool.PoolData> poolDataRepository, BatchPoolFactory poolFactory)
        {
            this.logger = logger;
            this.azureProxy = azureProxy;
            this.storageAccessProvider = storageAccessProvider;

            static bool GetBoolValue(IConfiguration configuration, string key, bool defaultValue) => string.IsNullOrWhiteSpace(configuration[key]) ? defaultValue : bool.Parse(configuration[key]);
            static string GetStringValue(IConfiguration configuration, string key, string defaultValue = "") => string.IsNullOrWhiteSpace(configuration[key]) ? defaultValue : configuration[key];

            this.allowedVmSizes = GetStringValue(configuration, "AllowedVmSizes", null)?.Split(',', StringSplitOptions.RemoveEmptyEntries).ToList();
            this.usePreemptibleVmsOnly = GetBoolValue(configuration, "UsePreemptibleVmsOnly", false);
            this.batchNodesSubnetId = GetStringValue(configuration, "BatchNodesSubnetId", string.Empty);
            this.dockerInDockerImageName = GetStringValue(configuration, "DockerInDockerImageName", "docker");
            this.blobxferImageName = GetStringValue(configuration, "BlobxferImageName", "mcr.microsoft.com/blobxfer");
            this.cromwellDrsLocalizerImageName = GetStringValue(configuration, "CromwellDrsLocalizerImageName", "broadinstitute/cromwell-drs-localizer:develop");
            this.disableBatchNodesPublicIpAddress = GetBoolValue(configuration, "DisableBatchNodesPublicIpAddress", false);
            this.enableBatchAutopool = GetBoolValue(configuration, "BatchAutopool", false);
            //this.defaultStorageAccountName = GetStringValue(configuration, "DefaultStorageAccountName", string.Empty);
            this.marthaUrl = GetStringValue(configuration, "MarthaUrl", string.Empty);
            this.marthaKeyVaultName = GetStringValue(configuration, "MarthaKeyVaultName", string.Empty);
            this.marthaSecretName = GetStringValue(configuration, "MarthaSecretName", string.Empty);

            if (!this.enableBatchAutopool)
            {
                batchPools = new(this, this.logger);
                _poolDataRepository = poolDataRepository;
                _poolFactory = poolFactory;
            }

            this.batchNodeInfo = new BatchNodeInfo
            {
                BatchImageOffer = GetStringValue(configuration, "BatchImageOffer"),
                BatchImagePublisher = GetStringValue(configuration, "BatchImagePublisher"),
                BatchImageSku = GetStringValue(configuration, "BatchImageSku"),
                BatchImageVersion = GetStringValue(configuration, "BatchImageVersion"),
                BatchNodeAgentSkuId = GetStringValue(configuration, "BatchNodeAgentSkuId")
            };

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

                // Only accurate when the task completes successfully, otherwise it's the Batch time as reported from Batch
                // TODO this could get large; why?
                //var timefromCoAScriptCompletionToBatchTaskDetectedComplete = tesTaskLog.EndTime - tesTaskExecutorLog.EndTime;

                tesTask.SetFailureReason(batchInfo.FailureReason);

                if (batchInfo.SystemLogItems is not null)
                {
                    tesTask.AddToSystemLog(batchInfo.SystemLogItems);
                }
            }

            async Task SetTaskCompleted(TesTask tesTask, CombinedBatchTaskInfo batchInfo)
            {
                await ReleaseComputeNodeReservationOrRemovePool(batchInfo);
                await azureProxy.DeleteBatchJobAsync(tesTask.Id, TryGet);
                SetTaskStateAndLog(tesTask, TesState.COMPLETEEnum, batchInfo);
            }

            async Task SetTaskExecutorError(TesTask tesTask, CombinedBatchTaskInfo batchInfo)
            {
                await ReleaseComputeNodeReservationOrRemovePool(batchInfo);
                await azureProxy.DeleteBatchJobAsync(tesTask.Id, TryGet);
                SetTaskStateAndLog(tesTask, TesState.EXECUTORERROREnum, batchInfo);
            }

            async Task SetTaskSystemError(TesTask tesTask, CombinedBatchTaskInfo batchInfo)
            {
                await ReleaseComputeNodeReservationOrRemovePool(batchInfo);
                await azureProxy.DeleteBatchJobAsync(tesTask.Id, TryGet);
                SetTaskStateAndLog(tesTask, TesState.SYSTEMERROREnum, batchInfo);
            }

            async Task DeleteBatchJobAndSetTaskStateAsync(TesTask tesTask, TesState newTaskState, CombinedBatchTaskInfo batchInfo)
            {
                await ReleaseComputeNodeReservationOrRemovePool(batchInfo);
                await this.azureProxy.DeleteBatchJobAsync(tesTask.Id, TryGet);
                SetTaskStateAndLog(tesTask, newTaskState, batchInfo); 
            }
            Task DeleteBatchJobAndSetTaskExecutorErrorAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo) => DeleteBatchJobAndSetTaskStateAsync(tesTask, TesState.EXECUTORERROREnum, batchInfo);
            Task DeleteBatchJobAndSetTaskSystemErrorAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo) => DeleteBatchJobAndSetTaskStateAsync(tesTask, TesState.SYSTEMERROREnum, batchInfo);

            Task DeleteBatchJobAndRequeueTaskAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo) => ++tesTask.ErrorCount > 3
                ? DeleteBatchJobAndSetTaskExecutorErrorAsync(tesTask, batchInfo)
                : DeleteBatchJobAndSetTaskStateAsync(tesTask, TesState.QUEUEDEnum, batchInfo);

            async Task CancelTaskAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo)
            {
                await ReleaseComputeNodeReservationOrRemovePool(batchInfo);
                await this.azureProxy.DeleteBatchJobAsync(tesTask.Id, TryGet);
                tesTask.IsCancelRequested = false; 
            }

            async Task ReleaseComputeNodeReservationOrRemovePool(CombinedBatchTaskInfo batchInfo)
            {
                if (enableBatchAutopool)
                {
                    if (!string.IsNullOrWhiteSpace(batchInfo.PoolId) && !batchInfo.PoolId.StartsWith("TES"))
                    {
                        await azureProxy.DeleteBatchPoolAsync(batchInfo.PoolId);
                    }
                }
                else
                {
                    if (TryGet(batchInfo.PoolId, out var pool))
                    {
                        pool.ReleaseNode(batchInfo.JobId);
                        pool.ReleaseNode(batchInfo.AffinityInformation);
                    }
                }
            }

            tesTaskStateTransitions = new List<TesTaskStateTransition>()
            {
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
                new TesTaskStateTransition(tesTaskIsInitializingOrRunning, BatchTaskState.NodePreempted, DeleteBatchJobAndRequeueTaskAsync)
            };
        }

        /// <summary>
        /// Iteratively manages execution of a <see cref="TesTask"/> on Azure Batch until completion or failure
        /// </summary>
        /// <param name="tesTask">The <see cref="TesTask"/></param>
        /// <returns>True if the TES task needs to be persisted.</returns>
        public async ValueTask<bool> ProcessTesTaskAsync(TesTask tesTask)
            => await HandleTesTaskTransitionAsync(tesTask, await GetBatchTaskStateAsync(tesTask));

        private static string GetCromwellExecutionDirectoryPath(TesTask task)
            => GetParentPath(task.Inputs?.FirstOrDefault(IsCromwellCommandScript)?.Path);

        private static string GetBatchExecutionDirectoryPath(TesTask task)
            => $"{GetCromwellExecutionDirectoryPath(task)}/{BatchExecutionDirectoryName}";

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
            => inputFile.Name.Equals("commandScript");

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

            return localPath is not null;
        }

        /// <summary>
        /// Adds a new Azure Batch pool/job/task for the given <see cref="TesTask"/>
        /// </summary>
        /// <param name="tesTask">The <see cref="TesTask"/> to schedule on Azure Batch</param>
        /// <returns>A task to await</returns>
        private async Task AddBatchJobAsync(TesTask tesTask)
        {
            PoolInformation poolInformation = null;

            try
            {
                var jobId = await azureProxy.GetNextBatchJobIdAsync(tesTask.Id);
                var virtualMachineInfo = await GetVmSizeAsync(tesTask);

                var IsIdentityProvided = tesTask.Resources?.ContainsBackendParameterValue(TesResources.SupportedBackendParameters.workflow_execution_identity) == true;
                var identityResourceId = IsIdentityProvided ? tesTask.Resources?.GetBackendParameterValue(TesResources.SupportedBackendParameters.workflow_execution_identity) : default;

                var poolName = GeneratePoolName(
                    IsIdentityProvided
                    ? $"{virtualMachineInfo.VmSize}-{Path.GetFileName(identityResourceId)}"
                    : virtualMachineInfo.VmSize);

                await CheckBatchAccountQuotas(virtualMachineInfo, poolName);

                var tesTaskLog = tesTask.AddTesTaskLog();
                tesTaskLog.VirtualMachineInfo = virtualMachineInfo;
                // TODO?: Support for multiple executors. Cromwell has single executor per task.
                var dockerImage = tesTask.Executors.First().Image;

                var containerConfiguration = new Lazy<ValueTask<ContainerConfiguration>>(async () => await ContainerConfigurationIfNeeded(dockerImage));
                var startTask = new Lazy<ValueTask<StartTask>>(async () => await StartTaskIfNeeded(tesTask));

                Func<ValueTask<AffinityInformation>> getAffinity = () => default;
                if (this.enableBatchAutopool)
                {
                    poolInformation = await CreateAutoPoolModePoolInformation(
                        virtualMachineInfo.VmSize,
                        virtualMachineInfo.LowPriority,
                        await containerConfiguration.Value,
                        await startTask.Value,
                        jobId,
                        identityResourceId);
                }
                else
                {
                    poolInformation = (await GetOrAddAsync(poolName, async id => ConvertPoolSpecificationToModelsPool(
                            name: id,
                            displayName: poolName,
                            GetBatchPoolIdentity(identityResourceId),
                            GetPoolSpecification(virtualMachineInfo.VmSize, null, await containerConfiguration.Value, await startTask.Value))))
                        .Pool;

                    getAffinity = () => TryGet(poolInformation.PoolId, out var pool) ? pool.PrepareNodeAsync(jobId, virtualMachineInfo.LowPriority) : ValueTask.FromResult<AffinityInformation>(default);
                }

                var cloudTask = await ConvertTesTaskToBatchTaskAsync(tesTask, getAffinity, (await containerConfiguration.Value) is not null);
                logger.LogInformation($"Creating batch job for TES task {tesTask.Id}. Using VM size {virtualMachineInfo.VmSize}.");
                await azureProxy.CreateBatchJobAsync(jobId, cloudTask, poolInformation, TryGet);

                tesTaskLog.StartTime = DateTimeOffset.UtcNow;
                tesTask.State = TesState.INITIALIZINGEnum;
                poolInformation = null;
            }
            catch (AggregateException ex) when (ex.InnerException is AzureBatchQuotaMaxedOutException)
            {
                logger.LogDebug($"Not enough quota available for task Id {tesTask.Id}. Reason: {ex.InnerException.Message}. Task will remain in queue.");
            }
            catch (AzureBatchQuotaMaxedOutException exception)
            {
                logger.LogDebug($"Not enough quota available for task Id {tesTask.Id}. Reason: {exception.Message}. Task will remain in queue.");
            }
            catch (AzureBatchLowQuotaException exception)
            {
                tesTask.State = TesState.SYSTEMERROREnum;
                tesTask.SetFailureReason("InsufficientBatchQuota", exception.Message);
                logger.LogError(exception.Message);
            }
            catch (AzureBatchVirtualMachineAvailabilityException exception)
            {
                tesTask.State = TesState.SYSTEMERROREnum;
                tesTask.AddTesTaskLog(); // Adding new log here because this exception is thrown from GetVmSizeAsync() and AddTesTaskLog() above is called after that. This way each attempt will have its own log entry.
                tesTask.SetFailureReason("NoVmSizeAvailable", exception.Message);
                logger.LogError(exception.Message);
            }
            catch (TesException exc)
            {
                tesTask.State = TesState.SYSTEMERROREnum;
                tesTask.SetFailureReason(exc);
                logger.LogError(exc, exc.Message);
            }
            catch (BatchClientException exc)
            {
                tesTask.State = TesState.SYSTEMERROREnum;
                tesTask.SetFailureReason("BatchClientException", string.Join(",", exc.Data.Values), exc.Message, exc.StackTrace);
                logger.LogError(exc, exc.Message + ", " + string.Join(",", exc.Data.Values));
            }
            catch (Exception exc)
            {
                tesTask.State = TesState.SYSTEMERROREnum;
                tesTask.SetFailureReason("UnknownError", exc.Message, exc.StackTrace);
                logger.LogError(exc, exc.Message);
            }
            finally
            {
                if (enableBatchAutopool && poolInformation is not null && poolInformation.AutoPoolSpecification is null)
                {
                    await azureProxy.DeleteBatchPoolAsync(poolInformation.PoolId);
                }
            }
        }

        private static string GeneratePoolName(string name)
            => $"CoA-TES-{PoolNameNamespace.GenerateGuid(name)}-pool";

        /// <summary>
        /// Gets the current state of the Azure Batch task
        /// </summary>
        /// <param name="tesTask"><see cref="TesTask"/></param>
        /// <returns>A higher-level abstraction of the current state of the Azure Batch task</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1826:Do not use Enumerable methods on indexable collections", Justification = "FirstOrDefault() is straightforward, the alternative is less clear.")]
        private async Task<CombinedBatchTaskInfo> GetBatchTaskStateAsync(TesTask tesTask)
        {
            var azureBatchJobAndTaskState = await azureProxy.GetBatchJobAndTaskStateAsync(tesTask.Id);

            static IEnumerable<string> ConvertNodeErrorsToSystemLogItems(AzureBatchJobAndTaskState azureBatchJobAndTaskState)
            {
                var systemLogItems = new List<string>();

                if (azureBatchJobAndTaskState.NodeErrorCode is not null)
                {
                    systemLogItems.Add(azureBatchJobAndTaskState.NodeErrorCode);
                }

                if (azureBatchJobAndTaskState.NodeErrorDetails is not null)
                {
                    systemLogItems.AddRange(azureBatchJobAndTaskState.NodeErrorDetails);
                }

                return systemLogItems;
            }

            if (azureBatchJobAndTaskState.ActiveJobWithMissingAutoPool)
            {
                var batchJobInfo = JsonConvert.SerializeObject(azureBatchJobAndTaskState);
                logger.LogWarning($"Found active job without auto pool for TES task {tesTask.Id}. Deleting the job and requeuing the task. BatchJobInfo: {batchJobInfo}");
                return new CombinedBatchTaskInfo
                {
                    BatchTaskState = BatchTaskState.ActiveJobWithMissingAutoPool,
                    FailureReason = BatchTaskState.ActiveJobWithMissingAutoPool.ToString(),
                    PoolId = azureBatchJobAndTaskState.PoolId,
                    JobId = azureBatchJobAndTaskState.JobId,
                    AffinityInformation = azureBatchJobAndTaskState.AffinityInformation,
                    ComputeNodeInformation = azureBatchJobAndTaskState.ComputeNodeInformation
                };
            }

            if (azureBatchJobAndTaskState.MoreThanOneActiveJobFound)
            {
                return new CombinedBatchTaskInfo
                {
                    BatchTaskState = BatchTaskState.MoreThanOneActiveJobFound,
                    FailureReason = BatchTaskState.MoreThanOneActiveJobFound.ToString(),
                    PoolId = azureBatchJobAndTaskState.PoolId,
                    JobId = azureBatchJobAndTaskState.JobId,
                    AffinityInformation = azureBatchJobAndTaskState.AffinityInformation,
                    ComputeNodeInformation = azureBatchJobAndTaskState.ComputeNodeInformation
                };
            }

            switch (azureBatchJobAndTaskState.JobState)
            {
                case null:
                case JobState.Deleting:
                    return new CombinedBatchTaskInfo
                    {
                        BatchTaskState = BatchTaskState.JobNotFound,
                        FailureReason = BatchTaskState.JobNotFound.ToString(),
                        PoolId = azureBatchJobAndTaskState.PoolId,
                        JobId = azureBatchJobAndTaskState.JobId,
                        AffinityInformation = azureBatchJobAndTaskState.AffinityInformation,
                        ComputeNodeInformation = azureBatchJobAndTaskState.ComputeNodeInformation
                    };
                case JobState.Active:
                    {
                        if (azureBatchJobAndTaskState.NodeAllocationFailed)
                        {
                            return new CombinedBatchTaskInfo
                            {
                                BatchTaskState = BatchTaskState.NodeAllocationFailed,
                                FailureReason = BatchTaskState.NodeAllocationFailed.ToString(),
                                SystemLogItems = ConvertNodeErrorsToSystemLogItems(azureBatchJobAndTaskState),
                                PoolId = azureBatchJobAndTaskState.PoolId,
                                JobId = azureBatchJobAndTaskState.JobId,
                                AffinityInformation = azureBatchJobAndTaskState.AffinityInformation,
                                ComputeNodeInformation = azureBatchJobAndTaskState.ComputeNodeInformation
                            };
                        }

                        if (azureBatchJobAndTaskState.NodeState == ComputeNodeState.Unusable)
                        {
                            return new CombinedBatchTaskInfo
                            {
                                BatchTaskState = BatchTaskState.NodeUnusable,
                                FailureReason = BatchTaskState.NodeUnusable.ToString(),
                                SystemLogItems = ConvertNodeErrorsToSystemLogItems(azureBatchJobAndTaskState),
                                PoolId = azureBatchJobAndTaskState.PoolId,
                                JobId = azureBatchJobAndTaskState.JobId,
                                AffinityInformation = azureBatchJobAndTaskState.AffinityInformation,
                                ComputeNodeInformation = azureBatchJobAndTaskState.ComputeNodeInformation
                            };
                        }

                        if (azureBatchJobAndTaskState.NodeState == ComputeNodeState.Preempted)
                        {
                            return new CombinedBatchTaskInfo
                            {
                                BatchTaskState = BatchTaskState.NodePreempted,
                                FailureReason = BatchTaskState.NodePreempted.ToString(),
                                SystemLogItems = ConvertNodeErrorsToSystemLogItems(azureBatchJobAndTaskState),
                                PoolId = azureBatchJobAndTaskState.PoolId,
                                JobId = azureBatchJobAndTaskState.JobId,
                                AffinityInformation = azureBatchJobAndTaskState.AffinityInformation,
                                ComputeNodeInformation = azureBatchJobAndTaskState.ComputeNodeInformation
                            };
                        }

                        if (azureBatchJobAndTaskState.NodeErrorCode is not null)
                        {
                            if (azureBatchJobAndTaskState.NodeErrorCode == "DiskFull")
                            {
                                return new CombinedBatchTaskInfo
                                {
                                    BatchTaskState = BatchTaskState.NodeFailedDuringStartupOrExecution,
                                    FailureReason = azureBatchJobAndTaskState.NodeErrorCode,
                                    PoolId = azureBatchJobAndTaskState.PoolId,
                                    JobId = azureBatchJobAndTaskState.JobId,
                                    AffinityInformation = azureBatchJobAndTaskState.AffinityInformation,
                                    ComputeNodeInformation = azureBatchJobAndTaskState.ComputeNodeInformation
                                };
                            }
                            else
                            {
                                return new CombinedBatchTaskInfo
                                {
                                    BatchTaskState = BatchTaskState.NodeFailedDuringStartupOrExecution,
                                    FailureReason = BatchTaskState.NodeFailedDuringStartupOrExecution.ToString(),
                                    SystemLogItems = ConvertNodeErrorsToSystemLogItems(azureBatchJobAndTaskState),
                                    PoolId = azureBatchJobAndTaskState.PoolId,
                                    JobId = azureBatchJobAndTaskState.JobId,
                                    AffinityInformation = azureBatchJobAndTaskState.AffinityInformation,
                                    ComputeNodeInformation = azureBatchJobAndTaskState.ComputeNodeInformation
                                };
                            }
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
                    return new CombinedBatchTaskInfo
                    {
                        BatchTaskState = BatchTaskState.MissingBatchTask,
                        FailureReason = BatchTaskState.MissingBatchTask.ToString(),
                        PoolId = azureBatchJobAndTaskState.PoolId,
                        JobId = azureBatchJobAndTaskState.JobId,
                        AffinityInformation = azureBatchJobAndTaskState.AffinityInformation,
                        ComputeNodeInformation = azureBatchJobAndTaskState.ComputeNodeInformation
                    };
                case TaskState.Active:
                case TaskState.Preparing:
                    return new CombinedBatchTaskInfo
                    {
                        BatchTaskState = BatchTaskState.Initializing,
                        PoolId = azureBatchJobAndTaskState.PoolId,
                        JobId = azureBatchJobAndTaskState.JobId,
                        AffinityInformation = azureBatchJobAndTaskState.AffinityInformation,
                        ComputeNodeInformation = azureBatchJobAndTaskState.ComputeNodeInformation
                    };
                case TaskState.Running:
                    return new CombinedBatchTaskInfo
                    {
                        BatchTaskState = BatchTaskState.Running,
                        PoolId = azureBatchJobAndTaskState.PoolId,
                        JobId = azureBatchJobAndTaskState.JobId,
                        AffinityInformation = azureBatchJobAndTaskState.AffinityInformation,
                        ComputeNodeInformation = azureBatchJobAndTaskState.ComputeNodeInformation
                    };
                case TaskState.Completed:
                    var batchJobInfo = JsonConvert.SerializeObject(azureBatchJobAndTaskState);

                    if (azureBatchJobAndTaskState.TaskExitCode == 0 && azureBatchJobAndTaskState.TaskFailureInformation is null)
                    {
                        var metrics = await GetBatchNodeMetricsAndCromwellResultCodeAsync(tesTask);

                        return new CombinedBatchTaskInfo
                        {
                            BatchTaskState = BatchTaskState.CompletedSuccessfully,
                            BatchTaskExitCode = azureBatchJobAndTaskState.TaskExitCode,
                            BatchTaskStartTime = metrics.TaskStartTime ?? azureBatchJobAndTaskState.TaskStartTime,
                            BatchTaskEndTime = metrics.TaskEndTime ?? azureBatchJobAndTaskState.TaskEndTime,
                            BatchNodeMetrics = metrics.BatchNodeMetrics,
                            CromwellRcCode = metrics.CromwellRcCode,
                            PoolId = azureBatchJobAndTaskState.PoolId,
                            JobId = azureBatchJobAndTaskState.JobId,
                            AffinityInformation = azureBatchJobAndTaskState.AffinityInformation,
                            ComputeNodeInformation = azureBatchJobAndTaskState.ComputeNodeInformation
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
                            SystemLogItems = new[] { azureBatchJobAndTaskState.TaskFailureInformation?.Details?.FirstOrDefault()?.Value },
                            PoolId = azureBatchJobAndTaskState.PoolId,
                            JobId = azureBatchJobAndTaskState.JobId,
                            AffinityInformation = azureBatchJobAndTaskState.AffinityInformation,
                            ComputeNodeInformation = azureBatchJobAndTaskState.ComputeNodeInformation
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
            await ScheduleReimage(combinedBatchTaskInfo.ComputeNodeInformation, combinedBatchTaskInfo.BatchTaskState);

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
                .FirstOrDefault(m => (m.Condition is null || m.Condition(tesTask)) && (m.CurrentBatchTaskState is null || m.CurrentBatchTaskState == combinedBatchTaskInfo.BatchTaskState));

            if (mapItem is not null)
            {
                if (mapItem.AsyncAction is not null)
                {
                    await mapItem.AsyncAction(tesTask, combinedBatchTaskInfo);
                    tesTaskChanged = true;
                }

                if (mapItem.Action is not null)
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
        /// <param name="GetAffinityInfo"></param>
        /// <param name="poolHasContainerConfig">Indicates that <see cref="CloudTask.ContainerSettings"/> must be set.</param>
        /// <returns>Job preparation and main Batch tasks</returns>
        private async Task<CloudTask> ConvertTesTaskToBatchTaskAsync(TesTask task, Func<ValueTask<AffinityInformation>> GetAffinityInfo, bool poolHasContainerConfig)
        {
            var cromwellPathPrefixWithoutEndSlash = CromwellPathPrefix.TrimEnd('/');
            var taskId = task.Id;

            var queryStringsToRemoveFromLocalFilePaths = task.Inputs
                .Select(i => i.Path)
                .Concat(task.Outputs.Select(o => o.Path))
                .Where(p => p is not null)
                .Select(p => queryStringRegex.Match(p).Groups[1].Value)
                .Where(qs => !string.IsNullOrEmpty(qs))
                .ToList();

            var inputFiles = task.Inputs.Distinct();

            var drsInputFiles = inputFiles
                .Where(f => f?.Url?.StartsWith("drs://", StringComparison.OrdinalIgnoreCase) == true)
                .ToList();

            var cromwellExecutionDirectoryPath = GetCromwellExecutionDirectoryPath(task);

            if (cromwellExecutionDirectoryPath is null)
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
            var metricsPath = $"{batchExecutionDirectoryPath}/metrics.txt";
            var metricsUrl = new Uri(await this.storageAccessProvider.MapLocalPathToSasUrlAsync(metricsPath, getContainerSas: true));

            // TODO: Cromwell bug: Cromwell command write_tsv() generates a file in the execution directory, for example execution/write_tsv_3922310b441805fc43d52f293623efbc.tmp. These are not passed on to TES inputs.
            // WORKAROUND: Get the list of files in the execution directory and add them to task inputs.
            var executionDirectoryUri = new Uri(await this.storageAccessProvider.MapLocalPathToSasUrlAsync(cromwellExecutionDirectoryPath, getContainerSas: true));
            var blobsInExecutionDirectory = (await azureProxy.ListBlobsAsync(executionDirectoryUri)).Where(b => !b.EndsWith($"/{CromwellScriptFileName}")).Where(b => !b.Contains($"/{BatchExecutionDirectoryName}/"));
            var additionalInputFiles = blobsInExecutionDirectory.Select(b => $"{CromwellPathPrefix}{b}").Select(b => new TesInput { Content = null, Path = b, Url = b, Name = Path.GetFileName(b), Type = TesFileType.FILEEnum });
            
            var filesToDownload = await Task.WhenAll(
                inputFiles
                .Where(f => f?.Url?.StartsWith("drs://", StringComparison.OrdinalIgnoreCase) != true) // do not attempt to download DRS input files since the cromwell-drs-localizer will
                .Union(additionalInputFiles)
                .Select(async f => await GetTesInputFileUrl(f, task.Id, queryStringsToRemoveFromLocalFilePaths)));

            const string exitIfDownloadedFileIsNotFound = "{ [ -f \"$path\" ] && : || { echo \"Failed to download file $url\" 1>&2 && exit 1; } }";
            const string incrementTotalBytesTransferred = "total_bytes=$(( $total_bytes + `stat -c %s \"$path\"` ))";

            // Using --include and not using --no-recursive as a workaround for https://github.com/Azure/blobxfer/issues/123
            var downloadFilesScriptContent = "total_bytes=0 && "
                + string.Join(" && ", filesToDownload.Select(f => {
                    var setVariables = $"path='{f.Path}' && url='{f.Url}'";

                    var downloadSingleFile = f.Url.Contains(".blob.core.")
                        ? $"blobxfer download --storage-url \"$url\" --local-path \"$path\" --chunk-size-bytes 104857600 --rename --include '{StorageAccountUrlSegments.Create(f.Url).BlobName}'"
                        : "mkdir -p $(dirname \"$path\") && wget -O \"$path\" \"$url\"";

                    return $"{setVariables} && {downloadSingleFile} && {exitIfDownloadedFileIsNotFound} && {incrementTotalBytesTransferred}"; }))
                + $" && echo FileDownloadSizeInBytes=$total_bytes >> {metricsPath}";

            var downloadFilesScriptPath = $"{batchExecutionDirectoryPath}/{DownloadFilesScriptFileName}";
            var downloadFilesScriptUrl = await this.storageAccessProvider.MapLocalPathToSasUrlAsync(downloadFilesScriptPath);
            await this.storageAccessProvider.UploadBlobAsync(downloadFilesScriptPath, downloadFilesScriptContent);

            var filesToUpload = await Task.WhenAll(
                task.Outputs.Select(async f =>
                    new TesOutput { Path = f.Path, Url = await this.storageAccessProvider.MapLocalPathToSasUrlAsync(f.Path, getContainerSas: true), Name = f.Name, Type = f.Type }));

            // Ignore missing stdout/stderr files. CWL workflows have an issue where if the stdout/stderr are redirected, they are still listed in the TES outputs
            // Ignore any other missing files and directories. WDL tasks can have optional output files.
            // Syntax is: If file or directory doesn't exist, run a noop (":") operator, otherwise run the upload command:
            // { if not exists do nothing else upload; } && { ... }
            var uploadFilesScriptContent = "total_bytes=0 && "
                + string.Join(" && ", filesToUpload.Select(f => {
                    var setVariables = $"path='{f.Path}' && url='{f.Url}'";
                    var blobxferCommand = $"blobxfer upload --storage-url \"$url\" --local-path \"$path\" --one-shot-bytes 104857600 {(f.Type == TesFileType.FILEEnum ? "--rename --no-recursive" : string.Empty)}";

                    return $"{{ {setVariables} && [ ! -e \"$path\" ] && : || {{ {blobxferCommand} && {incrementTotalBytesTransferred}; }} }}";
                }))
                + $" && echo FileUploadSizeInBytes=$total_bytes >> {metricsPath}";

            var uploadFilesScriptPath = $"{batchExecutionDirectoryPath}/{UploadFilesScriptFileName}";
            var uploadFilesScriptSasUrl = await this.storageAccessProvider.MapLocalPathToSasUrlAsync(uploadFilesScriptPath);
            await this.storageAccessProvider.UploadBlobAsync(uploadFilesScriptPath, uploadFilesScriptContent);

            var executor = task.Executors.First();

            var volumeMountsOption = $"-v /mnt{cromwellPathPrefixWithoutEndSlash}:{cromwellPathPrefixWithoutEndSlash}";

            var executorImageIsPublic = (await azureProxy.GetContainerRegistryInfoAsync(executor.Image)) is null;
            var dockerInDockerImageIsPublic = (await azureProxy.GetContainerRegistryInfoAsync(dockerInDockerImageName)) is null;
            var blobXferImageIsPublic = (await azureProxy.GetContainerRegistryInfoAsync(blobxferImageName)) is null;

            var sb = new StringBuilder();

            sb.AppendLine($"write_kv() {{ echo \"$1=$2\" >> /mnt{metricsPath}; }} && \\");  // Function that appends key=value pair to metrics.txt file
            sb.AppendLine($"write_ts() {{ write_kv $1 $(date -Iseconds); }} && \\");    // Function that appends key=<current datetime> to metrics.txt file
            sb.AppendLine($"mkdir -p /mnt{batchExecutionDirectoryPath} && \\");

            if (dockerInDockerImageIsPublic)
            {
                sb.AppendLine($"(grep -q alpine /etc/os-release && apk add bash || :) && \\");  // Install bash if running on alpine (will be the case if running inside "docker" image)
            }

            var vmSize = task.Resources?.GetBackendParameterValue(TesResources.SupportedBackendParameters.vm_size);

            if (drsInputFiles.Count > 0 && task.Resources?.ContainsBackendParameterValue(TesResources.SupportedBackendParameters.workflow_execution_identity) == true)
            {
                sb.AppendLine($"write_ts CromwellDrsLocalizerPullStart && \\");
                sb.AppendLine($"docker pull --quiet {cromwellDrsLocalizerImageName} && \\");
                sb.AppendLine($"write_ts CromwellDrsLocalizerPullEnd && \\");
            }

            if (blobXferImageIsPublic)
            {
                sb.AppendLine($"write_ts BlobXferPullStart && \\");
                sb.AppendLine($"docker pull --quiet {blobxferImageName} && \\");
                sb.AppendLine($"write_ts BlobXferPullEnd && \\");
            }

            if (executorImageIsPublic)
            {
                // Private executor images are pulled via pool ContainerConfiguration
                sb.AppendLine($"write_ts ExecutorPullStart && docker pull --quiet {executor.Image} && write_ts ExecutorPullEnd && \\");
            }

            // The remainder of the script downloads the inputs, runs the main executor container, and uploads the outputs, including the metrics.txt file
            // After task completion, metrics file is downloaded and used to populate the BatchNodeMetrics object
            sb.AppendLine($"write_kv ExecutorImageSizeInBytes $(docker inspect {executor.Image} | grep \\\"Size\\\" | grep -Po '(?i)\\\"Size\\\":\\K([^,]*)') && \\");

            if (drsInputFiles.Count > 0)
            {
                // resolve DRS input files with Cromwell DRS Localizer Docker image
                sb.AppendLine($"write_ts DrsLocalizationStart && \\");

                foreach (var drsInputFile in drsInputFiles)
                {
                    var drsUrl = drsInputFile.Url;
                    var localizedFilePath = drsInputFile.Path;
                    var drsLocalizationCommand = $"docker run --rm {volumeMountsOption} -e MARTHA_URL=\"{marthaUrl}\" {cromwellDrsLocalizerImageName} {drsUrl} {localizedFilePath} --access-token-strategy azure{(!string.IsNullOrWhiteSpace(marthaKeyVaultName) ? " --vault-name " + marthaKeyVaultName : string.Empty)}{(!string.IsNullOrWhiteSpace(marthaSecretName) ? " --secret-name " + marthaSecretName : string.Empty)} && \\";
                    sb.AppendLine(drsLocalizationCommand);
                }

                sb.AppendLine($"write_ts DrsLocalizationEnd && \\");
            }

            sb.AppendLine($"write_ts DownloadStart && \\");
            sb.AppendLine($"docker run --rm {volumeMountsOption} --entrypoint=/bin/sh {blobxferImageName} {downloadFilesScriptPath} && \\");
            sb.AppendLine($"write_ts DownloadEnd && \\");
            sb.AppendLine($"chmod -R o+rwx /mnt{cromwellPathPrefixWithoutEndSlash} && \\");
            sb.AppendLine($"write_ts ExecutorStart && \\");
            sb.AppendLine($"docker run --rm {volumeMountsOption} --entrypoint= --workdir / {executor.Image} {executor.Command[0]} -c \"{ string.Join(" && ", executor.Command.Skip(1))}\" && \\");
            sb.AppendLine($"write_ts ExecutorEnd && \\");
            sb.AppendLine($"write_ts UploadStart && \\");
            sb.AppendLine($"docker run --rm {volumeMountsOption} --entrypoint=/bin/sh {blobxferImageName} {uploadFilesScriptPath} && \\");
            sb.AppendLine($"write_ts UploadEnd && \\");
            sb.AppendLine($"/bin/bash -c 'disk=( `df -k /mnt | tail -1` ) && echo DiskSizeInKiB=${{disk[1]}} >> /mnt{metricsPath} && echo DiskUsedInKiB=${{disk[2]}} >> /mnt{metricsPath}' && \\");
            sb.AppendLine($"write_kv VmCpuModelName \"$(cat /proc/cpuinfo | grep -m1 name | cut -f 2 -d ':' | xargs)\" && \\");
            sb.AppendLine($"docker run --rm {volumeMountsOption} {blobxferImageName} upload --storage-url \"{metricsUrl}\" --local-path \"{metricsPath}\" --rename --no-recursive");

            var batchScriptPath = $"{batchExecutionDirectoryPath}/{BatchScriptFileName}";
            await this.storageAccessProvider.UploadBlobAsync(batchScriptPath, sb.ToString());

            var batchScriptSasUrl = await this.storageAccessProvider.MapLocalPathToSasUrlAsync(batchScriptPath);
            var batchExecutionDirectorySasUrl = await this.storageAccessProvider.MapLocalPathToSasUrlAsync($"{batchExecutionDirectoryPath}", getContainerSas: true);

            var cloudTask = new CloudTask(taskId, $"/bin/sh /mnt{batchScriptPath}")
            {
                UserIdentity = new UserIdentity(new AutoUserSpecification(elevationLevel: ElevationLevel.Admin, scope: AutoUserScope.Pool)),
                ResourceFiles = new List<ResourceFile> { ResourceFile.FromUrl(batchScriptSasUrl, $"/mnt{batchScriptPath}"), ResourceFile.FromUrl(downloadFilesScriptUrl, $"/mnt{downloadFilesScriptPath}"), ResourceFile.FromUrl(uploadFilesScriptSasUrl, $"/mnt{uploadFilesScriptPath}") },
                OutputFiles = new List<OutputFile> {
                    new OutputFile(
                        "../std*.txt",
                        new OutputFileDestination(new OutputFileBlobContainerDestination(batchExecutionDirectorySasUrl)),
                        new OutputFileUploadOptions(OutputFileUploadCondition.TaskFailure))
                }
            };

            if (poolHasContainerConfig)
            {
                // If the executor image is private, and in order to run multiple containers in the main task, the image has to be downloaded via pool ContainerConfiguration.
                // This also requires that the main task runs inside a container. So we run the "docker" container that in turn runs other containers.
                // If the executor image is public, there is no need for pool ContainerConfiguration and task can run normally, without being wrapped in a docker container.
                // Volume mapping for docker.sock below allows the docker client in the container to access host's docker daemon.
                var containerRunOptions = $"--rm -v /var/run/docker.sock:/var/run/docker.sock -v /mnt:/mnt ";
                cloudTask.ContainerSettings = new TaskContainerSettings(dockerInDockerImageName, containerRunOptions);
            }

            cloudTask.AffinityInformation = await GetAffinityInfo();
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
            if (inputFile.Path is not null && !inputFile.Path.StartsWith(CromwellPathPrefix, StringComparison.OrdinalIgnoreCase))
            {
                throw new TesException("InvalidInputFilePath", $"Unsupported input path '{inputFile.Path}' for task Id {taskId}. Must start with '{CromwellPathPrefix}'.");
            }

            if (inputFile.Url is not null && inputFile.Content is not null)
            {
                throw new TesException("InvalidInputFilePath", "Input Url and Content cannot be both set");
            }

            if (inputFile.Url is null && inputFile.Content is null)
            {
                throw new TesException("InvalidInputFilePath", "One of Input Url or Content must be set");
            }

            if (inputFile.Type == TesFileType.DIRECTORYEnum)
            {
                throw new TesException("InvalidInputFilePath", "Directory input is not supported.");
            }

            string inputFileUrl;

            if (inputFile.Content is not null || IsCromwellCommandScript(inputFile))
            {
                inputFileUrl = await this.storageAccessProvider.MapLocalPathToSasUrlAsync(inputFile.Path);

                var content = inputFile.Content ?? await this.storageAccessProvider.DownloadBlobAsync(inputFile.Path);
                content = IsCromwellCommandScript(inputFile) ? RemoveQueryStringsFromLocalFilePaths(content, queryStringsToRemoveFromLocalFilePaths) : content;

                await this.storageAccessProvider.UploadBlobAsync(inputFile.Path, content);
            }
            else if (TryGetCromwellTmpFilePath(inputFile.Url, out var localPath))
            {
                inputFileUrl = await this.storageAccessProvider.MapLocalPathToSasUrlAsync(inputFile.Path);
                await this.storageAccessProvider.UploadBlobFromFileAsync(inputFile.Path, localPath);
            }
            else if (await this.storageAccessProvider.IsPublicHttpUrl(inputFile.Url))
            {
                inputFileUrl = inputFile.Url;
            }
            else
            {
                // Convert file:///account/container/blob paths to /account/container/blob
                var url = Uri.TryCreate(inputFile.Url, UriKind.Absolute, out var tempUrl) && tempUrl.IsFile ? tempUrl.AbsolutePath : inputFile.Url;
                inputFileUrl = (await this.storageAccessProvider.MapLocalPathToSasUrlAsync(url)) ?? throw new TesException("InvalidInputFilePath", $"Unsupported input URL '{inputFile.Url}' for task Id {taskId}. Must start with 'http', '{CromwellPathPrefix}' or use '/accountName/containerName/blobName' pattern where TES service has Contributor access to the storage account.");
            }

            var path = RemoveQueryStringsFromLocalFilePaths(inputFile.Path, queryStringsToRemoveFromLocalFilePaths);
            return new TesInput { Url = inputFileUrl, Path = path };
        }

        /// <summary>
        /// Constructs an Azure Batch Container Configuration instance
        /// </summary>
        /// <param name="tesTask">The <see cref="TesTask"/> to schedule on Azure Batch</param>
        /// <returns></returns>
        private async Task<StartTask> StartTaskIfNeeded(TesTask tesTask)
        {
            await Task.Delay(1);
            // <param name="batchExecutionDirectoryPath">Relative path to the Batch execution location</param>
            // <param name="startTaskSasUrl">SAS URL for the start task</param>
            // <param name="startTaskPath">Local path on the Azure Batch node for the script</param>
            //var batchExecutionPath = GetBatchExecutionDirectoryPath(tesTask);
            //string startTaskSasUrl = null;

            // string startTaskSasUrl = null, string startTaskPath = null

            //BatchModels.StartTask result = default;

            //if (useStartTask)
            //{
            //    var scriptPath = $"{batchExecutionPath}/start-task.sh";
            //    await this.storageAccessProvider.UploadBlobAsync(scriptPath, BatchUtils.StartTaskScript);
            //    startTaskSasUrl = await this.storageAccessProvider.MapLocalPathToSasUrlAsync(scriptPath);
            //}

            //if (useStartTask)
            //{
            //    var scriptPath = $"{batchExecutionDirectoryPath}/{startTaskScriptFilename}";
            //    await this.storageAccessProvider.UploadBlobAsync(scriptPath, BatchUtils.StartTaskScript);
            //    var scriptSasUrl = await this.storageAccessProvider.MapLocalPathToSasUrlAsync(scriptPath);

            //    startTask = new Microsoft.Azure.Batch.StartTask
            //    {
            //        // Pool StartTask: install Docker as start task if it's not already
            //        CommandLine = $"sudo /bin/sh {batchStartTaskLocalPathOnBatchNode}",
            //        UserIdentity = new UserIdentity(new AutoUserSpecification(elevationLevel: ElevationLevel.Admin, scope: AutoUserScope.Pool)),
            //        ResourceFiles = new List<ResourceFile> { ResourceFile.FromUrl(scriptSasUrl, batchStartTaskLocalPathOnBatchNode) }
            //    };
            //}

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

            return default;
        }

        /// <summary>
        /// Constructs an Azure Batch Container Configuration instance
        /// </summary>
        /// <param name="executorImage">The image name for the current <see cref="TesTask"/></param>
        /// <returns></returns>
        private async Task<ContainerConfiguration> ContainerConfigurationIfNeeded(string executorImage)
        {
            BatchModels.ContainerConfiguration result = default;
            var containerRegistryInfo = await azureProxy.GetContainerRegistryInfoAsync(executorImage);

            if (containerRegistryInfo is not null)
            {
                var containerRegistry = new BatchModels.ContainerRegistry(
                    userName: containerRegistryInfo.Username,
                    registryServer: containerRegistryInfo.RegistryServer,
                    password: containerRegistryInfo.Password);

                // Download private images at node startup, since those cannot be downloaded in the main task that runs multiple containers.
                // Doing this also requires that the main task runs inside a container, hence downloading the "docker" image (contains docker client) as well.
                result = new BatchModels.ContainerConfiguration
                {
                    ContainerImageNames = new List<string> { executorImage, dockerInDockerImageName, blobxferImageName },
                    ContainerRegistries = new List<BatchModels.ContainerRegistry> { containerRegistry }
                };

                var containerRegistryInfoForDockerInDocker = await azureProxy.GetContainerRegistryInfoAsync(dockerInDockerImageName);

                if (containerRegistryInfoForDockerInDocker is not null && containerRegistryInfoForDockerInDocker.RegistryServer != containerRegistryInfo.RegistryServer)
                {
                    var containerRegistryForDockerInDocker = new BatchModels.ContainerRegistry(
                        userName: containerRegistryInfoForDockerInDocker.Username,
                        registryServer: containerRegistryInfoForDockerInDocker.RegistryServer,
                        password: containerRegistryInfoForDockerInDocker.Password);

                    result.ContainerRegistries.Add(containerRegistryForDockerInDocker);
                }

                var containerRegistryInfoForBlobXfer = await azureProxy.GetContainerRegistryInfoAsync(blobxferImageName);

                if (containerRegistryInfoForBlobXfer is not null && containerRegistryInfoForBlobXfer.RegistryServer != containerRegistryInfo.RegistryServer && containerRegistryInfoForBlobXfer.RegistryServer != containerRegistryInfoForDockerInDocker.RegistryServer)
                {
                    result.ContainerRegistries.Add(new(
                        userName: containerRegistryInfoForBlobXfer.Username,
                        registryServer: containerRegistryInfoForBlobXfer.RegistryServer,
                        password: containerRegistryInfoForBlobXfer.Password));
                }
            }

            return result is null ? null : new()
            {
                ContainerImageNames = result.ContainerImageNames,
                ContainerRegistries = result
                                    .ContainerRegistries
                                    .Select(r => new ContainerRegistry(
                                        userName: r.UserName,
                                        password: r.Password,
                                        registryServer: r.RegistryServer,
                                        identityReference: r.IdentityReference is null ? null : new() { ResourceId = r.IdentityReference.ResourceId }))
                                    .ToList()
            };
        }

        /// <summary>
        /// Constructs an Azure Batch PoolInformation instance
        /// </summary>
        /// <param name="vmSize">The Azure VM sku</param>
        /// <param name="preemptible">True if preemptible machine should be used</param>
        /// <param name="containerConfiguration">The configuration to download private images</param>
        /// <param name="startTask">The start task for all jobs/tasks in the pool</param>
        /// <param name="jobId"></param>
        /// <param name="identityResourceId"></param>
        /// <returns>An Azure Batch Pool specifier</returns>
        private async Task<PoolInformation> CreateAutoPoolModePoolInformation(string vmSize, bool preemptible, ContainerConfiguration containerConfiguration, StartTask startTask, string jobId = null, string identityResourceId = null)
        {
            if (!string.IsNullOrWhiteSpace(identityResourceId))
            {
                _ = jobId ?? throw new ArgumentNullException(nameof(jobId));
            }

            var poolSpecification = GetPoolSpecification(vmSize, preemptible, containerConfiguration, startTask);

            // By default, the pool will have the same name/ID as the job if the identity is provided, otherwise we return an actual autopool.
            return string.IsNullOrWhiteSpace(identityResourceId)
                ? new()
                {
                    AutoPoolSpecification = new()
                    {
                        AutoPoolIdPrefix = "TES",
                        PoolLifetimeOption = PoolLifetimeOption.Job,
                        PoolSpecification = poolSpecification,
                        KeepAlive = false
                    }
                }
                : await azureProxy.CreateBatchPoolAsync(
                    ConvertPoolSpecificationToModelsPool(
                        jobId,
                        jobId,
                        GetBatchPoolIdentity(identityResourceId),
                        poolSpecification));
        }

        /// <summary>
        /// Generate the BatchPoolIdentity object
        /// </summary>
        /// <param name="identityResourceId"></param>
        /// <returns></returns>
        private static BatchModels.BatchPoolIdentity GetBatchPoolIdentity(string identityResourceId)
            => string.IsNullOrWhiteSpace(identityResourceId) ? null : new(BatchModels.PoolIdentityType.UserAssigned, new Dictionary<string, BatchModels.UserAssignedIdentities>() { [identityResourceId] = new() });

        /// <summary>
        /// Generate the PoolSpecification object
        /// </summary>
        /// <param name="vmSize"></param>
        /// <param name="preemptible"></param>
        /// <param name="containerConfiguration"></param>
        /// <param name="startTask"></param>
        /// <returns></returns>
        private PoolSpecification GetPoolSpecification(string vmSize, bool? preemptible, ContainerConfiguration containerConfiguration, StartTask startTask)
        {
            var vmConfig = new VirtualMachineConfiguration(
                imageReference: new ImageReference(
                    batchNodeInfo.BatchImageOffer,
                    batchNodeInfo.BatchImagePublisher,
                    batchNodeInfo.BatchImageSku,
                    batchNodeInfo.BatchImageVersion),
                nodeAgentSkuId: batchNodeInfo.BatchNodeAgentSkuId)
            {
                ContainerConfiguration = containerConfiguration
            };

            var poolSpecification = new PoolSpecification
            {
                VirtualMachineConfiguration = vmConfig,
                VirtualMachineSize = vmSize,
                ResizeTimeout = TimeSpan.FromMinutes(30),
                TargetLowPriorityComputeNodes = preemptible == true ? 1 : 0,
                TargetDedicatedComputeNodes = preemptible == false ? 1 : 0,
                StartTask = startTask
            };

            if (!string.IsNullOrEmpty(this.batchNodesSubnetId))
            {
                poolSpecification.NetworkConfiguration = new()
                {
                    PublicIPAddressConfiguration = new PublicIPAddressConfiguration(this.disableBatchNodesPublicIpAddress ? IPAddressProvisioningType.NoPublicIPAddresses : IPAddressProvisioningType.BatchManaged),
                    SubnetId = this.batchNodesSubnetId
                };
            }

            return poolSpecification;
        }

        /// <summary>
        /// Convert PoolSpecification to Models.Pool, including any BatchPoolIdentity
        /// </summary>
        /// <param name="name"></param>
        /// <param name="displayName"></param>
        /// <param name="poolIdentity"></param>
        /// <param name="pool"></param>
        /// <returns></returns>
        private static BatchModels.Pool ConvertPoolSpecificationToModelsPool(string name, string displayName, BatchModels.BatchPoolIdentity poolIdentity, PoolSpecification pool)
        {
            return new(name: name, displayName: displayName, identity: poolIdentity)
            {
                VmSize = pool.VirtualMachineSize,
                ScaleSettings = new()
                {
                    FixedScale = new()
                    {
                        TargetDedicatedNodes = pool.TargetDedicatedComputeNodes,
                        TargetLowPriorityNodes = pool.TargetLowPriorityComputeNodes,
                        ResizeTimeout = pool.ResizeTimeout,
                        NodeDeallocationOption = BatchModels.ComputeNodeDeallocationOption.TaskCompletion
                    }
                },
                DeploymentConfiguration = new()
                {
                    VirtualMachineConfiguration = new(ConvertImageReference(pool.VirtualMachineConfiguration.ImageReference), pool.VirtualMachineConfiguration.NodeAgentSkuId, containerConfiguration: ConvertContainerConfiguration(pool.VirtualMachineConfiguration.ContainerConfiguration))
                },
                NetworkConfiguration = ConvertNetworkConfiguration(pool.NetworkConfiguration),
                StartTask = ConvertStartTask(pool.StartTask)
            };

            static BatchModels.ContainerConfiguration ConvertContainerConfiguration(ContainerConfiguration containerConfiguration)
                => containerConfiguration is null ? default : new(containerConfiguration.ContainerImageNames, containerConfiguration.ContainerRegistries?.Select(ConvertContainerRegistry).ToList());

            static BatchModels.StartTask ConvertStartTask(StartTask startTask)
                => startTask is null ? default : new(startTask.CommandLine, startTask.ResourceFiles?.Select(ConvertResourceFile).ToList(), startTask.EnvironmentSettings?.Select(ConvertEnvironmentSetting).ToList(), ConvertUserIdentity(startTask.UserIdentity), startTask.MaxTaskRetryCount, startTask.WaitForSuccess, ConvertTaskContainerSettings(startTask.ContainerSettings));

            static BatchModels.UserIdentity ConvertUserIdentity(UserIdentity userIdentity)
                => userIdentity is null ? default : new(userIdentity.UserName, ConvertAutoUserSpecification(userIdentity.AutoUser));

            static BatchModels.AutoUserSpecification ConvertAutoUserSpecification(AutoUserSpecification autoUserSpecification)
                => autoUserSpecification is null ? default : new((BatchModels.AutoUserScope?)autoUserSpecification.Scope, (BatchModels.ElevationLevel?)autoUserSpecification.ElevationLevel);

            static BatchModels.TaskContainerSettings ConvertTaskContainerSettings(TaskContainerSettings containerSettings)
                => containerSettings is null ? default : new(containerSettings.ImageName, containerSettings.ContainerRunOptions, ConvertContainerRegistry(containerSettings.Registry), (BatchModels.ContainerWorkingDirectory?)containerSettings.WorkingDirectory);
 
            static BatchModels.ContainerRegistry ConvertContainerRegistry(ContainerRegistry containerRegistry)
                => containerRegistry is null ? default : new(containerRegistry.UserName, containerRegistry.Password, containerRegistry.RegistryServer, ConvertComputeNodeIdentityReference(containerRegistry.IdentityReference));

            static BatchModels.ResourceFile ConvertResourceFile(ResourceFile resourceFile)
                => resourceFile is null ? default : new(resourceFile.AutoStorageContainerName, resourceFile.StorageContainerUrl, resourceFile.HttpUrl, resourceFile.BlobPrefix, resourceFile.FilePath, resourceFile.FileMode, ConvertComputeNodeIdentityReference(resourceFile.IdentityReference));

            static BatchModels.ComputeNodeIdentityReference ConvertComputeNodeIdentityReference(ComputeNodeIdentityReference computeNodeIdentityReference)
                => computeNodeIdentityReference is null ? default : new(computeNodeIdentityReference.ResourceId);

            static BatchModels.EnvironmentSetting ConvertEnvironmentSetting(EnvironmentSetting environmentSetting)
                => environmentSetting is null ? default : new(environmentSetting.Name, environmentSetting.Value);

            static BatchModels.ImageReference ConvertImageReference(ImageReference imageReference)
                => imageReference is null ? default : new(imageReference.Publisher, imageReference.Offer, imageReference.Sku, imageReference.Version);

            static BatchModels.NetworkConfiguration ConvertNetworkConfiguration(NetworkConfiguration networkConfiguration)
                => networkConfiguration is null ? default : new(subnetId: networkConfiguration.SubnetId, publicIPAddressConfiguration: ConvertPublicIPAddressConfiguration(networkConfiguration.PublicIPAddressConfiguration));

            static BatchModels.PublicIPAddressConfiguration ConvertPublicIPAddressConfiguration(PublicIPAddressConfiguration publicIPAddressConfiguration)
                => publicIPAddressConfiguration is null ? default : new(provision: (BatchModels.IPAddressProvisioningType?)publicIPAddressConfiguration.Provision);
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
                modifiedString = modifiedString.Replace(stringToRemove, string.Empty, StringComparison.OrdinalIgnoreCase);
            }

            return modifiedString;
        }

        /// <summary>
        /// Check quotas for available active jobs, pool and CPU cores.
        /// </summary>
        /// <param name="vmInfo">Dedicated virtual machine information.</param>
        /// <param name="poolName">Name of pool</param>
        private async Task CheckBatchAccountQuotas(VirtualMachineInformation vmInfo, string poolName)
        {
            var workflowCoresRequirement = vmInfo.NumberOfCores.Value;
            var preemptible = vmInfo.LowPriority;
            var vmFamily = vmInfo.VmFamily;

            var batchQuotas = await azureProxy.GetBatchAccountQuotasAsync();
            var coreQuota = preemptible ? batchQuotas.LowPriorityCoreQuota : batchQuotas.DedicatedCoreQuota;
            var vmFamQuota = preemptible || !batchQuotas.DedicatedCoreQuotaPerVMFamilyEnforced ? workflowCoresRequirement : batchQuotas.DedicatedCoreQuotaPerVMFamily.FirstOrDefault(q => vmFamily.Equals(q.Name, StringComparison.OrdinalIgnoreCase))?.CoreQuota ?? 0;
            var poolQuota = batchQuotas.PoolQuota;
            var activeJobAndJobScheduleQuota = batchQuotas.ActiveJobAndJobScheduleQuota;

            var activeJobsCount = azureProxy.GetBatchActiveJobCount();
            var activePoolsCount = azureProxy.GetBatchActivePoolCount();
            var activeNodeCountByVmSize = azureProxy.GetBatchActiveNodeCountByVmSize().ToList();
            var virtualMachineInfoList = await azureProxy.GetVmSizesAndPricesAsync();

            var totalCoresInUse = activeNodeCountByVmSize
                .Sum(x => virtualMachineInfoList.FirstOrDefault(vm => vm.VmSize.Equals(x.VirtualMachineSize, StringComparison.OrdinalIgnoreCase)).NumberOfCores * (preemptible ? x.LowPriorityNodeCount : x.DedicatedNodeCount));

            var totalCoresInUseByVmFam = preemptible ? 0 : activeNodeCountByVmSize
                .Where(x => vmInfo.VmSize.Equals(x.VirtualMachineSize, StringComparison.OrdinalIgnoreCase))
                .Sum(x => x.DedicatedNodeCount * workflowCoresRequirement);

            if (workflowCoresRequirement > coreQuota)
            {
                // Here, the workflow task requires more cores than the total Batch account's cores quota - FAIL
                throw new AzureBatchLowQuotaException($"Azure Batch Account does not have enough {(preemptible ? "low priority" : "dedicated")} cores quota to run a workflow with cpu core requirement of {workflowCoresRequirement}. Please submit an Azure Support request to increase your quota: {AzureSupportUrl}");
            }

            if (workflowCoresRequirement > vmFamQuota)
            {
                // Here, the workflow task requires more cores than the total Batch account's dedicated family quota - FAIL
                throw new AzureBatchLowQuotaException($"Azure Batch Account does not have enough dedicated {vmFamily} cores quota to run a workflow with cpu core requirement of {workflowCoresRequirement}. Please submit an Azure Support request to increase your quota: {AzureSupportUrl}");
            }

            if (activeJobsCount + 1 > activeJobAndJobScheduleQuota)
            {
                throw new AzureBatchQuotaMaxedOutException($"No remaining active jobs quota available. There are {activeJobsCount} active jobs out of {activeJobAndJobScheduleQuota}");
            }

            if ((this.enableBatchAutopool || !IsPoolAvailable(poolName)) && activePoolsCount + 1 > poolQuota)
            {
                throw new AzureBatchQuotaMaxedOutException($"No remaining pool quota available. There are {activePoolsCount} pools in use out of {poolQuota}");
            }

            if ((totalCoresInUse + workflowCoresRequirement) > coreQuota)
            {
                throw new AzureBatchQuotaMaxedOutException($"Not enough core quota remaining to schedule task requiring {workflowCoresRequirement} {(preemptible ? "low priority" : "dedicated")} cores. There are {totalCoresInUse} cores in use out of {coreQuota}");
            }

            if ((totalCoresInUseByVmFam + workflowCoresRequirement) > vmFamQuota)
            {
                throw new AzureBatchQuotaMaxedOutException($"Not enough core quota remaining to schedule task requiring {workflowCoresRequirement} dedicated {vmFamily} cores. There are {totalCoresInUseByVmFam} cores in use out of {vmFamQuota}");
            }
        }

        /// <summary>
        /// Gets the cheapest available VM size that satisfies the <see cref="TesTask"/> execution requirements
        /// </summary>
        /// <param name="tesTask"><see cref="TesTask"/></param>
        /// <param name="forcePreemptibleVmsOnly">Force consideration of preemptible virtual machines only.</param>
        /// <returns>The virtual machine info</returns>
        public async Task<VirtualMachineInformation> GetVmSizeAsync(TesTask tesTask, bool forcePreemptibleVmsOnly = false)
        {
            var tesResources = tesTask.Resources;

            var previouslyFailedVmSizes = tesTask.Logs?
                .Where(log => log.FailureReason == BatchTaskState.NodeAllocationFailed.ToString() && log.VirtualMachineInfo?.VmSize is not null)
                .Select(log => log.VirtualMachineInfo.VmSize)
                .Distinct()
                .ToList();

            var virtualMachineInfoList = await azureProxy.GetVmSizesAndPricesAsync();
            var preemptible = forcePreemptibleVmsOnly || usePreemptibleVmsOnly || tesResources.Preemptible.GetValueOrDefault(true);

            var eligibleVms = new List<VirtualMachineInformation>();
            var noVmFoundMessage = string.Empty;

            var vmSize = tesTask.Resources?.GetBackendParameterValue(TesResources.SupportedBackendParameters.vm_size);

            if (!string.IsNullOrWhiteSpace(vmSize))
            {
                eligibleVms = virtualMachineInfoList
                    .Where(vm =>
                        vm.LowPriority == preemptible
                        && vm.VmSize.Equals(vmSize, StringComparison.OrdinalIgnoreCase))
                    .ToList();

                noVmFoundMessage = $"No VM (out of {virtualMachineInfoList.Count}) available with the required resources (vmsize: {vmSize}, preemptible: {preemptible}) for task id {tesTask.Id}.";
            }
            else
            {
                var requiredNumberOfCores = tesResources.CpuCores.GetValueOrDefault(DefaultCoreCount);
                var requiredMemoryInGB = tesResources.RamGb.GetValueOrDefault(DefaultMemoryGb);
                var requiredDiskSizeInGB = tesResources.DiskGb.GetValueOrDefault(DefaultDiskGb);

                eligibleVms = virtualMachineInfoList
                    .Where(vm =>
                        vm.LowPriority == preemptible
                        && vm.NumberOfCores >= requiredNumberOfCores
                        && vm.MemoryInGB >= requiredMemoryInGB
                        && vm.ResourceDiskSizeInGB >= requiredDiskSizeInGB)
                    .ToList();

                noVmFoundMessage = $"No VM (out of {virtualMachineInfoList.Count}) available with the required resources (cores: {requiredNumberOfCores}, memory: {requiredMemoryInGB} GB, disk: {requiredDiskSizeInGB} GB, preemptible: {preemptible}) for task id {tesTask.Id}.";
            }

            var batchQuotas = await azureProxy.GetBatchAccountQuotasAsync();

            var selectedVm = eligibleVms
                .Where(vm => !(allowedVmSizes?.Any() ?? false) || allowedVmSizes.Contains(vm.VmSize, StringComparer.OrdinalIgnoreCase))
                .Where(vm => !(previouslyFailedVmSizes?.Contains(vm.VmSize, StringComparer.OrdinalIgnoreCase) ?? false))
                .Where(vm => preemptible
                    ? batchQuotas.LowPriorityCoreQuota >= vm.NumberOfCores
                    : batchQuotas.DedicatedCoreQuota >= vm.NumberOfCores
                        && (!batchQuotas.DedicatedCoreQuotaPerVMFamilyEnforced || batchQuotas.DedicatedCoreQuotaPerVMFamily.FirstOrDefault(x => vm.VmFamily.Equals(x.Name, StringComparison.OrdinalIgnoreCase))?.CoreQuota >= vm.NumberOfCores))
                .OrderBy(x => x.PricePerHour)
                .FirstOrDefault();

            if (!preemptible && selectedVm is not null)
            {
                var idealVm = eligibleVms
                    .Where(vm => !(allowedVmSizes?.Any() ?? false) || allowedVmSizes.Contains(vm.VmSize, StringComparer.OrdinalIgnoreCase))
                    .Where(vm => !(previouslyFailedVmSizes?.Contains(vm.VmSize, StringComparer.OrdinalIgnoreCase) ?? false))
                    .OrderBy(x => x.PricePerHour)
                    .FirstOrDefault();

                if (selectedVm.PricePerHour >= idealVm.PricePerHour * 2)
                {
                    tesTask.SetWarning("UsedLowPriorityInsteadOfDedicatedVm",
                        $"This task ran on low priority machine because dedicated quota was not available for VM Series '{idealVm.VmFamily}'.",
                        $"Increase the quota for VM Series '{idealVm.VmFamily}' to run this task on a dedicated VM. Please submit an Azure Support request to increase your quota: {AzureSupportUrl}");

                    return await GetVmSizeAsync(tesTask, true);
                }
            }

            if (selectedVm is not null)
            {
                return selectedVm;
            }
           
            if (!eligibleVms.Any())
            {
                noVmFoundMessage += $" There are no VM sizes that match the requirements. Review the task resources.";
            }

            if (previouslyFailedVmSizes is not null)
            {
                noVmFoundMessage += $" The following VM sizes were excluded from consideration because of {BatchTaskState.NodeAllocationFailed} error(s) on previous attempts: {string.Join(", ", previouslyFailedVmSizes)}.";
            }

            if (allowedVmSizes?.Any() ?? false)
            {
                var vmsExcludedByTheAllowedVmsConfiguration = eligibleVms.Where(vm => allowedVmSizes.Contains(vm.VmSize, StringComparer.OrdinalIgnoreCase));

                if (vmsExcludedByTheAllowedVmsConfiguration.Any())
                {
                    noVmFoundMessage += $" {vmsExcludedByTheAllowedVmsConfiguration.Count()} VM(s) were excluded by the allowed-vm-sizes configuration. Consider expanding the list of allowed VM sizes.";
                }
            }

            throw new AzureBatchVirtualMachineAvailabilityException(noVmFoundMessage.Trim());
        }

        private async Task<(BatchNodeMetrics BatchNodeMetrics, DateTimeOffset? TaskStartTime, DateTimeOffset? TaskEndTime, int? CromwellRcCode)> GetBatchNodeMetricsAndCromwellResultCodeAsync(TesTask tesTask)
        {
            var bytesInGB = Math.Pow(1000, 3);
            var kiBInGB = Math.Pow(1000, 3) / 1024;

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
                var cromwellRcContent = await this.storageAccessProvider.DownloadBlobAsync($"{GetCromwellExecutionDirectoryPath(tesTask)}/rc");

                if (cromwellRcContent is not null && int.TryParse(cromwellRcContent, out var temp))
                {
                    cromwellRcCode = temp;
                }

                var metricsContent = await this.storageAccessProvider.DownloadBlobAsync($"{GetBatchExecutionDirectoryPath(tesTask)}/metrics.txt");

                if (metricsContent is not null)
                {
                    try
                    {
                        var metrics = DelimitedTextToDictionary(metricsContent.Trim());

                        var diskSizeInGB = TryGetValueAsDouble(metrics, "DiskSizeInKiB", out var diskSizeInKiB)  ? diskSizeInKiB / kiBInGB : (double?)null;
                        var diskUsedInGB = TryGetValueAsDouble(metrics, "DiskUsedInKiB", out var diskUsedInKiB) ? diskUsedInKiB / kiBInGB : (double?)null;

                        batchNodeMetrics = new BatchNodeMetrics
                        {
                            BlobXferImagePullDurationInSeconds = GetDurationInSeconds(metrics, "BlobXferPullStart", "BlobXferPullEnd"),
                            ExecutorImagePullDurationInSeconds = GetDurationInSeconds(metrics, "ExecutorPullStart", "ExecutorPullEnd"),
                            ExecutorImageSizeInGB = TryGetValueAsDouble(metrics, "ExecutorImageSizeInBytes", out var executorImageSizeInBytes) ? executorImageSizeInBytes / bytesInGB : (double?)null,
                            FileDownloadDurationInSeconds = GetDurationInSeconds(metrics, "DownloadStart", "DownloadEnd"),
                            FileDownloadSizeInGB = TryGetValueAsDouble(metrics, "FileDownloadSizeInBytes", out var fileDownloadSizeInBytes) ? fileDownloadSizeInBytes / bytesInGB : (double?)null,
                            ExecutorDurationInSeconds = GetDurationInSeconds(metrics, "ExecutorStart", "ExecutorEnd"),
                            FileUploadDurationInSeconds = GetDurationInSeconds(metrics, "UploadStart", "UploadEnd"),
                            FileUploadSizeInGB = TryGetValueAsDouble(metrics, "FileUploadSizeInBytes", out var fileUploadSizeInBytes) ? fileUploadSizeInBytes / bytesInGB : (double?)null,
                            DiskUsedInGB = diskUsedInGB,
                            DiskUsedPercent = diskUsedInGB.HasValue && diskSizeInGB.HasValue && diskSizeInGB > 0 ? (float?)(diskUsedInGB / diskSizeInGB * 100 ) : null,
                            VmCpuModelName = metrics.GetValueOrDefault("VmCpuModelName")
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
        }

        private static Dictionary<string, string> DelimitedTextToDictionary(string text, string fieldDelimiter = "=", string rowDelimiter = "\n")
            => text.Split(rowDelimiter)
                .Select(line => { var parts = line.Split(fieldDelimiter); return new KeyValuePair<string, string>(parts[0], parts[1]); })
                .ToDictionary(kv => kv.Key, kv => kv.Value);

        #region BatchPools
        private readonly BatchPools batchPools;

        private bool TryGet(string poolId, out IBatchPool batchPool)
        {
            batchPool = batchPools.FirstOrDefault(p => p.Pool.PoolId.Equals(poolId, StringComparison.Ordinal));
            return batchPool is not null;
        }

        private bool IsPoolAvailable(string key)
            => batchPools.TryGetValue(key, out var pools) && pools.Any(p => p.IsAvailable);

        private async Task<IBatchPool> GetOrAddAsync(string key, Func<string, ValueTask<BatchModels.Pool>> valueFactory)
        {
            if (enableBatchAutopool)
            {
                return default;
            }

            _ = valueFactory ?? throw new ArgumentNullException(nameof(valueFactory));
            var keyLength = key?.Length ?? 0;
            if (keyLength > 50 || keyLength < 1)
            {
                throw new ArgumentException("Key must be between 1-50 chars in length", nameof(key));
            }
            // TODO: Make sure key doesn't contain any unsupported chars

            var pool = batchPools.TryGetValue(key, out var set) ? set.LastOrDefault(Available) : default;
            if (pool is null)
            {
                var poolQuota = azureProxy.GetBatchAccountQuotasAsync().Result.PoolQuota;
                var activePoolsCount = azureProxy.GetBatchActivePoolCount();
                if (activePoolsCount + 1 > poolQuota)
                {
                    throw new AzureBatchQuotaMaxedOutException($"No remaining pool quota available. There are {activePoolsCount} pools in use out of {poolQuota}");
                }
                var uniquifier = new byte[8]; // This always becomes 13 chars, when you remove the three '=' at the end. We won't ever decode this, so we don't need the '='s
                random.NextBytes(uniquifier);
                var poolId = $"{key}-{ConvertToBase32(uniquifier).TrimEnd('=')}"; // '-' is required by GetKeyFromPoolId()
                try
                {
                    pool = _poolFactory.CreateNew(await azureProxy.CreateBatchPoolAsync(await valueFactory(poolId)), this);
                }
                catch (RequestFailedException ex) when (ex.Status == 0 && ex.InnerException is WebException webException && webException.Status == WebExceptionStatus.Timeout)
                {
                    await HandleTimeout(poolId);
                }
                catch (Exception ex) when (IsInnermostExceptionSocketException125(ex))
                {
                    await HandleTimeout(poolId);
                }
                _ = await AddPoolAsync(pool);
            }
            return pool;

            static bool Available(IBatchPool pool)
                => pool.IsAvailable;

            async Task HandleTimeout(string poolId)
            {
                // When the batch management API creating the pool times out, it may or may not have created the pool. Add an inactive record to delete it if it did get created and try again later. That record will be removed later whether or not the pool was created.
                pool = _poolFactory.Retrieve(new BatchPool.PoolData { PoolId = poolId, IsAvailable = false, PendingReservations = new(), Reservations = new() }, this);
                _ = await AddPoolAsync(pool);
                throw new AzureBatchQuotaMaxedOutException($"Pool creation timed out");
            }

            static bool IsInnermostExceptionSocketException125(Exception ex)
            {
                // errno: ECANCELED 125 Operation canceled
                for (var e = ex; e is System.Net.Sockets.SocketException /*se && se.ErrorCode == 125*/; e = e.InnerException)
                {
                    if (e.InnerException is null) { return false; }
                }
                return true;
            }

            static string ConvertToBase32(byte[] bytes) // https://datatracker.ietf.org/doc/html/rfc4648#section-6
            {
                const int groupBitlength = 5;
                var Rfc4648Base32 = @"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567".ToArray();
                return new string(new BitArray(bytes)
                        .Cast<bool>()
                        .Select((b, i) => (Index: i, Value: b ? 1 << (groupBitlength - 1 - (i % groupBitlength)) : 0))
                        .GroupBy(t => t.Index / groupBitlength)
                        .Select(g => Rfc4648Base32[g.Sum(t => t.Value)])
                        .ToArray())
                    + (bytes.Length % groupBitlength) switch
                    {
                        0 => string.Empty,
                        1 => @"======",
                        2 => @"====",
                        3 => @"===",
                        4 => @"=",
                        _ => throw new InvalidOperationException(), // Keeps the compiler happy.
                    };
            }
        }

        private ValueTask ScheduleReimage(ComputeNodeInformation nodeInformation, BatchTaskState taskState)
        {
            if (!enableBatchAutopool && nodeInformation is not null)
            {
                if (TryGet(nodeInformation.PoolId, out var pool))
                {
                    return pool.ScheduleReimage(nodeInformation, taskState);
                }
            }

            return ValueTask.CompletedTask;
        }

        /// <inheritdoc/>
        public async ValueTask<bool> UpdateBatchPools(CancellationToken cancellationToken)
        {
            var changes = false;
            var activePoolsByKey = new Dictionary<string, List<string>>();
            var localPools = new List<string>();
            var remotePools = new List<string>();
            var poolsToRemove = Enumerable.Empty<Task>();

            // Get pool metadata from both known sources
            var remoteGroups = (await _poolDataRepository.GetItemsAsync(p => true)).GroupBy(p => GetKeyFromPoolId(p.PoolId)).ToList();
            var localGroups = batchPools.GetGroups().ToList();

            // Remove pools no longer active in batch account while gathering information needed by the rest of this method
            foreach (var group in remoteGroups)
            {
                _ = await GetActivePools(group.Key);
                foreach (var pool in group)
                {
                    remotePools.Add(pool.PoolId);
                }
            }

            foreach (var group in localGroups)
            {
                var activePools = await GetActivePools(group.Key);
                foreach(var pool in group.Value)
                {
                    if (!activePools.Contains(pool.Pool.PoolId))
                    {
                        logger.LogDebug("Pool '{PoolId}' is being removed because no pool by that name is active.", pool.Pool.PoolId);
                        poolsToRemove = poolsToRemove.Append(batchPools.RemoveAsync(pool).AsTask());
                        _ = remotePools.Remove(pool.Pool.PoolId);
                        changes = true;
                    }
                    else
                    {
                        localPools.Add(pool.Pool.PoolId);
                    }
                }
            }

            foreach (var pool in remotePools)
            {
                if (!activePoolsByKey.Values.SelectMany(g => g).Contains(pool))
                {
                    logger.LogDebug("Pool '{PoolId}' is being removed because no pool by that name is active.", pool);
                    poolsToRemove = poolsToRemove.Append(_poolDataRepository.DeleteItemAsync(pool));
                    changes = true;
                }
            }

            // Add pools found in remote repository not present in local memory
            foreach (var poolData in remoteGroups.SelectMany(g => g).Where(p => remotePools.Contains(p.PoolId)))
            {
                if (!batchPools.Select(p => p.Pool.PoolId).Contains(poolData.PoolId))
                {
                    logger.LogDebug("Pool '{PoolId}' from the repository is being added to the local store.", poolData.PoolId);
                    var pool = _poolFactory.Retrieve(poolData, this);
                    changes = true;
                    if (await batchPools.AddAsync(pool, true))
                    {
                        await pool.ServicePoolAsync(IBatchPool.ServiceKind.SyncSize, cancellationToken);
                    }
                }
            }

            // Add pools found in local memory not present in remote repository
            foreach (var pool in localGroups.Select(t => t.Value).SelectMany(g => g).Where(p => localPools.Contains(p.Pool.PoolId)))
            {
                if (!remotePools.Contains(pool.Pool.PoolId))
                {
                    logger.LogDebug("Pool '{PoolId}' from the local store is being added to the repository.", pool.Pool.PoolId);
                    pool.ReplaceRepositoryItem(await _poolDataRepository.CreateItemAsync(pool.RepositoryItem));
                    changes = true;
                }
            }

            // Update remote repository with all metadata updates
            if (await UpdateBatchPoolsMetadata())
            {
                changes = true;
            }

            // Wait for pool metadata deletions to complete.
            await Task.WhenAll(poolsToRemove);

            return changes;

            async Task<List<string>> GetActivePools(string key)
            {
                if (!activePoolsByKey.TryGetValue(key, out var pools))
                {
                    pools = (await azureProxy.GetActivePoolIdsAsync($"{key}-", TimeSpan.Zero, cancellationToken)).ToList();
                    activePoolsByKey.Add(key, pools);
                }
                return pools;
            }
        }

        private async Task<bool> UpdateBatchPoolsMetadata()
        {
            if (batchPools.HasChanges)
            {
                await Task.WhenAll(batchPools.Where(e => e.ChangesRepositoryItem.HasChanges).Select(async pool =>
                {
                    logger.LogDebug("Updating pool '{PoolId}'.", pool.Pool.PoolId);
                    pool.ReplaceRepositoryItem(await _poolDataRepository.UpdateItemAsync(pool.RepositoryItem));
                    pool.ChangesRepositoryItem.ResetChanges();
                }));
                return true;
            }
            return false;
        }

        /// <inheritdoc/>
        public async ValueTask<IEnumerable<Task>> GetShutdownCandidatePools(CancellationToken cancellationToken)
        {
            _ = await UpdateBatchPoolsMetadata(); // TODO: consider moving the actions called by the method in this line to be called by IBatchPool.ServicePoolAsync() 

            return (await batchPools
                .ToAsyncEnumerable()
                .WhereAwait(async p => await p.CanBeDeleted(cancellationToken))
                .ToListAsync(cancellationToken))
                .Select(t => azureProxy.DeleteBatchPoolAsync(t.Pool.PoolId));
        }

        /// <inheritdoc/>
        public IEnumerable<IBatchPool> GetPools()
            => batchPools;

        private ValueTask<bool> AddPoolAsync(IBatchPool pool)
            => batchPools.AddAsync(pool, false);

        private static string GetKeyFromPoolId(string poolId)
            => poolId[..poolId.LastIndexOf('-')];

        private class BatchPoolEqualityComparer : IEqualityComparer<IBatchPool>
        {
            bool IEqualityComparer<IBatchPool>.Equals(IBatchPool x, IBatchPool y)
                => x.Pool.PoolId.Equals(y.Pool.PoolId);

            int IEqualityComparer<IBatchPool>.GetHashCode(IBatchPool obj)
                => obj.Pool.PoolId.GetHashCode();
        }

        #region IBatchPoolsImpl
        /// <inheritdoc/>
        bool IBatchPoolsImpl.TryGet(string poolId, out IBatchPool batchPool)
            => TryGet(poolId, out batchPool);

        /// <inheritdoc/>
        bool IBatchPoolsImpl.IsPoolAvailable(string key)
            => IsPoolAvailable(key);

        /// <inheritdoc/>
        Task<IBatchPool> IBatchPoolsImpl.GetOrAddAsync(string key, Func<string, ValueTask<BatchModels.Pool>> valueFactory)
            => GetOrAddAsync(key, valueFactory);

        /// <inheritdoc/>
        ValueTask<bool> IBatchPoolsImpl.RemovePoolFromListAsync(IBatchPool pool)
            => batchPools.RemoveAsync(pool);

        /// <inheritdoc/>
        IEnumerable<string> IBatchPoolsImpl.GetPoolGroupKeys()
            => batchPools.Keys;
        #endregion

        private class BatchPools : KeyedGroupWithRepositoryElements<IBatchPool, GroupablSetWithRepositoryElements<IBatchPool, BatchPool.PoolData>, BatchPool.PoolData>
        {
            private readonly ILogger logger;
            private readonly BatchScheduler batchScheduler;
            public BatchPools(BatchScheduler batchScheduler, ILogger logger)
                : base(p => p is null ? default : GetKeyFromPoolId(p.Pool.PoolId), StringComparer.Ordinal)
            {
                this.batchScheduler = batchScheduler;
                this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            }

            protected override Func<IEnumerable<IBatchPool>, GroupablSetWithRepositoryElements<IBatchPool, BatchPool.PoolData>> CreateSetFunc
                => e => new(e, new BatchPoolEqualityComparer());

            public IBatchPool GetPoolOrDefault(string poolId)
                => TryGetValue(GetKeyFromPoolId(poolId), out var batchPools) ? batchPools.FirstOrDefault(p => p.Pool.PoolId.Equals(poolId, StringComparison.Ordinal)) : default;

            public async ValueTask<bool> AddAsync(IBatchPool element, bool isFromRepsitory)
            {
                var result = Add(element);
                if (!isFromRepsitory)
                {
                    try
                    {
                        element.ReplaceRepositoryItem(await batchScheduler._poolDataRepository.CreateItemAsync(element.RepositoryItem));
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Error adding pool {PoolId} repository record", element.Pool.PoolId);
                    }
                }

                logger.LogDebug("Adding pool {PoolId} returned {Result}", element.Pool.PoolId, result);
                return result;
            }

            public async ValueTask<bool> RemoveAsync(IBatchPool element)
            {
                var result = Remove(element);
                try
                {
                    await batchScheduler._poolDataRepository.DeleteItemAsync(element.Pool.PoolId);
                }
                catch(Exception ex)
                {
                    logger.LogError(ex, "Error removing pool {PoolId} repository record", element.Pool.PoolId);
                }

                logger.LogDebug("Removing pool {PoolId} returned {Result}", element.Pool.PoolId, result);
                return result;
            }
        }
        #endregion

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
            public string PoolId { get; set; }
            public string JobId { get; set; }
            public AffinityInformation AffinityInformation { get; set; }
            public ComputeNodeInformation ComputeNodeInformation { get; set; }
        }
    }

    /// <summary>
    /// Internal access to batch pool related methods for <see cref="BatchPool"/> and also unit/module tests.
    /// </summary>
    internal interface IBatchPoolsImpl
    {
        /// <summary>
        /// Indicates that a pool for the key value is available to service tasks.
        /// </summary>
        /// <param name="key">The key to locate the configured pool.</param>
        /// <returns>True if pool is available, otherwise a new pool would need to be created to service the indicated configuration.</returns>
        bool IsPoolAvailable(string key);

        /// <summary>
        /// Retrieves a pool that manages compute nodes of the related vmSize, creating the pool if the key doesn't exist.
        /// </summary>
        /// <param name="key">The key to locate the configured pool. Must be between 1 and 50 chars in length.</param>
        /// <param name="valueFactory">A delegate to create the pool. Only called (once, even if called by multiple threads) if a pool isn't available.</param>
        /// <remarks>The argument to <paramref name="valueFactory"/> needs to be the Name argument of its construtor. It's recommended to set the <see cref="BatchModels.Pool.DisplayName"/> to the <paramref name="key"/> value.</remarks>
        /// <returns></returns>
        Task<IBatchPool> GetOrAddAsync(string key, Func<string, ValueTask<BatchModels.Pool>> valueFactory);

        /// <summary>
        /// Retrieves the requested batch pool.
        /// </summary>
        /// <param name="poolId">The <see cref="PoolInformation.PoolId"/> of the requested <paramref name="batchPool"/>.</param>
        /// <param name="batchPool">Returns the requested <see cref="IBatchPool"/>.</param>
        /// <returns>True if the requested <paramref name="batchPool"/> was found, False otherwise.</returns>
        bool TryGet(string poolId, out IBatchPool batchPool);

        ValueTask<bool> RemovePoolFromListAsync(IBatchPool pool);

        IEnumerable<string> GetPoolGroupKeys();
    }
}
