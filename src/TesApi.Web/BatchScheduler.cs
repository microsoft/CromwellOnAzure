// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Security.Cryptography;
using System.Security.Policy;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc.Formatters;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Tes.Extensions;
using Tes.Models;
using BatchModels = Microsoft.Azure.Management.Batch.Models;

namespace TesApi.Web
{
    /// <summary>
    /// Orchestrates <see cref="TesTask"/>s on Azure Batch
    /// </summary>
    public class BatchScheduler : IBatchScheduler
    {
        private const string AzureSupportUrl = "https://portal.azure.com/#blade/Microsoft_Azure_Support/HelpAndSupportBlade/newsupportrequest";
        private const int DefaultCoreCount = 1;
        private const int DefaultMemoryGb = 2;
        private const int DefaultDiskGb = 10;
        //private const string HostConfigBlobsName = "host-config-blobs";
        private const string CromwellPathPrefix = "/cromwell-executions/";
        private const string CromwellScriptFileName = "script";
        private const string BatchExecutionDirectoryName = "__batch";
        private const string BatchScriptFileName = "batch_script";
        private const string UploadFilesScriptFileName = "upload_files_script";
        private const string DownloadFilesScriptFileName = "download_files_script";
        private const string StartTaskScriptFilename = "start-task.sh";
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
        private readonly BatchNodeInfo batchNodeInfo;
        private readonly string marthaUrl;
        private readonly string marthaKeyVaultName;
        private readonly string marthaSecretName;
        //private readonly string defaultStorageAccountName;
        private readonly Task InitializeHostBlobs;

        /// <summary>
        /// Orchestrates <see cref="TesTask"/>s on Azure Batch
        /// </summary>
        /// <param name="logger">Logger <see cref="ILogger"/></param>
        /// <param name="configuration">Configuration <see cref="IConfiguration"/></param>
        /// <param name="azureProxy">Azure proxy <see cref="IAzureProxy"/></param>
        /// <param name="storageAccessProvider">Storage access provider <see cref="IStorageAccessProvider"/></param>
        public BatchScheduler(ILogger logger, IConfiguration configuration, IAzureProxy azureProxy, IStorageAccessProvider storageAccessProvider)
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
            //this.defaultStorageAccountName = GetStringValue(configuration, "DefaultStorageAccountName", string.Empty);  // This account contains the HostConfigBlobsName container
            this.marthaUrl = GetStringValue(configuration, "MarthaUrl", string.Empty);
            this.marthaKeyVaultName = GetStringValue(configuration, "MarthaKeyVaultName", string.Empty);
            this.marthaSecretName = GetStringValue(configuration, "MarthaSecretName", string.Empty);
            
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
                await DeleteBatchJobAndPoolIfExists(azureProxy, tesTask);
                SetTaskStateAndLog(tesTask, TesState.COMPLETEEnum, batchInfo);
            }

            async Task SetTaskExecutorError(TesTask tesTask, CombinedBatchTaskInfo batchInfo)
            {
                await DeleteBatchJobAndPoolIfExists(azureProxy, tesTask);
                SetTaskStateAndLog(tesTask, TesState.EXECUTORERROREnum, batchInfo);
            }

            async Task SetTaskSystemError(TesTask tesTask, CombinedBatchTaskInfo batchInfo)
            {
                await DeleteBatchJobAndPoolIfExists(azureProxy, tesTask);
                SetTaskStateAndLog(tesTask, TesState.SYSTEMERROREnum, batchInfo);
            }

            async Task DeleteBatchJobAndSetTaskStateAsync(TesTask tesTask, TesState newTaskState, CombinedBatchTaskInfo batchInfo)
            { 
                await this.azureProxy.DeleteBatchJobAsync(tesTask.Id);
                await DeleteManualBatchPoolIfExistsAsync(tesTask);
                SetTaskStateAndLog(tesTask, newTaskState, batchInfo); 
            }
            Task DeleteBatchJobAndSetTaskExecutorErrorAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo) => DeleteBatchJobAndSetTaskStateAsync(tesTask, TesState.EXECUTORERROREnum, batchInfo);
            Task DeleteBatchJobAndSetTaskSystemErrorAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo) => DeleteBatchJobAndSetTaskStateAsync(tesTask, TesState.SYSTEMERROREnum, batchInfo);

            Task DeleteBatchJobAndRequeueTaskAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo) => ++tesTask.ErrorCount > 3
                ? DeleteBatchJobAndSetTaskExecutorErrorAsync(tesTask, batchInfo)
                : DeleteBatchJobAndSetTaskStateAsync(tesTask, TesState.QUEUEDEnum, batchInfo);

            async Task CancelTaskAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo)
            { 
                await this.azureProxy.DeleteBatchJobAsync(tesTask.Id);
                await DeleteManualBatchPoolIfExistsAsync(tesTask);
                tesTask.IsCancelRequested = false; 
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

            InitializeHostBlobs = new Task(async () =>
            {
                var writeStoredVersions = false;
                var storedVersions = BatchUtils.ReadApplicationVersions();
                try
                {
                    var knownExtant = Enumerable.Empty<(string, string)>();
                    foreach (var hash in BatchUtils.GetApplicationPayloadHashes().Select(p => (Name: p.Key, Hash: Convert.ToHexString(p.Value))))
                    {
                        if (storedVersions.TryGetValue(hash.Name, out var keyMetadata))
                        {
                            if (keyMetadata.Packages.TryGetValue(hash.Hash, out var hashMetadata))
                            {
                                knownExtant = knownExtant.Append((hash.Name, hash.Hash));
                            }
                            else
                            {
                                keyMetadata.Packages.Add(hash.Hash, (default, DoesPayloadContainStartScript(hash.Name)));
                                writeStoredVersions = true;
                            }
                        }
                        else
                        {
                            storedVersions.Add(hash.Name, (default, BatchUtils.BatchAppFromHostConfigName(hash.Name), new Dictionary<string, (int, bool)>(Enumerable.Empty<KeyValuePair<string, (int, bool)>>().Append(new(hash.Hash, (default, DoesPayloadContainStartScript(hash.Name)))))));
                            writeStoredVersions = true;
                        }
                    }

                    var serverAppList = Enumerable.Empty<(string Name, string Version)>();
                    foreach (var appVersion in (await azureProxy.ListApplications()).Select(a => (a.Name, azureProxy.ListApplicationPackages(a).Result)).SelectMany(t => t.Result.Select(p => (t.Name, p.Name))))
                    {
                        serverAppList = serverAppList.Append(appVersion);
                    }

                    var storedVersionsByServerName = new Dictionary<string, (IDictionary<string, (int, bool)> Packages, string)>(storedVersions.Select(p => new KeyValuePair<string, (IDictionary<string, (int, bool)>, string)>(p.Value.ServerAppName, (p.Value.Packages, p.Key))));
                    var appsToRemove = Enumerable.Empty<(string Name, string Version)>();
                    var storedVersionsFoundBuilder = Enumerable.Empty<(string Name, string Version)>();
                    foreach (var app in serverAppList)
                    {
                        if (storedVersionsByServerName.TryGetValue(app.Name, out var keyMetadata))
                        {
                            var storedHashes = new List<string>(keyMetadata.Packages.Keys);

                            foreach (var hash in keyMetadata.Packages)
                            {
                                if (hash.Key.Equals(app.Version, StringComparison.InvariantCulture))
                                {
                                    storedVersionsFoundBuilder = storedVersionsFoundBuilder.Append(new(app.Name, app.Version));
                                }
                                else
                                {
                                    appsToRemove = appsToRemove.Append(new(app.Name, app.Version));
                                }
                            }
                        }
                        else
                        {
                            // Unknown app. Ignore. TODO: Warn?
                        }
                    }

                    foreach (var app in appsToRemove)
                    {
                        //TODO: Remove(app.Package)
                    }

                    var storedVersionsFound = storedVersionsFoundBuilder.ToList();
                    var updates = Enumerable.Empty<KeyValuePair<string, (string, string, IDictionary<string, (int, bool)>)>>();
                    foreach (var app in storedVersions)
                    {
                        foreach (var version in app.Value.Packages)
                        {
                            if (!storedVersionsFound.Contains((app.Key, version.Key)))
                            {
                                using var payload = BatchUtils.GetApplicationPayload(app.Key);
                                updates = updates.Append(new KeyValuePair<string, (string, string, IDictionary<string, (int, bool)>)>(app.Key,
                                    (await azureProxy.CreateAndActivateBatchApplication(app.Value.ServerAppName, version.Key, payload), app.Value.ServerAppName, app.Value.Packages)));
                            }
                        }
                    }

                    foreach (var app in updates)
                    {
                        storedVersions[app.Key] = app.Value;
                    }
                }
                finally
                {
                    if (writeStoredVersions)
                    {
                        BatchUtils.WriteApplicationVersions(storedVersions);
                    }
                }

                bool DoesPayloadContainStartScript(string name)
                {
                    using var zip = new ZipArchive(BatchUtils.GetApplicationPayload(name));
                    return zip.Entries.Any(e => e.Name.Equals(StartTaskScriptFilename, StringComparison.InvariantCulture));
                }
            });

            if (!string.IsNullOrWhiteSpace(batchNodeInfo.BatchNodeAgentSkuId)) // When not testing (e.g. Production) or when testing HostConfigs
            {
                InitializeHostBlobs.Start();
            }
        }


        private async Task DeleteBatchJobAndPoolIfExists(IAzureProxy azureProxy, TesTask tesTask)
        {
            var batchDeletionExceptions = new List<Exception>();

            try
            {
                await azureProxy.DeleteBatchJobAsync(tesTask.Id);
            }
            catch (Exception exc)
            {
                logger.LogError(exc, $"Exception deleting batch job with tesTask.Id: {tesTask?.Id}");
                batchDeletionExceptions.Add(exc);
            }

            try
            {
                await DeleteManualBatchPoolIfExistsAsync(tesTask);
            }
            catch (Exception exc)
            {
                logger.LogError(exc, $"Exception deleting batch pool with tesTask.Id: {tesTask?.Id}");
                batchDeletionExceptions.Add(exc);
            }

            if (batchDeletionExceptions.Any())
            {
                throw new AggregateException(batchDeletionExceptions);
            }
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

        private async Task DeleteManualBatchPoolIfExistsAsync(TesTask tesTask)
        {
            if (tesTask.Resources?.ContainsBackendParameterValue(TesResources.SupportedBackendParameters.workflow_execution_identity) == true)
            {
                await azureProxy.DeleteBatchPoolIfExistsAsync(tesTask.Id);
            }
        }

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
            try
            {
                var jobId = await azureProxy.GetNextBatchJobIdAsync(tesTask.Id); // <-- TODO: should this be moved...
                var virtualMachineInfo = await GetVmSizeAsync(tesTask);

                await CheckBatchAccountQuotas(virtualMachineInfo);

                // <-- ... to here?

                var tesTaskLog = tesTask.AddTesTaskLog();
                tesTaskLog.VirtualMachineInfo = virtualMachineInfo;
                // TODO?: Support for multiple executors. Cromwell has single executor per task.
                var dockerImage = tesTask.Executors.First().Image;

                var IsIdentityProvided = tesTask.Resources?.ContainsBackendParameterValue(TesResources.SupportedBackendParameters.workflow_execution_identity) == true;
                var identityResourceId = IsIdentityProvided ? tesTask.Resources?.GetBackendParameterValue(TesResources.SupportedBackendParameters.workflow_execution_identity) : default;

                var containerConfiguration = await GetContainerConfigurationIfNeeded(dockerImage);
                var (startTask, nodeInfo, dockerParams, applicationPackages, preCommand) = await GetDockerHostConfiguration(tesTask);

                var poolInformation = await CreateAutoPoolModePoolInformation(
                    virtualMachineInfo.VmSize,
                    virtualMachineInfo.LowPriority,
                    nodeInfo ?? batchNodeInfo,
                    containerConfiguration,
                    applicationPackages,
                    startTask,
                    jobId,
                    identityResourceId);

                var cloudTask = await ConvertTesTaskToBatchTaskAsync(tesTask, dockerParams, preCommand, containerConfiguration is not null);
                logger.LogInformation($"Creating batch job for TES task {tesTask.Id}. Using VM size {virtualMachineInfo.VmSize}.");
                await azureProxy.CreateBatchJobAsync(jobId, cloudTask, poolInformation);

                tesTaskLog.StartTime = DateTimeOffset.UtcNow;
                tesTask.State = TesState.INITIALIZINGEnum;
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
        }

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
                    return new CombinedBatchTaskInfo { BatchTaskState = BatchTaskState.JobNotFound, FailureReason = BatchTaskState.JobNotFound.ToString() };
                case JobState.Active:
                    {
                        if (azureBatchJobAndTaskState.NodeAllocationFailed)
                        {
                            return new CombinedBatchTaskInfo { BatchTaskState = BatchTaskState.NodeAllocationFailed, FailureReason = BatchTaskState.NodeAllocationFailed.ToString(), SystemLogItems = ConvertNodeErrorsToSystemLogItems(azureBatchJobAndTaskState) };
                        }

                        if (azureBatchJobAndTaskState.NodeState == ComputeNodeState.Unusable)
                        {
                            return new CombinedBatchTaskInfo { BatchTaskState = BatchTaskState.NodeUnusable, FailureReason = BatchTaskState.NodeUnusable.ToString(), SystemLogItems = ConvertNodeErrorsToSystemLogItems(azureBatchJobAndTaskState) };
                        }

                        if (azureBatchJobAndTaskState.NodeState == ComputeNodeState.Preempted)
                        {
                            return new CombinedBatchTaskInfo { BatchTaskState = BatchTaskState.NodePreempted, FailureReason = BatchTaskState.NodePreempted.ToString(), SystemLogItems = ConvertNodeErrorsToSystemLogItems(azureBatchJobAndTaskState) };
                        }

                        if (azureBatchJobAndTaskState.NodeErrorCode is not null)
                        {
                            if (azureBatchJobAndTaskState.NodeErrorCode == "DiskFull")
                            {
                                return new CombinedBatchTaskInfo { BatchTaskState = BatchTaskState.NodeFailedDuringStartupOrExecution, FailureReason = azureBatchJobAndTaskState.NodeErrorCode };
                            }
                            else
                            {
                                return new CombinedBatchTaskInfo { BatchTaskState = BatchTaskState.NodeFailedDuringStartupOrExecution, FailureReason = BatchTaskState.NodeFailedDuringStartupOrExecution.ToString(), SystemLogItems = ConvertNodeErrorsToSystemLogItems(azureBatchJobAndTaskState) };
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
                    return new CombinedBatchTaskInfo { BatchTaskState = BatchTaskState.MissingBatchTask, FailureReason = BatchTaskState.MissingBatchTask.ToString() };
                case TaskState.Active:
                case TaskState.Preparing:
                    return new CombinedBatchTaskInfo { BatchTaskState = BatchTaskState.Initializing };
                case TaskState.Running:
                    return new CombinedBatchTaskInfo { BatchTaskState = BatchTaskState.Running };
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
        /// <param name="dockerRunParams">Additional docker run parameters, if any.</param>
        /// <param name="preCommandCommand">Command run before task script, if any.</param>
        /// <param name="poolHasContainerConfig">Indicates that <see cref="CloudTask.ContainerSettings"/> must be set.</param>
        /// <returns>Job preparation and main Batch tasks</returns>
        private async Task<CloudTask> ConvertTesTaskToBatchTaskAsync(TesTask task, string dockerRunParams, string[] preCommandCommand, bool poolHasContainerConfig)
        {
            dockerRunParams ??= string.Empty;
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
                                             && UrlContainsSas(f.Url) // Workaround for https://github.com/Azure/blobxfer/issues/132
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

            if (executor.Image.StartsWith("coa:65536/"))
            {
                sb.AppendLine($"write_ts ExecutorPullStart && EXECUTOR_IMAGE=$(docker load -i $(echo $(echo {executor.Image} | cut -d ':' -f 3).tar) | grep -Pxo 'Loaded image ID: (.*)' | cut -d ' ' -f 4)  && write_ts ExecutorPullEnd && \\");
            }
            else if (executorImageIsPublic)
            {
                // Private executor images are pulled via pool ContainerConfiguration
                sb.AppendLine($"EXECUTOR_IMAGE={executor.Image} && write_ts ExecutorPullStart && docker pull --quiet $EXECUTOR_IMAGE && write_ts ExecutorPullEnd && \\");
            }
            else
            {
                sb.AppendLine($"EXECUTOR_IMAGE={executor.Image} && \\");
            }

            // The remainder of the script downloads the inputs, runs the main executor container, and uploads the outputs, including the metrics.txt file
            // After task completion, metrics file is downloaded and used to populate the BatchNodeMetrics object
            sb.AppendLine($"write_kv ExecutorImageSizeInBytes $(docker inspect $EXECUTOR_IMAGE | grep \\\"Size\\\" | grep -Po '(?i)\\\"Size\\\":\\K([^,]*)') && \\");

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
            //if (preCommandCommand is not null)
            //{
            //    sb.Append($"docker run {dockerRunParams} --rm {volumeMountsOption} --entrypoint= --workdir / $EXECUTOR_IMAGE {preCommandCommand[0]}");
            //    if (preCommandCommand.Length > 1)
            //    {
            //        sb.AppendLine($" -c \"{ string.Join(" && ", preCommandCommand.Skip(1))}\"");
            //    }
            //    sb.AppendLine(" && \\");
            //}
            sb.AppendLine($"docker run {dockerRunParams} --rm {volumeMountsOption} --entrypoint= --workdir / $EXECUTOR_IMAGE {executor.Command[0]} -c \"{ string.Join(" && ", executor.Command.Skip(1))}\" && \\");
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

            var resourceFiles = Enumerable.Empty<ResourceFile>()
                .Append(ResourceFile.FromUrl(batchScriptSasUrl, $"/mnt{batchScriptPath}"))
                .Append(ResourceFile.FromUrl(downloadFilesScriptUrl, $"/mnt{downloadFilesScriptPath}"))
                .Append(ResourceFile.FromUrl(uploadFilesScriptSasUrl, $"/mnt{uploadFilesScriptPath}"));
            if (executor.Image.StartsWith("coa:65536/"))
            {
                var parts = executor.Image.Split('/', 3)[2].Split(':', 2);
                //resourceFiles = resourceFiles.Append(ResourceFile.FromAutoStorageContainer(HostConfigBlobsName, $"{batchExecutionDirectoryPath}/{parts[1]}.tar", $"{parts[0]}/task/{parts[1]}.tar"));
            }

            var cloudTask = new CloudTask(taskId, $"/bin/sh /mnt{batchScriptPath}")
            {
                UserIdentity = new UserIdentity(new AutoUserSpecification(elevationLevel: ElevationLevel.Admin, scope: AutoUserScope.Pool)),
                ResourceFiles = resourceFiles.ToList(),
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

            return cloudTask;

            static bool UrlContainsSas(string url)
            {
                var uri = new Uri(url, UriKind.Absolute);
                var query = uri.Query;
                return query?.Length > 1 && query[1..].Split('&').Any(QueryContainsSas);

                static bool QueryContainsSas(string arg)
                    => arg switch
                    {
                        var x when x.Split('=', 2)[0] == "sig" => true,
                        var x when x.Contains('=') => false,
                        var x when x.Contains("sas") => true, // PrivatePathsAndUrlsGetSasToken() uses this as a "sas" token
                        _ => false,
                    };
            }
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
        /// Gets the DockerHost configuration
        /// </summary>
        /// <param name="tesTask">The <see cref="TesTask"/> to schedule on Azure Batch</param>
        /// <returns></returns>
        private async Task<(StartTask startTask, BatchNodeInfo nodeInfo, string dockerParams, IEnumerable<ApplicationPackageReference> applicationPackages, string[] preCommand)> GetDockerHostConfiguration(TesTask tesTask)
        {
            StartTask taskResult = default;
            BatchNodeInfo batchResult = default;
            string dockerParams = null;
            string[] preCommand = null;
            var appPkgs = Enumerable.Empty<ApplicationPackageReference>();

            if (tesTask.Executors.First().Image.StartsWith("coa:65536/"))
            {
                await InitializeHostBlobs;

                var parts = tesTask.Executors.First().Image.Split('/', 3)[2].Split(':', 2);
                logger.LogInformation($"Verifying loadable container from docker host configuration '{parts[0]}'/'{parts[1]}.tar'");
                using var zip = new ZipArchive(BatchUtils.GetApplicationPayload($"{parts[0]}_task"));
                if (!zip.Entries.Any(e => e.Name.Equals($"{parts[1]}.tar", StringComparison.InvariantCulture)))
                {
                    throw new Exception();
                }
                var (Id, Version, _) = BatchUtils.GetApplicationDirectoryForHostConfigTask(parts[0], "task");
                appPkgs = appPkgs.Append(new() { ApplicationId = Id, Version = Version });
            }

            if (tesTask.Resources?.ContainsBackendParameterValue(TesResources.SupportedBackendParameters.docker_host_configuration) == true)
            {
                await InitializeHostBlobs;

                var hostConfigName = tesTask.Resources.GetBackendParameterValue(TesResources.SupportedBackendParameters.docker_host_configuration);
                logger.LogInformation($"Preparing pool and task using docker host configuration '{hostConfigName}'");
                HostConfig hostConfig = default;
                try
                {
                    using var hostConfigJson = new JsonTextReader(new StreamReader(BatchUtils.GetHostConfig(hostConfigName)));
                    hostConfig = JsonSerializer.CreateDefault().Deserialize<HostConfig>(hostConfigJson);
                }
                catch (FileNotFoundException e) { ThrowHostConfigNotFound(hostConfigName, e); }
                catch (DirectoryNotFoundException e) { ThrowHostConfigNotFound(hostConfigName, e); }

                batchResult = new()
                {
                    BatchImageOffer = hostConfig.Batch.Image.Offer,
                    BatchImagePublisher = hostConfig.Batch.Image.Publisher,
                    BatchImageSku = hostConfig.Batch.Image.Sku,
                    BatchImageVersion = hostConfig.Batch.Image.Version,
                    BatchNodeAgentSkuId = hostConfig.Batch.NodeAgentSkuId
                };

                dockerParams = hostConfig.DockerRun?.Parameters;
                preCommand = hostConfig.DockerRun?.PreTaskCmd;

                if (hostConfig.StartTask is not null)
                {
                    string appDir = default;
                    var isScriptInApp = false;
                    try
                    {
                        var (Id, Version, Variable) = BatchUtils.GetApplicationDirectoryForHostConfigTask(hostConfigName, "start");
                        appPkgs = appPkgs.Append(new() { ApplicationId = Id, Version = Version });
                        appDir = Variable;
                        isScriptInApp = BatchUtils.DoesHostConfigTaskIncludeTaskScript(hostConfigName, "start");
                    }
                    catch (KeyNotFoundException) { }

                    var resources = Enumerable.Empty<ResourceFile>();
                    foreach (var resource in hostConfig.StartTask.Resources ?? Array.Empty<HostConfig.StartTaskImpl.ResourceImpl>())
                    {
                        resources = resources.Append(await MakeResourceFile(resource.Url, resource.Path, resource.Mode));
                    }

                    taskResult = new(isScriptInApp ? $"/usr/bin/sh -c 'cd ${appDir} && ./{StartTaskScriptFilename}'" : $"/bin/env ./{StartTaskScriptFilename}")
                    {
                        UserIdentity = new UserIdentity(new AutoUserSpecification(elevationLevel: ElevationLevel.Admin, scope: AutoUserScope.Pool)),
                        ResourceFiles = resources.ToList(),
                        EnvironmentSettings = Enumerable.Empty<EnvironmentSetting>().Append(new("docker_host_configuration", hostConfigName)).ToList()
                    };
                }
            }

            return (taskResult, batchResult, dockerParams, appPkgs, preCommand);

            void ThrowHostConfigNotFound(string name, Exception e)
                => throw new TesException("NoHostConfig", $"Could not identify the configuration for the host config {name} for task {tesTask.Id}", e);

            async Task<ResourceFile> MakeResourceFile(string source, string relativeTargetDir, string mode = null)
            {
                var filePath = string.IsNullOrWhiteSpace(relativeTargetDir) ? null : relativeTargetDir;
                if (!Uri.TryCreate(source, UriKind.Absolute, out var uri))
                {
                    filePath ??= Path.GetFileName(source);
                    source = await storageAccessProvider.MapLocalPathToSasUrlAsync(source);
                }
                else if (!Uri.UriSchemeHttps.Equals(uri.Scheme, StringComparison.OrdinalIgnoreCase) && !Uri.UriSchemeHttp.Equals(uri.Scheme, StringComparison.OrdinalIgnoreCase))
                {
                    throw new Exception();
                }
                return ResourceFile.FromUrl(source, filePath ?? throw new Exception(), mode);
            }
        }

        /// <summary>
        /// Pool host configuration for <see cref="TesResources.SupportedBackendParameters.docker_host_configuration"/>
        /// </summary>
        [DataContract]
        public class HostConfig
        {
            /// <summary>
            /// Access to <see cref="BatchImpl"/>
            /// </summary>
            [DataMember(Name = "batch")]
            public BatchImpl Batch { get; set; }

            /// <summary>
            /// Configuration for <see cref="BatchNodeInfo"/>. <seealso cref="VirtualMachineConfiguration"/>
            /// </summary>
            [DataContract]
            public class BatchImpl
            {
                /// <summary>
                /// Access to <see cref="ImageImpl"/>
                /// </summary>
                [DataMember(Name = "image")]
                public ImageImpl Image { get; set; }

                /// <summary>
                /// Configures the SKU of Batch Node Agent to be provisioned on the compute node.
                /// </summary>
                [DataMember(Name = "node_agent_sku_id")]
                public string NodeAgentSkuId { get; set; }

                /// <summary>
                /// Configuration for <see cref="ImageReference"/>
                /// </summary>
                [DataContract]
                public class ImageImpl
                {
                    /// <summary>
                    /// Configures the offer type of the Azure Virtual Machines Marketplace Image.
                    /// </summary>
                    [DataMember(Name = "offer")]
                    public string Offer { get; set; }

                    /// <summary>
                    /// Configures the publisher of the Azure Virtual Machines Marketplace Image.
                    /// </summary>
                    [DataMember(Name = "publisher")]
                    public string Publisher { get; set; }

                    /// <summary>
                    /// Configures the SKU of the Azure Virtual Machines Marketplace Image.
                    /// </summary>
                    [DataMember(Name = "sku")]
                    public string Sku { get; set; }

                    /// <summary>
                    /// Configures the version of the Azure Virtual Machines Marketplace Image.
                    /// </summary>
                    [DataMember(Name = "version")]
                    public string Version { get; set; }
                }
            }

            /// <summary>
            /// Access to <see cref="StartTaskImpl"/>
            /// </summary>
            [DataMember(Name = "start_task")]
            public StartTaskImpl StartTask { get; set; }

            /// <summary>
            /// Configures <see cref="Microsoft.Azure.Batch.StartTask"/>
            /// </summary>
            [DataContract]
            public class StartTaskImpl
            {
                /// <summary>
                /// Access to <see cref="ResourceImpl"/>
                /// </summary>
                [DataMember(Name = "resources")]
                public ResourceImpl[] Resources { get; set; }

                /// <summary>
                /// Configures <see cref="ResourceFile"/>
                /// </summary>
                [DataContract]
                public class ResourceImpl
                {
                    /// <summary>
                    /// URL to download the file.
                    /// </summary>
                    [DataMember(Name = "url")]
                    public string Url { get; set; }

                    /// <summary>
                    /// Directory within the working directory to place file.
                    /// </summary>
                    /// <remarks>Must be set, and must include the filename</remarks>
                    [DataMember(Name = "path")]
                    public string Path { get; set; }

                    /// <summary>
                    /// The file permission mode attribute in octal format.
                    /// </summary>
                    /// <remarks>The default value is 0770 (aka ug rw).</remarks>
                    [DataMember(Name = "mode")]
                    public string Mode { get; set; }
                }
            }

            /// <summary>
            /// Access to <see cref="DockerRunImpl"/>
            /// </summary>
            [DataMember(Name = "docker_run")]
            public DockerRunImpl DockerRun { get; set; }

            /// <summary>
            /// Configures the built-in task command builder for each task in the pool.
            /// </summary>
            [DataContract]
            public class DockerRunImpl
            {
                /// <summary>
                /// Provides additional parameters to the &quot;docker run&quot; that runs each task's command.
                /// </summary>
                [DataMember(Name = "parameters")]
                public string Parameters { get; set; }

                /// <summary>
                /// Provides an additional command run in the same docker container as the task's command, in a separate invocation, called right before the task's own script.
                /// </summary>
                /// <remarks>This does not supply a shell nor does it insert a &apos;-c&apos; between the first and additional elements. It does, however, quote all except the first element.</remarks>
                [DataMember(Name = "pretask_command")]
                public string[] PreTaskCmd { get; set; }
            }
        }

        /// <summary>
        /// Constructs an Azure Batch Container Configuration instance
        /// </summary>
        /// <param name="executorImage">The image name for the current <see cref="TesTask"/></param>
        /// <returns></returns>
        private async Task<ContainerConfiguration> GetContainerConfigurationIfNeeded(string executorImage)
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
        /// <param name="nodeInfo"></param>
        /// <param name="containerConfiguration">The configuration to download private images</param>
        /// <param name="applicationPackages"></param>
        /// <param name="startTask">The start task for all jobs/tasks in the pool</param>
        /// <param name="jobId"></param>
        /// <param name="identityResourceId"></param>
        /// <returns>An Azure Batch Pool specifier</returns>
        private async Task<PoolInformation> CreateAutoPoolModePoolInformation(string vmSize, bool preemptible, BatchNodeInfo nodeInfo, ContainerConfiguration containerConfiguration, IEnumerable<ApplicationPackageReference> applicationPackages, StartTask startTask, string jobId = null, string identityResourceId = null)
        {
            if (!string.IsNullOrWhiteSpace(identityResourceId))
            {
                _ = jobId ?? throw new ArgumentNullException(nameof(jobId));
            }

            var poolSpecification = GetPoolSpecification(vmSize, preemptible, nodeInfo, containerConfiguration, applicationPackages, startTask);

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
            => new(BatchModels.PoolIdentityType.UserAssigned, new Dictionary<string, BatchModels.UserAssignedIdentities>() { [identityResourceId] = new() });

        /// <summary>
        /// Generate the PoolSpecification object
        /// </summary>
        /// <param name="vmSize"></param>
        /// <param name="preemptible"></param>
        /// <param name="nodeInfo"></param>
        /// <param name="containerConfiguration"></param>
        /// <param name="applicationPackages"></param>
        /// <param name="startTask"></param>
        /// <returns></returns>
        private PoolSpecification GetPoolSpecification(string vmSize, bool? preemptible, BatchNodeInfo nodeInfo, ContainerConfiguration containerConfiguration, IEnumerable<ApplicationPackageReference> applicationPackages, StartTask startTask)
        {
            var vmConfig = new VirtualMachineConfiguration(
                imageReference: new ImageReference(
                    nodeInfo.BatchImageOffer,
                    nodeInfo.BatchImagePublisher,
                    nodeInfo.BatchImageSku,
                    nodeInfo.BatchImageVersion),
                nodeAgentSkuId: nodeInfo.BatchNodeAgentSkuId)
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

            var appPkgs = applicationPackages?.ToList();
            if (appPkgs is not null && appPkgs.Count != 0)
            {
                poolSpecification.ApplicationPackageReferences = appPkgs;
            }

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
        private async Task CheckBatchAccountQuotas(VirtualMachineInformation vmInfo)
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

            if ((totalCoresInUseByVmFam + workflowCoresRequirement) > vmFamQuota)
            {
                throw new AzureBatchQuotaMaxedOutException($"Not enough core quota remaining to schedule task requiring {workflowCoresRequirement} dedicated {vmFamily} cores. There are {totalCoresInUseByVmFam} cores in use out of {vmFamQuota}.");
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

            throw new AzureBatchVirtualMachineAvailabilityException(noVmFoundMessage);
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
