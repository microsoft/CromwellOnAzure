// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
            usePreemptibleVmsOnly = bool.TryParse(configuration["UsePreemptibleVmsOnly"], out var temp) && temp;

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

            bool tesStateIsQueuedInitializingOrRunning(TesTask tesTask) => tesTask.State == TesState.QUEUEDEnum || tesTask.State == TesState.INITIALIZINGEnum || tesTask.State == TesState.RUNNINGEnum;
            bool tesStateIsInitializingOrRunning(TesTask tesTask) => tesTask.State == TesState.INITIALIZINGEnum || tesTask.State == TesState.RUNNINGEnum;
            bool tesStateIsQueuedOrInitializing(TesTask tesTask) => tesTask.State == TesState.QUEUEDEnum || tesTask.State == TesState.INITIALIZINGEnum;

            tesTaskStateTransitions = new List<TesTaskStateTransition>()
            {
                new TesTaskStateTransition(tesTask => tesTask.State == TesState.CANCELEDEnum && tesTask.IsCancelRequested, batchTaskState: null, async tesTask => { await this.azureProxy.DeleteBatchJobAsync(tesTask.Id); tesTask.IsCancelRequested = false; }),
                new TesTaskStateTransition(TesState.QUEUEDEnum, BatchTaskState.JobNotFound, tesTask => AddBatchJobAsync(tesTask)),
                new TesTaskStateTransition(TesState.QUEUEDEnum, BatchTaskState.MissingBatchTask, tesTask => this.azureProxy.DeleteBatchJobAsync(tesTask.Id), TesState.QUEUEDEnum),
                new TesTaskStateTransition(TesState.QUEUEDEnum, BatchTaskState.Initializing, TesState.INITIALIZINGEnum),
                new TesTaskStateTransition(TesState.INITIALIZINGEnum, BatchTaskState.NodeAllocationFailed, tesTask => this.azureProxy.DeleteBatchJobAsync(tesTask.Id), TesState.QUEUEDEnum),
                new TesTaskStateTransition(tesStateIsQueuedOrInitializing, BatchTaskState.Running, TesState.RUNNINGEnum),
                new TesTaskStateTransition(tesStateIsQueuedInitializingOrRunning, BatchTaskState.MoreThanOneActiveJobFound, tesTask => this.azureProxy.DeleteBatchJobAsync(tesTask.Id), TesState.SYSTEMERROREnum),
                new TesTaskStateTransition(tesStateIsQueuedInitializingOrRunning, BatchTaskState.CompletedSuccessfully, TesState.COMPLETEEnum),
                new TesTaskStateTransition(tesStateIsQueuedInitializingOrRunning, BatchTaskState.CompletedWithErrors, TesState.EXECUTORERROREnum),
                new TesTaskStateTransition(tesStateIsQueuedInitializingOrRunning, BatchTaskState.ActiveJobWithMissingAutoPool, tesTask => this.azureProxy.DeleteBatchJobAsync(tesTask.Id), TesState.QUEUEDEnum),
                new TesTaskStateTransition(tesStateIsQueuedInitializingOrRunning, BatchTaskState.NodeFailedDuringStartupOrExecution, tesTask => this.azureProxy.DeleteBatchJobAsync(tesTask.Id), TesState.EXECUTORERROREnum),
                new TesTaskStateTransition(tesStateIsInitializingOrRunning, BatchTaskState.JobNotFound, TesState.SYSTEMERROREnum),
                new TesTaskStateTransition(tesStateIsInitializingOrRunning, BatchTaskState.MissingBatchTask, tesTask => this.azureProxy.DeleteBatchJobAsync(tesTask.Id), TesState.SYSTEMERROREnum),
                new TesTaskStateTransition(tesStateIsInitializingOrRunning, BatchTaskState.NodePreempted, tesTask => this.azureProxy.DeleteBatchJobAsync(tesTask.Id), TesState.QUEUEDEnum) // TODO: Implement preemption detection
            };
        }

        /// <summary>
        /// Iteratively manages execution of a <see cref="TesTask"/> on Azure Batch until completion or failure
        /// </summary>
        /// <param name="tesTask">The <see cref="TesTask"/></param>
        /// <returns>True if the TES task needs to be persisted.</returns>
        public async Task<bool> ProcessTesTaskAsync(TesTask tesTask)
        {
            var (batchTaskState, executionInfo) = await GetBatchTaskStateAsync(tesTask.Id);
            var tesTaskChanged = await HandleTesTaskTransitionAsync(tesTask, batchTaskState, executionInfo);
            return tesTaskChanged;
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

                await CheckBatchAccountQuotas((int)tesTask.Resources.CpuCores.GetValueOrDefault(DefaultCoreCount), virtualMachineInfo.LowPriority);

                // TODO?: Support for multiple executors. Cromwell has single executor per task.
                var dockerImage = tesTask.Executors.First().Image;
                var cloudTask = await ConvertTesTaskToBatchTaskAsync(tesTask);
                var poolInformation = await CreatePoolInformation(dockerImage, virtualMachineInfo.VmSize, virtualMachineInfo.LowPriority);
                tesTask.Resources.VmInfo = virtualMachineInfo;

                logger.LogInformation($"Creating batch job for TES task {tesTask.Id}. Using VM size {virtualMachineInfo}.");
                await azureProxy.CreateBatchJobAsync(jobId, cloudTask, poolInformation);

                tesTask.State = TesState.INITIALIZINGEnum;
            }
            catch (AzureBatchQuotaMaxedOutException exception)
            {
                logger.LogInformation($"Not enough quota available for task Id {tesTask.Id}. Reason: {exception.Message}. Task will remain in queue.");
            }
            catch (Exception exc)
            {
                tesTask.State = TesState.SYSTEMERROREnum;
                tesTask.WriteToSystemLog(exc.Message, exc.StackTrace);
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
        private async Task<(BatchTaskState, string)> GetBatchTaskStateAsync(string tesTaskId)
        {
            var batchJobAndTaskState = await azureProxy.GetBatchJobAndTaskStateAsync(tesTaskId);

            if (batchJobAndTaskState.ActiveJobWithMissingAutoPool)
            {
                var batchJobInfo = JsonConvert.SerializeObject(batchJobAndTaskState);
                logger.LogWarning($"Found active job without auto pool for TES task {tesTaskId}. Deleting the job and requeuing the task. BatchJobInfo: {batchJobInfo}");
                return (BatchTaskState.ActiveJobWithMissingAutoPool, null);
            }

            if (batchJobAndTaskState.MoreThanOneActiveJobFound)
            {
                return (BatchTaskState.MoreThanOneActiveJobFound, null);
            }

            switch (batchJobAndTaskState.JobState)
            {
                case null:
                case JobState.Deleting:
                    return (BatchTaskState.JobNotFound, null); // Treating the deleting job as if it doesn't exist
                case JobState.Active:
                    {
                        if (batchJobAndTaskState.NodeAllocationFailed)
                        {
                            return (BatchTaskState.NodeAllocationFailed, null);
                        }

                        if (batchJobAndTaskState.NodeErrorCode != null)
                        {
                            var message = $"{batchJobAndTaskState.NodeErrorCode}: {(batchJobAndTaskState.NodeErrorDetails != null ? string.Join(", ", batchJobAndTaskState.NodeErrorDetails) : null)}";
                            return (BatchTaskState.NodeFailedDuringStartupOrExecution, message);
                        }

                        break;
                    }
                case JobState.Terminating:
                case JobState.Completed:
                    break;
                default:
                    throw new Exception($"Found batch job {tesTaskId} in unexpected state: {batchJobAndTaskState.JobState}");
            }

            switch (batchJobAndTaskState.TaskState)
            {
                case null:
                    return (BatchTaskState.MissingBatchTask, null);
                case TaskState.Active:
                case TaskState.Preparing:
                    return (BatchTaskState.Initializing, null);
                case TaskState.Running:
                    return (BatchTaskState.Running, null);
                case TaskState.Completed:
                    var batchJobInfo = JsonConvert.SerializeObject(batchJobAndTaskState);

                    if (batchJobAndTaskState.TaskExitCode == 0 && batchJobAndTaskState.TaskFailureInformation == null)
                    {
                        return (BatchTaskState.CompletedSuccessfully, batchJobInfo);
                    }
                    else
                    {
                        logger.LogError($"Task {tesTaskId} failed. ExitCode: {batchJobAndTaskState.TaskExitCode}, BatchJobInfo: {batchJobInfo}");
                        return (BatchTaskState.CompletedWithErrors, $"ExitCode: {batchJobAndTaskState.TaskExitCode}, BatchJobInfo: {batchJobInfo}");
                    }
                default:
                    throw new Exception($"Found batch task {tesTaskId} in unexpected state: {batchJobAndTaskState.TaskState}");
            }
        }

        /// <summary>
        /// Transitions the <see cref="TesTask"/> to the new state, based on the rules defined in the tesTaskStateTransitions list.
        /// </summary>
        /// <param name="tesTask">TES task</param>
        /// <param name="batchTaskState">Current Azure Batch task state</param>
        /// <param name="executionInfo">Batch job execution info</param>
        /// <returns>True if the TES task was changed.</returns>
        private async Task<bool> HandleTesTaskTransitionAsync(TesTask tesTask, BatchTaskState batchTaskState, string executionInfo)
        {
            var tesTaskChanged = false;

            var mapItem = tesTaskStateTransitions
                .FirstOrDefault(m => (m.Condition == null || m.Condition(tesTask)) && (m.CurrentBatchTaskState == null || m.CurrentBatchTaskState == batchTaskState));

            if (mapItem != null)
            {
                if (mapItem.Action != null)
                {
                    await mapItem.Action(tesTask);

                    if (executionInfo != null)
                    {
                        tesTask.WriteToSystemLog(executionInfo);
                    }

                    tesTaskChanged = true;
                }

                if (mapItem.NewTesTaskState != null && mapItem.NewTesTaskState != tesTask.State)
                {
                    tesTask.State = mapItem.NewTesTaskState.Value;

                    if (executionInfo != null)
                    {
                        tesTask.WriteToSystemLog(executionInfo);
                    }

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
            var cromwellExecutionDirectoryPath = GetParentPath(task.Inputs.FirstOrDefault(IsCromwellCommandScript)?.Path);

            if (cromwellExecutionDirectoryPath == null)
            {
                throw new Exception($"Could not identify Cromwell execution directory path for task {task.Id}. This TES instance supports Cromwell tasks only.");
            }

            foreach (var output in task.Outputs)
            {
                if (!output.Path.StartsWith(CromwellPathPrefix, StringComparison.OrdinalIgnoreCase))
                {
                    throw new Exception($"Unsupported output path '{output.Path}' for task Id {task.Id}. Must start with {CromwellPathPrefix}");
                }
            }

            var batchExecutionDirectoryPath = $"{cromwellExecutionDirectoryPath}/{BatchExecutionDirectoryName}";

            // TODO: Cromwell bug: Cromwell command write_tsv() generates a file in the execution directory, for example execution/write_tsv_3922310b441805fc43d52f293623efbc.tmp. These are not passed on to TES inputs.
            // WORKAROUND: Get the list of files in the execution directory and add them to task inputs.
            var executionDirectoryUri = new Uri(await MapLocalPathToSasUrlAsync(cromwellExecutionDirectoryPath, getContainerSas: true));
            var blobsInExecutionDirectory = (await azureProxy.ListBlobsAsync(executionDirectoryUri)).Where(b => !b.EndsWith($"/{CromwellScriptFileName}")).Where(b => !b.Contains($"/{BatchExecutionDirectoryName}/"));
            var additionalInputFiles = blobsInExecutionDirectory.Select(b => $"{CromwellPathPrefix}{b}").Select(b => new TesInput { Content = null, Path = b, Url = b, Name = Path.GetFileName(b), Type = TesFileType.FILEEnum });
            var filesToDownload = await Task.WhenAll(inputFiles.Union(additionalInputFiles).Select(async f => await GetTesInputFileUrl(f, task.Id, queryStringsToRemoveFromLocalFilePaths)));

            // Using --include and not using --no-recursive as a workaround for https://github.com/Azure/blobxfer/issues/123
            var downloadFilesScriptContent = string.Join(" && ", filesToDownload.Select(f => {
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

            var taskCommand = $@"
                docker pull --quiet {BlobxferImageName} && \
                {(executorImageIsPublic ? $"docker pull --quiet {executor.Image} && \\" : "")}
                docker run --rm {volumeMountsOption} --entrypoint=/bin/sh {BlobxferImageName} {downloadFilesScriptPath} && \
                chmod -R o+rwx /mnt{cromwellPathPrefixWithoutEndSlash} && \
                docker run --rm {volumeMountsOption} --entrypoint= --workdir / {executor.Image} {executor.Command[0]} -c '{ string.Join(" && ", executor.Command.Skip(1))}' && \
                docker run --rm {volumeMountsOption} --entrypoint=/bin/sh {BlobxferImageName} {uploadFilesScriptPath}
            ";

            var batchExecutionDirectoryUrl = await MapLocalPathToSasUrlAsync($"{batchExecutionDirectoryPath}", getContainerSas: true);

            var cloudTask = new CloudTask(taskId, $"/bin/sh -c \"{taskCommand.Trim()}\"")
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
                throw new Exception($"Unsupported input path '{inputFile.Path}' for task Id {taskId}. Must start with '{CromwellPathPrefix}'.");
            }

            if (inputFile.Url != null && inputFile.Content != null)
            {
                throw new Exception("Input Url and Content cannot be both set");
            }

            if (inputFile.Url == null && inputFile.Content == null)
            {
                throw new Exception("One of Input Url or Content must be set");
            }

            if (inputFile.Type == TesFileType.DIRECTORYEnum)
            {
                throw new Exception("Directory input is not supported.");
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
                    throw new Exception($"Unsupported input URL '{inputFile.Url}' for task Id {taskId}. Must start with 'http', '{CromwellPathPrefix}' or use '/accountName/containerName/blobName' pattern where TES service has Contributor access to the storage account.");
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

        /// <summary>
        /// Class that captures how <see cref="TesTask"/> transitions from current state to the new state, given the current Batch task state and optional condition. 
        /// Transitions typically include an action that needs to run in order for the task to move to the new state.
        /// </summary>
        private class TesTaskStateTransition
        {
            public TesTaskStateTransition(TesState currentTesTaskState, BatchTaskState batchTaskState, TesState newTesTaskState)
                : this(tesTask => tesTask.State == currentTesTaskState, batchTaskState, null, newTesTaskState)
            {
            }

            public TesTaskStateTransition(Func<TesTask, bool> condition, BatchTaskState batchTaskState, TesState newTesTaskState)
            : this(condition, batchTaskState, null, newTesTaskState)
            {
            }

            public TesTaskStateTransition(TesState currentTesTaskState, BatchTaskState batchTaskState, Func<TesTask, Task> transition, TesState? newTesTaskState = null)
                : this(tesTask => tesTask.State == currentTesTaskState, batchTaskState, transition, newTesTaskState)
            {
            }

            public TesTaskStateTransition(Func<TesTask, bool> condition, BatchTaskState? batchTaskState, Func<TesTask, Task> action, TesState? newTesTaskState = null)
            {
                Condition = condition;
                CurrentBatchTaskState = batchTaskState;
                Action = action;
                NewTesTaskState = newTesTaskState;
            }

            public Func<TesTask, bool> Condition { get; set; }
            public BatchTaskState? CurrentBatchTaskState { get; set; }
            public Func<TesTask, Task> Action { get; set; }
            public TesState? NewTesTaskState { get; set; }
        }

        private class ExternalStorageContainerInfo
        {
            public string AccountName { get; set; }
            public string ContainerName { get; set; }
            public string BlobEndpoint { get; set; }
            public string SasToken { get; set; }
        }
    }
}
