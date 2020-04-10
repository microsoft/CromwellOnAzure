// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
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
        private static readonly Regex externalStorageContainerRegex = new Regex(@"(https://([^\.]*).blob.core.windows.net/)([^\?/]+)/*?(\?.+)");
        private static readonly TimeSpan sasTokenDuration = TimeSpan.FromDays(3);
        private readonly ILogger logger;
        private readonly IAzureProxy azureProxy;
        private readonly string defaultStorageAccountName;
        private List<StorageAccountInfo> storageAccountCache;
        private List<ContainerRegistry> containerRegistryCache;
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
            usePreemptibleVmsOnly = bool.TryParse(configuration["UsePreemptibleVmsOnly"], out var temp) ? temp : false;

            externalStorageContainers = configuration["ExternalStorageContainers"]?.Split(new[] { ',', ';', '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries)
                .SelectMany(e => externalStorageContainerRegex.Matches(e).Cast<Match>()
                    .Select(m => new ExternalStorageContainerInfo { BlobEndpoint = m.Groups[1].Value, AccountName = m.Groups[2].Value, ContainerName = m.Groups[3].Value, SasToken = m.Groups[4].Value }))
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

        /// <summary>
        /// Returns an Azure Storage Blob or Container URL with SAS token given a file path that uses the following convention: /accountName/containerName/blobPath
        /// </summary>
        /// <param name="path">The file path to convert. Two-part path is treated as container path. Paths with three or more parts are treated as blobs.</param>
        /// <param name="getContainerSas">Get the container SAS even if path is longer than two parts</param>
        /// <returns>An Azure Block Blob or Container URL with SAS token</returns>
        private async Task<string> MapLocalPathToSasUrlAsync(string path, bool getContainerSas = false)
        {
            // TODO: Cache the keys for short period of time
            // TODO: Optional: If path is /container/... where container matches the name of the container in the default storage account, prepend the account name to the path.
            // This would allow the user to omit the account name for files stored in the default storage account

            // /cromwell-executions/... URLs become /defaultStorageAccountName/cromwell-executions/... to unify how URLs starting with /acct/container/... pattern are handled.
            if (path.StartsWith(CromwellPathPrefix, StringComparison.OrdinalIgnoreCase))
            {
                path = $"/{defaultStorageAccountName}{path}";
            }

            var pathParts = path.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);

            if (pathParts.Length < 2)
            {
                throw new Exception($"Invalid path '{path}'. Paths must have at least two parts.");
            }

            var accountName = pathParts[0];
            var containerName = pathParts[1];
            var isContainerPath = pathParts.Length == 2;
            var containerPath = string.Join("/", pathParts.Take(2));

            var externalStorageAccountInfo = externalStorageContainers?.FirstOrDefault(e => e.AccountName.Equals(accountName, StringComparison.OrdinalIgnoreCase) && e.ContainerName.Equals(containerName, StringComparison.OrdinalIgnoreCase));

            string url;
            string sas;

            if (externalStorageAccountInfo != null)
            {
                url = path.Replace(externalStorageAccountInfo.AccountName + "/", externalStorageAccountInfo.BlobEndpoint, StringComparison.OrdinalIgnoreCase).Trim('/');
                sas = externalStorageAccountInfo.SasToken;
            }
            else
            {
                try
                {
                    if (storageAccountCache == null || !storageAccountCache.Any(a => a.Name.Equals(accountName, StringComparison.OrdinalIgnoreCase)))
                    {
                        storageAccountCache = (await azureProxy.GetAccessibleStorageAccountsAsync()).ToList();
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, $"Could not get the list of storage accounts when trying to get URL of the path '{path}'. Most likely the TES app service does not have permission to any storage accounts.");
                    return null;
                }

                var storageAccountInfo = storageAccountCache.FirstOrDefault(a => a.Name.Equals(accountName, StringComparison.OrdinalIgnoreCase));

                if (storageAccountInfo == null)
                {
                    logger.LogError($"Could not find storage account '{accountName}' corresponding to path '{path}'. Either the account does not exist or the TES app service does not have permission to it.");
                    return null;
                }

                try
                {
                    var accountKey = await azureProxy.GetStorageAccountKeyAsync(storageAccountInfo);
                    url = path.Replace(storageAccountInfo.Name + "/", storageAccountInfo.BlobEndpoint, StringComparison.OrdinalIgnoreCase).Trim('/');

                    if (isContainerPath || getContainerSas)
                    {
                        var policy = new SharedAccessBlobPolicy()
                        {
                            Permissions = SharedAccessBlobPermissions.Add | SharedAccessBlobPermissions.Create | SharedAccessBlobPermissions.List | SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Write,
                            SharedAccessExpiryTime = DateTime.Now.Add(sasTokenDuration)
                        };

                        var containerUrl = containerPath.Replace(storageAccountInfo.Name + "/", storageAccountInfo.BlobEndpoint, StringComparison.OrdinalIgnoreCase).Trim('/');
                        sas = new CloudBlobContainer(new Uri(containerUrl), new StorageCredentials(storageAccountInfo.Name, accountKey)).GetSharedAccessSignature(policy, null, SharedAccessProtocol.HttpsOnly, null);
                    }
                    else
                    {
                        var policy = new SharedAccessBlobPolicy() { Permissions = SharedAccessBlobPermissions.Read, SharedAccessExpiryTime = DateTime.Now.Add(sasTokenDuration) };
                        sas = new CloudBlob(new Uri(url), new StorageCredentials(storageAccountInfo.Name, accountKey)).GetSharedAccessSignature(policy, null, null, SharedAccessProtocol.HttpsOnly, null);
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, $"Could not get the key of storage account '{accountName}'. Make sure that the TES app service has Contributor access to it.");
                    return null;
                }
            }

            return $"{url}{sas}";
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

            var downloadFilesScriptContent = string.Join(" && ", filesToDownload.Select(f => $"blobxfer download --storage-url '{f.Url}' --local-path '{f.Path}' --chunk-size-bytes 104857600 --rename --no-recursive"));
            var downloadFilesScriptPath = $"{batchExecutionDirectoryPath}/{DownloadFilesScriptFileName}";
            var writableDownloadFilesScriptUrl = new Uri(await MapLocalPathToSasUrlAsync(downloadFilesScriptPath, getContainerSas: true));
            var downloadFilesScriptUrl = await MapLocalPathToSasUrlAsync(downloadFilesScriptPath);
            await azureProxy.UploadBlobAsync(writableDownloadFilesScriptUrl, downloadFilesScriptContent);

            var filesToUpload = await Task.WhenAll(
                task.Outputs.Select(async f =>
                    new TesOutput { Path = f.Path, Url = await MapLocalPathToSasUrlAsync(f.Path, getContainerSas: true), Name = f.Name, Type = f.Type }));

            var uploadFilesScriptContent = string.Join(" && ", filesToUpload.Select(f => $"blobxfer upload --storage-url '{f.Url}' --local-path '{f.Path}' --one-shot-bytes 104857600 {(f.Type == TesFileType.FILEEnum ? "--rename --no-recursive" : "")}"));
            var uploadFilesScriptPath = $"{batchExecutionDirectoryPath}/{UploadFilesScriptFileName}";
            var writableUploadFilesScriptUrl = new Uri(await MapLocalPathToSasUrlAsync(uploadFilesScriptPath, getContainerSas: true));
            var uploadFilesScriptUrl = await MapLocalPathToSasUrlAsync(uploadFilesScriptPath);
            await azureProxy.UploadBlobAsync(writableUploadFilesScriptUrl, uploadFilesScriptContent);

            var executor = task.Executors.First();

            var volumeMountsOption = $"-v /mnt{cromwellPathPrefixWithoutEndSlash}:{cromwellPathPrefixWithoutEndSlash}";

            var executorImageIsPublic = (await GetContainerRegistryAsync(executor.Image)) == null;

            var taskCommand = $@"
                docker pull --quiet {BlobxferImageName} && \
                {(executorImageIsPublic ? $"docker pull --quiet {executor.Image} &&" : "")} \
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
            else if (inputFile.Url.StartsWith("http", StringComparison.OrdinalIgnoreCase))
            {
                inputFileUrl = inputFile.Url;
            }
            else
            {
                var mappedUrl = await MapLocalPathToSasUrlAsync(inputFile.Url);

                if (mappedUrl != null)
                {
                    inputFileUrl = mappedUrl;
                }
                else
                {
                    throw new Exception($"Unsupported input URL '{inputFile.Url}' for task Id {taskId}. Must start with 'http', '{CromwellPathPrefix}' or use '/accountName/containerName/blobPath' pattern where TES service has Contributor access to the storage account.");
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

            var containerRegistry = await GetContainerRegistryAsync(image);

            if (containerRegistry != null)
            {
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
        /// Gets the <see cref="ContainerRegistryInfo"/> associated with the given image
        /// </summary>
        /// <returns>The <see cref="ContainerRegistry"/></returns>
        private async Task<ContainerRegistry> GetContainerRegistryAsync(string imageName)
        {
            if (containerRegistryCache == null || !containerRegistryCache.Any(reg => imageName.StartsWith(reg.RegistryServer, StringComparison.OrdinalIgnoreCase)))
            {
                containerRegistryCache = (await azureProxy.GetAccessibleContainerRegistriesAsync())
                    .Select(c => new ContainerRegistry(userName: c.Username, registryServer: c.RegistryServer, password: c.Password))
                    .ToList();
            }

            return containerRegistryCache.FirstOrDefault(reg => imageName.StartsWith(reg.RegistryServer, StringComparison.OrdinalIgnoreCase));
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
            var preemptible = usePreemptibleVmsOnly || tesResources.Preemptible.GetValueOrDefault(false);

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
