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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Newtonsoft.Json;
using TesApi.Models;
using TesApi.Web;

namespace TesApi.Tests
{
    [TestClass]
    public class BatchSchedulerTests
    {
        private static readonly Regex downloadFilesScriptRegex = new Regex(@"--storage-url '([^']*)' --local-path '([^']*)'");

        [TestMethod]
        public async Task TesTaskFailsWithSystemErrorWhenNoSuitableVmExists()
        {
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;

            azureProxyReturnValues.VmSizesAndPrices = new List<VirtualMachineInfo> {
                new VirtualMachineInfo { VmSize = "VmSize1", LowPriority = true, NumberOfCores = 1, MemoryInGB = 4, ResourceDiskSizeInGB = 20, PricePerHour = 1 },
                new VirtualMachineInfo { VmSize = "VmSize2", LowPriority = true, NumberOfCores = 2, MemoryInGB = 8, ResourceDiskSizeInGB = 40, PricePerHour = 2 }};

            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 1, RamGb = 1, DiskGb = 10, Preemptible = false }, azureProxyReturnValues));
            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 4, RamGb = 1, DiskGb = 10, Preemptible = true }, azureProxyReturnValues));
            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 1, RamGb = 10, DiskGb = 10, Preemptible = true }, azureProxyReturnValues));
            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 1, RamGb = 1, DiskGb = 50, Preemptible = true }, azureProxyReturnValues));
        }

        [TestMethod]
        public async Task TesTaskFailsWithSystemErrorWhenTotalBatchQuotaIsSetTooLow()
        {
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.BatchQuotas = new AzureProxy.AzureBatchAccountQuotas { ActiveJobAndJobScheduleQuota = 1, PoolQuota = 1, DedicatedCoreQuota = 1, LowPriorityCoreQuota = 10 };

            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 2, RamGb = 1, Preemptible = false }, azureProxyReturnValues));
            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 11, RamGb = 1, Preemptible = true }, azureProxyReturnValues));
        }

        [TestMethod]
        public async Task TesTaskFailsWhenBatchNodeDiskIsFull()
        {
            var tesTask = GetTesTask();

            var errorMessage = await ProcessTesTaskAndGetFirstLogMessageAsync(tesTask, BatchJobAndTaskStates.NodeDiskFull);

            Assert.AreEqual(TesState.EXECUTORERROREnum, tesTask.State);
            Assert.AreEqual($"There is not enough disk space on the VM that was selected for the task.", errorMessage);
        }

        public async Task TesTaskRemainsQueuedWhenBatchQuotaIsTemporarilyUnavailable()
        {
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;

            azureProxyReturnValues.VmSizesAndPrices = new List<VirtualMachineInfo> {
                new VirtualMachineInfo { VmSize = "VmSize1", LowPriority = false, NumberOfCores = 2, MemoryInGB = 4, ResourceDiskSizeInGB = 20, PricePerHour = 1 },
                new VirtualMachineInfo { VmSize = "VmSize1", LowPriority = true, NumberOfCores = 2, MemoryInGB = 4, ResourceDiskSizeInGB = 20, PricePerHour = 2 }};

            azureProxyReturnValues.BatchQuotas = new AzureProxy.AzureBatchAccountQuotas { ActiveJobAndJobScheduleQuota = 1, PoolQuota = 1, DedicatedCoreQuota = 10, LowPriorityCoreQuota = 20 };

            azureProxyReturnValues.ActiveNodeCountByVmSize = new List<AzureProxy.AzureBatchNodeCount> {
                new AzureProxy.AzureBatchNodeCount { VirtualMachineSize = "VmSize1", DedicatedNodeCount = 4, LowPriorityNodeCount = 8 }  // 8 (4 * 2) dedicated and 16 ( 8 * 2) low pri cores are used
            };

            Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 3, RamGb = 1, Preemptible = false }, azureProxyReturnValues));
            Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 5, RamGb = 1, Preemptible = true }, azureProxyReturnValues));
            Assert.AreEqual(TesState.INITIALIZINGEnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 4, RamGb = 1, Preemptible = true }, azureProxyReturnValues));
        }

        [TestMethod]
        public async Task BatchTaskResourcesIncludeDownloadAndUploadScripts()
        {
            (_, var cloudTask, _) = await ProcessTesTaskAndGetBatchJobArgumentsAsync();

            Assert.AreEqual(2, cloudTask.ResourceFiles.Count());
            Assert.IsTrue(cloudTask.ResourceFiles.Any(f => f.FilePath.Equals("/mnt/cromwell-executions/workflow1/workflowId1/call-Task1/execution/_batch/upload_files_script")));
            Assert.IsTrue(cloudTask.ResourceFiles.Any(f => f.FilePath.Equals("/mnt/cromwell-executions/workflow1/workflowId1/call-Task1/execution/_batch/download_files_script")));
        }

        [TestMethod]
        public async Task BatchJobContainsExpectedPoolInformation()
        {
            (_, _, var poolInformation) = await ProcessTesTaskAndGetBatchJobArgumentsAsync();

            Assert.AreEqual("TES", poolInformation.AutoPoolSpecification.AutoPoolIdPrefix);
            Assert.AreEqual("VmSizeDedicated1", poolInformation.AutoPoolSpecification.PoolSpecification.VirtualMachineSize);
            Assert.AreEqual(1, poolInformation.AutoPoolSpecification.PoolSpecification.TargetDedicatedComputeNodes);
            Assert.AreEqual(1, poolInformation.AutoPoolSpecification.PoolSpecification.VirtualMachineConfiguration.ContainerConfiguration.ContainerRegistries.Count);
        }

        [TestMethod]
        public async Task NewTesTaskGetsScheduledSuccessfully()
        {
            var tesTask = GetTesTask();

            await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig(), GetMockAzureProxy(AzureProxyReturnValues.Defaults));

            Assert.AreEqual(TesState.INITIALIZINGEnum, tesTask.State);
        }

        [TestMethod]
        public async Task PreemptibleTesTaskGetsScheduledToLowPriorityVm()
        {
            var tesTask = GetTesTask();
            tesTask.Resources.Preemptible = true;

            (_, _, var poolInformation) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig(), GetMockAzureProxy(AzureProxyReturnValues.Defaults));

            Assert.AreEqual("VmSizeLowPri1", poolInformation.AutoPoolSpecification.PoolSpecification.VirtualMachineSize);
            Assert.AreEqual(1, poolInformation.AutoPoolSpecification.PoolSpecification.TargetLowPriorityComputeNodes);
            Assert.AreEqual(0, poolInformation.AutoPoolSpecification.PoolSpecification.TargetDedicatedComputeNodes);
        }

        [TestMethod]
        public async Task NonPreemptibleTesTaskGetsScheduledToDedicatedVm()
        {
            var tesTask = GetTesTask();
            tesTask.Resources.Preemptible = false;

            (_, _, var poolInformation) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig(), GetMockAzureProxy(AzureProxyReturnValues.Defaults));

            Assert.AreEqual("VmSizeDedicated1", poolInformation.AutoPoolSpecification.PoolSpecification.VirtualMachineSize);
            Assert.AreEqual(1, poolInformation.AutoPoolSpecification.PoolSpecification.TargetDedicatedComputeNodes);
            Assert.AreEqual(0, poolInformation.AutoPoolSpecification.PoolSpecification.TargetLowPriorityComputeNodes);
        }

        [TestMethod]
        public async Task TesTaskGetsScheduledToLowPriorityVmIfSettingUsePreemptibleVmsOnlyIsSet()
        {
            var tesTask = GetTesTask();
            tesTask.Resources.Preemptible = false;

            var config = GetMockConfig();
            config["UsePreemptibleVmsOnly"] = "true";

            (_, _, var poolInformation) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, config, GetMockAzureProxy(AzureProxyReturnValues.Defaults));

            Assert.AreEqual(1, poolInformation.AutoPoolSpecification.PoolSpecification.TargetLowPriorityComputeNodes);
        }

        [TestMethod]
        public async Task TaskStateTransitionsFromRunningState()
        {
            Assert.AreEqual(TesState.RUNNINGEnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchJobAndTaskStates.TaskActive));
            Assert.AreEqual(TesState.RUNNINGEnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchJobAndTaskStates.TaskPreparing));
            Assert.AreEqual(TesState.RUNNINGEnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchJobAndTaskStates.TaskRunning));
            Assert.AreEqual(TesState.COMPLETEEnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchJobAndTaskStates.TaskCompletedSuccessfully));
            Assert.AreEqual(TesState.EXECUTORERROREnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchJobAndTaskStates.TaskFailed));
            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchJobAndTaskStates.JobNotFound));
            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchJobAndTaskStates.TaskNotFound));
            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchJobAndTaskStates.MoreThanOneJobFound));
            Assert.AreEqual(TesState.EXECUTORERROREnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchJobAndTaskStates.NodeDiskFull));
            Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchJobAndTaskStates.ActiveJobWithMissingAutoPool));
        }

        [TestMethod]
        public async Task TaskStateTransitionsFromInitializingState()
        {
            Assert.AreEqual(TesState.INITIALIZINGEnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchJobAndTaskStates.TaskActive));
            Assert.AreEqual(TesState.INITIALIZINGEnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchJobAndTaskStates.TaskPreparing));
            Assert.AreEqual(TesState.RUNNINGEnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchJobAndTaskStates.TaskRunning));
            Assert.AreEqual(TesState.COMPLETEEnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchJobAndTaskStates.TaskCompletedSuccessfully));
            Assert.AreEqual(TesState.EXECUTORERROREnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchJobAndTaskStates.TaskFailed));
            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchJobAndTaskStates.JobNotFound));
            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchJobAndTaskStates.TaskNotFound));
            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchJobAndTaskStates.MoreThanOneJobFound));
            Assert.AreEqual(TesState.EXECUTORERROREnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchJobAndTaskStates.NodeDiskFull));
            Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchJobAndTaskStates.NodeAllocationFailed));
            Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchJobAndTaskStates.ActiveJobWithMissingAutoPool));
        }

        [TestMethod]
        public async Task TaskStateTransitionsFromQueuedState()
        {
            Assert.AreEqual(TesState.INITIALIZINGEnum, await GetNewTesTaskStateAsync(TesState.QUEUEDEnum, BatchJobAndTaskStates.TaskActive));
            Assert.AreEqual(TesState.INITIALIZINGEnum, await GetNewTesTaskStateAsync(TesState.QUEUEDEnum, BatchJobAndTaskStates.TaskPreparing));
            Assert.AreEqual(TesState.RUNNINGEnum, await GetNewTesTaskStateAsync(TesState.QUEUEDEnum, BatchJobAndTaskStates.TaskRunning));
            Assert.AreEqual(TesState.COMPLETEEnum, await GetNewTesTaskStateAsync(TesState.QUEUEDEnum, BatchJobAndTaskStates.TaskCompletedSuccessfully));
            Assert.AreEqual(TesState.EXECUTORERROREnum, await GetNewTesTaskStateAsync(TesState.QUEUEDEnum, BatchJobAndTaskStates.TaskFailed));
            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(TesState.QUEUEDEnum, BatchJobAndTaskStates.MoreThanOneJobFound));
            Assert.AreEqual(TesState.EXECUTORERROREnum, await GetNewTesTaskStateAsync(TesState.QUEUEDEnum, BatchJobAndTaskStates.NodeDiskFull));
            Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(TesState.QUEUEDEnum, BatchJobAndTaskStates.TaskNotFound));
        }

        [TestMethod]
        public async Task TesInputFilePathMustStartWithCromwellExecutions()
        {
            var tesTask = GetTesTask();

            tesTask.Inputs.Add(new TesInput
            {
                Path = "xyz/path"
            });

            var errorMessage = await ProcessTesTaskAndGetFirstLogMessageAsync(tesTask);

            Assert.AreEqual(TesState.SYSTEMERROREnum, tesTask.State);
            Assert.AreEqual($"Unsupported input path 'xyz/path' for task Id {tesTask.Id}. Must start with '/cromwell-executions/'.", errorMessage);
        }

        [TestMethod]
        public async Task TesInputFileMustHaveEitherUrlOrContent()
        {
            var tesTask = GetTesTask();

            tesTask.Inputs.Add(new TesInput
            {
                Url = null,
                Content = null
            });

            var errorMessage = await ProcessTesTaskAndGetFirstLogMessageAsync(tesTask);

            Assert.AreEqual(TesState.SYSTEMERROREnum, tesTask.State);
            Assert.AreEqual($"One of Input Url or Content must be set", errorMessage);
        }

        [TestMethod]
        public async Task TesInputFileMustNotHaveBothUrlAndContent()
        {
            var tesTask = GetTesTask();

            tesTask.Inputs.Add(new TesInput
            {
                Url = "/storageAccount1/container1/file1.txt",
                Content = "test content"
            });

            var errorMessage = await ProcessTesTaskAndGetFirstLogMessageAsync(tesTask);

            Assert.AreEqual(TesState.SYSTEMERROREnum, tesTask.State);
            Assert.AreEqual($"Input Url and Content cannot be both set", errorMessage);
        }

        [TestMethod]
        public async Task TesInputFileTypeMustNotBeDirectory()
        {
            var tesTask = GetTesTask();

            tesTask.Inputs.Add(new TesInput
            {
                Url = "/storageAccount1/container1/directory",
                Type = TesFileType.DIRECTORYEnum
            });

            var errorMessage = await ProcessTesTaskAndGetFirstLogMessageAsync(tesTask);

            Assert.AreEqual(TesState.SYSTEMERROREnum, tesTask.State);
            Assert.AreEqual($"Directory input is not supported.", errorMessage);
        }

        [TestMethod]
        public async Task QueryStringsAreRemovedFromLocalFilePathsWhenCommandScriptIsProvidedAsFile()
        {
            var tesTask = GetTesTask();

            var originalCommandScript = "cat /cromwell-executions/workflowpath/inputs/host/path?param=2";

            tesTask.Inputs = new List<TesInput>
            {
                new TesInput { Url = "/cromwell-executions/workflowpath/execution/script", Path = "/cromwell-executions/workflowpath/execution/script", Type = TesFileType.FILEEnum, Name = "commandScript", Content = null },
                new TesInput { Url = "http://host/path?param=1", Path = "/cromwell-executions/workflowpath/inputs/host/path?param=2", Type = TesFileType.FILEEnum, Name = "file1", Content = null }
            };

            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.DownloadedBlobContent = originalCommandScript;
            var azureproxy = GetMockAzureProxy(azureProxyReturnValues);

            await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig(), azureproxy);

            var modifiedCommandScript = (string)azureproxy.Invocations.FirstOrDefault(i => i.Method.Name == nameof(IAzureProxy.UploadBlobAsync) && i.Arguments[0].ToString().Contains("/script"))?.Arguments[1];
            var filesToDownload = GetFilesToDownload(azureproxy);

            Assert.AreEqual(TesState.INITIALIZINGEnum, tesTask.State);
            Assert.IsFalse(filesToDownload.Any(f => f.LocalPath.Contains("?") || f.LocalPath.Contains("param=1") || f.LocalPath.Contains("param=2")), "Query string was removed from local file path");
            Assert.AreEqual(1, filesToDownload.Count(f => f.StorageUrl.Contains("?param=1")), "Query string was not removed from blob URL");
            Assert.IsFalse(modifiedCommandScript.Contains("?param=2"), "Query string was removed from local file path in command script");
        }

        [TestMethod]
        public async Task QueryStringsAreRemovedFromLocalFilePathsWhenCommandScriptIsProvidedAsContent()
        {
            var tesTask = GetTesTask();

            var originalCommandScript = "cat /cromwell-executions/workflowpath/inputs/host/path?param=2";

            tesTask.Inputs = new List<TesInput>
            {
                new TesInput { Url = null, Path = "/cromwell-executions/workflowpath/execution/script", Type = TesFileType.FILEEnum, Name = "commandScript", Content = originalCommandScript },
                new TesInput { Url = "http://host/path?param=1", Path = "/cromwell-executions/workflowpath/inputs/host/path?param=2", Type = TesFileType.FILEEnum, Name = "file1", Content = null }
            };

            var azureproxy = GetMockAzureProxy(AzureProxyReturnValues.Defaults);

            await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig(), azureproxy);

            var modifiedCommandScript = (string)azureproxy.Invocations.FirstOrDefault(i => i.Method.Name == nameof(IAzureProxy.UploadBlobAsync) && i.Arguments[0].ToString().Contains("/script"))?.Arguments[1];
            var filesToDownload = GetFilesToDownload(azureproxy);

            Assert.AreEqual(TesState.INITIALIZINGEnum, tesTask.State);
            Assert.IsFalse(filesToDownload.Any(f => f.LocalPath.Contains("?") || f.LocalPath.Contains("param=1") || f.LocalPath.Contains("param=2")), "Query string was removed from local file path");
            Assert.AreEqual(1, filesToDownload.Count(f => f.StorageUrl.Contains("?param=1")), "Query string was not removed from blob URL");
            Assert.IsFalse(modifiedCommandScript.Contains("?param=2"), "Query string was removed from local file path in command script");
        }

        [TestMethod]
        public async Task LocalFilePathsAreResolvedToUrlsWhenStoredInExternalStorageAccounts()
        {
            var config = GetMockConfig();
            config["ExternalStorageContainers"] = "https://externalaccount1.blob.core.windows.net/container1?sas1, https://externalaccount2.blob.core.windows.net/container2/?sas2";

            var tesTask = GetTesTask();

            tesTask.Inputs = new List<TesInput>
            {
                new TesInput { Url = null, Path = "/cromwell-executions/workflowpath/execution/script", Type = TesFileType.FILEEnum, Name = "commandScript", Content = "echo hello" },
                new TesInput { Url = "/externalaccount1/container1/blob1", Path = "/cromwell-executions/workflowpath/inputs/account1/container1/blob1", Type = TesFileType.FILEEnum, Name = "file1", Content = null },
                new TesInput { Url = "/externalaccount2/container2/blob2", Path = "/cromwell-executions/workflowpath/inputs/account2/container2/blob2", Type = TesFileType.FILEEnum, Name = "file2", Content = null }
            };

            var azureproxy = GetMockAzureProxy(AzureProxyReturnValues.Defaults);

            await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, config, azureproxy);

            var filesToDownload = GetFilesToDownload(azureproxy).ToArray();

            Assert.AreEqual(3, filesToDownload.Count());
            Assert.AreEqual("https://externalaccount1.blob.core.windows.net/container1/blob1?sas1", filesToDownload[1].StorageUrl);
            Assert.AreEqual("https://externalaccount2.blob.core.windows.net/container2/blob2?sas2", filesToDownload[2].StorageUrl);
        }

        [TestMethod]
        public async Task LocalFilePathsAreResolvedToUrlsWhenStoredInStorageAccountsAccessibleToTesIdentity()
        {
            var tesTask = GetTesTask();
            var azureproxy = GetMockAzureProxy(AzureProxyReturnValues.Defaults);

            await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig(), azureproxy);

            var filesToDownload = GetFilesToDownload(azureproxy).ToArray();

            Assert.AreEqual(2, filesToDownload.Count());
            Assert.IsTrue(filesToDownload[0].StorageUrl.StartsWith("https://storageAccount1/container1/file1.txt?sv="));
            Assert.IsTrue(filesToDownload[1].StorageUrl.StartsWith("https://defaultstorageaccount/cromwell-executions/workflow1/workflowId1/call-Task1/execution/script?sv="));
        }

        private static async Task<string> ProcessTesTaskAndGetFirstLogMessageAsync(TesTask tesTask, AzureProxy.AzureBatchJobAndTaskState? azureBatchJobAndTaskState = null)
        {
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.BatchJobAndTaskState = azureBatchJobAndTaskState ?? azureProxyReturnValues.BatchJobAndTaskState;

            await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig(), GetMockAzureProxy(azureProxyReturnValues));

            return tesTask.Logs?.FirstOrDefault()?.SystemLogs?.FirstOrDefault();
        }

        private static Task<(string JobId, CloudTask CloudTask, PoolInformation PoolInformation)> ProcessTesTaskAndGetBatchJobArgumentsAsync()
        {
            return ProcessTesTaskAndGetBatchJobArgumentsAsync(GetTesTask(), GetMockConfig(), GetMockAzureProxy(AzureProxyReturnValues.Defaults));
        }

        private static async Task<(string JobId, CloudTask CloudTask, PoolInformation PoolInformation)> ProcessTesTaskAndGetBatchJobArgumentsAsync(TesTask tesTask, IConfiguration configuration, Mock<IAzureProxy> azureProxy)
        {
            var batchScheduler = new BatchScheduler(new Mock<ILogger>().Object, configuration, azureProxy.Object);

            await batchScheduler.ProcessTesTaskAsync(tesTask);

            var createBatchJobAsyncInvocation = azureProxy.Invocations.FirstOrDefault(i => i.Method.Name == nameof(IAzureProxy.CreateBatchJobAsync));

            var jobId = createBatchJobAsyncInvocation?.Arguments[0] as string;
            var cloudTask = createBatchJobAsyncInvocation?.Arguments[1] as CloudTask;
            var poolInformation = createBatchJobAsyncInvocation?.Arguments[2] as PoolInformation;

            return (jobId, cloudTask, poolInformation);
        }

        private static async Task<TesState> GetNewTesTaskStateAsync(TesState currentTesTaskState, AzureProxy.AzureBatchJobAndTaskState azureBatchJobAndTaskState)
        {
            var tesTask = new TesTask { Id = "test", State = currentTesTaskState };

            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.BatchJobAndTaskState = azureBatchJobAndTaskState;

            await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig(), GetMockAzureProxy(azureProxyReturnValues));

            return tesTask.State;
        }

        private static async Task<TesState> GetNewTesTaskStateAsync(TesResources resources, AzureProxyReturnValues azureProxyReturnValues)
        {
            var tesTask = GetTesTask();
            tesTask.Resources = resources;

            await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig(), GetMockAzureProxy(azureProxyReturnValues));

            return tesTask.State;
        }

        private static TesTask GetTesTask()
        {
            return JsonConvert.DeserializeObject<TesTask>(File.ReadAllText("testask1.json"));
        }

        private static Mock<IAzureProxy> GetMockAzureProxy(AzureProxyReturnValues azureProxyReturnValues)
        {
            var azureProxy = new Mock<IAzureProxy>();

            azureProxy.Setup(a => a.GetBatchJobAndTaskStateAsync(It.IsAny<string>())).Returns(Task.FromResult(azureProxyReturnValues.BatchJobAndTaskState));
            azureProxy.Setup(a => a.GetNextBatchJobIdAsync(It.IsAny<string>())).Returns(Task.FromResult(azureProxyReturnValues.NextBatchJobId));
            azureProxy.Setup(a => a.GetAccessibleStorageAccountsAsync()).Returns(Task.FromResult(azureProxyReturnValues.AccessibleStorageAccounts));
            azureProxy.Setup(a => a.GetAccessibleContainerRegistriesAsync()).Returns(Task.FromResult(azureProxyReturnValues.AccessibleContainerRegistries));
            azureProxy.Setup(a => a.GetStorageAccountKeyAsync(It.IsAny<StorageAccountInfo>())).Returns(Task.FromResult(azureProxyReturnValues.StorageAccountKey));
            azureProxy.Setup(a => a.GetVmSizesAndPricesAsync()).Returns(Task.FromResult(azureProxyReturnValues.VmSizesAndPrices));
            azureProxy.Setup(a => a.GetBatchAccountQuotasAsync()).Returns(Task.FromResult(azureProxyReturnValues.BatchQuotas));
            azureProxy.Setup(a => a.GetBatchActiveNodeCountByVmSize()).Returns(azureProxyReturnValues.ActiveNodeCountByVmSize);
            azureProxy.Setup(a => a.GetBatchActiveJobCount()).Returns(azureProxyReturnValues.ActiveJobCount);
            azureProxy.Setup(a => a.GetBatchActivePoolCount()).Returns(azureProxyReturnValues.ActivePoolCount);
            azureProxy.Setup(a => a.DownloadBlobAsync(It.IsAny<Uri>())).Returns(Task.FromResult(azureProxyReturnValues.DownloadedBlobContent));

            return azureProxy;
        }

        private static IConfiguration GetMockConfig()
        {
            var config = new ConfigurationBuilder().AddInMemoryCollection().Build();
            config["DefaultStorageAccountName"] = "defaultstorageaccount";

            return config;
        }

        private static IEnumerable<FileToDownload> GetFilesToDownload(Mock<IAzureProxy> azureProxy)
        {
            var downloadFilesScriptContent = (string)azureProxy.Invocations.FirstOrDefault(i => i.Method.Name == nameof(IAzureProxy.UploadBlobAsync) && i.Arguments[0].ToString().Contains("/download_files_script"))?.Arguments[1];

            if (string.IsNullOrEmpty(downloadFilesScriptContent))
            {
                return new List<FileToDownload>();
            }

            return downloadFilesScriptRegex.Matches(downloadFilesScriptContent)
                .Cast<System.Text.RegularExpressions.Match>()
                .Select(m => new FileToDownload { StorageUrl = m.Groups[1].Value, LocalPath = m.Groups[2].Value });
        }

        private struct BatchJobAndTaskStates
        {
            public static AzureProxy.AzureBatchJobAndTaskState TaskActive => new AzureProxy.AzureBatchJobAndTaskState { JobState = JobState.Active, TaskState = TaskState.Active };
            public static AzureProxy.AzureBatchJobAndTaskState TaskPreparing => new AzureProxy.AzureBatchJobAndTaskState { JobState = JobState.Active, TaskState = TaskState.Preparing };
            public static AzureProxy.AzureBatchJobAndTaskState TaskRunning => new AzureProxy.AzureBatchJobAndTaskState { JobState = JobState.Active, TaskState = TaskState.Running };
            public static AzureProxy.AzureBatchJobAndTaskState TaskCompletedSuccessfully => new AzureProxy.AzureBatchJobAndTaskState { JobState = JobState.Completed, TaskState = TaskState.Completed, TaskExitCode = 0 };
            public static AzureProxy.AzureBatchJobAndTaskState TaskFailed => new AzureProxy.AzureBatchJobAndTaskState { JobState = JobState.Completed, TaskState = TaskState.Completed, TaskExitCode = -1 };
            public static AzureProxy.AzureBatchJobAndTaskState JobNotFound => new AzureProxy.AzureBatchJobAndTaskState { JobState = null };
            public static AzureProxy.AzureBatchJobAndTaskState TaskNotFound => new AzureProxy.AzureBatchJobAndTaskState { JobState = JobState.Active, TaskState = null };
            public static AzureProxy.AzureBatchJobAndTaskState MoreThanOneJobFound => new AzureProxy.AzureBatchJobAndTaskState { MoreThanOneActiveJobFound = true };
            public static AzureProxy.AzureBatchJobAndTaskState NodeAllocationFailed => new AzureProxy.AzureBatchJobAndTaskState { JobState = JobState.Active, NodeAllocationFailed = true };
            public static AzureProxy.AzureBatchJobAndTaskState NodeDiskFull => new AzureProxy.AzureBatchJobAndTaskState { JobState = JobState.Active, NodeDiskFull = true };
            public static AzureProxy.AzureBatchJobAndTaskState ActiveJobWithMissingAutoPool => new AzureProxy.AzureBatchJobAndTaskState { ActiveJobWithMissingAutoPool = true };
        }

        private class AzureProxyReturnValues
        {
            public IEnumerable<StorageAccountInfo> AccessibleStorageAccounts { get; set; }
            public IEnumerable<ContainerRegistryInfo> AccessibleContainerRegistries { get; set; }
            public List<VirtualMachineInfo> VmSizesAndPrices { get; set; }
            public AzureProxy.AzureBatchAccountQuotas BatchQuotas { get; set; }
            public IEnumerable<AzureProxy.AzureBatchNodeCount> ActiveNodeCountByVmSize { get; set; }
            public int ActiveJobCount { get; set; }
            public int ActivePoolCount { get; set; }
            public AzureProxy.AzureBatchJobAndTaskState BatchJobAndTaskState { get; set; }
            public string NextBatchJobId { get; set; }
            public string StorageAccountKey { get; set; }
            public string DownloadedBlobContent { get; set; }

            public static AzureProxyReturnValues Defaults => new AzureProxyReturnValues
            {
                AccessibleStorageAccounts = new List<StorageAccountInfo> {
                    new StorageAccountInfo { Name = "defaultstorageaccount", Id = "Id", BlobEndpoint = "https://defaultstorageaccount/", SubscriptionId = "SubId" },
                    new StorageAccountInfo { Name = "storageAccount1", Id = "Id", BlobEndpoint = "https://storageAccount1/", SubscriptionId = "SubId" }
                },
                AccessibleContainerRegistries = new List<ContainerRegistryInfo> {
                    new ContainerRegistryInfo { RegistryServer = "registryServer1", Username = "default", Password = "placeholder"}
                },
                VmSizesAndPrices = new List<VirtualMachineInfo> {
                    new VirtualMachineInfo { VmSize = "VmSizeLowPri1", LowPriority = true, NumberOfCores = 1, MemoryInGB = 4, ResourceDiskSizeInGB = 20, PricePerHour = 1 },
                    new VirtualMachineInfo { VmSize = "VmSizeLowPri2", LowPriority = true, NumberOfCores = 2, MemoryInGB = 8, ResourceDiskSizeInGB = 40, PricePerHour = 2 },
                    new VirtualMachineInfo { VmSize = "VmSizeDedicated1", LowPriority = false, NumberOfCores = 1, MemoryInGB = 4, ResourceDiskSizeInGB = 20, PricePerHour = 11 },
                    new VirtualMachineInfo { VmSize = "VmSizeDedicated2", LowPriority = false, NumberOfCores = 2, MemoryInGB = 8, ResourceDiskSizeInGB = 40, PricePerHour = 22 }
                },
                BatchQuotas = new AzureProxy.AzureBatchAccountQuotas { ActiveJobAndJobScheduleQuota = 1, PoolQuota = 1, DedicatedCoreQuota = 5, LowPriorityCoreQuota = 10 },
                ActiveNodeCountByVmSize = new List<AzureProxy.AzureBatchNodeCount>(),
                ActiveJobCount = 0,
                ActivePoolCount = 0,
                BatchJobAndTaskState = BatchJobAndTaskStates.JobNotFound,
                NextBatchJobId = "JobId-1",
                StorageAccountKey = "Key1",
                DownloadedBlobContent = ""
            };
        }

        private class FileToDownload
        {
            public string StorageUrl { get; set; }
            public string LocalPath { get; set; }
        }
    }
}
