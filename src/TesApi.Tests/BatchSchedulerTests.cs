// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using LazyCache;
using LazyCache.Providers;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Azure.Management.Batch.Models;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Newtonsoft.Json;
using Tes.Extensions;
using Tes.Models;
using TesApi.Web;

namespace TesApi.Tests
{
    [TestClass]
    public class BatchSchedulerTests
    {
        private static readonly Regex downloadFilesBlobxferRegex = new(@"path='([^']*)' && url='([^']*)' && blobxfer download");
        private static readonly Regex downloadFilesWgetRegex = new(@"path='([^']*)' && url='([^']*)' && mkdir .* wget");

        [TestCategory("TES 1.1")]
        [TestMethod]
        public async Task BackendParametersVmSizeShallOverrideVmSelection()
        {
            // "vmsize" is not case sensitive
            // If vmsize is specified, (numberofcores, memoryingb, resourcedisksizeingb) are ignored

            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;

            azureProxyReturnValues.VmSizesAndPrices = new List<Tes.Models.VirtualMachineInformation> {
                new VirtualMachineInformation { VmSize = "VmSize1", LowPriority = true, NumberOfCores = 1, MemoryInGB = 4, ResourceDiskSizeInGB = 20, PricePerHour = 1 },
                new VirtualMachineInformation { VmSize = "VmSize2", LowPriority = true, NumberOfCores = 2, MemoryInGB = 8, ResourceDiskSizeInGB = 40, PricePerHour = 2 }};

            var state = await GetNewTesTaskStateAsync(new TesResources { Preemptible = true, BackendParameters = new() { { "vm_size", "VmSize1" } } }, azureProxyReturnValues);
            Assert.AreEqual(TesState.INITIALIZINGEnum, state);

            state = await GetNewTesTaskStateAsync(new TesResources { Preemptible = true, BackendParameters = new() { { "vm_size", "VMSIZE1" } } }, azureProxyReturnValues);
            Assert.AreEqual(TesState.INITIALIZINGEnum, state);

            state = await GetNewTesTaskStateAsync(new TesResources { Preemptible = true, BackendParameters = new() { { "vm_size", "VmSize1" } } , CpuCores = 1000, RamGb = 100000, DiskGb = 1000000 }, azureProxyReturnValues);
            Assert.AreEqual(TesState.INITIALIZINGEnum, state);

            state = await GetNewTesTaskStateAsync(new TesResources { Preemptible = true, BackendParameters = new(), CpuCores = 1000, RamGb = 100000, DiskGb = 1000000 }, azureProxyReturnValues);
            Assert.AreEqual(TesState.SYSTEMERROREnum, state);

            state = await GetNewTesTaskStateAsync(new TesResources { Preemptible = false, BackendParameters = new() { { "vm_size", "VmSize1" } } }, azureProxyReturnValues);
            Assert.AreEqual(TesState.SYSTEMERROREnum, state);

            state = await GetNewTesTaskStateAsync(new TesResources { Preemptible = true, BackendParameters = new() { { "vm_size", "VmSize3" } } }, azureProxyReturnValues);
            Assert.AreEqual(TesState.SYSTEMERROREnum, state);
        }

        [TestCategory("TES 1.1")]
        [TestMethod]
        public async Task BackendParametersWorkflowExecutionIdentityRequiresManualPool()
        {
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.BatchJobAndTaskState = new AzureBatchJobAndTaskState { JobState = null };

            var backendParameters = new Dictionary<string, string>
            {
                { "workflow_execution_identity", "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/coa/providers/Microsoft.ManagedIdentity/userAssignedIdentities/coa-test-uami" }
            };

            var task = GetTesTask();
            task.Resources.BackendParameters = backendParameters;

            (_, _, var poolInformation) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(task, GetMockConfig(), GetMockAzureProxy(AzureProxyReturnValues.Defaults));

            Assert.IsNull(poolInformation.AutoPoolSpecification);
            Assert.IsFalse(string.IsNullOrWhiteSpace(poolInformation.PoolId));
        }


        [TestCategory("TES 1.1")]
        [DataRow("VmSizeLowPri1", true)]
        [DataRow("VmSizeLowPri2", true)]
        [DataRow("VmSizeDedicated1", false)]
        [DataRow("VmSizeDedicated2", false)]
        [TestMethod]
        public async Task TestIfVmSizeIsAvailable(string vmSize, bool preemptible)
        {
            var backendParameters = new Dictionary<string, string> { { "vm_size", vmSize } };
            var proxy = GetMockAzureProxy(AzureProxyReturnValues.Defaults).Object;
            var task = GetTesTask();
            task.Resources.Preemptible = preemptible;
            task.Resources.BackendParameters = backendParameters;

            var batchScheduler = new BatchScheduler(
                new Mock<ILogger>().Object,
                GetMockConfig(),
                new CachingWithRetriesAzureProxy(proxy, new CachingService(new MemoryCacheProvider(new MemoryCache(new MemoryCacheOptions())))),
                new StorageAccessProvider(new Mock<ILogger>().Object, GetMockConfig(), proxy));

            var size = await batchScheduler.GetVmSizeAsync(task);
            Assert.AreEqual(vmSize, size.VmSize);
        }

        [TestMethod]
        public async Task TesTaskFailsWithSystemErrorWhenNoSuitableVmExists()
        {
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;

            azureProxyReturnValues.VmSizesAndPrices = new List<VirtualMachineInformation> {
                new VirtualMachineInformation { VmSize = "VmSize1", LowPriority = true, NumberOfCores = 1, MemoryInGB = 4, ResourceDiskSizeInGB = 20, PricePerHour = 1 },
                new VirtualMachineInformation { VmSize = "VmSize2", LowPriority = true, NumberOfCores = 2, MemoryInGB = 8, ResourceDiskSizeInGB = 40, PricePerHour = 2 }};

            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 1, RamGb = 1, DiskGb = 10, Preemptible = false }, azureProxyReturnValues));
            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 4, RamGb = 1, DiskGb = 10, Preemptible = true }, azureProxyReturnValues));
            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 1, RamGb = 10, DiskGb = 10, Preemptible = true }, azureProxyReturnValues));
            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 1, RamGb = 1, DiskGb = 50, Preemptible = true }, azureProxyReturnValues));
        }

        [TestMethod]
        public async Task TesTaskFailsWithSystemErrorWhenTotalBatchQuotaIsSetTooLow()
        {
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.BatchQuotas = new AzureBatchAccountQuotas { ActiveJobAndJobScheduleQuota = 1, PoolQuota = 1, DedicatedCoreQuota = 1, LowPriorityCoreQuota = 10 };

            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 2, RamGb = 1, Preemptible = false }, azureProxyReturnValues));
            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 11, RamGb = 1, Preemptible = true }, azureProxyReturnValues));

            var dedicatedCoreQuotaPerVMFamily = new List<VirtualMachineFamilyCoreQuota> { new VirtualMachineFamilyCoreQuota("VmFamily2", 1) };
            azureProxyReturnValues.BatchQuotas = new AzureBatchAccountQuotas { ActiveJobAndJobScheduleQuota = 1, PoolQuota = 1, DedicatedCoreQuota = 100, LowPriorityCoreQuota = 100, DedicatedCoreQuotaPerVMFamilyEnforced = true, DedicatedCoreQuotaPerVMFamily = dedicatedCoreQuotaPerVMFamily };

           Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 2, RamGb = 1, Preemptible = false }, azureProxyReturnValues));
        }

        [TestMethod]
        public async Task TesTaskFailsWhenBatchNodeDiskIsFull()
        {
            var tesTask = GetTesTask();

            (var failureReason, var systemLog) = await ProcessTesTaskAndGetFailureReasonAndSystemLogAsync(tesTask, BatchJobAndTaskStates.NodeDiskFull);

            Assert.AreEqual(TesState.EXECUTORERROREnum, tesTask.State);
            Assert.AreEqual("DiskFull", failureReason);
            Assert.AreEqual("DiskFull", systemLog[0]);
            Assert.AreEqual("DiskFull", tesTask.FailureReason);
        }

        [TestMethod]
        public async Task TesTaskRemainsQueuedWhenBatchQuotaIsTemporarilyUnavailable()
        {
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;

            azureProxyReturnValues.VmSizesAndPrices = new List<VirtualMachineInformation> {
                new VirtualMachineInformation { VmSize = "VmSize1", VmFamily = "VmFamily1", LowPriority = false, NumberOfCores = 2, MemoryInGB = 4, ResourceDiskSizeInGB = 20, PricePerHour = 1 },
                new VirtualMachineInformation { VmSize = "VmSize1", VmFamily = "VmFamily1", LowPriority = true, NumberOfCores = 2, MemoryInGB = 4, ResourceDiskSizeInGB = 20, PricePerHour = 2 }};

            azureProxyReturnValues.BatchQuotas = new AzureBatchAccountQuotas { ActiveJobAndJobScheduleQuota = 1, PoolQuota = 1, DedicatedCoreQuota = 9, LowPriorityCoreQuota = 17 };

            azureProxyReturnValues.ActiveNodeCountByVmSize = new List<AzureBatchNodeCount> {
                new AzureBatchNodeCount { VirtualMachineSize = "VmSize1", DedicatedNodeCount = 4, LowPriorityNodeCount = 8 }  // 8 (4 * 2) dedicated and 16 (8 * 2) low pri cores are in use, there is no more room for 2 cores
            };

            // The actual CPU core count (2) of the selected VM is used for quota calculation, not the TesResources CpuCores requirement
            Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 1, RamGb = 1, Preemptible = false }, azureProxyReturnValues));
            Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 1, RamGb = 1, Preemptible = true }, azureProxyReturnValues));

            azureProxyReturnValues.ActiveNodeCountByVmSize = new List<AzureBatchNodeCount> {
                new AzureBatchNodeCount { VirtualMachineSize = "VmSize1", DedicatedNodeCount = 4, LowPriorityNodeCount = 7 }  // 8 dedicated and 14 low pri cores are in use
            };

            Assert.AreEqual(TesState.INITIALIZINGEnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 1, RamGb = 1, Preemptible = true }, azureProxyReturnValues));

            var dedicatedCoreQuotaPerVMFamily = new List<VirtualMachineFamilyCoreQuota> { new VirtualMachineFamilyCoreQuota("VmFamily1", 9) };
            azureProxyReturnValues.BatchQuotas = new AzureBatchAccountQuotas { ActiveJobAndJobScheduleQuota = 1, PoolQuota = 1, DedicatedCoreQuota = 100, LowPriorityCoreQuota = 17, DedicatedCoreQuotaPerVMFamilyEnforced = true, DedicatedCoreQuotaPerVMFamily = dedicatedCoreQuotaPerVMFamily };

            azureProxyReturnValues.ActiveNodeCountByVmSize = new List<AzureBatchNodeCount> {
                new AzureBatchNodeCount { VirtualMachineSize = "VmSize1", DedicatedNodeCount = 4, LowPriorityNodeCount = 8 }  // 8 (4 * 2) dedicated and 16 (8 * 2) low pri cores are in use, there is no more room for 2 cores
            };

            Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 1, RamGb = 1, Preemptible = false }, azureProxyReturnValues));
        }

        [TestMethod]
        public async Task BatchTaskResourcesIncludeDownloadAndUploadScripts()
        {
            (_, var cloudTask, _) = await ProcessTesTaskAndGetBatchJobArgumentsAsync();

            Assert.AreEqual(3, cloudTask.ResourceFiles.Count);
            Assert.IsTrue(cloudTask.ResourceFiles.Any(f => f.FilePath.Equals("/mnt/cromwell-executions/workflow1/workflowId1/call-Task1/execution/__batch/batch_script")));
            Assert.IsTrue(cloudTask.ResourceFiles.Any(f => f.FilePath.Equals("/mnt/cromwell-executions/workflow1/workflowId1/call-Task1/execution/__batch/upload_files_script")));
            Assert.IsTrue(cloudTask.ResourceFiles.Any(f => f.FilePath.Equals("/mnt/cromwell-executions/workflow1/workflowId1/call-Task1/execution/__batch/download_files_script")));
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
        public async Task PreemptibleTesTaskGetsScheduledToLowPriorityVm_PerVMFamilyEnforced()
        {
            var tesTask = GetTesTask();
            tesTask.Resources.Preemptible = true;

            (_, _, var poolInformation) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig(), GetMockAzureProxy(AzureProxyReturnValues.DefaultsPerVMFamilyEnforced));

            Assert.AreEqual("VmSizeLowPri1", poolInformation.AutoPoolSpecification.PoolSpecification.VirtualMachineSize);
            Assert.AreEqual(1, poolInformation.AutoPoolSpecification.PoolSpecification.TargetLowPriorityComputeNodes);
            Assert.AreEqual(0, poolInformation.AutoPoolSpecification.PoolSpecification.TargetDedicatedComputeNodes);
        }

        [TestMethod]
        public async Task NonPreemptibleTesTaskGetsScheduledToDedicatedVm_PerVMFamilyEnforced()
        {
            var tesTask = GetTesTask();
            tesTask.Resources.Preemptible = false;

            (_, _, var poolInformation) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig(), GetMockAzureProxy(AzureProxyReturnValues.DefaultsPerVMFamilyEnforced));

            Assert.AreEqual("VmSizeDedicated1", poolInformation.AutoPoolSpecification.PoolSpecification.VirtualMachineSize);
            Assert.AreEqual(1, poolInformation.AutoPoolSpecification.PoolSpecification.TargetDedicatedComputeNodes);
            Assert.AreEqual(0, poolInformation.AutoPoolSpecification.PoolSpecification.TargetLowPriorityComputeNodes);
        }

        [TestMethod]
        public async Task NonPreemptibleTesTaskGetsWarningAndIsScheduledToLowPriorityVmIfPriceIsDoubleIdeal()
        {
            var tesTask = GetTesTask();
            tesTask.Resources.Preemptible = false;
            tesTask.Resources.CpuCores = 2;

            var azureProxyReturnValues = AzureProxyReturnValues.DefaultsPerVMFamilyEnforced;
            azureProxyReturnValues.VmSizesAndPrices.First(vm => vm.VmSize.Equals("VmSize3", StringComparison.OrdinalIgnoreCase)).PricePerHour = 44;
            var azureProxy = GetMockAzureProxy(azureProxyReturnValues);

            (_, _, var poolInformation) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig(), azureProxy);

            Assert.IsTrue(tesTask.Logs.Any(l => "UsedLowPriorityInsteadOfDedicatedVm".Equals(l.Warning)));
            Assert.AreEqual(1, poolInformation.AutoPoolSpecification.PoolSpecification.TargetLowPriorityComputeNodes);
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
        public async Task TesTaskGetsScheduledToAllowedVmSizeOnly()
        {
            static async Task RunTest(string allowedVmSizes, TesState expectedTaskState, string expectedSelectedVmSize = null)
            {
                var tesTask = GetTesTask();
                tesTask.Resources.Preemptible = true;

                var config = GetMockConfig();
                config["AllowedVmSizes"] = allowedVmSizes;

                (_, _, var poolInformation) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, config, GetMockAzureProxy(AzureProxyReturnValues.Defaults));
                Assert.AreEqual(expectedTaskState, tesTask.State);

                if (expectedSelectedVmSize is not null)
                {
                    Assert.AreEqual(expectedSelectedVmSize, poolInformation.AutoPoolSpecification.PoolSpecification.VirtualMachineSize);
                }
            }

            await RunTest(null, TesState.INITIALIZINGEnum, "VmSizeLowPri1");
            await RunTest(string.Empty, TesState.INITIALIZINGEnum, "VmSizeLowPri1");
            await RunTest("VmSizeLowPri1", TesState.INITIALIZINGEnum, "VmSizeLowPri1");
            await RunTest("VmSizeLowPri1,VmSizeLowPri2", TesState.INITIALIZINGEnum, "VmSizeLowPri1");
            await RunTest("VmSizeLowPri2", TesState.INITIALIZINGEnum, "VmSizeLowPri2");
            await RunTest("VmSizeLowPriNonExistent", TesState.SYSTEMERROREnum);
            await RunTest("VmSizeLowPriNonExistent,VmSizeLowPri1", TesState.INITIALIZINGEnum, "VmSizeLowPri1");
            await RunTest("VmFamily2", TesState.INITIALIZINGEnum, "VmSizeLowPri2");
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
            Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchJobAndTaskStates.NodePreempted));
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
            Assert.AreEqual(TesState.EXECUTORERROREnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchJobAndTaskStates.ImageDownloadFailed));
            Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchJobAndTaskStates.ActiveJobWithMissingAutoPool));
            Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchJobAndTaskStates.NodePreempted));
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
        public async Task TaskIsRequeuedUpToThreeTimesForTransientErrors()
        {
            var tesTask = GetTesTask();

            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;

            azureProxyReturnValues.VmSizesAndPrices = new List<VirtualMachineInformation> {
                new VirtualMachineInformation { VmSize = "VmSize1", LowPriority = false, NumberOfCores = 2, MemoryInGB = 4, ResourceDiskSizeInGB = 20, PricePerHour = 1 },
                new VirtualMachineInformation { VmSize = "VmSize2", LowPriority = false, NumberOfCores = 2, MemoryInGB = 4, ResourceDiskSizeInGB = 20, PricePerHour = 2 },
                new VirtualMachineInformation { VmSize = "VmSize3", LowPriority = false, NumberOfCores = 2, MemoryInGB = 4, ResourceDiskSizeInGB = 20, PricePerHour = 3 },
                new VirtualMachineInformation { VmSize = "VmSize4", LowPriority = false, NumberOfCores = 2, MemoryInGB = 4, ResourceDiskSizeInGB = 20, PricePerHour = 4 },
                new VirtualMachineInformation { VmSize = "VmSize5", LowPriority = false, NumberOfCores = 2, MemoryInGB = 4, ResourceDiskSizeInGB = 20, PricePerHour = 5 }
            };

            await GetNewTesTaskStateAsync(tesTask, azureProxyReturnValues);
            Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(tesTask, BatchJobAndTaskStates.NodeAllocationFailed));
            await GetNewTesTaskStateAsync(tesTask, azureProxyReturnValues);
            Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(tesTask, BatchJobAndTaskStates.NodeAllocationFailed));
            await GetNewTesTaskStateAsync(tesTask, azureProxyReturnValues);
            Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(tesTask, BatchJobAndTaskStates.NodeAllocationFailed));
            await GetNewTesTaskStateAsync(tesTask, azureProxyReturnValues);
            Assert.AreEqual(TesState.EXECUTORERROREnum, await GetNewTesTaskStateAsync(tesTask, BatchJobAndTaskStates.NodeAllocationFailed));
        }

        [TestMethod]
        public async Task TaskThatFailsWithNodeAllocationErrorIsRequeuedOnDifferentVmSize()
        {
            var tesTask = GetTesTask();

            await GetNewTesTaskStateAsync(tesTask);
            await GetNewTesTaskStateAsync(tesTask, BatchJobAndTaskStates.NodeAllocationFailed);
            var firstAttemptVmSize = tesTask.Logs[0].VirtualMachineInfo.VmSize;

            await GetNewTesTaskStateAsync(tesTask);
            await GetNewTesTaskStateAsync(tesTask, BatchJobAndTaskStates.NodeAllocationFailed);
            var secondAttemptVmSize = tesTask.Logs[1].VirtualMachineInfo.VmSize;

            Assert.AreNotEqual(firstAttemptVmSize, secondAttemptVmSize);

            // There are only two suitable VMs, and both have been excluded because of the NodeAllocationFailed error on the two earlier attempts
            await GetNewTesTaskStateAsync(tesTask);

            Assert.AreEqual(TesState.SYSTEMERROREnum, tesTask.State);
            Assert.AreEqual("NoVmSizeAvailable", tesTask.FailureReason);
        }

        [TestMethod]
        public async Task TaskGetsCancelled()
        {
            var tesTask = new TesTask { Id = "test", State = TesState.CANCELEDEnum, IsCancelRequested = true };

            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.BatchJobAndTaskState = BatchJobAndTaskStates.TaskActive;
            var azureProxy = GetMockAzureProxy(azureProxyReturnValues);

            await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig(), azureProxy);

            Assert.AreEqual(TesState.CANCELEDEnum, tesTask.State);
            Assert.IsFalse(tesTask.IsCancelRequested);
            azureProxy.Verify(i => i.DeleteBatchJobAsync(tesTask.Id, It.IsAny<System.Threading.CancellationToken>()));
        }

        [TestMethod]
        public async Task SuccessfullyCompletedTaskContainsBatchNodeMetrics()
        {
            var tesTask = GetTesTask();

            var metricsFileContent = @"
                BlobXferPullStart=2020-10-08T02:30:39+00:00
                BlobXferPullEnd=2020-10-08T02:31:39+00:00
                ExecutorPullStart=2020-10-08T02:32:39+00:00
                ExecutorPullEnd=2020-10-08T02:34:39+00:00
                ExecutorImageSizeInBytes=3000000000
                DownloadStart=2020-10-08T02:35:39+00:00
                DownloadEnd=2020-10-08T02:38:39+00:00
                ExecutorStart=2020-10-08T02:39:39+00:00
                ExecutorEnd=2020-10-08T02:43:39+00:00
                UploadStart=2020-10-08T02:44:39+00:00
                UploadEnd=2020-10-08T02:49:39+00:00
                DiskSizeInKiB=8000000
                DiskUsedInKiB=1000000
                FileDownloadSizeInBytes=2000000000
                FileUploadSizeInBytes=4000000000".Replace(" ", string.Empty);

            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.BatchJobAndTaskState = BatchJobAndTaskStates.TaskCompletedSuccessfully;
            azureProxyReturnValues.DownloadedBlobContent = metricsFileContent;
            var azureProxy = GetMockAzureProxy(azureProxyReturnValues);

            await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig(), azureProxy);

            Assert.AreEqual(TesState.COMPLETEEnum, tesTask.State);

            var batchNodeMetrics = tesTask.GetOrAddTesTaskLog().BatchNodeMetrics;
            Assert.IsNotNull(batchNodeMetrics);
            Assert.AreEqual(60, batchNodeMetrics.BlobXferImagePullDurationInSeconds);
            Assert.AreEqual(120, batchNodeMetrics.ExecutorImagePullDurationInSeconds);
            Assert.AreEqual(3, batchNodeMetrics.ExecutorImageSizeInGB);
            Assert.AreEqual(180, batchNodeMetrics.FileDownloadDurationInSeconds);
            Assert.AreEqual(240, batchNodeMetrics.ExecutorDurationInSeconds);
            Assert.AreEqual(300, batchNodeMetrics.FileUploadDurationInSeconds);
            Assert.AreEqual(1.024, batchNodeMetrics.DiskUsedInGB);
            Assert.AreEqual(12.5f, batchNodeMetrics.DiskUsedPercent);
            Assert.AreEqual(2, batchNodeMetrics.FileDownloadSizeInGB);
            Assert.AreEqual(4, batchNodeMetrics.FileUploadSizeInGB);

            var executorLog = tesTask.GetOrAddTesTaskLog().GetOrAddExecutorLog();
            Assert.IsNotNull(executorLog);
            Assert.AreEqual(0, executorLog.ExitCode);
            Assert.AreEqual(DateTimeOffset.Parse("2020-10-08T02:30:39+00:00"), executorLog.StartTime);
            Assert.AreEqual(DateTimeOffset.Parse("2020-10-08T02:49:39+00:00"), executorLog.EndTime);
        }

        [TestMethod]
        public async Task SuccessfullyCompletedTaskContainsCromwellResultCode()
        {
            var tesTask = GetTesTask();

            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.BatchJobAndTaskState = BatchJobAndTaskStates.TaskCompletedSuccessfully;
            azureProxyReturnValues.DownloadedBlobContent = "2";
            var azureProxy = GetMockAzureProxy(azureProxyReturnValues);

            await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig(), azureProxy);

            Assert.AreEqual(TesState.COMPLETEEnum, tesTask.State);
            Assert.AreEqual(2, tesTask.GetOrAddTesTaskLog().CromwellResultCode);
            Assert.AreEqual(2, tesTask.CromwellResultCode);
        }

        [TestMethod]
        public async Task TesInputFilePathMustStartWithCromwellExecutions()
        {
            var tesTask = GetTesTask();

            tesTask.Inputs.Add(new TesInput
            {
                Path = "xyz/path"
            });

            (var failureReason, var systemLog) = await ProcessTesTaskAndGetFailureReasonAndSystemLogAsync(tesTask);

            Assert.AreEqual(TesState.SYSTEMERROREnum, tesTask.State);
            Assert.AreEqual($"InvalidInputFilePath", failureReason);
            Assert.AreEqual($"InvalidInputFilePath", systemLog[0]);
            Assert.AreEqual($"Unsupported input path 'xyz/path' for task Id {tesTask.Id}. Must start with '/cromwell-executions/'.", systemLog[1]);
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

            (var failureReason, var systemLog) = await ProcessTesTaskAndGetFailureReasonAndSystemLogAsync(tesTask);

            Assert.AreEqual(TesState.SYSTEMERROREnum, tesTask.State);
            Assert.AreEqual($"InvalidInputFilePath", failureReason);
            Assert.AreEqual($"InvalidInputFilePath", systemLog[0]);
            Assert.AreEqual($"One of Input Url or Content must be set", systemLog[1]);
        }

        [TestMethod]
        public async Task TesInputFileMustNotHaveBothUrlAndContent()
        {
            var tesTask = GetTesTask();

            tesTask.Inputs.Add(new TesInput
            {
                Url = "/storageaccount1/container1/file1.txt",
                Content = "test content"
            });

            (var failureReason, var systemLog) = await ProcessTesTaskAndGetFailureReasonAndSystemLogAsync(tesTask);

            Assert.AreEqual(TesState.SYSTEMERROREnum, tesTask.State);
            Assert.AreEqual($"InvalidInputFilePath", failureReason);
            Assert.AreEqual($"InvalidInputFilePath", systemLog[0]);
            Assert.AreEqual($"Input Url and Content cannot be both set", systemLog[1]);
        }

        [TestMethod]
        public async Task TesInputFileTypeMustNotBeDirectory()
        {
            var tesTask = GetTesTask();

            tesTask.Inputs.Add(new TesInput
            {
                Url = "/storageaccount1/container1/directory",
                Type = TesFileType.DIRECTORYEnum
            });

            (var failureReason, var systemLog) = await ProcessTesTaskAndGetFailureReasonAndSystemLogAsync(tesTask);

            Assert.AreEqual(TesState.SYSTEMERROREnum, tesTask.State);
            Assert.AreEqual($"InvalidInputFilePath", failureReason);
            Assert.AreEqual($"InvalidInputFilePath", systemLog[0]);
            Assert.AreEqual($"Directory input is not supported.", systemLog[1]);
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
            var azureProxy = GetMockAzureProxy(azureProxyReturnValues);

            await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig(), azureProxy);

            var modifiedCommandScript = (string)azureProxy.Invocations.FirstOrDefault(i => i.Method.Name == nameof(IAzureProxy.UploadBlobAsync) && i.Arguments[0].ToString().Contains("/script"))?.Arguments[1];
            var filesToDownload = GetFilesToDownload(azureProxy);

            Assert.AreEqual(TesState.INITIALIZINGEnum, tesTask.State);
            Assert.IsFalse(filesToDownload.Any(f => f.LocalPath.Contains("?") || f.LocalPath.Contains("param=1") || f.LocalPath.Contains("param=2")), "Query string was not removed from local file path");
            Assert.AreEqual(1, filesToDownload.Count(f => f.StorageUrl.Contains("?param=1")), "Query string was removed from blob URL");
            Assert.IsFalse(modifiedCommandScript.Contains("?param=2"), "Query string was not removed from local file path in command script");
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

            var azureProxy = GetMockAzureProxy(AzureProxyReturnValues.Defaults);

            await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig(), azureProxy);

            var modifiedCommandScript = (string)azureProxy.Invocations.FirstOrDefault(i => i.Method.Name == nameof(IAzureProxy.UploadBlobAsync) && i.Arguments[0].ToString().Contains("/script"))?.Arguments[1];
            var filesToDownload = GetFilesToDownload(azureProxy);

            Assert.AreEqual(TesState.INITIALIZINGEnum, tesTask.State);
            Assert.AreEqual(2, filesToDownload.Count());
            Assert.IsFalse(filesToDownload.Any(f => f.LocalPath.Contains("?") || f.LocalPath.Contains("param=1") || f.LocalPath.Contains("param=2")), "Query string was not removed from local file path");
            Assert.AreEqual(1, filesToDownload.Count(f => f.StorageUrl.Contains("?param=1")), "Query string was removed from blob URL");
            Assert.IsFalse(modifiedCommandScript.Contains("?param=2"), "Query string was not removed from local file path in command script");
        }

        [TestMethod]
        public async Task PublicHttpUrlsAreKeptIntact()
        {
            var config = GetMockConfig();
            config["ExternalStorageContainers"] = "https://externalaccount1.blob.core.windows.net/container1?sas1; https://externalaccount2.blob.core.windows.net/container2/?sas2; https://externalaccount2.blob.core.windows.net?accountsas;";

            var tesTask = GetTesTask();

            tesTask.Inputs = new List<TesInput>
            {
                new TesInput { Url = null, Path = "/cromwell-executions/workflowpath/execution/script", Type = TesFileType.FILEEnum, Name = "commandScript", Content = "echo hello" },
                new TesInput { Url = "https://storageaccount1.blob.core.windows.net/container1/blob1?sig=sassignature", Path = "/cromwell-executions/workflowpath/inputs/blob1", Type = TesFileType.FILEEnum, Name = "blob1", Content = null },
                new TesInput { Url = "https://externalaccount1.blob.core.windows.net/container1/blob2?sig=sassignature", Path = "/cromwell-executions/workflowpath/inputs/blob2", Type = TesFileType.FILEEnum, Name = "blob2", Content = null },
                new TesInput { Url = "https://publicaccount1.blob.core.windows.net/container1/blob3", Path = "/cromwell-executions/workflowpath/inputs/blob3", Type = TesFileType.FILEEnum, Name = "blob3", Content = null }
            };

            var azureProxy = GetMockAzureProxy(AzureProxyReturnValues.Defaults);

            await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, config, azureProxy);

            var filesToDownload = GetFilesToDownload(azureProxy);

            Assert.AreEqual(4, filesToDownload.Count());
            Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.Equals("https://storageaccount1.blob.core.windows.net/container1/blob1?sig=sassignature")));
            Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.Equals("https://externalaccount1.blob.core.windows.net/container1/blob2?sig=sassignature")));
            Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.Equals("https://publicaccount1.blob.core.windows.net/container1/blob3")));
        }

        [TestMethod]
        public async Task PrivatePathsAndUrlsGetSasToken()
        {
            var config = GetMockConfig();
            config["ExternalStorageContainers"] = "https://externalaccount1.blob.core.windows.net/container1?sas1; https://externalaccount2.blob.core.windows.net/container2/?sas2; https://externalaccount2.blob.core.windows.net?accountsas;";

            var tesTask = GetTesTask();

            tesTask.Inputs = new List<TesInput>
            {
                // defaultstorageaccount and storageaccount1 are accessible to TES identity
                new TesInput { Url = null, Path = "/cromwell-executions/workflowpath/execution/script", Type = TesFileType.FILEEnum, Name = "commandScript", Content = "echo hello" },

                new TesInput { Url = "/defaultstorageaccount/container1/blob1", Path = "/cromwell-executions/workflowpath/inputs/blob1", Type = TesFileType.FILEEnum, Name = "blob1", Content = null },
                new TesInput { Url = "/storageaccount1/container1/blob2", Path = "/cromwell-executions/workflowpath/inputs/blob2", Type = TesFileType.FILEEnum, Name = "blob2", Content = null },
                new TesInput { Url = "/externalaccount1/container1/blob3", Path = "/cromwell-executions/workflowpath/inputs/blob3", Type = TesFileType.FILEEnum, Name = "blob3", Content = null },
                new TesInput { Url = "/externalaccount2/container2/blob4", Path = "/cromwell-executions/workflowpath/inputs/blob4", Type = TesFileType.FILEEnum, Name = "blob4", Content = null },

                new TesInput { Url = "file:///defaultstorageaccount/container1/blob5", Path = "/cromwell-executions/workflowpath/inputs/blob5", Type = TesFileType.FILEEnum, Name = "blob5", Content = null },
                new TesInput { Url = "file:///storageaccount1/container1/blob6", Path = "/cromwell-executions/workflowpath/inputs/blob6", Type = TesFileType.FILEEnum, Name = "blob6", Content = null },
                new TesInput { Url = "file:///externalaccount1/container1/blob7", Path = "/cromwell-executions/workflowpath/inputs/blob7", Type = TesFileType.FILEEnum, Name = "blob7", Content = null },
                new TesInput { Url = "file:///externalaccount2/container2/blob8", Path = "/cromwell-executions/workflowpath/inputs/blob8", Type = TesFileType.FILEEnum, Name = "blob8", Content = null },

                new TesInput { Url = "https://defaultstorageaccount.blob.core.windows.net/container1/blob9", Path = "/cromwell-executions/workflowpath/inputs/blob9", Type = TesFileType.FILEEnum, Name = "blob9", Content = null },
                new TesInput { Url = "https://storageaccount1.blob.core.windows.net/container1/blob10", Path = "/cromwell-executions/workflowpath/inputs/blob10", Type = TesFileType.FILEEnum, Name = "blob10", Content = null },
                new TesInput { Url = "https://externalaccount1.blob.core.windows.net/container1/blob11", Path = "/cromwell-executions/workflowpath/inputs/blob11", Type = TesFileType.FILEEnum, Name = "blob11", Content = null },
                new TesInput { Url = "https://externalaccount2.blob.core.windows.net/container2/blob12", Path = "/cromwell-executions/workflowpath/inputs/blob12", Type = TesFileType.FILEEnum, Name = "blob12", Content = null },

                // ExternalStorageContainers entry exists for externalaccount2/container2 and for externalaccount2 (account level SAS), so this uses account SAS:
                new TesInput { Url = "https://externalaccount2.blob.core.windows.net/container3/blob13", Path = "/cromwell-executions/workflowpath/inputs/blob12", Type = TesFileType.FILEEnum, Name = "blob12", Content = null },

                // ExternalStorageContainers entry exists for externalaccount1/container1, but not for externalaccount1/publiccontainer, so this is treated as public URL:
                new TesInput { Url = "https://externalaccount1.blob.core.windows.net/publiccontainer/blob14", Path = "/cromwell-executions/workflowpath/inputs/blob14", Type = TesFileType.FILEEnum, Name = "blob14", Content = null }
            };

            var azureProxy = GetMockAzureProxy(AzureProxyReturnValues.Defaults);

            await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, config, azureProxy);

            var filesToDownload = GetFilesToDownload(azureProxy);

            Assert.AreEqual(15, filesToDownload.Count());

            Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.StartsWith("https://defaultstorageaccount.blob.core.windows.net/container1/blob1?sv=")));
            Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.StartsWith("https://storageaccount1.blob.core.windows.net/container1/blob2?sv=")));
            Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.Equals("https://externalaccount1.blob.core.windows.net/container1/blob3?sas1")));
            Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.Equals("https://externalaccount2.blob.core.windows.net/container2/blob4?sas2")));

            Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.StartsWith("https://defaultstorageaccount.blob.core.windows.net/container1/blob5?sv=")));
            Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.StartsWith("https://storageaccount1.blob.core.windows.net/container1/blob6?sv=")));
            Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.Equals("https://externalaccount1.blob.core.windows.net/container1/blob7?sas1")));
            Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.Equals("https://externalaccount2.blob.core.windows.net/container2/blob8?sas2")));

            Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.StartsWith("https://defaultstorageaccount.blob.core.windows.net/container1/blob9?sv=")));
            Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.StartsWith("https://storageaccount1.blob.core.windows.net/container1/blob10?sv=")));
            Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.Equals("https://externalaccount1.blob.core.windows.net/container1/blob11?sas1")));
            Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.Equals("https://externalaccount2.blob.core.windows.net/container2/blob12?sas2")));

            Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.Equals("https://externalaccount2.blob.core.windows.net/container3/blob13?accountsas")));

            Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.Equals("https://externalaccount1.blob.core.windows.net/publiccontainer/blob14")));
        }

        [TestMethod]
        public async Task PrivateImagesArePulledUsingPoolConfiguration()
        {
            var tesTask = GetTesTask();
            var azureProxy = GetMockAzureProxy(AzureProxyReturnValues.Defaults);

            (_, var cloudTask, var poolInformation) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig(), azureProxy);
            var batchScript = (string)azureProxy.Invocations.FirstOrDefault(i => i.Method.Name == nameof(IAzureProxy.UploadBlobAsync) && i.Arguments[0].ToString().Contains("/batch_script"))?.Arguments[1];

            Assert.IsNotNull(poolInformation.AutoPoolSpecification.PoolSpecification.VirtualMachineConfiguration.ContainerConfiguration);
            Assert.AreEqual("registryServer1", poolInformation.AutoPoolSpecification.PoolSpecification.VirtualMachineConfiguration.ContainerConfiguration.ContainerRegistries.FirstOrDefault()?.RegistryServer);
            Assert.AreEqual(2, Regex.Matches(batchScript, tesTask.Executors.First().Image, RegexOptions.IgnoreCase).Count);
            Assert.IsFalse(batchScript.Contains($"docker pull --quiet {tesTask.Executors.First().Image}"));
        }

        [TestMethod]
        public async Task PublicImagesArePulledInTaskCommand()
        {
            var tesTask = GetTesTask();
            tesTask.Executors.First().Image = "ubuntu";
            var azureProxy = GetMockAzureProxy(AzureProxyReturnValues.Defaults);

            (_, var cloudTask, var poolInformation) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig(), azureProxy);
            var batchScript = (string)azureProxy.Invocations.FirstOrDefault(i => i.Method.Name == nameof(IAzureProxy.UploadBlobAsync) && i.Arguments[0].ToString().Contains("/batch_script"))?.Arguments[1];

            Assert.IsNull(poolInformation.AutoPoolSpecification.PoolSpecification.VirtualMachineConfiguration.ContainerConfiguration);
            Assert.AreEqual(3, Regex.Matches(batchScript, tesTask.Executors.First().Image, RegexOptions.IgnoreCase).Count);
            Assert.IsTrue(batchScript.Contains("docker pull --quiet ubuntu"));
        }

        [TestMethod]
        public async Task PrivateContainersRunInsideDockerInDockerContainer()
        {
            var tesTask = GetTesTask();

            (_, var cloudTask, _) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig(), GetMockAzureProxy(AzureProxyReturnValues.Defaults));

            Assert.IsNotNull(cloudTask.ContainerSettings);
            Assert.AreEqual("docker", cloudTask.ContainerSettings.ImageName);
        }

        [TestMethod]
        public async Task PublicContainersRunInsideRegularTaskCommand()
        {
            var tesTask = GetTesTask();
            tesTask.Executors.First().Image = "ubuntu";

            (_, var cloudTask, _) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig(), GetMockAzureProxy(AzureProxyReturnValues.Defaults));

            Assert.IsNull(cloudTask.ContainerSettings);
        }

        [TestMethod]
        public async Task LocalFilesInCromwellTmpDirectoryAreDiscoveredAndUploaded()
        {
            var config = GetMockConfig();
            var tesTask = GetTesTask();

            tesTask.Inputs = new List<TesInput>
            {
                new TesInput { Url = null, Path = "/cromwell-executions/workflowpath/execution/script", Type = TesFileType.FILEEnum, Name = "commandScript", Content = "echo hello" },
                new TesInput { Url = "file:///cromwell-tmp/tmp12345/blob1", Path = "/cromwell-executions/workflowpath/inputs/blob1", Type = TesFileType.FILEEnum, Name = "blob1", Content = null },
            };

            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.LocalFileExists = true;
            var azureProxy = GetMockAzureProxy(azureProxyReturnValues);

            await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, config, azureProxy);

            var filesToDownload = GetFilesToDownload(azureProxy);

            Assert.AreEqual(2, filesToDownload.Count());
            var inputFileUrl = filesToDownload.SingleOrDefault(f => f.StorageUrl.StartsWith("https://defaultstorageaccount.blob.core.windows.net/cromwell-executions/workflowpath/inputs/blob1?sv=")).StorageUrl;
            Assert.IsNotNull(inputFileUrl);
            azureProxy.Verify(i => i.LocalFileExists("/cromwell-tmp/tmp12345/blob1"));
            azureProxy.Verify(i => i.UploadBlobFromFileAsync(It.Is<Uri>(uri => uri.AbsoluteUri.StartsWith("https://defaultstorageaccount.blob.core.windows.net/cromwell-executions/workflowpath/inputs/blob1?sv=")), "/cromwell-tmp/tmp12345/blob1"));
        }

        [TestMethod]
        public async Task PoolIsCreatedInSubnetWhenBatchNodesSubnetIdIsSet()
        {
            var config = GetMockConfig();
            config["BatchNodesSubnetId"] = "subnet1";

            var tesTask = GetTesTask();
            var azureProxy = GetMockAzureProxy(AzureProxyReturnValues.Defaults);

            (_, _, var poolInformation) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, config, azureProxy);

            var poolNetworkConfiguration = poolInformation.AutoPoolSpecification.PoolSpecification.NetworkConfiguration;

            Assert.AreEqual(Microsoft.Azure.Batch.Common.IPAddressProvisioningType.BatchManaged, poolNetworkConfiguration?.PublicIPAddressConfiguration?.Provision);
            Assert.AreEqual("subnet1", poolNetworkConfiguration?.SubnetId);
        }

        [TestMethod]
        public async Task PoolIsCreatedWithoutPublicIpWhenSubnetAndDisableBatchNodesPublicIpAddressAreSet()
        {
            var config = GetMockConfig();
            config["BatchNodesSubnetId"] = "subnet1";
            config["DisableBatchNodesPublicIpAddress"] = "true";

            var tesTask = GetTesTask();
            var azureProxy = GetMockAzureProxy(AzureProxyReturnValues.Defaults);

            (_, _, var poolInformation) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, config, azureProxy);

            var poolNetworkConfiguration = poolInformation.AutoPoolSpecification.PoolSpecification.NetworkConfiguration;

            Assert.AreEqual(Microsoft.Azure.Batch.Common.IPAddressProvisioningType.NoPublicIPAddresses, poolNetworkConfiguration?.PublicIPAddressConfiguration?.Provision);
            Assert.AreEqual("subnet1", poolNetworkConfiguration?.SubnetId);
        }

        private static async Task<(string FailureReason, string[] SystemLog)> ProcessTesTaskAndGetFailureReasonAndSystemLogAsync(TesTask tesTask, AzureBatchJobAndTaskState? azureBatchJobAndTaskState = null)
        {
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.BatchJobAndTaskState = azureBatchJobAndTaskState ?? azureProxyReturnValues.BatchJobAndTaskState;

            await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig(), GetMockAzureProxy(azureProxyReturnValues));

            return (tesTask.Logs?.LastOrDefault()?.FailureReason, tesTask.Logs?.LastOrDefault()?.SystemLogs?.ToArray());
        }

        private static Task<(string JobId, CloudTask CloudTask, PoolInformation PoolInformation)> ProcessTesTaskAndGetBatchJobArgumentsAsync()
            => ProcessTesTaskAndGetBatchJobArgumentsAsync(GetTesTask(), GetMockConfig(), GetMockAzureProxy(AzureProxyReturnValues.Defaults));

        private static async Task<(string JobId, CloudTask CloudTask, PoolInformation PoolInformation)> ProcessTesTaskAndGetBatchJobArgumentsAsync(TesTask tesTask, IConfiguration configuration, Mock<IAzureProxy> azureProxy)
        {
            var batchScheduler = new BatchScheduler(
                new Mock<ILogger>().Object, 
                configuration, 
                new CachingWithRetriesAzureProxy(azureProxy.Object, new CachingService(new MemoryCacheProvider(new MemoryCache(new MemoryCacheOptions())))),
                new StorageAccessProvider(new Mock<ILogger>().Object, configuration, azureProxy.Object)
            );

            await batchScheduler.ProcessTesTaskAsync(tesTask);

            var createBatchJobAsyncInvocation = azureProxy.Invocations.FirstOrDefault(i => i.Method.Name == nameof(IAzureProxy.CreateBatchJobAsync));

            var jobId = createBatchJobAsyncInvocation?.Arguments[0] as string;
            var cloudTask = createBatchJobAsyncInvocation?.Arguments[1] as CloudTask;
            var poolInformation = createBatchJobAsyncInvocation?.Arguments[2] as PoolInformation;

            return (jobId, cloudTask, poolInformation);
        }

        private static async Task<TesState> GetNewTesTaskStateAsync(TesTask tesTask, AzureProxyReturnValues azureProxyReturnValues)
        {
            await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig(), GetMockAzureProxy(azureProxyReturnValues));

            return tesTask.State;
        }

        private static Task<TesState> GetNewTesTaskStateAsync(TesState currentTesTaskState, AzureBatchJobAndTaskState azureBatchJobAndTaskState)
            => GetNewTesTaskStateAsync(new TesTask { Id = "test", State = currentTesTaskState }, azureBatchJobAndTaskState);

        private static Task<TesState> GetNewTesTaskStateAsync(TesTask tesTask, AzureBatchJobAndTaskState? azureBatchJobAndTaskState = null)
        {
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.BatchJobAndTaskState = azureBatchJobAndTaskState ?? azureProxyReturnValues.BatchJobAndTaskState;

            return GetNewTesTaskStateAsync(tesTask, azureProxyReturnValues);
        }

        private static Task<TesState> GetNewTesTaskStateAsync(TesResources resources, AzureProxyReturnValues azureProxyReturnValues)
        {
            var tesTask = GetTesTask();
            tesTask.Resources = resources;

            return GetNewTesTaskStateAsync(tesTask, azureProxyReturnValues);
        }

        private static TesTask GetTesTask()
            => JsonConvert.DeserializeObject<TesTask>(File.ReadAllText("testask1.json"));

        private static Mock<IAzureProxy> GetMockAzureProxy(AzureProxyReturnValues azureProxyReturnValues)
        {
            var azureProxy = new Mock<IAzureProxy>();

            azureProxy.Setup(a => a.GetBatchJobAndTaskStateAsync(It.IsAny<string>())).Returns(Task.FromResult(azureProxyReturnValues.BatchJobAndTaskState));
            azureProxy.Setup(a => a.GetNextBatchJobIdAsync(It.IsAny<string>())).Returns(Task.FromResult(azureProxyReturnValues.NextBatchJobId));
            azureProxy.Setup(a => a.GetStorageAccountInfoAsync("defaultstorageaccount")).Returns(Task.FromResult(azureProxyReturnValues.StorageAccountInfos["defaultstorageaccount"]));
            azureProxy.Setup(a => a.GetStorageAccountInfoAsync("storageaccount1")).Returns(Task.FromResult(azureProxyReturnValues.StorageAccountInfos["storageaccount1"]));
            azureProxy.Setup(a => a.GetContainerRegistryInfoAsync("registryServer1/imageName1:tag1")).Returns(Task.FromResult(azureProxyReturnValues.ContainerRegistryInfo));
            azureProxy.Setup(a => a.GetStorageAccountKeyAsync(It.IsAny<StorageAccountInfo>())).Returns(Task.FromResult(azureProxyReturnValues.StorageAccountKey));
            azureProxy.Setup(a => a.GetVmSizesAndPricesAsync()).Returns(Task.FromResult(azureProxyReturnValues.VmSizesAndPrices));
            azureProxy.Setup(a => a.GetBatchAccountQuotasAsync()).Returns(Task.FromResult(azureProxyReturnValues.BatchQuotas));
            azureProxy.Setup(a => a.GetBatchActiveNodeCountByVmSize()).Returns(azureProxyReturnValues.ActiveNodeCountByVmSize);
            azureProxy.Setup(a => a.GetBatchActiveJobCount()).Returns(azureProxyReturnValues.ActiveJobCount);
            azureProxy.Setup(a => a.GetBatchActivePoolCount()).Returns(azureProxyReturnValues.ActivePoolCount);
            azureProxy.Setup(a => a.DownloadBlobAsync(It.IsAny<Uri>())).Returns(Task.FromResult(azureProxyReturnValues.DownloadedBlobContent));
            azureProxy.Setup(a => a.LocalFileExists(It.IsAny<string>())).Returns(azureProxyReturnValues.LocalFileExists);
            azureProxy.Setup(a => a.CreateManualBatchPoolAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<string>(), It.IsAny<BatchNodeInfo>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<IEnumerable<string>>(), It.IsAny<bool>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>())).Returns(Task.FromResult(new ManualBatchPoolCreationResult()));

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

            var blobxferFilesToDownload = downloadFilesBlobxferRegex.Matches(downloadFilesScriptContent)
                .Cast<System.Text.RegularExpressions.Match>()
                .Select(m => new FileToDownload { LocalPath = m.Groups[1].Value, StorageUrl = m.Groups[2].Value });

            var wgetFilesToDownload = downloadFilesWgetRegex.Matches(downloadFilesScriptContent)
                .Cast<System.Text.RegularExpressions.Match>()
                .Select(m => new FileToDownload { LocalPath = m.Groups[1].Value, StorageUrl = m.Groups[2].Value });

            return blobxferFilesToDownload.Union(wgetFilesToDownload);
        }

        private struct BatchJobAndTaskStates
        {
            public static AzureBatchJobAndTaskState TaskActive => new() { JobState = JobState.Active, TaskState = TaskState.Active };
            public static AzureBatchJobAndTaskState TaskPreparing => new() { JobState = JobState.Active, TaskState = TaskState.Preparing };
            public static AzureBatchJobAndTaskState TaskRunning => new() { JobState = JobState.Active, TaskState = TaskState.Running };
            public static AzureBatchJobAndTaskState TaskCompletedSuccessfully => new() { JobState = JobState.Completed, TaskState = TaskState.Completed, TaskExitCode = 0 };
            public static AzureBatchJobAndTaskState TaskFailed => new() { JobState = JobState.Completed, TaskState = TaskState.Completed, TaskExitCode = -1 };
            public static AzureBatchJobAndTaskState JobNotFound => new() { JobState = null };
            public static AzureBatchJobAndTaskState TaskNotFound => new() { JobState = JobState.Active, TaskState = null };
            public static AzureBatchJobAndTaskState MoreThanOneJobFound => new() { MoreThanOneActiveJobFound = true };
            public static AzureBatchJobAndTaskState NodeAllocationFailed => new() { JobState = JobState.Active, NodeAllocationFailed = true };
            public static AzureBatchJobAndTaskState NodePreempted => new() { JobState = JobState.Active, NodeState = ComputeNodeState.Preempted };
            public static AzureBatchJobAndTaskState NodeDiskFull => new() { JobState = JobState.Active, NodeErrorCode = "DiskFull" };
            public static AzureBatchJobAndTaskState ActiveJobWithMissingAutoPool => new() { ActiveJobWithMissingAutoPool = true };
            public static AzureBatchJobAndTaskState ImageDownloadFailed => new() { JobState = JobState.Active, NodeErrorCode = "ContainerInvalidImage" };
        }

        private class AzureProxyReturnValues
        {
            public Dictionary<string, StorageAccountInfo> StorageAccountInfos { get; set; }
            public ContainerRegistryInfo ContainerRegistryInfo { get; set; }
            public List<VirtualMachineInformation> VmSizesAndPrices { get; set; }
            public AzureBatchAccountQuotas BatchQuotas { get; set; }
            public IEnumerable<AzureBatchNodeCount> ActiveNodeCountByVmSize { get; set; }
            public int ActiveJobCount { get; set; }
            public int ActivePoolCount { get; set; }
            public AzureBatchJobAndTaskState BatchJobAndTaskState { get; set; }
            public string NextBatchJobId { get; set; }
            public string StorageAccountKey { get; set; }
            public string DownloadedBlobContent { get; set; }
            public bool LocalFileExists { get; set; }

            public static AzureProxyReturnValues Defaults => new()
            {
                StorageAccountInfos = new Dictionary<string, StorageAccountInfo> {
                    { "defaultstorageaccount", new StorageAccountInfo { Name = "defaultstorageaccount", Id = "Id", BlobEndpoint = "https://defaultstorageaccount.blob.core.windows.net/", SubscriptionId = "SubId" } },
                    { "storageaccount1", new StorageAccountInfo { Name = "storageaccount1", Id = "Id", BlobEndpoint = "https://storageaccount1.blob.core.windows.net/", SubscriptionId = "SubId" } }
                },
                ContainerRegistryInfo = new ContainerRegistryInfo { RegistryServer = "registryServer1", Username = "default", Password = "placeholder" },
                VmSizesAndPrices = new List<VirtualMachineInformation> {
                    new VirtualMachineInformation { VmSize = "VmSizeLowPri1", VmFamily = "VmFamily1", LowPriority = true, NumberOfCores = 1, MemoryInGB = 4, ResourceDiskSizeInGB = 20, PricePerHour = 1 },
                    new VirtualMachineInformation { VmSize = "VmSizeLowPri2", VmFamily = "VmFamily2", LowPriority = true, NumberOfCores = 2, MemoryInGB = 8, ResourceDiskSizeInGB = 40, PricePerHour = 2 },
                    new VirtualMachineInformation { VmSize = "VmSizeDedicated1", VmFamily = "VmFamily1", LowPriority = false, NumberOfCores = 1, MemoryInGB = 4, ResourceDiskSizeInGB = 20, PricePerHour = 11 },
                    new VirtualMachineInformation { VmSize = "VmSizeDedicated2", VmFamily = "VmFamily2", LowPriority = false, NumberOfCores = 2, MemoryInGB = 8, ResourceDiskSizeInGB = 40, PricePerHour = 22 }
                },
                BatchQuotas = new AzureBatchAccountQuotas { ActiveJobAndJobScheduleQuota = 1, PoolQuota = 1, DedicatedCoreQuota = 5, LowPriorityCoreQuota = 10 },
                ActiveNodeCountByVmSize = new List<AzureBatchNodeCount>(),
                ActiveJobCount = 0,
                ActivePoolCount = 0,
                BatchJobAndTaskState = BatchJobAndTaskStates.JobNotFound,
                NextBatchJobId = "JobId-1",
                StorageAccountKey = "Key1",
                DownloadedBlobContent = string.Empty,
                LocalFileExists = true
            };

            public static AzureProxyReturnValues DefaultsPerVMFamilyEnforced => DefaultsPerVMFamilyEnforcedImpl();

            private static AzureProxyReturnValues DefaultsPerVMFamilyEnforcedImpl()
            {
                var proxy = Defaults;
                proxy.VmSizesAndPrices.Add(new VirtualMachineInformation { VmSize = "VmSize3", VmFamily = "VmFamily3", LowPriority = false, NumberOfCores = 4, MemoryInGB = 12, ResourceDiskSizeInGB = 80, PricePerHour = 33 });
                proxy.BatchQuotas = new AzureBatchAccountQuotas()
                {
                    DedicatedCoreQuotaPerVMFamilyEnforced = true,
                    DedicatedCoreQuotaPerVMFamily = new[] { new VirtualMachineFamilyCoreQuota("VmFamily1", proxy.BatchQuotas.DedicatedCoreQuota), new VirtualMachineFamilyCoreQuota("VmFamily2", 0), new VirtualMachineFamilyCoreQuota("VmFamily3", 4) },
                    DedicatedCoreQuota = proxy.BatchQuotas.DedicatedCoreQuota,
                    ActiveJobAndJobScheduleQuota = proxy.BatchQuotas.ActiveJobAndJobScheduleQuota,
                    LowPriorityCoreQuota = proxy.BatchQuotas.LowPriorityCoreQuota,
                    PoolQuota = proxy.BatchQuotas.PoolQuota
                };
                return proxy;
            }
        }

        private class FileToDownload
        {
            public string StorageUrl { get; set; }
            public string LocalPath { get; set; }
        }
    }
}
