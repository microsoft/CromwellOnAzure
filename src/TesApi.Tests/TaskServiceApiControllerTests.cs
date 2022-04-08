// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Tes.Models;
using TesApi.Controllers;

namespace TesApi.Tests
{
    [TestClass]
    public class TaskServiceApiControllerTests
    {
        [TestCategory("TES 1.1")]
        [TestMethod]
        public async Task TES_Supports_BackendParameter_vmsize()
        {
            const string backend_parameter_key = "vm_size";

            var backendParameters = new Dictionary<string, string>
            {
                { backend_parameter_key, "VmSize1" }
            };

            var tesTask = new TesTask
            {
                Executors = new List<TesExecutor> { new TesExecutor { Image = "ubuntu" } },
                Resources = new TesResources { BackendParameters = backendParameters, BackendParametersStrict = true }
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            var result = await controller.CreateTaskAsync(tesTask) as ObjectResult;

            Assert.IsNotNull(result);
            services.TesTaskRepository.Verify(x => x.CreateItemAsync(tesTask));
            Assert.AreEqual(32, tesTask.Id.Length);
            Assert.AreEqual(TesState.QUEUEDEnum, tesTask.State);
            Assert.IsTrue(tesTask.Resources.BackendParameters.ContainsKey(backend_parameter_key));
            Assert.AreEqual(200, result.StatusCode);
        }

        [TestCategory("TES 1.1")]
        [TestMethod]
        public async Task TES_Supports_BackendParameter_workflow_execution_identity()
        {
            const string backend_parameter_key = "workflow_execution_identity";

            var backendParameters = new Dictionary<string, string>
            {
                { backend_parameter_key, "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/coa/providers/Microsoft.ManagedIdentity/userAssignedIdentities/coa-test-uami" }
            };

            var tesTask = new TesTask
            {
                Executors = new List<TesExecutor> { new TesExecutor { Image = "ubuntu" } },
                Resources = new TesResources { BackendParameters = backendParameters, BackendParametersStrict = true }
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            var result = await controller.CreateTaskAsync(tesTask) as ObjectResult;

            Assert.IsNotNull(result);
            services.TesTaskRepository.Verify(x => x.CreateItemAsync(tesTask));
            Assert.AreEqual(32, tesTask.Id.Length);
            Assert.AreEqual(TesState.QUEUEDEnum, tesTask.State);
            Assert.IsTrue(tesTask.Resources.BackendParameters.ContainsKey(backend_parameter_key));
            Assert.AreEqual(200, result.StatusCode);
        }

        [TestCategory("TES 1.1")]
        [TestMethod]
        public async Task CreateTaskAsync_ReturnsTesCreateTaskResponseWithBackendParameters_UnsupportedKey()
        {
            const string unsupportedKey = "unsupported_key_2021";

            var backendParameters = new Dictionary<string, string>
            {
                { unsupportedKey, Guid.NewGuid().ToString() }
            };

            var tesTask = new TesTask
            {
                Executors = new List<TesExecutor> { new TesExecutor { Image = "ubuntu" } },
                Resources = new TesResources { BackendParameters = backendParameters }
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            var result = await controller.CreateTaskAsync(tesTask) as ObjectResult;

            Assert.IsNotNull(result);
            services.TesTaskRepository.Verify(x => x.CreateItemAsync(tesTask));
            Assert.AreEqual(32, tesTask.Id.Length);
            Assert.AreEqual(TesState.QUEUEDEnum, tesTask.State);

            // Unsupported keys should not be persisted
            Assert.IsFalse(tesTask?.Resources?.BackendParameters?.ContainsKey(unsupportedKey));
            Assert.AreEqual(200, result.StatusCode);
        }

        [TestCategory("TES 1.1")]
        [TestMethod]
        public async Task CreateTaskAsync_ReturnsBadRequest_ForBackendParametersStrict_UnsupportedKey()
        {
            const string unsupportedKey = "unsupported_key_2021";

            var backendParameters = new Dictionary<string, string>
            {
                { unsupportedKey, Guid.NewGuid().ToString() }
            };

            var tesTask = new TesTask
            {
                Executors = new List<TesExecutor> { new TesExecutor { Image = "ubuntu" } },
                Resources = new TesResources { BackendParameters = backendParameters, BackendParametersStrict = true }
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            var result = await controller.CreateTaskAsync(tesTask) as BadRequestObjectResult;

            Assert.IsNotNull(result);

            // Unsupported keys should cause a bad request when BackendParametersStrict = true
            Assert.AreEqual(400, result.StatusCode);

            // Unsupported keys should be returned in the warning message
            Assert.IsTrue(result.Value.ToString().Contains(unsupportedKey));
        }

        [TestCategory("TES 1.1")]
        [TestMethod]
        public async Task CreateTaskAsync_ReturnsBadRequest_ForBackendParametersStrict_DuplicateKeys()
        {
            const string backend_parameter_key = "vmsize";

            var backendParameters = new Dictionary<string, string>
            {
                { backend_parameter_key, Guid.NewGuid().ToString() },
                { "VmSize", Guid.NewGuid().ToString() }
            };

            var tesTask = new TesTask
            {
                Executors = new List<TesExecutor> { new TesExecutor { Image = "ubuntu" } },
                Resources = new TesResources { BackendParameters = backendParameters, BackendParametersStrict = true }
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            var result = await controller.CreateTaskAsync(tesTask) as BadRequestObjectResult;

            Assert.IsNotNull(result);

            Assert.AreEqual(400, result.StatusCode);
        }

        [TestMethod]
        public async Task CreateTaskAsync_ReturnsBadRequest_ForInvalidId()
        {
            var tesTask = new TesTask { Id = "ClientProvidedId", Executors = new List<TesExecutor> { new TesExecutor { Image = "ubuntu" } } };
            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            var result = await controller.CreateTaskAsync(tesTask) as BadRequestObjectResult;

            Assert.IsNotNull(result);
            Assert.AreEqual(400, result.StatusCode);
        }

        [TestMethod]
        public async Task CreateTaskAsync_ReturnsBadRequest_ForMissingDockerImage()
        {
            var tesTask = new TesTask();
            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            var result = await controller.CreateTaskAsync(tesTask) as ObjectResult;

            Assert.IsNotNull(result);
            Assert.AreEqual(400, result.StatusCode);
        }

        [TestMethod]
        public async Task CreateTaskAsync_ReturnsTesCreateTaskResponse()
        {
            var tesTask = new TesTask() { Executors = new List<TesExecutor> { new TesExecutor { Image = "ubuntu" } } };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            var result = await controller.CreateTaskAsync(tesTask) as ObjectResult;

            Assert.IsNotNull(result);
            services.TesTaskRepository.Verify(x => x.CreateItemAsync(tesTask));
            Assert.AreEqual(32, tesTask.Id.Length);
            Assert.AreEqual(TesState.QUEUEDEnum, tesTask.State);
            Assert.AreEqual(200, result.StatusCode);
        }

        [TestMethod]
        public async Task CreateTaskAsync_CromwellWorkflowIdIsUsedAsTaskIdPrefix()
        {
            var cromwellWorkflowId = Guid.NewGuid().ToString();
            var cromwellSubWorkflowId = Guid.NewGuid().ToString();
            var taskDescription = $"{cromwellSubWorkflowId}:BackendJobDescriptorKey_CommandCallNode_wf_hello.hello:-1:1";

            var tesTask = new TesTask()
            {
                Description = taskDescription,
                Executors = new List<TesExecutor> { new TesExecutor { Image = "ubuntu" } },
                Inputs = new List<TesInput> { new TesInput { Name = "commandScript", Path = $"/cromwell-executions/test/{cromwellWorkflowId}/call-hello/test-subworkflow/{cromwellSubWorkflowId}/call-subworkflow/shard-8/execution/script" } }
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            await controller.CreateTaskAsync(tesTask);

            Assert.AreEqual(41, tesTask.Id.Length); // First eight characters of Cromwell's job id + underscore + GUID without dashes
            Assert.IsTrue(tesTask.Id.StartsWith(cromwellWorkflowId[..8] + "_"));
        }

        [TestMethod]
        public async Task CancelTaskAsync_ReturnsBadRequest_ForInvalidId()
        {
            var tesTaskId = "IdDoesNotExist";

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: r =>
                r.Setup(repo => repo.TryGetItemAsync(tesTaskId, It.IsAny<Action<TesTask>>()))
                .Callback<string, Action<TesTask>>((id, action) =>
                {
                    action(null);
                })
                .ReturnsAsync(false));
            var controller = services.GetT();

            var result = await controller.CancelTask(tesTaskId) as NotFoundObjectResult;

            Assert.IsNotNull(result);
            Assert.AreEqual(404, result.StatusCode);
        }

        [TestMethod]
        public async Task CancelTaskAsync_ReturnsEmptyObject()
        {
            var tesTask = new TesTask() { Id = "testTaskId", State = TesState.QUEUEDEnum };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: r =>
                r.Setup(repo => repo.TryGetItemAsync(tesTask.Id, It.IsAny<Action<TesTask>>()))
                .Callback<string, Action<TesTask>>((id, action) =>
                {
                    action(tesTask);
                })
                .ReturnsAsync(true));
            var controller = services.GetT();

            var result = await controller.CancelTask(tesTask.Id) as ObjectResult;

            Assert.IsNotNull(result);
            Assert.AreEqual(200, result.StatusCode);
            Assert.AreEqual(TesState.CANCELEDEnum, tesTask.State);
            services.TesTaskRepository.Verify(x => x.UpdateItemAsync(tesTask));
        }

        [TestMethod]
        public void GetServiceInfo_ReturnsInfo()
        {
            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            var result = controller.GetServiceInfo() as ObjectResult;

            Assert.IsNotNull(result);
            Assert.AreEqual(200, result.StatusCode);
        }

        [TestMethod]
        public async Task GetTaskAsync_ReturnsNotFound_ForInvalidId()
        {
            var tesTaskId = "IdDoesNotExist";

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: r =>
                r.Setup(repo => repo.TryGetItemAsync(tesTaskId, It.IsAny<Action<TesTask>>()))
                    .ReturnsAsync(false));
            var controller = services.GetT();

            var result = await controller.GetTaskAsync(tesTaskId, "MINIMAL") as NotFoundObjectResult;

            Assert.IsNotNull(result);
            Assert.AreEqual(404, result.StatusCode);
        }

        [TestMethod]
        public async Task GetTaskAsync_ReturnsBadRequest_ForInvalidViewValue()
        {
            var tesTask = new TesTask();

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: r =>
                r.Setup(repo => repo.TryGetItemAsync(tesTask.Id, It.IsAny<Action<TesTask>>()))
                .Callback<string, Action<TesTask>>((id, action) =>
                {
                    action(tesTask);
                })
                .ReturnsAsync(true));
            var controller = services.GetT();

            var result = await controller.GetTaskAsync(tesTask.Id, "INVALID") as BadRequestObjectResult;

            Assert.IsNotNull(result);
            Assert.AreEqual(400, result.StatusCode);
        }

        [TestMethod]
        public async Task GetTaskAsync_ReturnsJsonResult()
        {
            var tesTask = new TesTask
            {
                State = TesState.RUNNINGEnum
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: r =>
                r.Setup(repo => repo.TryGetItemAsync(tesTask.Id, It.IsAny<Action<TesTask>>()))
                .Callback<string, Action<TesTask>>((id, action) =>
                {
                    action(tesTask);
                })
                .ReturnsAsync(true));
            var controller = services.GetT();

            var result = await controller.GetTaskAsync(tesTask.Id, "MINIMAL") as JsonResult;

            Assert.IsNotNull(result);
            services.TesTaskRepository.Verify(x => x.TryGetItemAsync(tesTask.Id, It.IsAny<Action<TesTask>>()));
            Assert.AreEqual(TesState.RUNNINGEnum, tesTask.State);
            Assert.AreEqual(200, result.StatusCode);
        }

        [TestMethod]
        public async Task ListTasks_ReturnsBadRequest_ForInvalidPageSize()
        {
            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            var result = await controller.ListTasks(null, 0, null, "BASIC") as BadRequestObjectResult;

            Assert.IsNotNull(result);
            Assert.AreEqual(400, result.StatusCode);
        }

        [TestMethod]
        public async Task ListTasks_ReturnsJsonResult()
        {
            var firstTesTask = new TesTask { Id = "tesTaskId1", State = TesState.COMPLETEEnum, Name = "tesTask", ETag = Guid.NewGuid().ToString() };
            var secondTesTask = new TesTask { Id = "tesTaskId2", State = TesState.EXECUTORERROREnum, Name = "tesTask2", ETag = Guid.NewGuid().ToString() };
            var thirdTesTask = new TesTask { Id = "tesTaskId3", State = TesState.EXECUTORERROREnum, Name = "someOtherTask2", ETag = Guid.NewGuid().ToString() };
            var namePrefix = "tesTask";

            var tesTasks = new[] { firstTesTask, secondTesTask, thirdTesTask };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: r =>
                r.Setup(repo => repo
                .GetItemsAsync(It.IsAny<Expression<Func<TesTask, bool>>>(), It.IsAny<int>(), It.IsAny<string>()))
                .ReturnsAsync((Expression<Func<TesTask, bool>> predicate, int pageSize, string continuationToken) =>
                    (string.Empty, tesTasks.Where(i => predicate.Compile().Invoke(i)).Take(pageSize))));
            var controller = services.GetT();

            var result = await controller.ListTasks(namePrefix, 1, null, "BASIC") as JsonResult;
            var listOfTesTasks = (TesListTasksResponse)result.Value;

            Assert.IsNotNull(result);
            Assert.AreEqual(1, listOfTesTasks.Tasks.Count);
            Assert.AreEqual(200, result.StatusCode);
        }

        [TestMethod]
        public async Task CreateTaskAsync_ExtractsWorkflowId()
        {
            var cromwellWorkflowId = Guid.NewGuid().ToString();
            var cromwellSubWorkflowId = Guid.NewGuid().ToString();
            var taskDescription = $"{cromwellSubWorkflowId}:BackendJobDescriptorKey_CommandCallNode_wf_hello.hello:-1:1";

            var tesTask = new TesTask()
            {
                Description = taskDescription,
                Executors = new List<TesExecutor> { new TesExecutor { Image = "ubuntu" } },
                Inputs = new List<TesInput> { new TesInput { Name = "commandScript", Path = $"/cromwell-executions/test/{cromwellWorkflowId}/call-hello/test-subworkflow/{cromwellSubWorkflowId}/call-subworkflow/shard-8/execution/script" } }
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            await controller.CreateTaskAsync(tesTask);

            Assert.AreEqual(cromwellWorkflowId, tesTask.WorkflowId);
        }

        [TestMethod]
        public async Task CreateTaskAsync_InvalidInputsAndPathDoNotThrow()
        {
            var cromwellWorkflowId = "daf1a044-d741-4db9-8eb5-d6fd0519b1f1";
            var taskDescription = $"{cromwellWorkflowId}:BackendJobDescriptorKey_CommandCallNode_wf_hello.hello:-1:1";

            var tesTask1 = new TesTask()
            {
                Description = taskDescription,
                Executors = new List<TesExecutor> { new TesExecutor { Image = "ubuntu" } }
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            await controller.CreateTaskAsync(tesTask1);

            Assert.IsNull(tesTask1.WorkflowId);

            var tesTask2 = new TesTask()
            {
                Description = taskDescription,
                Executors = new List<TesExecutor> { new TesExecutor { Image = "ubuntu" } },
                Inputs = new List<TesInput> { new TesInput { Path = "/cromwell-executions/" } }
            };

            await controller.CreateTaskAsync(tesTask2);

            Assert.IsNull(tesTask2.WorkflowId);

            var tesTask3 = new TesTask()
            {
                Description = taskDescription,
                Executors = new List<TesExecutor> { new TesExecutor { Image = "ubuntu" } },
                Inputs = new List<TesInput> { new TesInput { Path = "/cromwell-executions/" } }
            };

            await controller.CreateTaskAsync(tesTask3);

            Assert.IsNull(tesTask3.WorkflowId);

            var tesTask4 = new TesTask()
            {
                Description = taskDescription,
                Executors = new List<TesExecutor> { new TesExecutor { Image = "ubuntu" } },
                Inputs = new List<TesInput> { new TesInput { Path = "/cromwell-executions/test/" } }
            };

            await controller.CreateTaskAsync(tesTask4);

            Assert.IsNull(tesTask4.WorkflowId);
        }

        [TestMethod]
        public async Task CreateCwlTaskAsync_CwlDiskSizeIsUsedIfTesTaskHasItNull()
        {
            var cwlFileContent = @"
                hints:
                  - class: ResourceRequirement
                    tmpdirMin: 1024
                    outdirMin: 2048";

            var tesTask = await CreateCwlTesTaskAsync(cwlFileContent, tesResourcesReceivedFromCromwell: null);

            Assert.AreEqual(3, tesTask.Resources.DiskGb);

            cwlFileContent = @"
                hints:
                  - class: ResourceRequirement
                    tmpdirMin: 1024";

            tesTask = await CreateCwlTesTaskAsync(cwlFileContent, tesResourcesReceivedFromCromwell: null);

            Assert.AreEqual(1, tesTask.Resources.DiskGb);

            cwlFileContent = @"
                hints:
                  - class: ResourceRequirement
                    tmpdirMax: 1024
                    outdirMax: 2048";

            tesTask = await CreateCwlTesTaskAsync(cwlFileContent, tesResourcesReceivedFromCromwell: null);

            Assert.AreEqual(3, tesTask.Resources.DiskGb);
        }

        [TestMethod]
        public async Task CreateCwlTaskAsync_TesResourceNamingCanBeUsedInCwl()
        {
            var cwlFileContent = @"
                hints:
                  - class: ResourceRequirement
                    cpu: 11
                    memory: 22 GB
                    disk: 33 GB";

            var tesTask = await CreateCwlTesTaskAsync(cwlFileContent, tesResourcesReceivedFromCromwell: null);

            Assert.AreEqual(11, tesTask.Resources.CpuCores);
            Assert.AreEqual(22, tesTask.Resources.RamGb);
            Assert.AreEqual(33, tesTask.Resources.DiskGb);
        }

        [TestMethod]
        public async Task CreateCwlTaskAsync_TesTaskDiskOverridesCwl()
        {
            // Cromwell is currently ignoring any disk specification in CWL workflows and not passing it on to TES.
            // When this is fixed, we want the Cromwell value to be used instead of retrieving it from the CWL file.
            var cwlFileContent = @"
                hints:
                  - class: ResourceRequirement
                    tmpdirMin: 1024
                    outdirMin: 2048
                    disk: 15 GB";

            var tesTask = await CreateCwlTesTaskAsync(cwlFileContent, tesResourcesReceivedFromCromwell: new TesResources { DiskGb = 5 });

            Assert.AreEqual(5, tesTask.Resources.DiskGb);
        }

        [TestMethod]
        public async Task CreateCwlTaskAsync_TesTaskCpuAndMemoryOverrideCwl()
        {
            var cwlFileContent = @"
                hints:
                  - class: ResourceRequirement
                    cpu: 11
                    memory: 22 GB";

            var tesTask = await CreateCwlTesTaskAsync(cwlFileContent, tesResourcesReceivedFromCromwell: new TesResources { CpuCores = 33, RamGb = 44 });

            Assert.AreEqual(33, tesTask.Resources.CpuCores);
            Assert.AreEqual(44, tesTask.Resources.RamGb);
        }

        [TestMethod]
        public async Task CreateCwlTaskAsync_NonNullCwlPreemptibleOverridesTesTaskPreemptible()
        {
            // CWL has no concept of preemptible and Cromwell always passes the defualt value (TRUE).
            // Cromwell ignores any hints in CWL workflow that it does not know about.
            // If preemptible hint exists in CWL, it needs to override the default one passed by Cromwell to TES.
            var cwlFileContent = @"
                hints:
                  - class: ResourceRequirement
                    preemptible: false";

            var tesTask = await CreateCwlTesTaskAsync(cwlFileContent, tesResourcesReceivedFromCromwell: new TesResources { Preemptible = true });

            Assert.AreEqual(false, tesTask.Resources.Preemptible);

            cwlFileContent = @"
                hints:
                  - class: ResourceRequirement
                    preemptible: true";

            tesTask = await CreateCwlTesTaskAsync(cwlFileContent, tesResourcesReceivedFromCromwell: new TesResources { Preemptible = false });

            Assert.AreEqual(true, tesTask.Resources.Preemptible);

            cwlFileContent = @"
                hints:
                  - class: ResourceRequirement
                    preemptible: false";

            tesTask = await CreateCwlTesTaskAsync(cwlFileContent, tesResourcesReceivedFromCromwell: null);

            Assert.AreEqual(false, tesTask.Resources.Preemptible);

            cwlFileContent = @"
                hints:
                  - class: ResourceRequirement";

            tesTask = await CreateCwlTesTaskAsync(cwlFileContent, tesResourcesReceivedFromCromwell: new TesResources { Preemptible = true });

            Assert.AreEqual(true, tesTask.Resources.Preemptible);
        }

        private static async Task<TesTask> CreateCwlTesTaskAsync(string cwlFileContent, TesResources tesResourcesReceivedFromCromwell)
        {
            var tesTask = new TesTask()
            {
                Name = "test.cwl",
                Executors = new List<TesExecutor> { new TesExecutor { Image = "ubuntu" } },
                Inputs = new List<TesInput> { new TesInput { Name = "commandScript", Path = "/cromwell-executions/test.cwl/daf1a044-d741-4db9-8eb5-d6fd0519b1f1/call-hello/execution/script" } },
                Resources = tesResourcesReceivedFromCromwell
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(
                azureProxy: a => a.Setup(a => a.TryReadCwlFile(It.IsAny<string>(), out cwlFileContent)).Returns(true));
            var controller = services.GetT();

            await controller.CreateTaskAsync(tesTask);

            return tesTask;
        }
    }
}
