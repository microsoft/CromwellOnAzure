// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using TesApi.Controllers;
using TesApi.Models;
using TesApi.Web;

namespace TesApi.Tests
{
    [TestClass]
    public class TaskServiceApiControllerTests
    {
        [TestMethod]
        public async Task CreateTaskAsync_ReturnsBadRequest_ForInvalidId()
        {
            // Arrange
            var mockRepo = new Mock<IRepository<TesTask>>();
            var tesTask = new TesTask { Id = "ClientProvidedId", Executors = new List<TesExecutor> { new TesExecutor { Image = "ubuntu" } } };

            var controller = new TaskServiceApiController(mockRepo.Object, new NullLogger<TaskServiceApiController>());

            // Act
            var result = await controller.CreateTaskAsync(tesTask) as BadRequestObjectResult;

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(400, result.StatusCode);
        }

        [TestMethod]
        public async Task CreateTaskAsync_ReturnsBadRequest_ForMissingDockerImage()
        {
            // Arrange
            var mockRepo = new Mock<IRepository<TesTask>>();
            var tesTask = new TesTask();

            var controller = new TaskServiceApiController(mockRepo.Object, new NullLogger<TaskServiceApiController>());

            // Act
            var result = await controller.CreateTaskAsync(tesTask) as ObjectResult;

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(400, result.StatusCode);
        }

        [TestMethod]
        public async Task CreateTaskAsync_ReturnsTesCreateTaskResponse()
        {
            // Arrange
            var mockRepo = new Mock<IRepository<TesTask>>();
            var tesTask = new TesTask() { Executors = new List<TesExecutor> { new TesExecutor { Image = "ubuntu" } } };

            var controller = new TaskServiceApiController(mockRepo.Object, new NullLogger<TaskServiceApiController>());

            // Act
            var result = await controller.CreateTaskAsync(tesTask) as ObjectResult;

            // Assert
            Assert.IsNotNull(result);
            mockRepo.Verify(x => x.CreateItemAsync(tesTask));
            Assert.AreEqual(32, tesTask.Id.Length);
            Assert.AreEqual(TesState.QUEUEDEnum, tesTask.State);
            Assert.AreEqual(200, result.StatusCode);
        }

        [TestMethod]
        public async Task CreateTaskAsync_CromwellJobIdIsUsedAsTaskIdPrefix()
        {
            var mockRepo = new Mock<IRepository<TesTask>>();
            var cromwellJobId = Guid.NewGuid().ToString();
            var taskDescription = $"{cromwellJobId}:BackendJobDescriptorKey_CommandCallNode_wf_hello.hello:-1:1";
            var tesTask = new TesTask() { Description = taskDescription, Executors = new List<TesExecutor> { new TesExecutor { Image = "ubuntu" } } };
            var controller = new TaskServiceApiController(mockRepo.Object, new NullLogger<TaskServiceApiController>());

            await controller.CreateTaskAsync(tesTask);

            Assert.AreEqual(41, tesTask.Id.Length); // First eight characters of Cromwell's job id + underscore + GUID without dashes
            Assert.IsTrue(tesTask.Id.StartsWith(cromwellJobId.Substring(0, 8) + "_"));
        }

        [TestMethod]
        public async Task CancelTaskAsync_ReturnsBadRequest_ForInvalidId()
        {
            // Arrange
            var mockRepo = new Mock<IRepository<TesTask>>();
            var tesTaskId = "IdDoesNotExist";

            mockRepo.Setup(repo => repo.GetItemAsync(tesTaskId)).ReturnsAsync((RepositoryItem<TesTask>)null);
            var controller = new TaskServiceApiController(mockRepo.Object, new NullLogger<TaskServiceApiController>());

            // Act
            var result = await controller.CancelTask(tesTaskId) as BadRequestObjectResult;

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(400, result.StatusCode);
        }

        [TestMethod]
        public async Task CancelTaskAsync_ReturnsEmptyObject()
        {
            // Arrange
            var tesTask = new TesTask() { Id = "testTaskId", State = TesState.QUEUEDEnum };
            var repositoryItem = new RepositoryItem<TesTask> { ETag = Guid.NewGuid().ToString(), Value = tesTask };

            var mockRepo = new Mock<IRepository<TesTask>>();
            mockRepo.Setup(repo => repo.GetItemAsync(tesTask.Id)).ReturnsAsync(repositoryItem);
            var controller = new TaskServiceApiController(mockRepo.Object, new NullLogger<TaskServiceApiController>());

            // Act
            var result = await controller.CancelTask(tesTask.Id) as ObjectResult;

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(200, result.StatusCode);
            Assert.AreEqual(TesState.CANCELEDEnum, repositoryItem.Value.State);
            mockRepo.Verify(x => x.UpdateItemAsync(tesTask.Id, repositoryItem));
        }

        [TestMethod]
        public void GetServiceInfo_ReturnsInfo()
        {
            // Arrange
            var mockRepo = new Mock<IRepository<TesTask>>();
            var controller = new TaskServiceApiController(mockRepo.Object, new NullLogger<TaskServiceApiController>());

            // Act
            var result = controller.GetServiceInfo() as ObjectResult;

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(200, result.StatusCode);
        }

        [TestMethod]
        public async Task GetTaskAsync_ReturnsNotFound_ForInvalidId()
        {
            // Arrange
            var mockRepo = new Mock<IRepository<TesTask>>();
            var tesTaskId = "IdDoesNotExist";

            mockRepo.Setup(repo => repo.GetItemAsync(tesTaskId)).ReturnsAsync((RepositoryItem<TesTask>)null);
            var controller = new TaskServiceApiController(mockRepo.Object, new NullLogger<TaskServiceApiController>());

            // Act
            var result = await controller.GetTaskAsync(tesTaskId, "MINIMAL") as NotFoundObjectResult;

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(404, result.StatusCode);
        }

        [TestMethod]
        public async Task GetTaskAsync_ReturnsBadRequest_ForInvalidViewValue()
        {
            // Arrange
            var mockRepo = new Mock<IRepository<TesTask>>();
            var tesTask = new TesTask();

            var repositoryItem = new RepositoryItem<TesTask> { ETag = Guid.NewGuid().ToString(), Value = tesTask };
            mockRepo.Setup(repo => repo.GetItemAsync(tesTask.Id)).ReturnsAsync(repositoryItem);
            var controller = new TaskServiceApiController(mockRepo.Object, new NullLogger<TaskServiceApiController>());

            // Act
            var result = await controller.GetTaskAsync(tesTask.Id, "INVALID") as BadRequestObjectResult;

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(400, result.StatusCode);
        }

        [TestMethod]
        public async Task GetTaskAsync_ReturnsJsonResult()
        {
            // Arrange
            var mockRepo = new Mock<IRepository<TesTask>>();
            var tesTask = new TesTask
            {
                State = TesState.RUNNINGEnum
            };

            var repositoryItem = new RepositoryItem<TesTask> { ETag = Guid.NewGuid().ToString(), Value = tesTask };
            mockRepo.Setup(repo => repo.GetItemAsync(tesTask.Id)).ReturnsAsync(repositoryItem);
            var controller = new TaskServiceApiController(mockRepo.Object, new NullLogger<TaskServiceApiController>());

            // Act
            var result = await controller.GetTaskAsync(tesTask.Id, "MINIMAL") as JsonResult;

            // Assert
            Assert.IsNotNull(result);
            mockRepo.Verify(x => x.GetItemAsync(tesTask.Id));
            Assert.AreEqual(TesState.RUNNINGEnum, tesTask.State);
            Assert.AreEqual(200, result.StatusCode);
        }

        [TestMethod]
        public async Task ListTasks_ReturnsBadRequest_ForInvalidPageSize()
        {
            // Arrange
            var controller = new TaskServiceApiController(new Mock<IRepository<TesTask>>().Object, new NullLogger<TaskServiceApiController>());

            // Act
            var result = await controller.ListTasks(null, 0, null, "BASIC") as BadRequestObjectResult;

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(400, result.StatusCode);
        }

        [TestMethod]
        public async Task ListTasks_ReturnsJsonResult()
        {
            // Arrange
            var mockRepo = new Mock<IRepository<TesTask>>();
            var firstTesTask = new TesTask { Id = "tesTaskId1", State = TesState.COMPLETEEnum, Name = "tesTask" };
            var secondTesTask = new TesTask { Id = "tesTaskId2", State = TesState.EXECUTORERROREnum, Name = "tesTask2" };
            var thirdTesTask = new TesTask { Id = "tesTaskId3", State = TesState.EXECUTORERROREnum, Name = "someOtherTask2" };
            var namePrefix = "tesTask";

            var repositoryItem1 = new RepositoryItem<TesTask> { ETag = Guid.NewGuid().ToString(), Value = firstTesTask };
            var repositoryItem2 = new RepositoryItem<TesTask> { ETag = Guid.NewGuid().ToString(), Value = secondTesTask };
            var repositoryItem3 = new RepositoryItem<TesTask> { ETag = Guid.NewGuid().ToString(), Value = thirdTesTask };
            var repositoryItems = new[] { repositoryItem1, repositoryItem2, repositoryItem3 };

            mockRepo.Setup(repo => repo
                .GetItemsAsync(It.IsAny<Expression<Func<TesTask, bool>>>(), It.IsAny<int>(), It.IsAny<string>()))
                .ReturnsAsync((Expression<Func<TesTask, bool>> predicate, int pageSize, string continuationToken) =>
                    ("", repositoryItems.Where(i => predicate.Compile().Invoke(i.Value)).Take(pageSize)));

            var controller = new TaskServiceApiController(mockRepo.Object, new NullLogger<TaskServiceApiController>());

            // Act
            var result = await controller.ListTasks(namePrefix, 1, null, "BASIC") as JsonResult;
            var listOfTesTasks = (TesListTasksResponse)result.Value;

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1, listOfTesTasks.Tasks.Count);
            Assert.AreEqual(200, result.StatusCode);
        }
    }
}
