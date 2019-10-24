using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using TesApi.Models;
using TesApi.Web;

namespace TesApi.Tests
{
    [TestClass]
    public class DeleteCompletedBatchJobsHostedServiceTests
    {
        private static readonly TimeSpan oldestJobAge = TimeSpan.FromDays(7);

        private Mock<IAzureProxy> azureProxy;
        private Mock<IRepository<TesTask>> mockRepo;
        private DeleteCompletedBatchJobsHostedService deleteCompletedBatchJobsHostedService;

        [TestInitialize]
        public void InitializeTests()
        {
            var mockConfig = new Mock<IConfiguration>();
            azureProxy = new Mock<IAzureProxy>();
            mockRepo = new Mock<IRepository<TesTask>>();
            deleteCompletedBatchJobsHostedService = new DeleteCompletedBatchJobsHostedService(
                mockConfig.Object,
                azureProxy.Object,
                mockRepo.Object,
                new NullLogger<DeleteCompletedBatchJobsHostedService>());
        }

        [TestMethod]
        public async Task DeleteCompletedBatchJobs_DeletesJobs_TesStateCompleted()
        {
            // Arrange & Act
            var firstTesTask = new TesTask { Id = "tesTaskId1", State = TesState.COMPLETEEnum };
            var secondTesTask = new TesTask { Id = "tesTaskId2", State = TesState.RUNNINGEnum };
            var thirdTesTask = new TesTask { Id = "tesTaskId3", State = TesState.INITIALIZINGEnum };
            var azureProxy = await ArrangeTest(new[] { firstTesTask, secondTesTask, thirdTesTask });

            // Assert
            azureProxy.Verify(i => i.ListOldJobsToDeleteAsync(oldestJobAge));
            azureProxy.Verify(i => i.DeleteBatchJobAsync("tesTaskId1"));
            azureProxy.VerifyNoOtherCalls();
        }

        [TestMethod]
        public async Task DeleteCompletedBatchJobs_DeletesJobs_TesStateError()
        {
            // Arrange & Act
            var firstTesTask = new TesTask { Id = "tesTaskId1", State = TesState.SYSTEMERROREnum };
            var secondTesTask = new TesTask { Id = "tesTaskId2", State = TesState.EXECUTORERROREnum };
            var thirdTesTask = new TesTask { Id = "tesTaskId3", State = TesState.PAUSEDEnum };
            var azureProxy = await ArrangeTest(new[] { firstTesTask, secondTesTask, thirdTesTask });

            // Assert
            azureProxy.Verify(i => i.ListOldJobsToDeleteAsync(oldestJobAge));
            azureProxy.Verify(i => i.DeleteBatchJobAsync("tesTaskId1"));
            azureProxy.Verify(i => i.DeleteBatchJobAsync("tesTaskId2"));
            azureProxy.VerifyNoOtherCalls();
        }

        [TestMethod]
        public async Task DeleteCompletedBatchJobs_DeletesJobs_TesStateCanceled()
        {
            // Arrange & Act
            var firstTesTask = new TesTask { Id = "tesTaskId1", State = TesState.CANCELEDEnum };
            var secondTesTask = new TesTask { Id = "tesTaskId2", State = TesState.QUEUEDEnum };
            var thirdTesTask = new TesTask { Id = "tesTaskId3", State = TesState.INITIALIZINGEnum };
            var azureProxy = await ArrangeTest(new[] { firstTesTask, secondTesTask, thirdTesTask });

            // Assert
            azureProxy.Verify(i => i.ListOldJobsToDeleteAsync(oldestJobAge));
            azureProxy.Verify(i => i.DeleteBatchJobAsync("tesTaskId1"));
            azureProxy.VerifyNoOtherCalls();
        }

        [TestMethod]
        public async Task DeleteCompletedBatchJobs_DeletesJobs_TesStateUnknown()
        {
            // Arrange & Act
            var firstTesTask = new TesTask { Id = "tesTaskId1", State = TesState.UNKNOWNEnum };
            var secondTesTask = new TesTask { Id = "tesTaskId2", State = TesState.PAUSEDEnum };
            var thirdTesTask = new TesTask { Id = "tesTaskId3", State = TesState.RUNNINGEnum };
            var azureProxy = await ArrangeTest(new[] { firstTesTask, secondTesTask, thirdTesTask });

            // Assert
            azureProxy.Verify(i => i.ListOldJobsToDeleteAsync(oldestJobAge));
            azureProxy.Verify(i => i.DeleteBatchJobAsync("tesTaskId1"));
            azureProxy.VerifyNoOtherCalls();
        }

        private async Task<Mock<IAzureProxy>> ArrangeTest(TesTask[] tasks)
        {
            // Arrange
            var repositoryItems = tasks.Select(
                t => new RepositoryItem<TesTask> { ETag = Guid.NewGuid().ToString(), Value = t });
            mockRepo.Setup(r => r.GetItemAsync(It.IsAny<string>()))
                    .ReturnsAsync((string id) => repositoryItems.Single(i => i.Value.Id == id));
            azureProxy.Setup(p => p.ListOldJobsToDeleteAsync(oldestJobAge))
                    .ReturnsAsync(repositoryItems.Select(i => i.Value.Id + "-1"));

            // Act
            await deleteCompletedBatchJobsHostedService.StartAsync(new System.Threading.CancellationToken());
            return azureProxy;
        }
    }
}
