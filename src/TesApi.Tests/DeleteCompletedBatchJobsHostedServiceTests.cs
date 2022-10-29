// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

﻿using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Tes.Models;
using Tes.Repository;
using TesApi.Web;

namespace TesApi.Tests
{
    [TestClass]
    public class DeleteCompletedBatchJobsHostedServiceTests
    {
        private static readonly TimeSpan oldestJobAge = TimeSpan.FromDays(7);

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
            azureProxy.Verify(i => i.DeleteBatchJobAsync("tesTaskId1", It.IsAny<System.Threading.CancellationToken>()));
            azureProxy.Verify(i => i.DeleteBatchPoolIfExistsAsync("tesTaskId1", It.IsAny<System.Threading.CancellationToken>()));
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
            azureProxy.Verify(i => i.DeleteBatchJobAsync("tesTaskId1", It.IsAny<System.Threading.CancellationToken>()));
            azureProxy.Verify(i => i.DeleteBatchJobAsync("tesTaskId2", It.IsAny<System.Threading.CancellationToken>()));
            azureProxy.Verify(i => i.DeleteBatchPoolIfExistsAsync("tesTaskId1", It.IsAny<System.Threading.CancellationToken>()));
            azureProxy.Verify(i => i.DeleteBatchPoolIfExistsAsync("tesTaskId2", It.IsAny<System.Threading.CancellationToken>()));
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
            azureProxy.Verify(i => i.DeleteBatchJobAsync("tesTaskId1", It.IsAny<System.Threading.CancellationToken>()));
            azureProxy.Verify(i => i.DeleteBatchPoolIfExistsAsync("tesTaskId1", It.IsAny<System.Threading.CancellationToken>()));
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
            azureProxy.Verify(i => i.DeleteBatchJobAsync("tesTaskId1", It.IsAny<System.Threading.CancellationToken>()));
            azureProxy.Verify(i => i.DeleteBatchPoolIfExistsAsync("tesTaskId1", It.IsAny<System.Threading.CancellationToken>()));
            azureProxy.VerifyNoOtherCalls();
        }

        private async Task<Mock<IAzureProxy>> ArrangeTest(TesTask[] tasks)
        {
            void SetupRepository(Mock<IRepository<TesTask>> mockRepo)
            {
                foreach (var item in tasks)
                {
                    mockRepo.Setup(repo => repo.TryGetItemAsync(item.Id, It.IsAny<Action<TesTask>>()))
                        .Callback<string, Action<TesTask>>((id, action) =>
                        {
                            action(item);
                        })
                        .ReturnsAsync(true);
                }
            }

            using var serviceProvider = new TestServices.TestServiceProvider<DeleteCompletedBatchJobsHostedService>(
                configuration: Enumerable.Repeat(("UseLegacyBatchImplementationWithAutopools", true.ToString()), 1),
                azureProxy: a => a.Setup(p => p.ListOldJobsToDeleteAsync(oldestJobAge))
                    .ReturnsAsync(tasks.Select(i => i.Id + "-1")),
                tesTaskRepository: SetupRepository);

            await serviceProvider.GetT().StartAsync(new System.Threading.CancellationToken());
            return serviceProvider.AzureProxy;
        }
    }
}
