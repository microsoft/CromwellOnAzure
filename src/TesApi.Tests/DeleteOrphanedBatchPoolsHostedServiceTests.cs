// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using TesApi.Web;

namespace TesApi.Tests
{
    [TestClass]
    public class DeleteOrphanedBatchPoolsHostedServiceTests
    {
        [TestMethod]
        public async Task DeleteOrphanedAutoPoolsServiceOnlyDeletesPoolsNotReferencedByJobs()
        {
            var activePoolIds = new List<string> { "1", "2", "3" };
            var poolIdsReferencedByJobs = new List<string> { "3", "4" };

            using var services = new TestServices.TestServiceProvider<DeleteOrphanedAutoPoolsHostedService>(
                configuration: Enumerable.Repeat(("BatchAutopool", true.ToString()), 1),
                azureProxy: a =>
                {
                    a.Setup(p => p.GetActivePoolIdsAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>())).ReturnsAsync(activePoolIds);
                    a.Setup(p => p.GetPoolIdsReferencedByJobsAsync(It.IsAny<CancellationToken>())).ReturnsAsync(poolIdsReferencedByJobs);
                });

            var deleteCompletedBatchJobsHostedService = services.GetT();

            await deleteCompletedBatchJobsHostedService.StartAsync(default);

            services.AzureProxy.Verify(i => i.GetActivePoolIdsAsync("TES_", TimeSpan.FromMinutes(30), It.IsAny<CancellationToken>()));
            services.AzureProxy.Verify(i => i.GetPoolIdsReferencedByJobsAsync(It.IsAny<CancellationToken>()));
            services.AzureProxy.Verify(i => i.DeleteBatchPoolAsync("1", It.IsAny<CancellationToken>()), Times.Once);
            services.AzureProxy.Verify(i => i.DeleteBatchPoolAsync("2", It.IsAny<CancellationToken>()), Times.Once);
            services.AzureProxy.VerifyNoOtherCalls();
        }
    }
}
