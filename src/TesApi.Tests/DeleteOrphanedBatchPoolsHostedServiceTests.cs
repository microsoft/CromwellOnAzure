using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
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

            var azureProxy = new Mock<IAzureProxy>();
            azureProxy.Setup(p => p.GetActivePoolIdsAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>())).ReturnsAsync(activePoolIds);
            azureProxy.Setup(p => p.GetPoolIdsReferencedByJobsAsync(It.IsAny<CancellationToken>())).ReturnsAsync(poolIdsReferencedByJobs);

            var deleteCompletedBatchJobsHostedService = new DeleteOrphanedAutoPoolsHostedService(azureProxy.Object, new NullLogger<DeleteOrphanedAutoPoolsHostedService>());

            await deleteCompletedBatchJobsHostedService.StartAsync(default);

            azureProxy.Verify(i => i.GetActivePoolIdsAsync("TES_", TimeSpan.FromMinutes(30), It.IsAny<CancellationToken>()));
            azureProxy.Verify(i => i.GetPoolIdsReferencedByJobsAsync(It.IsAny<CancellationToken>()));
            azureProxy.Verify(i => i.DeleteBatchPoolAsync("1", It.IsAny<CancellationToken>()), Times.Once);
            azureProxy.Verify(i => i.DeleteBatchPoolAsync("2", It.IsAny<CancellationToken>()), Times.Once);
            azureProxy.VerifyNoOtherCalls();
        }
    }
}
