using System;
using System.Net.Http;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace TriggerService.Tests
{
    [TestClass]
    public class IntegrationTests
    {
        /// <summary>
        /// To run this test, specify a testStorageAccountName, a workflowsContainerSasToken, and remove the [Ignore] attribute
        /// </summary>
        /// <returns></returns>
        [Ignore]
        [TestCategory("Integration")]
        [TestMethod]
        public async Task RunScaleTestWithMutect2Async()
        {
            const string testStorageAccountName = "";
            const string workflowsContainerSasToken = "";
            const int countOfWorkflowsToRun = 100;
            const string triggerFile = "https://raw.githubusercontent.com/microsoft/gatk4-somatic-snvs-indels-azure/main-azure/mutect2.trigger.json";
            const string containerName = "workflows";

            var n = DateTime.UtcNow;
            var workflowFriendlyName = $"mutect2-{n.Year}-{n.Month}-{n.Day}-{n.Hour}-{n.Minute}";
            using var client = new HttpClient();
            var response = await client.GetAsync(triggerFile);
            var content = await response.Content.ReadAsStringAsync();

            for (var i = 1; i <= countOfWorkflowsToRun; i++)
            {
                var blobName = $"new/{workflowFriendlyName}-{i}-of-{countOfWorkflowsToRun}.json";
                var blobClient = new BlobServiceClient(new Uri($"https://{testStorageAccountName}.blob.core.windows.net/{containerName}/{blobName}?{workflowsContainerSasToken.TrimStart('?')}"));
                var container = blobClient.GetBlobContainerClient(containerName);
                await container.GetBlobClient(blobName).UploadAsync(BinaryData.FromString(content), true);
            }
        }
    }
}
