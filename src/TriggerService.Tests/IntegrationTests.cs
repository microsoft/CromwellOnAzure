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
        [TestMethod]
        public async Task RunScaleTestWithMutect2Async()
        {
            const string testStorageAccountName = "";
            const string workflowsContainerSasToken = "";

            const string workflowFriendlyName = "mutect2-v5";
            const string triggerFile = "https://raw.githubusercontent.com/microsoft/gatk4-somatic-snvs-indels-azure/main-azure/mutect2.trigger.json";
            const string containerName = "workflows";
            using var client = new HttpClient();
            var response = await client.GetAsync(triggerFile);
            var content = await response.Content.ReadAsStringAsync();

            for (int i = 0; i < 1; i++)
            {
                string blobName = $"new/{workflowFriendlyName}-{i}.json";
                var blobClient = new BlobServiceClient(new Uri($"https://{testStorageAccountName}.blob.core.windows.net/{containerName}/{blobName}?{workflowsContainerSasToken}"));
                var container = blobClient.GetBlobContainerClient(containerName);
                await container.GetBlobClient(blobName).UploadAsync(BinaryData.FromString(content), true);
            }
        }
    }
}
