// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Net.Http;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace TriggerService.Tests
{
    [TestClass]
    public class IntegrationTests
    {
        private const string testStorageAccountName = "";
        private const string workflowsContainerSasToken = "";

        /// <summary>
        /// To run this test, specify a testStorageAccountName, a workflowsContainerSasToken, and remove the [Ignore] attribute
        /// </summary>
        /// <returns></returns>
        [Ignore]
        [TestCategory("Integration")]
        [TestMethod]
        public async Task RunScaleTestWithMutect2Async()
        {
            const int countOfWorkflowsToRun = 100;
            const string triggerFile = "https://raw.githubusercontent.com/microsoft/gatk4-somatic-snvs-indels-azure/main-azure/mutect2.trigger.json";
            const string workflowFriendlyName = $"mutect2";

            await StartWorkflowsAsync(countOfWorkflowsToRun, triggerFile, workflowFriendlyName);
        }

        /// <summary>
        /// To run this test, specify a testStorageAccountName, a workflowsContainerSasToken, and remove the [Ignore] attribute
        /// </summary>
        /// <returns></returns>
        [Ignore]
        [TestCategory("Integration")]
        [TestMethod]
        public async Task RunScaleTestWithWholeGenomeGermlineSingleSampleAsync()
        {
            const int countOfWorkflowsToRun = 1500;
            const string triggerFile = "https://raw.githubusercontent.com/microsoft/gatk4-genome-processing-pipeline-azure/main-azure/WholeGenomeGermlineSingleSample.trigger.json";
            const string workflowFriendlyName = $"wgs-germline";

            await StartWorkflowsAsync(countOfWorkflowsToRun, triggerFile, workflowFriendlyName);
        }

        [Ignore]
        [TestCategory("Integration")]
        [TestMethod]
        public async Task DeleteOldBatchPoolsAsync()
        {
            const string accountName = "";
            const string accountKey = "";
            const string batchUrl = "";
            var maxAge = TimeSpan.FromHours(2);

            var credentials = new BatchSharedKeyCredentials(batchUrl, accountName, accountKey);
            using var batchClient = BatchClient.Open(credentials);
            var cutoffTime = DateTime.UtcNow.Subtract(maxAge);
            var pools = await batchClient.PoolOperations.ListPools().ToListAsync();

            int count = 0;

            foreach (var pool in pools)
            {
                if (pool.CreationTime < cutoffTime
                    && pool.CurrentLowPriorityComputeNodes == 0
                    && pool.TargetLowPriorityComputeNodes == 0
                    && pool.CurrentDedicatedComputeNodes == 0
                    && pool.TargetDedicatedComputeNodes == 0)
                {
                    Console.WriteLine($"Deleting Batch pool {pool.Id}...");
                    await batchClient.PoolOperations.DeletePoolAsync(pool.Id);
                    count++;
                }
            }

            Console.WriteLine($"Deleted {count} pools.");
        }

        private static async Task StartWorkflowsAsync(int countOfWorkflowsToRun, string triggerFile, string workflowFriendlyName)
        {
            const string containerName = "workflows";
            var n = DateTime.UtcNow;
            var date = $"{n.Year}-{n.Month}-{n.Day}-{n.Hour}-{n.Minute}";
            using var httpClient = new HttpClient();
            var triggerFileJson = await (await httpClient.GetAsync(triggerFile)).Content.ReadAsStringAsync();

            for (var i = 1; i <= countOfWorkflowsToRun; i++)
            {
                // example: new/mutect2-001-of-100-2023-4-7-3-9.0fb0858a-3166-4a22-85b6-4337df2f53c5.json
                var blobName = $"new/{workflowFriendlyName}-{i:D4}-of-{countOfWorkflowsToRun:D4}-{date}.json";
                var blobClient = new BlobServiceClient(new Uri($"https://{testStorageAccountName}.blob.core.windows.net/{containerName}/{blobName}?{workflowsContainerSasToken.TrimStart('?')}"));
                var container = blobClient.GetBlobContainerClient(containerName);
                await container.GetBlobClient(blobName).UploadAsync(BinaryData.FromString(triggerFileJson), true);
            }
        }
    }
}
