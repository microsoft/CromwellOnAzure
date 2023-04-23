// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Common;
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
        public async Task RunGlobTestWdlAsync()
        {
            const string containerName = "inputs";
            var wdlBlobName = $"globtest.wdl";
            var wdlPath = Path.Combine(Path.GetFullPath(@"..\..\..\test-wdls\globtest"), wdlBlobName);
            string wdlUrl = $"https://{testStorageAccountName}.blob.core.windows.net/{containerName}/test/{wdlBlobName}?{workflowsContainerSasToken.TrimStart('?')}";
            var blobClient = new BlobServiceClient(new Uri(wdlUrl));
            var container = blobClient.GetBlobContainerClient(containerName);
            var text = (await File.ReadAllTextAsync(wdlPath)).Replace(@"\r\n\", @"\n");
            await container.GetBlobClient("test/" + wdlBlobName).UploadAsync(BinaryData.FromString(text), true);

            var wdlInputsBlobName = $"globtestinputs.json";
            var wdlInputsPath = Path.Combine(Path.GetFullPath(@"..\..\..\test-wdls\globtest"), wdlInputsBlobName);
            string wdlInputsUrl = $"https://{testStorageAccountName}.blob.core.windows.net/{containerName}/test/{wdlInputsBlobName}?{workflowsContainerSasToken.TrimStart('?')}";
            blobClient = new BlobServiceClient(new Uri(wdlInputsUrl));
            container = blobClient.GetBlobContainerClient(containerName);
            text = (await File.ReadAllTextAsync(wdlInputsPath)).Replace(@"\r\n\", @"\n");
            await container.GetBlobClient("test/" + wdlInputsBlobName).UploadAsync(BinaryData.FromString(text), true);

            var workflowTrigger = new Workflow
            {
                WorkflowUrl = wdlUrl,
                WorkflowInputsUrl = wdlInputsUrl
            };

            var triggerFileBlobName = $"new/globtesttrigger.json";
            string triggerJson = System.Text.Json.JsonSerializer.Serialize(workflowTrigger).Replace(@"\r\n\", @"\n");
            var triggerUrl = $"https://{testStorageAccountName}.blob.core.windows.net?{workflowsContainerSasToken.TrimStart('?')}";
            blobClient = new BlobServiceClient(new Uri(triggerUrl));
            container = blobClient.GetBlobContainerClient("workflows");
            await container.GetBlobClient(triggerFileBlobName).UploadAsync(BinaryData.FromString(triggerJson), true);
        }

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

        /// <summary>
        /// To run this test, specify a testStorageAccountName, a workflowsContainerSasToken, and remove the [Ignore] attribute
        /// </summary>
        /// <returns></returns>
        [Ignore]
        [TestCategory("Integration")]
        [TestMethod]
        public async Task CancelAllRunningWorkflowsAsync()
        {
            const string containerName = "workflows";

            var blobClient = new BlobServiceClient(new Uri($"https://{testStorageAccountName}.blob.core.windows.net?{workflowsContainerSasToken.TrimStart('?')}"));
            var container = blobClient.GetBlobContainerClient(containerName);
            var enumerator = container.GetBlobsAsync(prefix: "inprogress/").GetAsyncEnumerator();

            while (await enumerator.MoveNextAsync())
            {
                // example: inprogress/mutect2-001-of-100-2023-4-7-3-9.0fb0858a-3166-4a22-85b6-4337df2f53c5.json
                var blobName = enumerator.Current.Name;
                await container.GetBlobClient($"abort/{System.IO.Path.GetFileName(blobName)}").UploadAsync(BinaryData.FromString(string.Empty), true);
            }
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
                // example: new/mutect2-001-of-100-2023-4-7-3-9.json
                var blobName = $"new/{workflowFriendlyName}-{i:D4}-of-{countOfWorkflowsToRun:D4}-{date}.json";
                var blobClient = new BlobServiceClient(new Uri($"https://{testStorageAccountName}.blob.core.windows.net/{containerName}/{blobName}?{workflowsContainerSasToken.TrimStart('?')}"));
                var container = blobClient.GetBlobContainerClient(containerName);
                await container.GetBlobClient(blobName).UploadAsync(BinaryData.FromString(triggerFileJson), true);
            }
        }
    }
}
