// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Azure.Identity;
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
            var blobServiceClient = new BlobServiceClient(new Uri($"https://{testStorageAccountName}.blob.core.windows.net/"), new AzureCliCredential());
            var wdlBlobName = $"globtest.wdl";
            var wdlPath = Path.Combine(Path.GetFullPath(@"..\..\..\test-wdls\globtest"), wdlBlobName);
            var container = blobServiceClient.GetBlobContainerClient(containerName);
            var text = (await File.ReadAllTextAsync(wdlPath)).Replace(@"\r\n\", @"\n");
            await container.GetBlobClient("test/" + wdlBlobName).UploadAsync(BinaryData.FromString(text), true);

            var wdlInputsBlobName = $"globtestinputs.json";
            var wdlInputsPath = Path.Combine(Path.GetFullPath(@"..\..\..\test-wdls\globtest"), wdlInputsBlobName);
            text = (await File.ReadAllTextAsync(wdlInputsPath)).Replace(@"\r\n\", @"\n");
            await container.GetBlobClient("test/" + wdlInputsBlobName).UploadAsync(BinaryData.FromString(text), true);

            var workflowTrigger = new Workflow
            {
                WorkflowUrl = blobServiceClient.Uri + "/" + containerName + "/test/" + wdlBlobName,
                WorkflowInputsUrl = blobServiceClient.Uri + "/" + containerName + "/test/" + wdlInputsBlobName
            };

            var n = DateTime.UtcNow;
            var date = $"{n.Year}-{n.Month}-{n.Day}-{n.Hour}-{n.Minute}";
            var triggerFileBlobName = $"new/globtest-{date}.json";
            string triggerJson = System.Text.Json.JsonSerializer.Serialize(workflowTrigger).Replace(@"\r\n\", @"\n");
            container = blobServiceClient.GetBlobContainerClient("workflows");
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

            await StartWorkflowsAsync(countOfWorkflowsToRun, triggerFile, workflowFriendlyName, testStorageAccountName);
        }

        /// <summary>
        /// To run this test, specify a testStorageAccountName, a workflowsContainerSasToken, and remove the [Ignore] attribute
        /// </summary>
        /// <returns></returns>
        [TestCategory("Integration")]
        [TestMethod]
        public async Task RunScaleTestWithMutect2WaitTilDoneAsync()
        {
            // This is set in the Azure Devops pipeline, which writes the file to the .csproj directory
            // The current working directory is this: /mnt/vss/_work/r1/a/CoaArtifacts/AllSource/TriggerService.Tests/bin/Debug/net7.0/
            // And the file is available here: /mnt/vss/_work/r1/a/CoaArtifacts/AllSource/TriggerService.Tests/temp_storage_account_name.txt
            const string storageAccountNamePath = "../../../temp_storage_account_name.txt";
            var path = storageAccountNamePath;

            if (!File.Exists(path))
            {
                Console.WriteLine($"Path not found - exiting integration test: {path}");
                return;
            }

            Console.WriteLine($"Found path: {path}");
            var lines = await File.ReadAllLinesAsync(path);
            string storageAccountName = lines[0].Trim();
            string workflowsContainerSasToken = lines[1].Trim('"');

            const int countOfWorkflowsToRun = 1;
            const string triggerFile = "https://raw.githubusercontent.com/microsoft/gatk4-somatic-snvs-indels-azure/main-azure/mutect2.trigger.json";
            const string workflowFriendlyName = $"mutect2";

            await StartWorkflowsAsync(countOfWorkflowsToRun, triggerFile, workflowFriendlyName, storageAccountName, waitTilDone: true, workflowsContainerSasToken);
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

            await StartWorkflowsAsync(countOfWorkflowsToRun, triggerFile, workflowFriendlyName, testStorageAccountName);
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
            var blobServiceClient = new BlobServiceClient(new Uri($"https://{testStorageAccountName}.blob.core.windows.net/"), new AzureCliCredential());
            var container = blobServiceClient.GetBlobContainerClient(containerName);
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

        private static async Task StartWorkflowsAsync(
            int countOfWorkflowsToRun,
            string triggerFile,
            string workflowFriendlyName,
            string storageAccountName,
            bool waitTilDone = false,
            string workflowsContainerSasToken = null)
        {
            var startTime = DateTime.UtcNow;
            const string containerName = "workflows";

            BlobServiceClient blobServiceClient;

            if (string.IsNullOrEmpty(workflowsContainerSasToken))
            {
                Console.WriteLine("No container SAS token specified; using AzureCliCredential");
                blobServiceClient = new BlobServiceClient(new Uri($"https://{storageAccountName}.blob.core.windows.net/"), new AzureCliCredential());
            }
            else
            { 
                blobServiceClient = new BlobServiceClient(new Uri($"https://{storageAccountName}.blob.core.windows.net/"));
            }

            var container = blobServiceClient.GetBlobContainerClient(containerName);

            if (!string.IsNullOrEmpty(workflowsContainerSasToken))
            {
                Console.WriteLine("Using the specified container SAS token.");
                var containerSasUri = new Uri($"https://{storageAccountName}.blob.core.windows.net/{containerName}?{workflowsContainerSasToken}");
                container = new BlobContainerClient(containerSasUri);
            }

            // 1.  Get the publically available trigger file
            using var httpClient = new HttpClient();
            var triggerFileJson = await (await httpClient.GetAsync(triggerFile)).Content.ReadAsStringAsync();

            // 2.  Start the workflows by uploading new trigger files
            var blobNames = new List<string>();
            var date = $"{startTime.Year}-{startTime.Month}-{startTime.Day}-{startTime.Hour}-{startTime.Minute}";

            for (var i = 1; i <= countOfWorkflowsToRun; i++)
            {
                // example: new/mutect2-001-of-100-2023-4-7-3-9.json
                var blobName = $"new/{workflowFriendlyName}-{i:D4}-of-{countOfWorkflowsToRun:D4}-{date}.json";
                blobNames.Add(blobName);
                await container.GetBlobClient(blobName).UploadAsync(BinaryData.FromString(triggerFileJson), true);
            }

            // 3.  Loop forever until they are all in a terminal state (succeeded or failed)
            if (waitTilDone)
            {
                await WaitTilAllWorkflowsInTerminalStateAsync(countOfWorkflowsToRun, startTime, container, blobNames);
            }
        }

        private static async Task WaitTilAllWorkflowsInTerminalStateAsync(int countOfWorkflowsToRun, DateTime startTime, BlobContainerClient container, List<string> originalBlobNames)
        {
            int succeededCount = 0;
            int failedCount = 0;

            while (succeededCount + failedCount < countOfWorkflowsToRun)
            {
                try
                {
                    var enumerator = container.GetBlobsAsync().GetAsyncEnumerator();
                    var existingBlobNames = new List<string>();

                    while (await enumerator.MoveNextAsync())
                    {
                        // example: inprogress/mutect2-001-of-100-2023-4-7-3-9.0fb0858a-3166-4a22-85b6-4337df2f53c5.json
                        var blobName = enumerator.Current.Name;
                        existingBlobNames.Add(blobName);
                    }

                    succeededCount = existingBlobNames
                        .Count(existingBlobName => originalBlobNames.Any(b => b
                            .Equals(existingBlobName.Replace("succeeded/", "new/"), StringComparison.OrdinalIgnoreCase)));

                    failedCount = existingBlobNames
                        .Count(name => originalBlobNames.Any(b => b
                            .Equals(name.Replace("failed/", "new/"), StringComparison.OrdinalIgnoreCase))));

                    var elapsed = DateTime.UtcNow - startTime;
                    Console.WriteLine($"[{elapsed.TotalMinutes:n0}m] Succeeded count: {succeededCount}");
                    Console.WriteLine($"[{elapsed.TotalMinutes:n0}m] Failed count: {failedCount}");
                }
                catch (Exception exc)
                {
                    Console.WriteLine(exc);
                }

                if (succeededCount + failedCount >= countOfWorkflowsToRun)
                {
                    break;
                }

                await Task.Delay(TimeSpan.FromMinutes(1));
            }

            Console.WriteLine($"Completed in {(DateTime.UtcNow - startTime).TotalHours:n1} hours");
            Console.WriteLine($"Succeeded count: {succeededCount}");
            Console.WriteLine($"Failed count: {failedCount}");

            Assert.IsTrue(failedCount == 0);
        }
    }
}
