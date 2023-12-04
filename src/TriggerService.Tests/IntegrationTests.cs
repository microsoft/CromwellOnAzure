// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Diagnostics;
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
        /// To run this test, specify a testStorageAccountName, a workflowsContainerSasToken
        /// </summary>
        /// <returns></returns>
        [TestCategory("Integration")]
        [TestMethod]
        public async Task RunPrivateAcrTestWaitTilDoneAsync()
        {
            const string triggerFile = "https://raw.githubusercontent.com/microsoft/CromwellOnAzure/mattmcl4475/private-acr-test/src/TriggerService.Tests/test-wdls/privateacr/privateacr.trigger.json";
            const string workflowFriendlyName = $"privateacr";

            await RunIntegrationTestAsync(new List<(string triggerFileBlobUrl, string workflowFriendlyName)> { (triggerFile, workflowFriendlyName) });
        }

        /// <summary>
        /// To run this test, specify a testStorageAccountName, a workflowsContainerSasToken
        /// </summary>
        /// <returns></returns>
        [TestCategory("Integration")]
        [TestMethod]
        public async Task RunScaleTestWithMutect2WaitTilDoneAsync()
        {
            const string triggerFile = "https://raw.githubusercontent.com/microsoft/CromwellOnAzure/main/src/TriggerService.Tests/test-wdls/mutect2/mutect2.trigger.json";
            const string workflowFriendlyName = $"mutect2";

            await RunIntegrationTestAsync(new List<(string triggerFileBlobUrl, string workflowFriendlyName)> { (triggerFile, workflowFriendlyName) });
        }

        /// <summary>
        /// To run this test, specify a testStorageAccountName, a workflowsContainerSasToken
        /// </summary>
        /// <returns></returns>
        [TestCategory("Integration")]
        [TestMethod]
        public async Task RunAllCommonWorkflowsWaitTilDoneAsync()
        {
            var workflowTriggerFiles = new List<(string triggerFileBlobUrl, string workflowFriendlyName)> {
                ("https://raw.githubusercontent.com/microsoft/gatk4-data-processing-azure/main-azure/processing-for-variant-discovery-gatk4.b37.trigger.json", "preprocessing-b37"),
                ("https://raw.githubusercontent.com/microsoft/gatk4-data-processing-azure/main-azure/processing-for-variant-discovery-gatk4.hg38.trigger.json", "preprocessing-hg38"),
                ("https://raw.githubusercontent.com/microsoft/gatk4-genome-processing-pipeline-azure/main-azure/WholeGenomeGermlineSingleSample.trigger.json", "germline"),
                ("https://raw.githubusercontent.com/microsoft/gatk4-somatic-snvs-indels-azure/main-azure/mutect2.trigger.json", "mutect2"),
                ("https://raw.githubusercontent.com/microsoft/gatk4-rnaseq-germline-snps-indels-azure/jsaun/gatk4-rna-germline-variant-calling.trigger.json", "rna-germline"),
                ("https://raw.githubusercontent.com/microsoft/gatk4-cnn-variant-filter-azure/main-azure/cram2filtered.trigger.json", "cram-to-filtered"),
                ("https://raw.githubusercontent.com/microsoft/seq-format-conversion-azure/main-azure/interleaved-fastq-to-paired-fastq.trigger.json", "fastq-to-paired"),
                ("https://raw.githubusercontent.com/microsoft/seq-format-conversion-azure/main-azure/paired-fastq-to-unmapped-bam.trigger.json", "paired-fastq-to-unmapped-bam"),
                ("https://raw.githubusercontent.com/microsoft/seq-format-conversion-azure/main-azure/cram-to-bam.trigger.json", "cram-to-bam") };

            await RunIntegrationTestAsync(workflowTriggerFiles);
        }

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
            const string triggerFile = "https://raw.githubusercontent.com/microsoft/CromwellOnAzure/main/src/TriggerService.Tests/test-wdls/mutect2/mutect2.trigger.json";
            const string workflowFriendlyName = $"mutect2";
            var triggerFiles = new List<(string triggerFileBlobUrl, string workflowFriendlyName)> { (triggerFile, workflowFriendlyName) };
            await StartWorkflowsAsync(countOfWorkflowsToRun, triggerFiles, testStorageAccountName);
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

            var triggerFiles = new List<(string triggerFileBlobUrl, string workflowFriendlyName)> { (triggerFile, workflowFriendlyName) };
            await StartWorkflowsAsync(countOfWorkflowsToRun, triggerFiles, testStorageAccountName);
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
                    await batchClient.JobOperations.DeleteJobAsync(pool.Id);
                    await batchClient.PoolOperations.DeletePoolAsync(pool.Id);
                    count++;
                }
            }

            Console.WriteLine($"Deleted {count} pools.");
        }

        [TestMethod]
        public void CountCompletedWorkflowsTest()
        {
            var originalBlobNames = new List<string> {
                "new/mutect2-0001-of-0001-2023-7-19-2-49.json",
                "new/mutect2-0001-of-0001-2023-7-19-2-50.json"
            };

            var currentBlobNames = new List<string> {
                "failed/mutect2-0001-of-0001-2023-7-19-2-49.817f052c-81f8-45ff-863b-03f9655eee5c.json",
                "succeeded/mutect2-0001-of-0001-2023-7-19-2-50.817f052c-81f8-45ff-863b-03f9655eee5c.json"
            };

            Assert.IsTrue(CountWorkflowsByState(originalBlobNames, currentBlobNames, WorkflowState.Failed) == 1);
            Assert.IsTrue(CountWorkflowsByState(originalBlobNames, currentBlobNames, WorkflowState.Succeeded) == 1);
        }

        private async Task RunIntegrationTestAsync(List<(string triggerFileBlobUrl, string workflowFriendlyName)> triggerFiles)
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

            int countOfEachWorkflowToRun = 1;

            if (lines.Length > 2)
            {
                int.TryParse(lines[2].Trim('"'), out countOfEachWorkflowToRun);
            }

            await StartWorkflowsAsync(countOfEachWorkflowToRun, triggerFiles, storageAccountName, waitTilDone: true, workflowsContainerSasToken);
        }

        private async Task StartWorkflowsAsync(
            int countOfEachWorkflowToRun,
            List<(string triggerFileBlobUrl, string workflowFriendlyName)> triggerFiles,
            string storageAccountName,
            bool waitTilDone = false,
            string workflowsContainerSasToken = null)
        {
            var startTime = DateTime.UtcNow;
            const string workflowsContainerName = "workflows";

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

            var workflowsContainer = blobServiceClient.GetBlobContainerClient(workflowsContainerName);

            if (!string.IsNullOrEmpty(workflowsContainerSasToken))
            {
                Console.WriteLine("Using the specified container SAS token.");
                var containerSasUri = new Uri($"https://{storageAccountName}.blob.core.windows.net/{workflowsContainerName}?{workflowsContainerSasToken}");
                workflowsContainer = new BlobContainerClient(containerSasUri);
            }

            // 1.  Get the publically available trigger file
            using var httpClient = new HttpClient();
            var blobNames = new List<string>();

            foreach (var triggerFile in triggerFiles)
            {
                var triggerFileJson = await (await httpClient.GetAsync(triggerFile.triggerFileBlobUrl)).Content.ReadAsStringAsync();

                // 2.  Start the workflows by uploading new trigger files
                var date = $"{startTime.Year}-{startTime.Month}-{startTime.Day}-{startTime.Hour}-{startTime.Minute}";
                Console.WriteLine($"Starting {countOfEachWorkflowToRun} workflows...");

                for (var i = 1; i <= countOfEachWorkflowToRun; i++)
                {
                    // example: new/mutect2-001-of-100-2023-4-7-3-9.json
                    var blobName = $"new/{triggerFile.workflowFriendlyName}-{i:D4}-of-{countOfEachWorkflowToRun:D4}-{date}.json";
                    blobNames.Add(blobName);
                    await workflowsContainer.GetBlobClient(blobName).UploadAsync(BinaryData.FromString(triggerFileJson), true);
                }
            }

            // 3.  Loop forever until they are all in a terminal state (succeeded or failed)
            if (waitTilDone)
            {
                await WaitTilAllWorkflowsInTerminalStateAsync(workflowsContainer, blobNames, startTime);
            }
        }

        private async Task<List<string>> ListContainerBlobNamesAsync(BlobContainerClient container)
        {
            var enumerator = container.GetBlobsAsync().GetAsyncEnumerator();
            var existingBlobNames = new List<string>();

            while (await enumerator.MoveNextAsync())
            {
                // example: inprogress/mutect2-001-of-100-2023-4-7-3-9.0fb0858a-3166-4a22-85b6-4337df2f53c5.json
                var blobName = enumerator.Current.Name;
                existingBlobNames.Add(blobName);
            }

            return existingBlobNames;
        }

        private int CountWorkflowsByState(List<string> originalBlobNames, List<string> currentBlobNames, WorkflowState state)
        {
            return GetWorkflowsByState(originalBlobNames, currentBlobNames, state).Count();
        }

        private List<string> GetWorkflowsByState(List<string> originalBlobNames, List<string> currentBlobNames, WorkflowState state)
        {
            var stateString = state.ToString().ToLowerInvariant();

            return currentBlobNames
                .Where(currentBlob => originalBlobNames
                    .Any(originalBlob => currentBlob.StartsWith(
                        originalBlob
                        .Replace("new/", $"{stateString}/", StringComparison.OrdinalIgnoreCase)
                        .Replace(".json", "", StringComparison.OrdinalIgnoreCase), StringComparison.OrdinalIgnoreCase)))
                .ToList();
        }

        [TestMethod]
        public void GetWorkflowsByStateTest()
        {
            var originalBlobNames = new List<string> {
                "new/mutect2-0001-of-0001-2023-7-19-2-49.json",
                "new/mutect2-0001-of-0001-2023-7-19-2-50.json"
            };

            var currentBlobNames = new List<string> {
                "failed/mutect2-0001-of-0001-2023-7-19-2-49.817f052c-81f8-45ff-863b-03f9655eee5c.json",
                "succeeded/mutect2-0001-of-0001-2023-7-19-2-50.817f052c-81f8-45ff-863b-03f9655eee5c.json"
            };

            var failed = GetWorkflowsByState(originalBlobNames, currentBlobNames, WorkflowState.Failed);
            var succeeded = GetWorkflowsByState(originalBlobNames, currentBlobNames, WorkflowState.Succeeded);
            Assert.IsTrue(failed.Single() == currentBlobNames.First());
            Assert.IsTrue(succeeded.Single() == currentBlobNames.Skip(1).First());
        }

        private async Task WaitTilAllWorkflowsInTerminalStateAsync(BlobContainerClient container, List<string> originalBlobNames, DateTime startTime)
        {
            int succeededCount = 0;
            int failedCount = 0;

            var sw = Stopwatch.StartNew();

            while (succeededCount + failedCount < originalBlobNames.Count) // && sw.Elapsed.TotalHours < 5)
            {
                try
                {
                    var existingBlobNames = await ListContainerBlobNamesAsync(container);
                    succeededCount = CountWorkflowsByState(originalBlobNames, existingBlobNames, WorkflowState.Succeeded);
                    failedCount = CountWorkflowsByState(originalBlobNames, existingBlobNames, WorkflowState.Failed);
                    Console.WriteLine($"[{sw.Elapsed.TotalMinutes:n0}m] Succeeded count: {succeededCount}, Failed count: {failedCount}");
                }
                catch (Exception exc)
                {
                    Console.WriteLine(exc);
                }

                if (succeededCount + failedCount == originalBlobNames.Count)
                {
                    break;
                }

                await Task.Delay(TimeSpan.FromMinutes(1));
            }

            Console.WriteLine($"Completed in {(DateTime.UtcNow - startTime).TotalHours:n1} hours");
            Console.WriteLine($"Succeeded count: {succeededCount}");
            Console.WriteLine($"Failed count: {failedCount}");

            Assert.IsTrue(succeededCount + failedCount == originalBlobNames.Count);

            if (failedCount > 0)
            {
                Console.WriteLine("Failed workflow details:");

                var existingBlobNames = await ListContainerBlobNamesAsync(container);
                var failedWorkflowBlobNames = GetWorkflowsByState(originalBlobNames, existingBlobNames, WorkflowState.Failed);

                foreach (var workflowBlobName in failedWorkflowBlobNames)
                {
                    var content = (await container.GetBlobClient(workflowBlobName).DownloadContentAsync()).Value.Content.ToString();
                    Console.WriteLine($"Failed workflow blob name: {workflowBlobName}");
                    Console.WriteLine("Failed workflow triggerfile content:");
                    Console.WriteLine(content);
                }
            }

            Assert.IsTrue(failedCount == 0, $"{failedCount} workflows FAILED.");
        }
    }
}
