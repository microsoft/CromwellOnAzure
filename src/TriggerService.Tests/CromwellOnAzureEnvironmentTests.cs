// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using CromwellApiClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Newtonsoft.Json;
using Tes.Repository;
using Tes.Models;

namespace TriggerService.Tests
{
    [TestClass]
    public class CromwellOnAzureEnvironmentTests
    {
        private const string azureName = "test";
        private readonly byte[] blobData = new byte[3] { 1, 2, 3 };
        private readonly byte[] httpClientData = new byte[4] { 4, 3, 2, 1 };
        private readonly string fakeAzureWdl = $"https://fake.azure.storage.account/{azureName}/test.wdl";
        private readonly string fakeAzureInput = $"https://fake.azure.storage.account/{azureName}/test.input.json";
        private readonly List<string> fakeAzureInputs = new() {
            $"https://fake.azure.storage.account/{azureName}/test.input1.json",
            $"https://fake.azure.storage.account/{azureName}/test.input_2.json"
        };
        private readonly string fakeAzureWdlWithSas = @"https://fake.azure.storage.account/{azureName}/test.wdl?sp=r&st=2019-12-18T18:55:41Z&se=2019-12-19T02:55:41Z&spr=https&sv=2019-02-02&sr=b&sig=EMJyBMOxdG2NvBqiwUsg71ZdYqwqMWda9242KU43%2F5Y%3D";

        public CromwellOnAzureEnvironmentTests()
        {
            Common.NewtonsoftJsonSafeInit.SetDefaultSettings();
        }

        [TestMethod]
        public async Task GetBlobFileNameAndDataWithDefaultStorageAccountUsingUrl()
        {
            var accountAuthority = new Uri(fakeAzureWdl).Authority;

            var processedWorkflowItem = await GetBlobFileNameAndDataUsingMocksAsync(fakeAzureWdl, accountAuthority);

            Assert.AreEqual(azureName, processedWorkflowItem.Filename, "azureName compared with Filename");
            Assert.IsNotNull(processedWorkflowItem.Data, "data");
            Assert.AreEqual(blobData.Length, processedWorkflowItem.Data.Length, "unexpected length of Data");
            for (var i = 0; i < blobData.Length; i++)
            {
                Assert.AreEqual(blobData[i], processedWorkflowItem.Data[i], $"unexpected value of Data[{i}]");
            }
        }

        [TestMethod]
        public async Task GetBlobFileNameAndDataWithDefaultStorageAccountUsingLocalPath()
        {
            var accountAuthority = "fake";
            var url = $"/{accountAuthority}/test/test.wdl";

            var processedWorkflowItem = await GetBlobFileNameAndDataUsingMocksAsync(url, accountAuthority);

            Assert.AreEqual(azureName, processedWorkflowItem.Filename, "azureName compared with Filename");
            Assert.IsNotNull(processedWorkflowItem.Data, "data");
            Assert.AreEqual(blobData.Length, processedWorkflowItem.Data.Length, "unexpected length of Data");
            for (var i = 0; i < blobData.Length; i++)
            {
                Assert.AreEqual(blobData[i], processedWorkflowItem.Data[i], $"unexpected value of Data[{i}]");
            }
        }

        [TestMethod]
        public async Task GetBlobFileNameAndDataWithDifferentStorageAccountUsingLocalPath()
        {
            var defaultAuthority = "fake";
            var accountAuthority = "alternate";
            var url = $"/{accountAuthority}/test/test.wdl";

            var storages = Enumerable.Repeat(MockAzureStorage(defaultAuthority), 1).Append(MockAzureStorage(accountAuthority));

            var processedWorkflowItem  = await GetBlobFileNameAndDataUsingMocksAsync(url, defaultAuthority, storages);

            Assert.AreEqual(azureName, processedWorkflowItem.Filename, "azureName compared with Filename");
            Assert.IsNotNull(processedWorkflowItem.Data, "data");
            Assert.AreEqual(blobData.Length, processedWorkflowItem.Data.Length, "unexpected length of Data");
            for (var i = 0; i < blobData.Length; i++)
            {
                Assert.AreEqual(blobData[i], processedWorkflowItem.Data[i], $"unexpected value of Data[{i}]");
            }
        }

        [TestMethod]
        public async Task GetBlobFileNameAndDataWithDefaultStorageAccountWithSasToken()
        {
            var accountAuthority = new Uri(fakeAzureWdlWithSas).Authority;

            var processedWorkflowItem = await GetBlobFileNameAndDataUsingMocksAsync(fakeAzureWdlWithSas, accountAuthority);

            Assert.AreEqual(azureName, processedWorkflowItem.Filename, "azureName compared with Filename");
            Assert.IsNotNull(processedWorkflowItem.Data, "data");
            Assert.AreEqual(httpClientData.Length, processedWorkflowItem.Data.Length, "unexpected length of Data");
            for (var i = 0; i < httpClientData.Length; i++)
            {
                Assert.AreEqual(httpClientData[i], processedWorkflowItem.Data[i], $"unexpected value of Data[{i}]");
            }
        }

        private async Task<ProcessedWorkflowItem> GetBlobFileNameAndDataUsingMocksAsync(string url, string accountAuthority, IEnumerable<IAzureStorage> azureStorages = default)
            => await SetCromwellOnAzureEnvironment(accountAuthority, azureStorages).GetBlobFileNameAndData(url);

        private IAzureStorage MockAzureStorage(string accountAuthority)
        {
            var azStorageMock = new Mock<IAzureStorage>();


            azStorageMock.Setup(az => az
                .DownloadBlockBlobAsync(It.IsAny<string>()))
                .Returns(Task.FromResult(blobData));

            azStorageMock.Setup(az => az
                .DownloadFileUsingHttpClientAsync(It.IsAny<string>()))
                .Returns(Task.FromResult(httpClientData));

            azStorageMock.SetupGet(az => az.AccountAuthority).Returns(accountAuthority);

            var accountName = accountAuthority;
            var subdomainEndIndex = accountAuthority.IndexOf(".");

            if (subdomainEndIndex > 0)
            {
                accountName = accountAuthority.Substring(0, subdomainEndIndex);
            }

            azStorageMock.SetupGet(az => az.AccountName).Returns(accountName);

            return azStorageMock.Object;
        }

        private CromwellOnAzureEnvironment SetCromwellOnAzureEnvironment(string accountAuthority, IEnumerable<IAzureStorage> azureStorages = default)
        {
            var serviceCollection = new ServiceCollection()
                .AddLogging(loggingBuilder => loggingBuilder.AddConsole());

            var serviceProvider = serviceCollection.BuildServiceProvider();

            var cosmosdbRepositoryMock = new Mock<IRepository<TesTask>>();

            if (azureStorages is null)
            {
                azureStorages = Enumerable.Repeat(MockAzureStorage(accountAuthority), 1);
            }

            var environment = new CromwellOnAzureEnvironment(
                serviceProvider.GetRequiredService<ILoggerFactory>(),
                azureStorages.First(),
                new CromwellApiClient.CromwellApiClient("http://cromwell:8000"),
                cosmosdbRepositoryMock.Object,
                azureStorages);

            return environment;
        }

        private async Task<(ProcessedTriggerInfo, CromwellOnAzureEnvironment)> ProcessBlobTriggerWithMocksAsync(string triggerData)
        {
            var environment = SetCromwellOnAzureEnvironment("fake");
            var processedTriggerInfo = await environment.ProcessBlobTrigger(triggerData);

            return (processedTriggerInfo, environment);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException), "must have data in the Trigger File")]
        public async Task ProcessBlobTrigger_Empty()
            => await ProcessBlobTriggerWithMocksAsync(string.Empty);

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException), "must have data in the Trigger File")]
        public async Task ProcessBlobTrigger_Null()
            => await ProcessBlobTriggerWithMocksAsync(null);

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException), "must specify a WorkflowUrl in the Trigger File")]
        public async Task ProcessBlobTrigger_WorkflowUrlMissing()
            => await ProcessBlobTriggerWithMocksAsync(
                @"{
                }"
            );

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException), "must specify a WorkflowUrl in the Trigger File")]
        public async Task ProcessBlobTrigger_WorkflowUrlEmpty()
            => await ProcessBlobTriggerWithMocksAsync(
                @"{
                    ""WorkflowUrl"":""""
                }"
            );

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException), "must specify a WorkflowUrl in the Trigger File")]
        public async Task ProcessBlobTrigger_WorkflowUrlWhitespace()
            => await ProcessBlobTriggerWithMocksAsync(
                @"{
                    ""WorkflowUrl"":"" ""
                }"
            );

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public async Task ProcessBlobTrigger_WorkflowUrlNotUrl()
            => await ProcessBlobTriggerWithMocksAsync(
                @"{
                    ""WorkflowUrl"":""not url""
                }"
            );

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException), "'must specify a WorkflowUrl in the Trigger File")]
        public async Task ProcessBlobTrigger_WorkflowUrlNull()
            => await ProcessBlobTriggerWithMocksAsync(
                @"{
                    ""WorkflowUrl"":null
                }"
            );

        [TestMethod]
        public async Task ProcessBlobTrigger_NoInput()
        {
            var triggerFileContent = 
                @"{
                    ""WorkflowUrl"":""" + fakeAzureWdl + @""",
                    ""WorkflowInputsUrl"":null,
                    ""WorkflowOptionsUrl"":null,
                    ""WorkflowDependenciesUrl"":null
                }";

            await ExecuteTriggerFileTest(triggerFileContent, 0);
        }

        [TestMethod]
        public async Task ProcessBlobTrigger_SingleInput()
        {
            var triggerFileContent = 
                @"{
                    ""WorkflowUrl"":""" + fakeAzureWdl + @""",
                    ""WorkflowInputsUrl"":""" + fakeAzureInput + @""",
                    ""WorkflowOptionsUrl"":null,
                    ""WorkflowDependenciesUrl"":null
                }";

            await ExecuteTriggerFileTest(triggerFileContent, 1);
        }

        [TestMethod]
        public async Task ProcessBlobTrigger_MultiInput()
        {
            var triggerFileContent = 
                @"{
                    ""WorkflowUrl"":""" + fakeAzureWdl + @""",
                    ""WorkflowInputsUrls"":" + JsonConvert.SerializeObject(fakeAzureInputs) + @",
                    ""WorkflowOptionsUrl"":null,
                    ""WorkflowDependenciesUrl"":null
                }";

            await ExecuteTriggerFileTest(triggerFileContent, fakeAzureInputs.Count);
        }

        [TestMethod]
        public async Task ProcessBlobTrigger_CombinedInputs()
        {
            var triggerFileContent = 
                @"{
                    ""WorkflowUrl"":""" + fakeAzureWdl + @""",
                    ""WorkflowInputsUrl"":""" + fakeAzureInput + @""",
                    ""WorkflowInputsUrls"":" + JsonConvert.SerializeObject(fakeAzureInputs) + @",
                    ""WorkflowOptionsUrl"":null,
                    ""WorkflowDependenciesUrl"":null
                }";

            await ExecuteTriggerFileTest(triggerFileContent, fakeAzureInputs.Count + 1);
        }

        [TestMethod]
        public async Task ProcessBlobTrigger_SingleInputWithNull()
        {
            var triggerFileContent = 
                @"{
                    ""WorkflowUrl"":""" + fakeAzureWdl + @""",
                    ""WorkflowInputsUrl"":""" + fakeAzureInput + @""",
                    ""WorkflowInputsUrls"":null,
                    ""WorkflowOptionsUrl"":null,
                    ""WorkflowDependenciesUrl"":null
                }";

            await ExecuteTriggerFileTest(triggerFileContent, 1);
        }

        [TestMethod]
        public async Task ProcessBlobTrigger_MultiInputWithNull()
        {
            var triggerFileContent =
                @"{
                    ""WorkflowUrl"":""" + fakeAzureWdl + @""",
                    ""WorkflowInputsUrl"":null,
                    ""WorkflowInputsUrls"":" + JsonConvert.SerializeObject(fakeAzureInputs) + @",
                    ""WorkflowOptionsUrl"":null,
                    ""WorkflowDependenciesUrl"":null
                }";

            await ExecuteTriggerFileTest(triggerFileContent, fakeAzureInputs.Count);
        }

        [TestMethod]
        public async Task ProcessBlobTrigger_AllInputsNull()
        {
            var triggerFileContent = 
                @"{
                    ""WorkflowUrl"":""" + fakeAzureWdl + @""",
                    ""WorkflowInputsUrl"":null,
                    ""WorkflowInputsUrls"":null,
                    ""WorkflowOptionsUrl"":null,
                    ""WorkflowDependenciesUrl"":null
                }";

            await ExecuteTriggerFileTest(triggerFileContent, 0);
        }

        private async Task ExecuteTriggerFileTest(string triggerFileContent, int inputFilesCount)
        {
            (var processedTriggerInfo, _) = await ProcessBlobTriggerWithMocksAsync(triggerFileContent);
            VerifyTriggerFileProcessing(processedTriggerInfo, inputFilesCount);
            VerifyPostFiles(processedTriggerInfo);
        }

        private void VerifyTriggerFileProcessing(ProcessedTriggerInfo processedTriggerInfo, int inputFilesCount)
        {
            Assert.AreEqual(azureName, processedTriggerInfo.WorkflowSource.Filename, "comparing azureName to workflowSourceFilename");
            AssertBytesEqual(processedTriggerInfo.WorkflowSource.Data, httpClientData, "workflowSourceData");

            AssertNamesEqual(processedTriggerInfo.WorkflowInputs.Select(a => a.Filename).ToList(), inputFilesCount, azureName, "workflowInputsFilenames");
            AssertBytesEqual(processedTriggerInfo.WorkflowInputs.Select(a => a.Data).ToList(), inputFilesCount, httpClientData, "workflowInputsData");

            AssertExtraDataNull(processedTriggerInfo);
        }

        private static void AssertExtraDataNull(ProcessedTriggerInfo processedTriggerInfo)
        {
            Assert.IsNull(processedTriggerInfo.WorkflowOptions.Filename, "WorkflowOptions.Filename");
            Assert.IsNull(processedTriggerInfo.WorkflowOptions.Data, "WorkflowOptions.Data");
            Assert.IsNull(processedTriggerInfo.WorkflowDependencies.Filename, "WorkflowDependencies.Filename");
            Assert.IsNull(processedTriggerInfo.WorkflowDependencies.Data, "WorkflowDependencies.Data");
        }

        private static void AssertNamesEqual(List<string> filenames, int expectedLength, string expectedName, string filenameType)
        {
            Assert.IsNotNull(filenames, filenameType);
            Assert.AreEqual(expectedLength, filenames.Count, $"unexpected length of {filenameType}");

            for (var i = 0; i < expectedLength; i++)
            {
                Assert.AreEqual(expectedName, filenames[i], $"unexpected name for {filenameType}[{i}]");
            }
        }

        private static void AssertBytesEqual(List<byte[]> data, int expectedLength, byte[] expectedData, string dataName)
        {
            Assert.IsNotNull(data, dataName);
            Assert.AreEqual(expectedLength, data.Count, $"comparing expectedLength to the length of {dataName}");

            for (var i = 0; i < expectedLength; i++)
            {
                AssertBytesEqual(expectedData, data[i], $"{dataName}[{i}]");
            }
        }

        private static void AssertBytesEqual(byte[] data, byte[] expectedData, string dataName)
        {
            Assert.IsNotNull(data, dataName);
            Assert.AreEqual(expectedData.Length, data.Length, $"unexpected length of {dataName}");

            for (var i = 0; i < expectedData.Length; i++)
            {
                Assert.AreEqual(expectedData[i], data[i], $"unexpected value of {dataName}[{i}]");
            }
        }

        private static List<CromwellApiClient.CromwellApiClient.FileToPost> RetrievePostFiles(ProcessedTriggerInfo processedTriggerInfo)
            => CromwellApiClient.CromwellApiClient.AccumulatePostFiles(
                processedTriggerInfo.WorkflowSource.Filename,
                processedTriggerInfo.WorkflowSource.Data,
                processedTriggerInfo.WorkflowInputs.Select(a => a.Filename).ToList(),
                processedTriggerInfo.WorkflowInputs.Select(a => a.Data).ToList(),
                processedTriggerInfo.WorkflowOptions.Filename,
                processedTriggerInfo.WorkflowOptions.Data,
                processedTriggerInfo.WorkflowDependencies.Filename,
                processedTriggerInfo.WorkflowDependencies.Data);

        private static void VerifyPostFiles(ProcessedTriggerInfo processedTriggerInfo)
        {
            var files = RetrievePostFiles(processedTriggerInfo);
         
            Assert.AreEqual(processedTriggerInfo.WorkflowInputs.Count + 1, files.Count, "unexpected number of files");

            Assert.AreEqual("workflowSource", files[0].ParameterName, $"unexpected ParameterName for the 0th file");
            Assert.AreEqual(processedTriggerInfo.WorkflowSource.Filename, files[0].Filename, $"unexpected Filename for the 0th file");
            AssertBytesEqual(processedTriggerInfo.WorkflowSource.Data, files[0].Data, "files[0].Data");

            for (var i = 0; i < processedTriggerInfo.WorkflowInputs.Count; i++)
            {
                var ip1 = i + 1;

                if (i == 0)
                {
                    Assert.AreEqual("workflowInputs", files[ip1].ParameterName, $"unexpected ParameterName for file #{ip1}");
                }
                else
                {
                    Assert.AreEqual("workflowInputs_" + ip1, files[ip1].ParameterName, $"unexpected ParameterName for file #{ip1}");
                }

                Assert.AreEqual(processedTriggerInfo.WorkflowInputs[i].Filename, files[ip1].Filename, $"unexpected Filename for file #{ip1}");
                AssertBytesEqual(processedTriggerInfo.WorkflowInputs[i].Data, files[ip1].Data, $"files[{ip1}].Data");
            }
        }
    }
}
