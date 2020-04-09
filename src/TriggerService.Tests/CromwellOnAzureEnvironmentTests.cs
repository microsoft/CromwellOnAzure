// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.KeyVault.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Newtonsoft.Json;

namespace TriggerService.Tests
{
    [TestClass]
    public class CromwellOnAzureEnvironmentTests
    {
        private const string azureName = "test";
        private byte[] blobData = new byte[3] { 1, 2, 3 };
        private byte[] httpClientData = new byte[4] { 4, 3, 2, 1 };
        private string fakeAzureWdl = $"https://fake.azure.storage.account/{azureName}/test.wdl";
        private string fakeAzureInput = $"https://fake.azure.storage.account/{azureName}/test.input.json";
        private List<string> fakeAzureInputs = new List<string>() { 
            $"https://fake.azure.storage.account/{azureName}/test.input1.json",
            $"https://fake.azure.storage.account/{azureName}/test.input_2.json"
        };
        private string fakeAzureWdlWithSas = "https://fake.azure.storage.account/{azureName}/test.wdl?sp=r&st=2019-12-18T18:55:41Z&se=2019-12-19T02:55:41Z&spr=https&sv=2019-02-02&sr=b&sig=EMJyBMOxdG2NvBqiwUsg71ZdYqwqMWda9242KU43%2F5Y%3D";

        [TestMethod]
        public async Task GetBlobFileNameAndDataWithDefaultStorageAccountUsingUrl()
        {
            var accountAuthority = new Uri(fakeAzureWdl).Authority;

            (var name, var data) = await GetBlobFileNameAndDataUsingMocksAsync(fakeAzureWdl, accountAuthority);

            Assert.AreEqual(azureName, name, "azureName compared with name");
            Assert.IsNotNull(data, "data");
            Assert.AreEqual(blobData.Length, data.Length, "length of blobData vs length of data");
            for (var i = 0; i < blobData.Length; i++)
            {
                Assert.AreEqual(blobData[i], data[i], $"comparing blobData[{i}] to data[{i}]");
            }
        }

        [TestMethod]
        public async Task GetBlobFileNameAndDataWithDefaultStorageAccountUsingLocalPath()
        {
            var accountAuthority = "fake";
            var url = $"/{accountAuthority}/test/test.wdl";

            (var name, var data) = await GetBlobFileNameAndDataUsingMocksAsync(url, accountAuthority);

            Assert.AreEqual(azureName, name, "azureName compared with name");
            Assert.IsNotNull(data, "data");
            Assert.AreEqual(blobData.Length, data.Length, "length of blobData vs length of data");
            for (var i = 0; i < blobData.Length; i++)
            {
                Assert.AreEqual(blobData[i], data[i], $"comparing blobData[{i}] to data[{i}]");
            }
        }

        [TestMethod]
        public async Task GetBlobFileNameAndDataWithDefaultStorageAccountWithSasToken()
        {
            var accountAuthority = new Uri(fakeAzureWdlWithSas).Authority;

            (var name, var data) = await GetBlobFileNameAndDataUsingMocksAsync(fakeAzureWdlWithSas, accountAuthority);

            Assert.AreEqual(azureName, name, "azureName compared with name");
            Assert.IsNotNull(data, "data");
            Assert.AreEqual(httpClientData.Length, data.Length, "length of httpClientData vs length of data");
            for (var i = 0; i < httpClientData.Length; i++)
            {
                Assert.AreEqual(httpClientData[i], data[i], $"comparing httpClientData[{i}] to data[{i}]");
            }

        }

        private async Task<(string, byte[])> GetBlobFileNameAndDataUsingMocksAsync(string url, string accountAuthority)
        {
            var environment = SetCromwellOnAzureEnvironment(accountAuthority);
            return await environment.GetBlobFileNameAndData(url);
        }

        private CromwellOnAzureEnvironment SetCromwellOnAzureEnvironment(string accountAuthority)
        {
            var serviceCollection = new ServiceCollection()
                .AddLogging(loggingBuilder => loggingBuilder.AddConsole());

            var serviceProvider = serviceCollection.BuildServiceProvider();

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

            var environment = new CromwellOnAzureEnvironment(
                serviceProvider.GetRequiredService<ILoggerFactory>(),
                azStorageMock.Object,
                new CromwellApiClient.CromwellApiClient("http://cromwell:8000"));

            return environment;
        }

        private async Task<(string, byte[], List<string>, List<byte[]>, string, byte[], string, byte[], CromwellOnAzureEnvironment)> ProcessBlobTriggerWithMocksAsync(string triggerData)
        {
            var environment = SetCromwellOnAzureEnvironment("fake");
            (var workflowSourceFilename, var workflowSourceData, var workflowInputsFilenames, var workflowInputsData, var workflowOptionsFilename,
                        var workflowOptionsData, var workflowDependenciesFilename, var workflowDependenciesData) = await environment.ProcessBlobTrigger(triggerData);

            return (workflowSourceFilename, workflowSourceData, workflowInputsFilenames, workflowInputsData, workflowOptionsFilename,
                        workflowOptionsData, workflowDependenciesFilename, workflowDependenciesData, environment);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException), "must have data in the Trigger File")]
        public async Task ProcessBlobTrigger_Empty()
        {
            await ProcessBlobTriggerWithMocksAsync("");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException), "must have data in the Trigger File")]
        public async Task ProcessBlobTrigger_Null()
        {
            await ProcessBlobTriggerWithMocksAsync(null);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException), "must specify a WorkflowUrl in the Trigger File")]
        public async Task ProcessBlobTrigger_WorkflowUrlMissing()
        {
            await ProcessBlobTriggerWithMocksAsync(
                @"{
                }"
            );
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException), "must specify a WorkflowUrl in the Trigger File")]
        public async Task ProcessBlobTrigger_WorkflowUrlEmpty()
        {
            await ProcessBlobTriggerWithMocksAsync(
                @"{
                    ""WorkflowUrl"":""""
                }"
            );
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException), "must specify a WorkflowUrl in the Trigger File")]
        public async Task ProcessBlobTrigger_WorkflowUrlWhitespace()
        {
            await ProcessBlobTriggerWithMocksAsync(
                @"{
                    ""WorkflowUrl"":"" ""
                }"
            );
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public async Task ProcessBlobTrigger_WorkflowUrlNotUrl()
        {
            await ProcessBlobTriggerWithMocksAsync(
                @"{
                    ""WorkflowUrl"":""not url""
                }"
            );
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException), "'must specify a WorkflowUrl in the Trigger File")]
        public async Task ProcessBlobTrigger_WorkflowUrlNull()
        {
            await ProcessBlobTriggerWithMocksAsync(
                @"{
                    ""WorkflowUrl"":null
                }"
            );
        }

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
            (var workflowSourceFilename, var workflowSourceData, 
                var workflowInputsFilenames, var workflowInputsData, 
                var workflowOptionsFilename, var workflowOptionsData, 
                var workflowDependenciesFilename, var workflowDependenciesData, 
                var environment)
                    = await ProcessBlobTriggerWithMocksAsync(triggerFileContent);


            VerifyTriggerFileProcessing(workflowSourceFilename, workflowSourceData,
                workflowInputsFilenames, workflowInputsData,
                workflowOptionsFilename, workflowOptionsData,
                workflowDependenciesFilename, workflowDependenciesData,
                inputFilesCount);

            VerifyPostFiles(workflowSourceFilename, workflowSourceData,
                workflowInputsFilenames, workflowInputsData,
                workflowOptionsFilename, workflowOptionsData,
                workflowDependenciesFilename, workflowDependenciesData,
                environment);
        }

        private void VerifyTriggerFileProcessing(string workflowSourceFilename, byte[] workflowSourceData, 
            List<string> workflowInputsFilenames, List<byte[]> workflowInputsData, 
            string workflowOptionsFilename, byte[] workflowOptionsData, 
            string workflowDependenciesFilename, byte[] workflowDependenciesData, 
            int inputFilesCount)
        {
            Assert.AreEqual(azureName, workflowSourceFilename, "comparing azureName to workflowSourceFilename");
            AssertBytesEqual(workflowSourceData, httpClientData, "workflowSourceData");

            AssertNamesEqual(workflowInputsFilenames, inputFilesCount, azureName, "workflowInputsFilenames");
            AssertBytesEqual(workflowInputsData, inputFilesCount, httpClientData, "workflowInputsData");

            AssertExtraDataNull(workflowOptionsFilename, workflowOptionsData, workflowDependenciesFilename, workflowDependenciesData);
        }

        private static void AssertExtraDataNull(string workflowOptionsFilename, byte[] workflowOptionsData, string workflowDependenciesFilename, byte[] workflowDependenciesData)
        {
            Assert.IsNull(workflowOptionsFilename, "workflowOptionsFilename");
            Assert.IsNull(workflowOptionsData, "workflowOptionsData");
            Assert.IsNull(workflowDependenciesFilename, "workflowDependenciesFilename");
            Assert.IsNull(workflowDependenciesData, "workflowDependenciesData");
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

        private void AssertBytesEqual(List<byte[]> data, int expectedLength, byte[] expectedData, string dataName)
        {
            Assert.IsNotNull(data, dataName);
            Assert.AreEqual(expectedLength, data.Count, $"comparing expectedLength to the length of {dataName}");
            for (var i = 0; i < expectedLength; i++)
            {
                AssertBytesEqual(expectedData, data[i], $"{dataName}[{i}]");
            }
        }

        private void AssertBytesEqual(byte[] data, byte[] expectedData, string dataName)
        {
            Assert.IsNotNull(data, dataName);
            Assert.AreEqual(expectedData.Length, data.Length, $"unexpected length of {dataName}");
            for (var i = 0; i < expectedData.Length; i++)
            {
                Assert.AreEqual(expectedData[i], data[i], $"unexpected value of {dataName}[{i}]");
            }
        }

        private static List<CromwellApiClient.CromwellApiClient.FileToPost> RetrievePostFiles(string workflowSourceFilename, byte[] workflowSourceData, List<string> workflowInputsFilenames, List<byte[]> workflowInputsData, string workflowOptionsFilename, byte[] workflowOptionsData, string workflowDependenciesFilename, byte[] workflowDependenciesData, CromwellOnAzureEnvironment environment)
        {
            var cromwellApiClient = environment.cromwellApiClient;
            return ((CromwellApiClient.CromwellApiClient)cromwellApiClient).AccumulatePostFiles(workflowSourceFilename, workflowSourceData,
                        workflowInputsFilenames, workflowInputsData, workflowOptionsFilename,
                        workflowOptionsData, workflowDependenciesFilename, workflowDependenciesData);
        }

        private void VerifyPostFiles(string workflowSourceFilename, byte[] workflowSourceData, 
            List<string> workflowInputsFilenames, List<byte[]> workflowInputsData,
                string workflowOptionsFilename, byte[] workflowOptionsData,
                string workflowDependenciesFilename, byte[] workflowDependenciesData, 
                CromwellOnAzureEnvironment environment)
        {
            var files = RetrievePostFiles(workflowSourceFilename, workflowSourceData, 
                workflowInputsFilenames, workflowInputsData, 
                workflowOptionsFilename, workflowOptionsData, 
                workflowDependenciesFilename, workflowDependenciesData, environment);
         
            Assert.AreEqual(workflowInputsFilenames.Count + 1, files.Count, "unexpected number of files");

            Assert.AreEqual("workflowSource", files[0].ParameterName, $"unexpected ParameterName for the 0th file");
            Assert.AreEqual(workflowSourceFilename, files[0].Filename, $"unexpected Filename for the 0th file");
            AssertBytesEqual(workflowSourceData, files[0].Data, "files[0].Data");

            for (var i = 0; i < workflowInputsFilenames.Count; i++)
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
                Assert.AreEqual(workflowInputsFilenames[i], files[ip1].Filename, $"unexpected Filename for file #{ip1}");
                AssertBytesEqual(workflowInputsData[i], files[ip1].Data, $"files[{ip1}].Data");
            }
        }
    }
}
