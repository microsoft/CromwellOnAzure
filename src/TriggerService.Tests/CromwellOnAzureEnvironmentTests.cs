// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
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
        private byte[] blobData = new byte[1] { 0 };
        private byte[] httpClientData = new byte[1] { 1 };
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

            Assert.AreEqual(azureName, name);
            Assert.IsNotNull(data);
            Assert.AreEqual(data.Length, 1);
            Assert.AreEqual(blobData[0], data[0]);
        }

        [TestMethod]
        public async Task GetBlobFileNameAndDataWithDefaultStorageAccountUsingLocalPath()
        {
            var accountAuthority = "fake";
            var url = $"/{accountAuthority}/test/test.wdl";

            (var name, var data) = await GetBlobFileNameAndDataUsingMocksAsync(url, accountAuthority);

            Assert.AreEqual(azureName, name);
            Assert.IsNotNull(data);
            Assert.AreEqual(data.Length, 1);
            Assert.AreEqual(blobData[0], data[0]);
        }

        [TestMethod]
        public async Task GetBlobFileNameAndDataWithDefaultStorageAccountWithSasToken()
        {
            var accountAuthority = new Uri(fakeAzureWdlWithSas).Authority;

            (var name, var data) = await GetBlobFileNameAndDataUsingMocksAsync(fakeAzureWdlWithSas, accountAuthority);

            Assert.AreEqual(azureName, name);
            Assert.IsNotNull(data);
            Assert.AreEqual(data.Length, 1);
            Assert.AreEqual(httpClientData[0], data[0]);
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
            await ProcessBlobTriggerWithMocksAsync(@"{
                }"
            );
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException), "must specify a WorkflowUrl in the Trigger File")]
        public async Task ProcessBlobTrigger_WorkflowUrlEmpty()
        {
            await ProcessBlobTriggerWithMocksAsync(@"{
                    ""WorkflowUrl"":""""
                }"
            );
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException), "must specify a WorkflowUrl in the Trigger File")]
        public async Task ProcessBlobTrigger_WorkflowUrlWhitespace()
        {
            await ProcessBlobTriggerWithMocksAsync(@"{
                    ""WorkflowUrl"":"" ""
                }"
            );
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public async Task ProcessBlobTrigger_WorkflowUrlNotUrl()
        {
            await ProcessBlobTriggerWithMocksAsync(@"{
                    ""WorkflowUrl"":""not url""
                }"
            );
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException), "'must specify a WorkflowUrl in the Trigger File")]
        public async Task ProcessBlobTrigger_WorkflowUrlNull()
        {
            await ProcessBlobTriggerWithMocksAsync(@"{
                    ""WorkflowUrl"":null
                }"
            );
        }

        [TestMethod]
        public async Task ProcessBlobTrigger_NoInput()
        {
            (var workflowSourceFilename, var workflowSourceData, var workflowInputsFilenames, var workflowInputsData, var workflowOptionsFilename,
                        var workflowOptionsData, var workflowDependenciesFilename, var workflowDependenciesData, var environment)
                = await ProcessBlobTriggerWithMocksAsync(@"{
                    ""WorkflowUrl"":""" + fakeAzureWdl + @""",
                    ""WorkflowInputsUrl"":null,
                    ""WorkflowOptionsUrl"":null,
                    ""WorkflowDependenciesUrl"":null
            }");

            Assert.AreEqual(azureName, workflowSourceFilename);
            AssertBytesEqual(workflowSourceData, httpClientData);

            AssertNamesEqual(workflowInputsFilenames, 0, azureName);
            AssertBytesEqual(workflowInputsData, 0, httpClientData);

            AssertExtraDataNull(workflowOptionsFilename, workflowOptionsData, workflowDependenciesFilename, workflowDependenciesData);

            var files = RetrievePostFiles(workflowSourceFilename, workflowSourceData, workflowInputsFilenames, workflowInputsData, workflowOptionsFilename, workflowOptionsData, workflowDependenciesFilename, workflowDependenciesData, environment);
            VerifyPostFiles(workflowSourceFilename, workflowSourceData, workflowInputsFilenames, workflowInputsData, files);
        }

        [TestMethod]
        public async Task ProcessBlobTrigger_SingleInput()
        {
            (var workflowSourceFilename, var workflowSourceData, var workflowInputsFilenames, var workflowInputsData, var workflowOptionsFilename,
                        var workflowOptionsData, var workflowDependenciesFilename, var workflowDependenciesData, var environment)
                = await ProcessBlobTriggerWithMocksAsync(@"{
                    ""WorkflowUrl"":""" + fakeAzureWdl + @""",
                    ""WorkflowInputsUrl"":""" + fakeAzureInput + @""",
                    ""WorkflowOptionsUrl"":null,
                    ""WorkflowDependenciesUrl"":null
            }");

            Assert.AreEqual(azureName, workflowSourceFilename);
            AssertBytesEqual(workflowSourceData, httpClientData);

            AssertNamesEqual(workflowInputsFilenames, 1, azureName);
            AssertBytesEqual(workflowInputsData, 1, httpClientData);

            AssertExtraDataNull(workflowOptionsFilename, workflowOptionsData, workflowDependenciesFilename, workflowDependenciesData);

            var files = RetrievePostFiles(workflowSourceFilename, workflowSourceData, workflowInputsFilenames, workflowInputsData, workflowOptionsFilename, workflowOptionsData, workflowDependenciesFilename, workflowDependenciesData, environment);
            VerifyPostFiles(workflowSourceFilename, workflowSourceData, workflowInputsFilenames, workflowInputsData, files);
        }

        [TestMethod]
        public async Task ProcessBlobTrigger_MultiInput()
        {
            (var workflowSourceFilename, var workflowSourceData, var workflowInputsFilenames, var workflowInputsData, var workflowOptionsFilename,
                        var workflowOptionsData, var workflowDependenciesFilename, var workflowDependenciesData, var environment)
                = await ProcessBlobTriggerWithMocksAsync(@"{
                    ""WorkflowUrl"":""" + fakeAzureWdl + @""",
                    ""WorkflowInputsUrls"":" + JsonConvert.SerializeObject(fakeAzureInputs) + @",
                    ""WorkflowOptionsUrl"":null,
                    ""WorkflowDependenciesUrl"":null
            }");

            Assert.AreEqual(azureName, workflowSourceFilename);
            AssertBytesEqual(workflowSourceData, httpClientData);

            AssertNamesEqual(workflowInputsFilenames, fakeAzureInputs.Count, azureName);
            AssertBytesEqual(workflowInputsData, fakeAzureInputs.Count, httpClientData);

            AssertExtraDataNull(workflowOptionsFilename, workflowOptionsData, workflowDependenciesFilename, workflowDependenciesData);

            var files = RetrievePostFiles(workflowSourceFilename, workflowSourceData, workflowInputsFilenames, workflowInputsData, workflowOptionsFilename, workflowOptionsData, workflowDependenciesFilename, workflowDependenciesData, environment);
            Assert.AreEqual(workflowInputsFilenames.Count + 1, files.Count);
            VerifyPostFiles(workflowSourceFilename, workflowSourceData, workflowInputsFilenames, workflowInputsData, files);
        }

        [TestMethod]
        public async Task ProcessBlobTrigger_CombinedInputs()
        {
            (var workflowSourceFilename, var workflowSourceData, var workflowInputsFilenames, var workflowInputsData, var workflowOptionsFilename,
                        var workflowOptionsData, var workflowDependenciesFilename, var workflowDependenciesData, var environment)
                = await ProcessBlobTriggerWithMocksAsync(@"{
                    ""WorkflowUrl"":""" + fakeAzureWdl + @""",
                    ""WorkflowInputsUrl"":""" + fakeAzureInput + @""",
                    ""WorkflowInputsUrls"":" + JsonConvert.SerializeObject(fakeAzureInputs) + @",
                    ""WorkflowOptionsUrl"":null,
                    ""WorkflowDependenciesUrl"":null
            }");

            Assert.AreEqual(azureName, workflowSourceFilename);
            AssertBytesEqual(workflowSourceData, httpClientData);

            AssertNamesEqual(workflowInputsFilenames, fakeAzureInputs.Count + 1, azureName);
            AssertBytesEqual(workflowInputsData, fakeAzureInputs.Count + 1, httpClientData);

            AssertExtraDataNull(workflowOptionsFilename, workflowOptionsData, workflowDependenciesFilename, workflowDependenciesData);

            var files = RetrievePostFiles(workflowSourceFilename, workflowSourceData, workflowInputsFilenames, workflowInputsData, workflowOptionsFilename, workflowOptionsData, workflowDependenciesFilename, workflowDependenciesData, environment);
            VerifyPostFiles(workflowSourceFilename, workflowSourceData, workflowInputsFilenames, workflowInputsData, files);
        }

        [TestMethod]
        public async Task ProcessBlobTrigger_SingleInputWithNull()
        {
            (var workflowSourceFilename, var workflowSourceData, var workflowInputsFilenames, var workflowInputsData, var workflowOptionsFilename,
                        var workflowOptionsData, var workflowDependenciesFilename, var workflowDependenciesData, var environment)
                = await ProcessBlobTriggerWithMocksAsync(@"{
                    ""WorkflowUrl"":""" + fakeAzureWdl + @""",
                    ""WorkflowInputsUrl"":""" + fakeAzureInput + @""",
                    ""WorkflowInputsUrls"":null,
                    ""WorkflowOptionsUrl"":null,
                    ""WorkflowDependenciesUrl"":null
            }");

            Assert.AreEqual(azureName, workflowSourceFilename);
            AssertBytesEqual(workflowSourceData, httpClientData);

            AssertNamesEqual(workflowInputsFilenames, 1, azureName);
            AssertBytesEqual(workflowInputsData, 1, httpClientData);

            AssertExtraDataNull(workflowOptionsFilename, workflowOptionsData, workflowDependenciesFilename, workflowDependenciesData);

            var files = RetrievePostFiles(workflowSourceFilename, workflowSourceData, workflowInputsFilenames, workflowInputsData, workflowOptionsFilename, workflowOptionsData, workflowDependenciesFilename, workflowDependenciesData, environment);
            VerifyPostFiles(workflowSourceFilename, workflowSourceData, workflowInputsFilenames, workflowInputsData, files);
        }

        [TestMethod]
        public async Task ProcessBlobTrigger_MultiInputWithNull()
        {
            (var workflowSourceFilename, var workflowSourceData, var workflowInputsFilenames, var workflowInputsData, var workflowOptionsFilename,
                        var workflowOptionsData, var workflowDependenciesFilename, var workflowDependenciesData, var environment)
                = await ProcessBlobTriggerWithMocksAsync(@"{
                    ""WorkflowUrl"":""" + fakeAzureWdl + @""",
                    ""WorkflowInputsUrl"":null,
                    ""WorkflowInputsUrls"":" + JsonConvert.SerializeObject(fakeAzureInputs) + @",
                    ""WorkflowOptionsUrl"":null,
                    ""WorkflowDependenciesUrl"":null
            }");

            Assert.AreEqual(azureName, workflowSourceFilename);
            AssertBytesEqual(workflowSourceData, httpClientData);

            AssertNamesEqual(workflowInputsFilenames, fakeAzureInputs.Count, azureName);
            AssertBytesEqual(workflowInputsData, fakeAzureInputs.Count, httpClientData);

            AssertExtraDataNull(workflowOptionsFilename, workflowOptionsData, workflowDependenciesFilename, workflowDependenciesData);

            var files = RetrievePostFiles(workflowSourceFilename, workflowSourceData, workflowInputsFilenames, workflowInputsData, workflowOptionsFilename, workflowOptionsData, workflowDependenciesFilename, workflowDependenciesData, environment);
            VerifyPostFiles(workflowSourceFilename, workflowSourceData, workflowInputsFilenames, workflowInputsData, files);
        }

        [TestMethod]
        public async Task ProcessBlobTrigger_AllInputsNull()
        {
            (var workflowSourceFilename, var workflowSourceData, var workflowInputsFilenames, var workflowInputsData, var workflowOptionsFilename,
                        var workflowOptionsData, var workflowDependenciesFilename, var workflowDependenciesData, var environment)
                = await ProcessBlobTriggerWithMocksAsync(@"{
                    ""WorkflowUrl"":""" + fakeAzureWdl + @""",
                    ""WorkflowInputsUrl"":null,
                    ""WorkflowInputsUrls"":null,
                    ""WorkflowOptionsUrl"":null,
                    ""WorkflowDependenciesUrl"":null
            }");

            Assert.AreEqual(azureName, workflowSourceFilename);
            AssertBytesEqual(workflowSourceData, httpClientData);

            AssertNamesEqual(workflowInputsFilenames, 0, azureName);
            AssertBytesEqual(workflowInputsData, 0, httpClientData);

            AssertExtraDataNull(workflowOptionsFilename, workflowOptionsData, workflowDependenciesFilename, workflowDependenciesData);

            var files = RetrievePostFiles(workflowSourceFilename, workflowSourceData, workflowInputsFilenames, workflowInputsData, workflowOptionsFilename, workflowOptionsData, workflowDependenciesFilename, workflowDependenciesData, environment);
            VerifyPostFiles(workflowSourceFilename, workflowSourceData, workflowInputsFilenames, workflowInputsData, files);
        }

        private static void AssertExtraDataNull(string workflowOptionsFilename, byte[] workflowOptionsData, string workflowDependenciesFilename, byte[] workflowDependenciesData)
        {
            Assert.IsNull(workflowOptionsFilename);
            Assert.IsNull(workflowOptionsData);
            Assert.IsNull(workflowDependenciesFilename);
            Assert.IsNull(workflowDependenciesData);
        }

        private static void AssertNamesEqual(List<string> workflowInputsFilenames, int expectedLength, string expectedName)
        {
            Assert.IsNotNull(workflowInputsFilenames);
            Assert.AreEqual(expectedLength, workflowInputsFilenames.Count);
            for (var i = 0; i < expectedLength; i++)
            {
                Assert.AreEqual(expectedName, workflowInputsFilenames[i]);
            }
        }

        private void AssertBytesEqual(List<byte[]> workflowInputsData, int expectedLength, byte[] expectedData)
        {
            Assert.IsNotNull(workflowInputsData);
            Assert.AreEqual(expectedLength, workflowInputsData.Count);
            for (var i = 0; i < expectedLength; i++)
            {
                AssertBytesEqual(workflowInputsData[i], expectedData);
            }
        }

        private void AssertBytesEqual(byte[] workflowInputsData, byte[] expectedData)
        {
            Assert.IsNotNull(workflowInputsData);
            Assert.AreEqual(expectedData.Length, workflowInputsData.Length);
            for (var i = 0; i < expectedData.Length; i++)
            {
                Assert.AreEqual(expectedData[i], workflowInputsData[i]);
            }
        }

        private static List<CromwellApiClient.CromwellApiClient.FileToPost> RetrievePostFiles(string workflowSourceFilename, byte[] workflowSourceData, List<string> workflowInputsFilenames, List<byte[]> workflowInputsData, string workflowOptionsFilename, byte[] workflowOptionsData, string workflowDependenciesFilename, byte[] workflowDependenciesData, CromwellOnAzureEnvironment environment)
        {
            var cromwellApiClient = environment.cromwellApiClient;
            var files = ((CromwellApiClient.CromwellApiClient)cromwellApiClient).AccumulatePostFiles(workflowSourceFilename, workflowSourceData,
                        workflowInputsFilenames, workflowInputsData, workflowOptionsFilename,
                        workflowOptionsData, workflowDependenciesFilename, workflowDependenciesData);
            return files;
        }

        private void VerifyPostFiles(string workflowSourceFilename, byte[] workflowSourceData, 
            List<string> workflowInputsFilenames, List<byte[]> workflowInputsData,
            List<CromwellApiClient.CromwellApiClient.FileToPost> files)
        {
            Assert.AreEqual(workflowInputsFilenames.Count + 1, files.Count);

            Assert.AreEqual("workflowSource", files[0].ParameterName);
            Assert.AreEqual(workflowSourceFilename, files[0].Filename);
            AssertBytesEqual(workflowSourceData, files[0].Data);

            for (var i = 0; i < workflowInputsFilenames.Count; i++)
            {
                if (i == 0)
                {
                    Assert.AreEqual("workflowInputs", files[i + 1].ParameterName);
                }
                else
                {
                    Assert.AreEqual("workflowInputs_" + (i + 1), files[i + 1].ParameterName);
                }
                Assert.AreEqual(workflowInputsFilenames[i], files[i + 1].Filename);
                AssertBytesEqual(workflowInputsData[i], files[i + 1].Data);
            }

        }
    }
}
