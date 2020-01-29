// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;

namespace TriggerService.Tests
{
    [TestClass]
    public class CromwellOnAzureEnvironmentTests
    {
        private byte[] blobData = new byte[1] { 0 };
        private byte[] httpClientData = new byte[1] { 1 };

        [TestMethod]
        public async Task GetBlobFileNameAndDataWithDefaultStorageAccountUsingUrl()
        {
            const string url = "https://fake.azure.storage.account/test/test.wdl";
            var accountAuthority = new Uri(url).Authority;

            (var name, var data) = await GetBlobFileNameAndDataUsingMocksAsync(url, accountAuthority);

            Assert.IsNotNull(name);
            Assert.IsNotNull(data);
            Assert.IsTrue(data.Length > 0);

            // Test if Azure credentials code path is used
            Assert.AreEqual(data, blobData);
        }

        [TestMethod]
        public async Task GetBlobFileNameAndDataWithDefaultStorageAccountUsingLocalPath()
        {
            var accountAuthority = "fake";
            var url = $"/{accountAuthority}/test/test.wdl";

            (var name, var data) = await GetBlobFileNameAndDataUsingMocksAsync(url, accountAuthority);

            Assert.IsNotNull(name);
            Assert.IsNotNull(data);
            Assert.IsTrue(data.Length > 0);

            // Test if Azure credentials code path is used
            Assert.AreEqual(data, blobData);
        }

        [TestMethod]
        public async Task GetBlobFileNameAndDataWithDefaultStorageAccountWithSasToken()
        {
            const string url = "https://fake.azure.storage.account/test/test.wdl?sp=r&st=2019-12-18T18:55:41Z&se=2019-12-19T02:55:41Z&spr=https&sv=2019-02-02&sr=b&sig=EMJyBMOxdG2NvBqiwUsg71ZdYqwqMWda9242KU43%2F5Y%3D";
            var accountAuthority = new Uri(url).Authority;

            (var name, var data) = await GetBlobFileNameAndDataUsingMocksAsync(url, accountAuthority);

            Assert.IsNotNull(name);
            Assert.IsNotNull(data);
            Assert.IsTrue(data.Length > 0);

            // Test if HttpClient code path is used
            Assert.AreEqual(data, httpClientData);
        }

        private async Task<(string, byte[])> GetBlobFileNameAndDataUsingMocksAsync(string url, string accountAuthority)
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

            return await environment.GetBlobFileNameAndData(url);
        }
    }
}
