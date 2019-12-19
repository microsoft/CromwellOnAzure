using System;
using System.Text;
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
        private string azureCredentialCodePathMethodName { get; set; }
        private string httpClientCodePathMethodName { get; set; }

        [TestMethod]
        public async Task GetBlobFileNameAndDataWithDefaultStorageAccount()
        {
            const string url = "https://fake.azure.storage.account/test/test.wdl";
            var accountAuthority = new Uri(url).Authority;

            (var name, var data) = await GetBlobFileNameAndDataUsingMocksAsync(url, accountAuthority);

            Assert.IsNotNull(name);
            Assert.IsNotNull(data);
            Assert.IsTrue(data.Length > 0);

            // Test if Azure credentials code path is used
            Assert.AreEqual(azureCredentialCodePathMethodName, Encoding.UTF8.GetString(data, 0, data.Length));
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
            Assert.AreEqual(httpClientCodePathMethodName, Encoding.UTF8.GetString(data, 0, data.Length));
        }

        private async Task<(string, byte[])> GetBlobFileNameAndDataUsingMocksAsync(string url, string accountAuthority)
        {
            var serviceCollection = new ServiceCollection()
                .AddLogging(loggingBuilder => loggingBuilder.AddConsole());

            var serviceProvider = serviceCollection.BuildServiceProvider();

            var azStorageMock = new Mock<IAzureStorage>();
            azureCredentialCodePathMethodName = nameof(azStorageMock.Object.DownloadBlobAsync);
            httpClientCodePathMethodName = nameof(azStorageMock.Object.GetByteArrayAsync);

            azStorageMock.Setup(az => az
                .DownloadBlobAsync(It.IsAny<string>()))
                .Returns(Task.FromResult(Encoding.UTF8.GetBytes(azureCredentialCodePathMethodName)));

            azStorageMock.Setup(az => az
                .GetByteArrayAsync(It.IsAny<string>()))
                .Returns(Task.FromResult(Encoding.UTF8.GetBytes(httpClientCodePathMethodName)));

            azStorageMock.SetupGet(az => az.AccountAuthority).Returns(accountAuthority);

            var environment = new CromwellOnAzureEnvironment(
                serviceProvider.GetRequiredService<ILoggerFactory>(),
                azStorageMock.Object,
                new CromwellApiClient.CromwellApiClient("http://cromwell:8000"));

            return await environment.GetBlobFileNameAndData(url);
        }
    }
}
