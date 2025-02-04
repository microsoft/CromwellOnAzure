// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Azure.ResourceManager;
using Azure.ResourceManager.Storage;
using Azure.Storage.Blobs;
using CommonUtilities.AzureCloud;
using Microsoft.Extensions.Configuration;


namespace TriggerService
{
    public interface IAzureStorageUtility
    {
        Task<(List<IAzureStorage>, IAzureStorage)> GetStorageAccountsUsingMsiAsync(string accountName);
    }

    public class AzureStorageUtility(AzureCloudConfig azureCloudConfig, IConfiguration configuration) : IAzureStorageUtility
    {
        private readonly AzureCloudConfig azureCloudConfig = azureCloudConfig;
        private readonly IConfiguration configuration = configuration;

        public async Task<(List<IAzureStorage>, IAzureStorage)> GetStorageAccountsUsingMsiAsync(string accountName)
        {
            IAzureStorage defaultAzureStorage = default;
            (var accounts, var defaultAccount) = await GetCloudStorageAccountsUsingMsiAsync(accountName);
            return (accounts.Select(GetAzureStorage).ToList(), defaultAzureStorage ?? throw new Exception($"Azure Storage account with name: {accountName} not found in list of {accounts.Count} storage accounts."));

            IAzureStorage GetAzureStorage(BlobServiceClient cloudStorage)
            {
                var azureStorage = new AzureStorage(cloudStorage, new HttpClient());
                if (cloudStorage.Equals(defaultAccount))
                {
                    defaultAzureStorage = azureStorage;
                }
                return azureStorage;
            }
        }

        private async Task<(IList<BlobServiceClient>, BlobServiceClient)> GetCloudStorageAccountsUsingMsiAsync(string accountName)
        {
            BlobServiceClient defaultAccount = default;
            var accounts = GetAccessibleStorageAccountsAsync();
            return (await accounts.Select(GetCloudAccountFromStorageAccountInfo).ToListAsync(), defaultAccount ?? throw new Exception($"Azure Storage account with name: {accountName} not found in list of {await accounts.CountAsync()} storage accounts."));

            BlobServiceClient GetCloudAccountFromStorageAccountInfo(StorageAccountInfo account)
            {
                CommonUtilities.AzureServicesConnectionStringCredential storageCredentials = new(new(configuration, azureCloudConfig));
                BlobServiceClient storageAccount = new(account.BlobEndpoint, storageCredentials);

                if (account.Name == accountName)
                {
                    defaultAccount = storageAccount;
                }

                return storageAccount;
            }
        }

        private IAsyncEnumerable<StorageAccountInfo> GetAccessibleStorageAccountsAsync()
        {
            var azureClient = GetAzureManagementClient();

            var subscriptions = azureClient.GetSubscriptions().GetAllAsync();

            return
                subscriptions.Select(subId =>
                    subId.GetStorageAccountsAsync()
                        .Select(a => new StorageAccountInfo { StorageAccount = a, Name = a.Id.Name, Subscription = subId, BlobEndpoint = a.Data.PrimaryEndpoints.BlobUri }))
                    .SelectMany(a => a);
        }

        /// <summary>
        /// Gets an authenticated Azure Client instance
        /// </summary>
        /// <returns>An authenticated Azure Client instance</returns>
        private ArmClient GetAzureManagementClient()
            => new(new CommonUtilities.AzureServicesConnectionStringCredential(new(configuration, azureCloudConfig)),
                default,
                new() { Environment = azureCloudConfig.ArmEnvironment });
    }
}
