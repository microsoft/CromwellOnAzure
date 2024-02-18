// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Azure.Identity;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Rest;
using Microsoft.WindowsAzure.Storage;
using FluentAzure = Microsoft.Azure.Management.Fluent.Azure;


namespace TriggerService
{
    public interface IAzureStorageUtility
    {
        Task<(List<IAzureStorage>, IAzureStorage)> GetStorageAccountsUsingMsiAsync(string accountName);
    }

    public class AzureStorageUtility : IAzureStorageUtility
    {
        public async Task<(List<IAzureStorage>, IAzureStorage)> GetStorageAccountsUsingMsiAsync(string accountName)
        {
            IAzureStorage defaultAzureStorage = default;
            (var accounts, var defaultAccount) = await GetCloudStorageAccountsUsingMsiAsync(accountName);
            return (accounts.Select(GetAzureStorage).ToList(), defaultAzureStorage ?? throw new Exception($"Azure Storage account with name: {accountName} not found in list of {accounts.Count} storage accounts."));

            IAzureStorage GetAzureStorage(CloudStorageAccount cloudStorage)
            {
                var azureStorage = new AzureStorage(cloudStorage, new HttpClient());
                if (cloudStorage.Equals(defaultAccount))
                {
                    defaultAzureStorage = azureStorage;
                }
                return azureStorage;
            }
        }

        private static async Task<(IList<CloudStorageAccount>, CloudStorageAccount)> GetCloudStorageAccountsUsingMsiAsync(string accountName)
        {
            CloudStorageAccount defaultAccount = default;
            var accounts = await GetAccessibleStorageAccountsAsync();
            return (await Task.WhenAll(accounts.Select(GetCloudAccountFromStorageAccountInfo)), defaultAccount ?? throw new Exception($"Azure Storage account with name: {accountName} not found in list of {accounts.Count} storage accounts."));

            async Task<CloudStorageAccount> GetCloudAccountFromStorageAccountInfo(StorageAccountInfo account)
            {
                var key = await GetStorageAccountKeyAsync(account);
                var storageCredentials = new Microsoft.WindowsAzure.Storage.Auth.StorageCredentials(account.Name, key);
                var storageAccount = new CloudStorageAccount(storageCredentials, true);
                if (account.Name == accountName)
                {
                    defaultAccount = storageAccount;
                }
                return storageAccount;
            }
        }

        private static async Task<List<StorageAccountInfo>> GetAccessibleStorageAccountsAsync()
        {
            var azureClient = await GetAzureManagementClientAsync();

            var subscriptionIds = (await azureClient.Subscriptions.ListAsync()).Select(s => s.SubscriptionId);

            return (await Task.WhenAll(
                subscriptionIds.Select(async subId =>
                    (await azureClient.WithSubscription(subId).StorageAccounts.ListAsync())
                        .Select(a => new StorageAccountInfo { Id = a.Id, Name = a.Name, SubscriptionId = subId, BlobEndpoint = a.EndPoints.Primary.Blob }))))
                .SelectMany(a => a)
                .ToList();
        }

        private static async Task<string> GetStorageAccountKeyAsync(StorageAccountInfo storageAccountInfo)
        {
            var azureClient = await GetAzureManagementClientAsync();
            var storageAccount = await azureClient.WithSubscription(storageAccountInfo.SubscriptionId).StorageAccounts.GetByIdAsync(storageAccountInfo.Id);

            return (await storageAccount.GetKeysAsync())[0].Value;
        }

        public static async Task<string> GetAzureAccessTokenAsync(string scope = "https://management.azure.com/.default")
            => (await (new DefaultAzureCredential()).GetTokenAsync(new Azure.Core.TokenRequestContext(new string[] { scope }))).Token;

        /// <summary>
        /// Gets an authenticated Azure Client instance
        /// </summary>
        /// <returns>An authenticated Azure Client instance</returns>
        private static async Task<FluentAzure.IAuthenticated> GetAzureManagementClientAsync()
        {
            var accessToken = await GetAzureAccessTokenAsync();
            var azureCredentials = new AzureCredentials(new TokenCredentials(accessToken), null, null, AzureEnvironment.AzureGlobalCloud);
            var azureClient = FluentAzure.Authenticate(azureCredentials);

            return azureClient;
        }
    }
}
