﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;
using Common;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Rest;
using Tes.Models;
using Tes.Repository;
using FluentAzure = Microsoft.Azure.Management.Fluent.Azure;

namespace TriggerService
{
    internal class Program
    {
        public static async Task Main()
        {
            await InitAndRunAsync();
        }

        private static async Task InitAndRunAsync()
        {
            var instrumentationKey = await AzureStorage.GetAppInsightsInstrumentationKeyAsync(Environment.GetEnvironmentVariable("ApplicationInsightsAccountName"));
            var defaultStorageAccountName = Environment.GetEnvironmentVariable("DefaultStorageAccountName");
            var cosmosDbAccountName = Environment.GetEnvironmentVariable("CosmosDbAccountName");
            var cromwellUrl = ConfigurationManager.AppSettings.Get("CromwellUrl");

            var serviceCollection = new ServiceCollection()
                .AddLogging(loggingBuilder =>
                {
                    if (!string.IsNullOrWhiteSpace(instrumentationKey))
                    {
                        loggingBuilder.AddApplicationInsights(instrumentationKey,
                            options =>
                            {
                                options.TrackExceptionsAsExceptionTelemetry = false;
                            });
                    }
                    else
                    {
                        Console.WriteLine("Warning: AppInsights key was null, and so AppInsights logging will not be enabled.  Check if this VM has Contributor access to the Application Insights instance.");
                    }

                    loggingBuilder.AddConsole();
                });

            var serviceProvider = serviceCollection.BuildServiceProvider();

            var cloudStorageAccount = await AzureStorage.GetCloudStorageAccountUsingMsiAsync(defaultStorageAccountName);

            (var cosmosDbEndpoint, var cosmosDbKey) = await GetCosmosDbEndpointAndKeyAsync(cosmosDbAccountName);

            var environment = new CromwellOnAzureEnvironment(
                            serviceProvider.GetRequiredService<ILoggerFactory>(),
                            new AzureStorage(serviceProvider.GetRequiredService<ILoggerFactory>().CreateLogger<AzureStorage>(), cloudStorageAccount, new System.Net.Http.HttpClient()),
                            new CromwellApiClient.CromwellApiClient(cromwellUrl),
                            new CosmosDbRepository<TesTask>(
                                cosmosDbEndpoint,
                                cosmosDbKey,
                                Constants.CosmosDbDatabaseId,
                                Constants.CosmosDbContainerId,
                                Constants.CosmosDbPartitionId));

            serviceCollection.AddSingleton(s => new TriggerEngine(s.GetRequiredService<ILoggerFactory>(), environment));
            serviceProvider = serviceCollection.BuildServiceProvider();
            var engine = serviceProvider.GetService<TriggerEngine>();
            await engine.RunAsync();
        }

        /// <summary>
        /// Asynchronously Get CosmosDbEndpoint And Key
        /// </summary>
        /// <param name="cosmosDbAccountName"></param>
        /// <returns></returns>
        private static async Task<(string, string)> GetCosmosDbEndpointAndKeyAsync(string cosmosDbAccountName)
        {
            if (string.IsNullOrWhiteSpace(cosmosDbAccountName))
            {
                throw new Exception($"CosmosDbAccountName cannot be null.");
            }

            var azureClient = await GetAzureManagementClientAsync();
            var subscriptionIds = (await azureClient.Subscriptions.ListAsync()).Select(s => s.SubscriptionId);

            var account = (await Task.WhenAll(subscriptionIds.Select(async subId => await azureClient.WithSubscription(subId).CosmosDBAccounts.ListAsync())))
                .SelectMany(a => a)
                .FirstOrDefault(a => a.Name.Equals(cosmosDbAccountName, StringComparison.OrdinalIgnoreCase));

            if (account == null)
            {
                throw new Exception($"CosmosDB account '{cosmosDbAccountName} does not exist or the TES app service does not have Account Reader role on the account.");
            }

            var key = (await azureClient.WithSubscription(account.Manager.SubscriptionId).CosmosDBAccounts.ListKeysAsync(account.ResourceGroupName, account.Name)).PrimaryMasterKey;

            return (account.DocumentEndpoint, key);
        }

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

        /// <summary>
        ///  Asynchronously Get Azure Access Token
        /// </summary>
        /// <param name="resource"></param>
        /// <returns></returns>
        private static Task<string> GetAzureAccessTokenAsync(string resource = "https://management.azure.com/")
        {
            return new AzureServiceTokenProvider().GetAccessTokenAsync(resource);
        }
    }
}
