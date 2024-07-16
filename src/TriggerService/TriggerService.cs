// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.ResourceManager;
using Azure.ResourceManager.ApplicationInsights;
using Azure.ResourceManager.Resources;
using CommonUtilities.AzureCloud;
using CromwellApiClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Tes.Models;
using Tes.Repository;

namespace TriggerService
{
    internal class TriggerService
    {
        public TriggerService()
            => Common.NewtonsoftJsonSafeInit.SetDefaultSettings();

        internal static string applicationInsightsConnectionString = "";

        public static async Task Main()
        {
            AzureCloudConfig azureCloudConfig = null;

            await Host.CreateDefaultBuilder()
                .ConfigureAppConfiguration((hostBuilderContext, configurationBuilder) =>
                {
                    configurationBuilder.AddJsonFile("appsettings.json");
                    configurationBuilder.AddEnvironmentVariables();
                    var config = configurationBuilder.Build();
                    azureCloudConfig = GetAzureCloudConfig(config);
                    var triggerServiceOptions = new TriggerServiceOptions();
                    config.Bind(TriggerServiceOptions.TriggerServiceOptionsSectionName, triggerServiceOptions);
                    const string legacyApplicationInsightsConnectionStringKey = "APPLICATIONINSIGHTS_CONNECTION_STRING";

                    if (!string.IsNullOrWhiteSpace(config[legacyApplicationInsightsConnectionStringKey]))
                    {
                        // Legacy CoA setting
                        Console.WriteLine($"Using {legacyApplicationInsightsConnectionStringKey}");
                        applicationInsightsConnectionString = config[legacyApplicationInsightsConnectionStringKey];
                    }
                    else if (!string.IsNullOrWhiteSpace(triggerServiceOptions.ApplicationInsightsAccountName))
                    {
                        Console.WriteLine($"Getting Azure subscriptions and Application Insights Connection string");

                        // name was specified, get the subscription, then the connection string from the account
                        applicationInsightsConnectionString = GetApplicationInsightsConnectionString(azureCloudConfig, triggerServiceOptions);

                        Console.WriteLine($"Successfully retrieved applicationInsightsConnectionString: {!string.IsNullOrWhiteSpace(applicationInsightsConnectionString)}");
                    }
                    else
                    {
                        Console.WriteLine("No ApplicationInsights configuration found!");
                    }
                })
                .ConfigureLogging((hostBuilderContext, loggingBuilder) =>
                    {
                        loggingBuilder.AddConsole();
                        loggingBuilder.AddApplicationInsights(
                            configuration =>
                            {
                                configuration.ConnectionString = applicationInsightsConnectionString;
                            },
                            options => { });

                    })
                .ConfigureServices((hostBuilderContext, serviceCollection) =>
                {
                    serviceCollection.Configure<CromwellApiClientOptions>(hostBuilderContext.Configuration.GetSection(CromwellApiClientOptions.CromwellApiClientOptionsSectionName));
                    serviceCollection.Configure<TriggerServiceOptions>(hostBuilderContext.Configuration.GetSection(TriggerServiceOptions.TriggerServiceOptionsSectionName));
                    serviceCollection.Configure<PostgreSqlOptions>(hostBuilderContext.Configuration.GetSection(PostgreSqlOptions.GetConfigurationSectionName("Tes")));
                    serviceCollection.AddSingleton(azureCloudConfig);
                    serviceCollection.AddSingleton<ICromwellApiClient, CromwellApiClient.CromwellApiClient>();
                    serviceCollection.AddSingleton<IRepository<TesTask>, TesTaskPostgreSqlRepository>();
                    serviceCollection.AddSingleton<IAzureStorageUtility, AzureStorageUtility>();
                    serviceCollection.AddHostedService<TriggerHostedService>();
                })
                .Build()
                .RunAsync();

            static AzureCloudConfig GetAzureCloudConfig(IConfiguration configuration)
            {
                var options = new TriggerServiceOptions();
                configuration.Bind(TriggerServiceOptions.TriggerServiceOptionsSectionName, options);
                Console.WriteLine($"TriggerServiceOptions.AzureCloudName: {options.AzureCloudName}");
                return AzureCloudConfig.CreateAsync(options.AzureCloudName, options.AzureCloudMetadataUrlApiVersion).Result;
            }
        }

        private static string GetApplicationInsightsConnectionString(AzureCloudConfig azureCloudConfig, TriggerServiceOptions triggerServiceOptions)
        {
            try
            {
                string applicationInsightsConnectionString;
                var tokenCredential = new DefaultAzureCredential(new DefaultAzureCredentialOptions { AuthorityHost = new Uri(azureCloudConfig.Authentication.LoginEndpointUrl) });
                ArmClient armClient = new(tokenCredential, null, new() { Environment = azureCloudConfig.ArmEnvironment });
                var subscriptionId = armClient.GetSubscriptions().GetAllAsync().Select(s => s.Id.SubscriptionId).FirstAsync().Result;
                Console.WriteLine($"Running in subscriptionId: {subscriptionId}");
                applicationInsightsConnectionString = armClient.GetSubscriptionResource(SubscriptionResource.CreateResourceIdentifier(subscriptionId))
                    .GetApplicationInsightsComponentsAsync()
                    .SelectAwait(async c => (await c.GetAsync()).Value)
                    .FirstAsync(c => c.Data.ApplicationId.Equals(triggerServiceOptions.ApplicationInsightsAccountName, StringComparison.OrdinalIgnoreCase))
                    .Result.Data.ConnectionString;

                return applicationInsightsConnectionString;
            }
            catch (Exception exc)
            {
                Console.WriteLine($"Exception in {nameof(GetApplicationInsightsConnectionString)}: {exc.Message} {exc}");
                throw;
            }
        }
    }
}
