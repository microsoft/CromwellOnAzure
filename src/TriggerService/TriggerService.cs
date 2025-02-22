// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
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

        public static async Task Main()
        {
            Console.WriteLine($"TriggerService Build: {Assembly.GetExecutingAssembly().GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion}");
            AzureCloudConfig azureCloudConfig = null;
            var applicationInsightsConnectionString = string.Empty;
            IConfiguration configuration = null;

            await Host.CreateDefaultBuilder()
                .ConfigureAppConfiguration((hostBuilderContext, configurationBuilder) =>
                {
                    configurationBuilder.AddJsonFile("appsettings.json");
                    configurationBuilder.AddEnvironmentVariables();
                    configuration = configurationBuilder.Build();
                    azureCloudConfig = GetAzureCloudConfig(configuration);
                    var triggerServiceOptions = new TriggerServiceOptions();
                    configuration.Bind(TriggerServiceOptions.TriggerServiceOptionsSectionName, triggerServiceOptions);
                    const string legacyApplicationInsightsConnectionStringKey = "APPLICATIONINSIGHTS_CONNECTION_STRING";

                    if (!string.IsNullOrWhiteSpace(configuration[legacyApplicationInsightsConnectionStringKey]))
                    {
                        // Legacy CoA setting
                        Console.WriteLine($"Using {legacyApplicationInsightsConnectionStringKey}");
                        applicationInsightsConnectionString = configuration[legacyApplicationInsightsConnectionStringKey];
                    }
                    else if (!string.IsNullOrWhiteSpace(triggerServiceOptions.ApplicationInsightsAccountName))
                    {
                        Console.WriteLine($"Getting Azure subscriptions and Application Insights Connection string");

                        // name was specified, get the subscription, then the connection string from the account
                        applicationInsightsConnectionString = GetApplicationInsightsConnectionStringAsync(configuration, azureCloudConfig, triggerServiceOptions).GetAwaiter().GetResult();

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
                            if (string.IsNullOrWhiteSpace(applicationInsightsConnectionString))
                            {
                                configuration.DisableTelemetry = true;
                            }
                            else
                            {
                                configuration.ConnectionString = applicationInsightsConnectionString;
                            }
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
                    // Cache is not needed for this usage of the TesTask repository. Ensure that the middleware doesn't provide it one.
                    serviceCollection.AddSingleton<IRepository<TesTask>, TesTaskPostgreSqlRepository>(sp => new(
                        sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<PostgreSqlOptions>>(),
                        sp.GetRequiredService<IHostApplicationLifetime>(),
                        sp.GetRequiredService<ILogger<TesTaskPostgreSqlRepository>>()));
                    serviceCollection.AddSingleton<IAzureStorageUtility, AzureStorageUtility>(sp => ActivatorUtilities.CreateInstance<AzureStorageUtility>(sp, configuration));
                    serviceCollection.AddHostedService<TriggerHostedService>();
                })
                .Build()
                .RunAsync();

            static AzureCloudConfig GetAzureCloudConfig(IConfiguration configuration)
            {
                var options = new TriggerServiceOptions();
                configuration.Bind(TriggerServiceOptions.TriggerServiceOptionsSectionName, options);
                Console.WriteLine($"TriggerServiceOptions.AzureCloudName: {options.AzureCloudName}");
                return AzureCloudConfig.FromKnownCloudNameAsync(cloudName: options.AzureCloudName, azureCloudMetadataUrlApiVersion: options.AzureCloudMetadataUrlApiVersion).Result;
            }
        }

        private static async Task<string> GetApplicationInsightsConnectionStringAsync(IConfiguration config, AzureCloudConfig azureCloudConfig, TriggerServiceOptions triggerServiceOptions)
        {
            try
            {
                string applicationInsightsConnectionString;
                var tokenCredential = new CommonUtilities.AzureServicesConnectionStringCredential(new(config, azureCloudConfig));
                ArmClient armClient = new(tokenCredential, null, new() { Environment = azureCloudConfig.ArmEnvironment });
                var subscriptionId = await armClient.GetSubscriptions().GetAllAsync().Select(s => s.Id.SubscriptionId).FirstAsync();
                Console.WriteLine($"Running in subscriptionId: {subscriptionId}");
                applicationInsightsConnectionString = (await armClient.GetSubscriptionResource(SubscriptionResource.CreateResourceIdentifier(subscriptionId))
                        .GetApplicationInsightsComponentsAsync()
                        .SelectAwait(async c => (await c.GetAsync()).Value)
                        .FirstAsync(c => c.Data.ApplicationId.Equals(triggerServiceOptions.ApplicationInsightsAccountName, StringComparison.OrdinalIgnoreCase)))
                    .Data.ConnectionString;

                return applicationInsightsConnectionString;
            }
            catch (Exception exc)
            {
                Console.WriteLine($"Exception in {nameof(GetApplicationInsightsConnectionStringAsync)}: {exc.Message} {exc}");
                throw;
            }
        }
    }
}
