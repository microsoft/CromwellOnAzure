// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading.Tasks;
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
            => await Host.CreateDefaultBuilder()
                .ConfigureAppConfiguration((hostBuilderContext, configurationBuilder) =>
                {
                    configurationBuilder.AddJsonFile("appsettings.json");
                    configurationBuilder.AddEnvironmentVariables();
                })
                .ConfigureLogging(async (hostBuilderContext, loggingBuilder) =>
                    {
                        loggingBuilder.AddConsole();
                        var triggerServiceOptions = new TriggerServiceOptions();
                        hostBuilderContext.Configuration.GetSection(TriggerServiceOptions.TriggerServiceOptionsSectionName).Bind(triggerServiceOptions);

                        if (string.IsNullOrWhiteSpace(triggerServiceOptions.ApplicationInsightsAccountName))
                        {
                            return;
                        }

                        Console.WriteLine($"ApplicationInsightsAccountName: {triggerServiceOptions.ApplicationInsightsAccountName}");
                        var connectionString = await AzureStorage.GetAppInsightsConnectionStringAsync(triggerServiceOptions.ApplicationInsightsAccountName);

                        if (string.IsNullOrWhiteSpace(connectionString))
                        {
                            throw new Exception($"No connection string found for {triggerServiceOptions.ApplicationInsightsAccountName}, does this service have Contributor access or equivalent to {triggerServiceOptions.ApplicationInsightsAccountName}?");
                        }

                        loggingBuilder.AddApplicationInsights(
                            configuration =>
                            {
                                configuration.ConnectionString = connectionString;
                            },
                            options =>
                            {
                            });

                    })
                .ConfigureServices((hostBuilderContext, serviceCollection) =>
                {
                    serviceCollection.Configure<CromwellApiClientOptions>(hostBuilderContext.Configuration.GetSection(CromwellApiClientOptions.CromwellApiClientOptionsSectionName));
                    serviceCollection.Configure<TriggerServiceOptions>(hostBuilderContext.Configuration.GetSection(TriggerServiceOptions.TriggerServiceOptionsSectionName));
                    serviceCollection.Configure<PostgreSqlOptions>(hostBuilderContext.Configuration.GetSection(PostgreSqlOptions.GetConfigurationSectionName("Tes")));
                    serviceCollection.AddSingleton(AzureCloudConfig.CreateAsync().Result);
                    serviceCollection.AddSingleton<ICromwellApiClient, CromwellApiClient.CromwellApiClient>();
                    serviceCollection.AddSingleton<IRepository<TesTask>, TesTaskPostgreSqlRepository>();
                    serviceCollection.AddSingleton<IAzureStorageUtility, AzureStorageUtility>();
                    serviceCollection.AddHostedService<TriggerHostedService>();
                })
                .Build()
                .RunAsync();
    }
}
