// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading.Tasks;
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
                .ConfigureAppConfiguration((context, config) =>
                {
                    config.AddJsonFile("appsettings.json");
                    config.AddEnvironmentVariables();
                })
                .ConfigureLogging(async (context, logging) =>
                    {
                        logging.AddConsole();
                        var options = new TriggerServiceOptions();
                        context.Configuration.GetSection(TriggerServiceOptions.TriggerServiceOptionsSectionName).Bind(options);

                        if (string.IsNullOrWhiteSpace(options.ApplicationInsightsAccountName))
                        {
                            return;
                        }

                        Console.WriteLine($"ApplicationInsightsAccountName: {options.ApplicationInsightsAccountName}");
                        var instrumentationKey = await AzureStorage.GetAppInsightsInstrumentationKeyAsync(options.ApplicationInsightsAccountName);

                        if (string.IsNullOrWhiteSpace(instrumentationKey))
                        {
                            throw new Exception($"No instrumentation key found for {options.ApplicationInsightsAccountName}, does this service have Contributor access or equivalent to {options.ApplicationInsightsAccountName}?");
                        }

                        logging.AddApplicationInsights(
                            configuration =>
                            {
                                configuration.ConnectionString = $"InstrumentationKey={instrumentationKey}";
                            },
                            options =>
                            {
                                options.TrackExceptionsAsExceptionTelemetry = false;
                            });

                    })
                .ConfigureServices((hostBuilderContext, serviceCollection) =>
                {
                    serviceCollection.Configure<CromwellApiClientOptions>(hostBuilderContext.Configuration.GetSection(CromwellApiClientOptions.CromwellApiClientOptionsSectionName));
                    serviceCollection.Configure<TriggerServiceOptions>(hostBuilderContext.Configuration.GetSection(TriggerServiceOptions.TriggerServiceOptionsSectionName));
                    serviceCollection.Configure<PostgreSqlOptions>(hostBuilderContext.Configuration.GetSection(PostgreSqlOptions.GetConfigurationSectionName("Tes")));
                    serviceCollection.AddSingleton<ICromwellApiClient, CromwellApiClient.CromwellApiClient>();
                    serviceCollection.AddSingleton<IRepository<TesTask>, TesTaskPostgreSqlRepository>();
                    serviceCollection.AddSingleton<IAzureStorageUtility, AzureStorageUtility>();
                    serviceCollection.AddHostedService<TriggerHostedService>();
                })
                .Build()
                .RunAsync();
    }
}
