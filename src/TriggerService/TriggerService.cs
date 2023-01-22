// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading.Tasks;
using CromwellApiClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

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
                    config.AddEnvironmentVariables();
                    config.AddJsonFile("appsettings.json");
                })
                .ConfigureLogging(async (context, logging) =>
                    {
                        var options = new TriggerServiceOptions();
                        context.Configuration.GetSection(TriggerServiceOptions.TriggerServiceOptionsSectionName).Bind(options);
                        Console.WriteLine($"ApplicationInsightsAccountName: {options.ApplicationInsightsAccountName}");
                        var instrumentationKey = await AzureStorage.GetAppInsightsInstrumentationKeyAsync(options.ApplicationInsightsAccountName);

                        if (!string.IsNullOrWhiteSpace(instrumentationKey))
                        {
                            logging.AddApplicationInsights(
                                configuration =>
                                {
                                    configuration.ConnectionString = $"InstrumentationKey={instrumentationKey}";
                                },
                                options =>
                                {
                                    options.TrackExceptionsAsExceptionTelemetry = false;
                                });
                        }
                    })
                .ConfigureServices((hostBuilderContext, serviceCollection) =>
                {
                    serviceCollection.Configure<CromwellApiClientOptions>(hostBuilderContext.Configuration.GetSection(CromwellApiClientOptions.CromwellApiClientOptionsSectionName));
                    serviceCollection.Configure<TriggerServiceOptions>(hostBuilderContext.Configuration.GetSection(TriggerServiceOptions.TriggerServiceOptionsSectionName));
                    serviceCollection.AddTransient<ICromwellApiClient, CromwellApiClient.CromwellApiClient>();
                    serviceCollection.AddHostedService<TriggerHostedService>();
                })
                .Build()
                .RunAsync();
    }
}
