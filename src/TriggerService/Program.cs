// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TriggerService
{
    internal class Program
    {
        public Program()
            => Common.NewtonsoftJsonSafeInit.SetDefaultSettings();

        public static async Task Main()
            => await Host.CreateDefaultBuilder()
                .ConfigureAppConfiguration((context, config) =>
                {
                    config.AddEnvironmentVariables();
                })
                .ConfigureLogging(async (context, logging) =>
                    {
                        try
                        {
                            if (context.HostingEnvironment.IsProduction())
                            {
                                var applicationInsightsAccountName = context.Configuration["ApplicationInsightsAccountName"];
                                Console.WriteLine($"ApplicationInsightsAccountName: {applicationInsightsAccountName}");
                                var instrumentationKey = await AzureStorage.GetAppInsightsInstrumentationKeyAsync(Environment.GetEnvironmentVariable("ApplicationInsightsAccountName"));

                                if (instrumentationKey is not null)
                                {
                                    var connectionString = $"InstrumentationKey={instrumentationKey}";
                                    logging.AddApplicationInsights(
                                        configuration =>
                                        {
                                            configuration.ConnectionString = connectionString;
                                        },
                                        options =>
                                        {
                                            options.TrackExceptionsAsExceptionTelemetry = false;
                                        });
                                }
                            }
                            else
                            {
                                logging.AddApplicationInsights();
                                logging.AddConsole();
                            }

                            //// Optional: Apply filters to configure LogLevel Trace or above is sent to
                            //// ApplicationInsights for all categories.
                            //logging.AddFilter<ApplicationInsightsLoggerProvider>("System", LogLevel.Warning);

                            //// Additional filtering For category starting in "Microsoft",
                            //// only Warning or above will be sent to Application Insights.
                            //logging.AddFilter<ApplicationInsightsLoggerProvider>("Microsoft", LogLevel.Warning);

                            //// The following configures LogLevel Information or above to be sent to
                            //// Application Insights for categories starting with "TesApi".
                            //logging.AddFilter<ApplicationInsightsLoggerProvider>("TesApi", LogLevel.Information);
                        }
                        catch (Exception exc)
                        {
                            Console.WriteLine($"Exception while configuring logging: {exc}");
                            throw;
                        }
                    })
                .ConfigureServices(services =>
                {
                    services.AddSingleton<ICromwellOnAzureEnvironment, CromwellOnAzureEnvironment>();
                })
                .Build().RunAsync();
    }
}
