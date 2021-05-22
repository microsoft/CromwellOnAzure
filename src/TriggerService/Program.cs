// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Configuration;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

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

            var environment = new CromwellOnAzureEnvironment(
                            serviceProvider.GetRequiredService<ILoggerFactory>(),
                            new AzureStorage(serviceProvider.GetRequiredService<ILoggerFactory>().CreateLogger<AzureStorage>(), cloudStorageAccount, new System.Net.Http.HttpClient()),
                            new CromwellApiClient.CromwellApiClient(cromwellUrl));

            serviceCollection.AddSingleton(s => new TriggerEngine(s.GetRequiredService<ILoggerFactory>(), environment, TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(30)));
            serviceProvider = serviceCollection.BuildServiceProvider();
            var engine = serviceProvider.GetService<TriggerEngine>();
            await engine.RunAsync();
        }
    }
}
