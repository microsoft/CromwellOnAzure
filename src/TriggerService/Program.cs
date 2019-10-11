// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Configuration;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using TriggerService.Core;

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
            var labName = Environment.GetEnvironmentVariable("LAB_NAME");
            var azStorageName = Environment.GetEnvironmentVariable("DefaultStorageAccountName");
            var cromwellUrl = ConfigurationManager.AppSettings.Get("CromwellUrl");

            var serviceCollection = new ServiceCollection()
                .AddLogging(loggingBuilder =>
                {
                    if (!string.IsNullOrWhiteSpace(instrumentationKey))
                    {
                        loggingBuilder.AddApplicationInsights(instrumentationKey);
                    }
                    else
                    {
                        Console.WriteLine("Warning: AppInsights key was null, and so AppInsights logging will not be enabled.  Check if this VM has Contributor access to the Application Insights instance.");
                    }

                    loggingBuilder.AddConsole();
                });

            var serviceProvider = serviceCollection.BuildServiceProvider();

            var lab = new Lab(
                            serviceProvider.GetRequiredService<ILoggerFactory>(),
                            labName,
                            new AzureStorage(serviceProvider.GetRequiredService<ILoggerFactory>().CreateLogger<AzureStorage>(), azStorageName, true),
                            new CromwellApiClient.CromwellApiClient(cromwellUrl));

            serviceCollection.AddTransient(s => new TriggerEngine(s.GetRequiredService<ILoggerFactory>(), lab));
            serviceProvider = serviceCollection.BuildServiceProvider();
            var engine = serviceProvider.GetService<TriggerEngine>();
            await engine.RunAsync();
        }
    }
}
