// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Security.Cryptography;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.ApplicationInsights;

namespace TesApi.Web
{
    /// <summary>
    /// Program
    /// </summary>
    public class Program
    {
        /// <summary>
        /// Main
        /// </summary>
        /// <param name="args"></param>
        public static void Main(string[] args)
            => CreateWebHostBuilder(args).Build().Run();

        private static void EnsureHostname(WebHostBuilderContext context, IConfigurationBuilder config)
        {
            //Environment.SetEnvironmentVariable("HOSTNAME", System.Net.Dns.GetHostName());
            if (Environment.GetEnvironmentVariable("Name") is null)
            {
                // Path.Combine(AppContext.BaseDirectory, "DefaultVmPrices.json")
                var hostJson = new System.IO.FileInfo(System.IO.Path.Combine(context.HostingEnvironment.ContentRootPath, "host.json"));

                if (!hostJson.Exists)
                {
                    var blob = new byte[6];
                    RandomNumberGenerator.Fill(blob);
                    var host = BatchUtils.ConvertToBase32(blob).TrimEnd('=');
                    Console.WriteLine($"Creating host.json with Name={host}");
                    using var writer = hostJson.CreateText();
                    writer.Write(BatchUtils.WriteJson(new HostJson { Name = host }));
                    writer.Close();
                }

                config.AddJsonFile(hostJson.FullName);
            }

            config.AddEnvironmentVariables(); // For Docker-Compose
        }

        /// <summary>
        /// Create the web host builder.
        /// </summary>
        /// <param name="args"></param>
        /// <returns><see cref="IWebHostBuilder"/></returns>
        public static IWebHostBuilder CreateWebHostBuilder(string[] args)
            => WebHost.CreateDefaultBuilder<Startup>(args)
                .UseUrls("http://0.0.0.0:80/")
                .ConfigureAppConfiguration(EnsureHostname)
                .ConfigureLogging((context, logging) =>
                {
                    try
                    {
                        if (context.HostingEnvironment.IsProduction())
                        {
                            var applicationInsightsAccountName = context.Configuration["ApplicationInsightsAccountName"];
                            Console.WriteLine($"ApplicationInsightsAccountName: {applicationInsightsAccountName}");
                            var instrumentationKey = AzureProxy.GetAppInsightsInstrumentationKeyAsync(applicationInsightsAccountName).Result;

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
                                    });
                            }
                        }
                        else
                        {
                            logging.AddApplicationInsights();
                            logging.AddDebug();
                            logging.AddConsole();
                        }

                        // Optional: Apply filters to configure LogLevel Trace or above is sent to
                        // ApplicationInsights for all categories.
                        logging.AddFilter<ApplicationInsightsLoggerProvider>("System", LogLevel.Warning);

                        // Additional filtering For category starting in "Microsoft",
                        // only Warning or above will be sent to Application Insights.
                        logging.AddFilter<ApplicationInsightsLoggerProvider>("Microsoft", LogLevel.Warning);

                        // The following configures LogLevel Information or above to be sent to
                        // Application Insights for categories starting with "TesApi".
                        logging.AddFilter<ApplicationInsightsLoggerProvider>("TesApi", LogLevel.Information);
                    }
                    catch (Exception exc)
                    {
                        Console.WriteLine($"Exception while configuring logging: {exc}");
                        throw;
                    }
                });

        private class HostJson
        {
            public string Name { get; set; }
        }
    }
}
