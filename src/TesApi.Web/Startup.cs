// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.IO;
using System.Linq;
using System.Reflection;
using Common;
using LazyCache;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Serialization;
using Tes.Models;
using Tes.Repository;
using TesApi.Filters;

namespace TesApi.Web
{
    /// <summary>
    /// Startup
    /// </summary>
    public class Startup
    {
        private const string DefaultAzureOfferDurableId = "MS-AZR-0003p";

        private readonly ILogger logger;
        private readonly IWebHostEnvironment hostingEnvironment;
        private readonly string azureOfferDurableId;

        /// <summary>
        /// Startup class for ASP.NET core
        /// </summary>
        public Startup(IConfiguration configuration, ILogger<Startup> logger, IWebHostEnvironment hostingEnvironment)
        {
            Configuration = configuration;
            this.hostingEnvironment = hostingEnvironment;
            this.logger = logger;
            azureOfferDurableId = Configuration.GetValue("AzureOfferDurableId", DefaultAzureOfferDurableId);
        }

        /// <summary>
        /// The application configuration
        /// </summary>
        private IConfiguration Configuration { get; }

        /// <summary>
        /// This method gets called by the runtime. Use this method to add services to the container.
        /// </summary>
        /// <param name="services">The Microsoft.Extensions.DependencyInjection.IServiceCollection to add the services to.</param>
        public void ConfigureServices(IServiceCollection services)
            => services.AddSingleton<IAppCache>(sp => new CachingService())

            .AddSingleton(sp => (IAzureProxy)ActivatorUtilities.CreateInstance<CachingWithRetriesAzureProxy>(sp, (IAzureProxy)sp.GetService(typeof(AzureProxy))))
            .AddSingleton(sp => ActivatorUtilities.CreateInstance<AzureProxy>(sp, Configuration["BatchAccountName"], azureOfferDurableId))

            .AddSingleton<CosmosCredentials>(sp =>
            {
                (var cosmosDbEndpoint, var cosmosDbKey) = ((IAzureProxy)sp.GetService(typeof(IAzureProxy))).GetCosmosDbEndpointAndKeyAsync(Configuration["CosmosDbAccountName"]).Result;
                return new(cosmosDbEndpoint, cosmosDbKey);
            })
            .AddSingleton<BatchPoolFactory>()

            .AddControllers()
            .AddNewtonsoftJson(opts =>
            {
                opts.SerializerSettings.ContractResolver = new CamelCasePropertyNamesContractResolver();
                opts.SerializerSettings.Converters.Add(new StringEnumConverter(new CamelCaseNamingStrategy()));
            }).Services

            .AddSingleton(sp =>
            {
                var cosmos = sp.GetService<CosmosCredentials>();
                return (IRepository<TesTask>)ActivatorUtilities.CreateInstance<CachingWithRetriesRepository<TesTask>>(sp, new CosmosDbRepository<TesTask>(cosmos.CosmosDbEndpoint, cosmos.CosmosDbKey, Constants.CosmosDbDatabaseId, Constants.CosmosDbContainerId, Constants.CosmosDbPartitionId));
            })

            .AddSingleton<IBatchScheduler, BatchScheduler>()
            .AddSingleton<IStorageAccessProvider, StorageAccessProvider>()

            .AddSwaggerGen(c =>
            {
                c.SwaggerDoc("0.3.0", new OpenApiInfo
                {
                    Version = "0.3.0",
                    Title = "Task Execution Service",
                    Description = "Task Execution Service (ASP.NET 5.0)",
                    Contact = new OpenApiContact()
                    {
                        Name = "Microsoft Genomics",
                        Url = new Uri("https://github.com/microsoft/CromwellOnAzure")
                    },
                    License = new OpenApiLicense()
                    {
                        Name = "MIT License",
                        // Identifier = "MIT" //TODO: when available, remove Url -- https://spec.openapis.org/oas/v3.1.0#fixed-fields-2
                        Url = new("https://spdx.org/licenses/MIT", UriKind.Absolute)
                    },
                });
                c.CustomSchemaIds(type => type.FullName);
                c.IncludeXmlComments($"{AppContext.BaseDirectory}{Path.DirectorySeparatorChar}{Assembly.GetEntryAssembly().GetName().Name}.xml");
                c.OperationFilter<GeneratePathParamsValidationFilter>();
            })

            .AddHostedService<Scheduler>()
            .AddHostedService<DeleteCompletedBatchJobsHostedService>()
            .AddHostedService<DeleteOrphanedBatchJobsHostedService>()
            .AddHostedService<DeleteOrphanedAutoPoolsHostedService>()
            .AddHostedService<RefreshVMSizesAndPricesHostedService>()

            // Configure AppInsights Azure Service when in PRODUCTION environment
            .IfThenElse(hostingEnvironment.IsProduction(),
                s =>
                {
                    var applicationInsightsAccountName = Configuration["ApplicationInsightsAccountName"];
                    var instrumentationKey = AzureProxy.GetAppInsightsInstrumentationKeyAsync(applicationInsightsAccountName).Result;

                    if (instrumentationKey is not null)
                    {
                        return s.AddApplicationInsightsTelemetry(instrumentationKey);
                    }

                    return s;
                },
                s => s.AddApplicationInsightsTelemetry());

        /// <summary>
        /// This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        /// </summary>
        /// <param name="app">An Microsoft.AspNetCore.Builder.IApplicationBuilder for the app to configure.</param>
        public void Configure(IApplicationBuilder app)
            => app.UseRouting()
            .UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            })

            .UseHttpsRedirection()

            .UseDefaultFiles()
            .UseStaticFiles()
            .UseSwagger(c =>
            {
                c.RouteTemplate = "swagger/{documentName}/openapi.json";
            })
            .UseSwaggerUI(c =>
            {
                c.SwaggerEndpoint("/swagger/0.3.0/openapi.json", "Task Execution Service");
            })

            .IfThenElse(hostingEnvironment.IsDevelopment(),
                s =>
                {
                    var r = s.UseDeveloperExceptionPage();
                    logger.LogInformation("Configuring for Development environment");
                    return r;
                },
                s =>
                {
                    var r = s.UseHsts();
                    logger.LogInformation("Configuring for Production environment");
                    return r;
                })

            .IfThenElse(false, s => s, s =>
            {
                var configurationUtils = ActivatorUtilities.GetServiceOrCreateInstance<ConfigurationUtils>(s.ApplicationServices);
                configurationUtils.ProcessAllowedVmSizesConfigurationFileAsync().Wait();
                return s;
            });
    }

    /// <summary>
    /// Metadata to access an Azure Cosmos DB.
    /// </summary>
    public sealed class CosmosCredentials
    {
        /// <summary>
        /// Creates a <see cref="CosmosCredentials"/>.
        /// </summary>
        /// <param name="cosmosDbEndpoint">The connection endpoint for the CosmosDB database account</param>
        /// <param name="cosmosDbKey">The Base 64 encoded value of the primary read-write key</param>
        /// <exception cref="ArgumentNullException"></exception>
        public CosmosCredentials(string cosmosDbEndpoint, string cosmosDbKey)
        {
            CosmosDbEndpoint = cosmosDbEndpoint ?? throw new ArgumentNullException(nameof(cosmosDbEndpoint));
            CosmosDbKey = cosmosDbKey ?? throw new ArgumentNullException(nameof(cosmosDbKey));
        }

        /// <summary>
        /// Gets the connection endpoint for the CosmosDB database account.
        /// </summary>
        public string CosmosDbEndpoint { get; }

        /// <summary>
        /// Gets Base 64 encoded value of the primary read-write key.
        /// </summary>
        public string CosmosDbKey { get; }
    }

    internal static class BooleanMethodSelectorExtensions
    {
        public static IApplicationBuilder IfThenElse(this IApplicationBuilder builder, bool @if, Func<IApplicationBuilder, IApplicationBuilder> then, Func<IApplicationBuilder, IApplicationBuilder> @else)
            => @if ? then(builder) : @else(builder);

        public static IServiceCollection IfThenElse(this IServiceCollection services, bool @if, Func<IServiceCollection, IServiceCollection> then, Func<IServiceCollection, IServiceCollection> @else)
            => @if ? then(services) : @else(services);
    }
}
