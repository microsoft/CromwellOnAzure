// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.IO;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using LazyCache;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using TesApi.Filters;
using TesApi.Models;

namespace TesApi.Web
{
    /// <summary>
    /// Startup
    /// </summary>
    public class Startup
    {
        private const string CosmosDbDatabaseId = "TES";
        private const string CosmosDbContainerId = "Tasks";
        private const string CosmosDbPartitionId = "01";
        private const string defaultAzureOfferDurableId = "MS-AZR-0003p";

        private readonly ILogger logger;
        private readonly ILoggerFactory loggerFactory;
        private readonly IWebHostEnvironment hostingEnvironment;
        private readonly string azureOfferDurableId;

        /// <summary>
        /// Startup class for ASP.NET core
        /// </summary>
        public Startup(IConfiguration configuration, ILoggerFactory loggerFactory, IWebHostEnvironment hostingEnvironment)
        {
            Configuration = configuration;
            this.hostingEnvironment = hostingEnvironment;
            logger = loggerFactory.CreateLogger<Startup>();
            this.loggerFactory = loggerFactory;
            azureOfferDurableId = Configuration.GetValue("AzureOfferDurableId", defaultAzureOfferDurableId);
        }

        /// <summary>
        /// The application configuration
        /// </summary>
        public IConfiguration Configuration { get; }

        /// <summary>
        /// This method gets called by the runtime. Use this method to add services to the container.
        /// </summary>
        /// <param name="services"></param>
        public void ConfigureServices(IServiceCollection services)
        {
            var cache = new CachingService();
            services.AddSingleton<IAppCache>(cache);

            var azureProxy = new AzureProxy(Configuration["BatchAccountName"], azureOfferDurableId, loggerFactory.CreateLogger<AzureProxy>());
            IAzureProxy cachingAzureProxy = new CachingWithRetriesAzureProxy(azureProxy, cache);

            services.AddSingleton(cachingAzureProxy);
            services.AddSingleton(azureProxy);

            services
                .AddControllers()
                .AddJsonOptions(opts =>
                {
                    opts.JsonSerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
                    opts.JsonSerializerOptions.Converters.Add(new JsonStringEnumConverter());
                });

            (var cosmosDbEndpoint, var cosmosDbKey) = azureProxy.GetCosmosDbEndpointAndKeyAsync(Configuration["CosmosDbAccountName"]).Result;

            var cosmosDbRepository = new CosmosDbRepository<TesTask>(cosmosDbEndpoint, cosmosDbKey, CosmosDbDatabaseId, CosmosDbContainerId, CosmosDbPartitionId);
            var repository = new CachingWithRetriesRepository<TesTask>(cosmosDbRepository);

            services.AddSingleton<IRepository<TesTask>>(repository);
            services.AddSingleton<IBatchScheduler>(new BatchScheduler(loggerFactory.CreateLogger<BatchScheduler>(), Configuration, cachingAzureProxy));

            services
                .AddSwaggerGen(c =>
                {
                    c.SwaggerDoc("0.3.0", new OpenApiInfo
                    {
                        Version = "0.3.0",
                        Title = "Task Execution Service",
                        Description = "Task Execution Service (ASP.NET Core 3.1)",
                        Contact = new OpenApiContact()
                        {
                            Name = "Microsoft Genomics",
                            Url = new Uri("https://github.com/microsoft/CromwellOnAzure")
                        },
                    });
                    c.CustomSchemaIds(type => type.FullName);
                    c.IncludeXmlComments($"{AppContext.BaseDirectory}{Path.DirectorySeparatorChar}{Assembly.GetEntryAssembly().GetName().Name}.xml");
                    c.OperationFilter<GeneratePathParamsValidationFilter>();
                });

            services.AddHostedService<Scheduler>();
            services.AddHostedService<DeleteCompletedBatchJobsHostedService>();
            services.AddHostedService<DeleteOrphanedAutoPoolsHostedService>();
            services.AddHostedService<RefreshVMSizesAndPricesHostedService>();

            // Configure AppInsights Azure Service when in PRODUCTION environment
            if (hostingEnvironment.IsProduction())
            {
                var applicationInsightsAccountName = Configuration["ApplicationInsightsAccountName"];
                var instrumentationKey = AzureProxy.GetAppInsightsInstrumentationKeyAsync(applicationInsightsAccountName).Result;

                if (instrumentationKey != null)
                {
                    services.AddApplicationInsightsTelemetry(instrumentationKey);
                }
            }
            else
            {
                services.AddApplicationInsightsTelemetry();
            }
        }

        /// <summary>
        /// This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        /// </summary>
        /// <param name="app"></param>
        /// <param name="env"></param>
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            app.UseRouting();
            app.UseEndpoints(endpoints =>
                {
                    endpoints.MapControllers();
                });

            app.UseHttpsRedirection();

            app
                .UseDefaultFiles()
                .UseStaticFiles()
                .UseSwagger(c =>
                {
                    c.RouteTemplate = "swagger/{documentName}/openapi.json";
                })
                .UseSwaggerUI(c =>
                {
                    c.SwaggerEndpoint("/swagger/0.3.0/openapi.json", "Task Execution Service");
                });

            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
                logger.LogInformation("Configuring for Development environment");
            }
            else
            {
                app.UseHsts();
                logger.LogInformation("Configuring for Production environment");
            }
        }
    }
}
