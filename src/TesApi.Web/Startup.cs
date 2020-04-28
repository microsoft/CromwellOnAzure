﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.IO;
using System.Reflection;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Swashbuckle.AspNetCore.Swagger;
using Swashbuckle.AspNetCore.SwaggerGen;
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
        private const string CosmosDbCollectionId = "Tasks";
        private const string CosmosDbPartitionId = "01";
        private const string defaultAzureOfferDurableId = "MS-AZR-0003p";

        private readonly ILogger logger;
        private readonly ILoggerFactory loggerFactory;
        private readonly IHostingEnvironment hostingEnvironment;
        private readonly string azureOfferDurableId;

        /// <summary>
        /// Startup class for ASP.NET core
        /// </summary>
        public Startup(IConfiguration configuration, ILoggerFactory loggerFactory, IHostingEnvironment hostingEnvironment)
        {
            Configuration = configuration;
            this.hostingEnvironment = hostingEnvironment;
            logger = loggerFactory.CreateLogger<Startup>();
            this.loggerFactory = loggerFactory;
            azureOfferDurableId = Configuration["AzureOfferDurableId"] ?? defaultAzureOfferDurableId;
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
            IAzureProxy azureProxy = new AzureProxy(Configuration["BatchAccountName"], azureOfferDurableId, loggerFactory.CreateLogger<AzureProxy>());
            IAzureProxy cachingAzureProxy = new CachingAzureProxy(azureProxy, loggerFactory.CreateLogger<CachingAzureProxy>());

            services.AddSingleton(cachingAzureProxy);
            services.AddMvc().SetCompatibilityVersion(CompatibilityVersion.Latest);

            (var cosmosDbEndpoint, var cosmosDbKey) = azureProxy.GetCosmosDbEndpointAndKeyAsync(Configuration["CosmosDbAccountName"]).Result;
            services.AddSingleton<IRepository<TesTask>>(new CosmosDbRepository<TesTask>(cosmosDbEndpoint, cosmosDbKey, CosmosDbDatabaseId, CosmosDbCollectionId, CosmosDbPartitionId));
            services.AddSingleton<IBatchScheduler>(new BatchScheduler(loggerFactory.CreateLogger<BatchScheduler>(), Configuration, cachingAzureProxy));

            services
                .AddSwaggerGen(c =>
                {
                    c.SwaggerDoc("0.3.0", new Info
                    {
                        Version = "0.3.0",
                        Title = "Task Execution Service",
                        Description = "Task Execution Service (ASP.NET Core 2.0)",
                        Contact = new Contact()
                        {
                            Name = "OpenAPI-Generator Contributors",
                            Url = "https://github.com/openapitools/openapi-generator",
                            Email = ""
                        },
                        TermsOfService = ""
                    });
                    c.CustomSchemaIds(type => type.FriendlyId(true));
                    c.DescribeAllEnumsAsStrings();
                    c.IncludeXmlComments($"{AppContext.BaseDirectory}{Path.DirectorySeparatorChar}{Assembly.GetEntryAssembly().GetName().Name}.xml");
                    c.OperationFilter<GeneratePathParamsValidationFilter>();
                });

            services.AddHostedService<Scheduler>();
            services.AddHostedService<DeleteCompletedBatchJobsHostedService>();
            services.AddHostedService<DeleteOrphanedAutoPoolsHostedService>();

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
        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {
            app.UseHttpsRedirection();
            app
                .UseMvc()
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
