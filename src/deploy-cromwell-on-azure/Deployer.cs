// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Azure.Management.Fluent;
using Microsoft.Azure.Management.Graph.RBAC.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Extensions.Logging;
using Microsoft.Rest;
using Microsoft.Rest.Azure;
using Newtonsoft.Json;
using TriggerService.Core;

namespace CromwellOnAzureDeployer
{
    public class Deployer
    {
        private const string LabName = "lab1";
        private const string InstallScriptUrl = "https://raw.githubusercontent.com/microsoft/CromwellOnAzure/master/src/deploy-cromwell-on-azure/scripts/install-cromwellazure.sh?token=AFSS5TZ4HZMK2PSXMCWGNHS5VGQFG";
        private readonly ILoggerFactory loggerFactory;
        private readonly ILogger<Deployer> logger;
        private IAzureStorage azureStorage { get; set; }
        private Configuration configuration { get; set; }
        private IAzure azureFluentApi { get; set; }
        private AzureCredentials azureCredentials { get; set; }
        private Stopwatch stepTimer { get; set; } = new Stopwatch();

        public Deployer(ILoggerFactory loggerFactory, Configuration configuration)
        {
            this.loggerFactory = loggerFactory;
            logger = loggerFactory.CreateLogger<Deployer>();
            this.configuration = configuration;
        }

        public async Task DeployAsync()
        {
            Console.WriteLine("Running...");

            await ValidateConfigurationAsync();

            var region = Region.Create(configuration.RegionName);

            var mainTimer = new Stopwatch();
            mainTimer.Start();

            try
            {
                await RefreshAzureTokenAsync();

                Console.WriteLine();
                Console.WriteLine($"VM host: {configuration.VmName}.{region.Name}.cloudapp.azure.com");
                Console.WriteLine($"VM username: {configuration.VmUsername}");
                Console.WriteLine($"VM password: {configuration.VmPassword}");
                Console.WriteLine();

                stepTimer.Start();

                await ExecuteWithCredentialsAsync(async (azureCredentials, azure) =>
                {
                    Console.WriteLine($"Creating Resource Group: {configuration.ResourceGroupName}");

                    var resourceGroup = await azure.ResourceGroups.Define(configuration.ResourceGroupName)
                        .WithRegion(region)
                        .CreateAsync();

                    WriteExecutionTime();
                    return resourceGroup;
                });

                var batchAccount = await ExecuteWithCredentialsAsync(async (azureCredentials, azure) =>
                {
                    Console.WriteLine($"Creating Batch Account: {configuration.BatchAccountName}");

                    var batchAccountResource = await azure.BatchAccounts.Define(configuration.BatchAccountName)
                        .WithRegion(region)
                        .WithExistingResourceGroup(configuration.ResourceGroupName)
                        .CreateAsync();

                    WriteExecutionTime();
                    return batchAccountResource;
                });

                var appInsights = await ExecuteWithCredentialsAsync(async (azureCredentials, azure) =>
                {
                    Console.WriteLine($"Creating Application Insights: {configuration.ApplicationInsightsAccountName}");

                    var appInsightsResource = await ResourceManager
                        .Configure()
                        .Authenticate(azureCredentials)
                        .WithSubscription(configuration.SubscriptionId)
                        .GenericResources.Define(configuration.ApplicationInsightsAccountName)
                        .WithRegion(region)
                        .WithExistingResourceGroup(configuration.ResourceGroupName)
                        .WithResourceType("components")
                        .WithProviderNamespace("microsoft.insights")
                        .WithoutPlan()
                        .WithApiVersion("2015-05-01")
                        .WithParentResource(string.Empty)
                        .WithProperties(new Dictionary<string, string>() { { "Application_Type", "other" } })
                        .CreateAsync();

                    WriteExecutionTime();

                    return appInsightsResource;
                });

                var linuxVm = await ExecuteWithCredentialsAsync(async (azureCredentials, azure) =>
                {
                    const int dataDiskSizeGiB = 32;
                    const int dataDiskLun = 0;

                    Console.WriteLine($"Creating Linux VM: {configuration.VmName}");

                    var vm = await azure.VirtualMachines.Define(configuration.VmName)
                       .WithRegion(region)
                       .WithExistingResourceGroup(configuration.ResourceGroupName)
                       .WithNewPrimaryNetwork(configuration.VnetAddressSpace)
                       .WithPrimaryPrivateIPAddressDynamic()
                       .WithNewPrimaryPublicIPAddress(configuration.VmName)
                       .WithPopularLinuxImage(configuration.VmImage)
                       .WithRootUsername(configuration.VmUsername)
                       .WithRootPassword(configuration.VmPassword)
                       .WithNewDataDisk(dataDiskSizeGiB, dataDiskLun, Microsoft.Azure.Management.Compute.Fluent.Models.CachingTypes.None)
                       .WithSize(configuration.VmSize)
                       .WithSystemAssignedManagedServiceIdentity()
                       .CreateAsync();

                    WriteExecutionTime();
                    return vm;
                });

                var vmManagedIdentity = linuxVm.SystemAssignedManagedServiceIdentityPrincipalId;

                await ExecuteWithCredentialsAsync(async (azureCredentials, azure) =>
                {
                    Console.WriteLine($"Assigning {BuiltInRole.Contributor} role for VM to App Insights resource scope.");

                    var roleAssignment = await azure.AccessManagement.RoleAssignments.Define(Guid.NewGuid().ToString())
                     .ForObjectId(vmManagedIdentity)
                     .WithBuiltInRole(BuiltInRole.Contributor)
                     .WithResourceScope(appInsights)
                     .CreateAsync();

                    WriteExecutionTime();
                    return roleAssignment;
                });

                await ExecuteWithCredentialsAsync(async (azureCredentials, azure) =>
                {
                    Console.WriteLine($"Assigning {BuiltInRole.BillingReader} role for VM to Subscription scope.");

                    var roleAssignment = await azure.AccessManagement.RoleAssignments.Define(Guid.NewGuid().ToString())
                        .ForObjectId(vmManagedIdentity)
                        .WithBuiltInRole(BuiltInRole.BillingReader)
                        .WithSubscriptionScope(configuration.SubscriptionId)
                        .CreateAsync();

                    WriteExecutionTime();
                    return roleAssignment;
                });

                var cosmosDb = await ExecuteWithCredentialsAsync(async (azureCredentials, azure) =>
                {
                    Console.WriteLine($"Creating Cosmos DB: {configuration.CosmosDbAccountName}");

                    var cosmosDbResource = await azure.CosmosDBAccounts.Define(configuration.CosmosDbAccountName)
                       .WithRegion(region)
                       .WithExistingResourceGroup(configuration.ResourceGroupName)
                       .WithDataModelSql()
                       .WithSessionConsistency()
                       .WithWriteReplication(region)
                       .CreateAsync();

                    WriteExecutionTime();
                    return cosmosDbResource;
                });

                await ExecuteWithCredentialsAsync(async (azureCredentials, azure) =>
                {
                    Console.WriteLine($"Assigning {BuiltInRole.Contributor} role for VM to Cosmos DB resource scope.");

                    var roleAssignment = await azure.AccessManagement.RoleAssignments.Define(Guid.NewGuid().ToString())
                     .ForObjectId(vmManagedIdentity)
                     .WithBuiltInRole(BuiltInRole.Contributor)
                     .WithResourceScope(cosmosDb)
                     .CreateAsync();

                    WriteExecutionTime();
                    return roleAssignment;
                });

                await ExecuteWithCredentialsAsync(async (azureCredentials, azure) =>
                {
                    Console.WriteLine($"Assigning {BuiltInRole.Contributor} role for VM to Batch Account resource scope.");

                    var roleAssignment = await azure.AccessManagement.RoleAssignments.Define(Guid.NewGuid().ToString())
                     .ForObjectId(vmManagedIdentity)
                     .WithBuiltInRole(BuiltInRole.Contributor)
                     .WithResourceScope(batchAccount)
                     .CreateAsync();

                    WriteExecutionTime();
                    return roleAssignment;
                });

                var storageAccount = await ExecuteWithCredentialsAsync(async (azureCredentials, azure) =>
                {
                    Console.WriteLine($"Creating Storage Account: {configuration.StorageAccountName}");

                    var storageAccountResource = await azure.StorageAccounts.Define(configuration.StorageAccountName)
                        .WithRegion(region)
                        .WithExistingResourceGroup(configuration.ResourceGroupName)
                        .CreateAsync();

                    azureStorage = new AzureStorage(loggerFactory.CreateLogger<AzureStorage>(), configuration.StorageAccountName, true);
                    await azureStorage.CreateDefaultContainersAsync();

                    WriteExecutionTime();
                    return storageAccountResource;
                });

                await ExecuteWithCredentialsAsync(async (azureCredentials, azure) =>
                {
                    Console.WriteLine($"Assigning {BuiltInRole.Contributor} role for VM to Storage Account resource scope.");

                    var roleAssignment = await azure.AccessManagement.RoleAssignments.Define(Guid.NewGuid().ToString())
                     .ForObjectId(vmManagedIdentity)
                     .WithBuiltInRole(BuiltInRole.Contributor)
                     .WithResourceScope(storageAccount)
                     .CreateAsync();

                    WriteExecutionTime();
                    return roleAssignment;
                });

                var linuxCustomScriptExtensionName = "initial-setup";

                await ExecuteWithCredentialsAsync(async (azureCredentials, azure) =>
                {
                    const string linuxCustomScriptExtensionPublisherName = "Microsoft.Azure.Extensions";
                    const string linuxCustomScriptExtensionTypeName = "CustomScript";
                    const string linuxCustomScriptExtensionVersionName = "2.0";
                    var acrAccount = configuration.ContainerRegistryServer.Substring(0, configuration.ContainerRegistryServer.IndexOf("."));
                    var bashCommand = $"bash install-cromwellazure.sh {LabName} {configuration.StorageAccountName} {configuration.CosmosDbAccountName} {configuration.BatchAccountName} {configuration.ApplicationInsightsAccountName}";
                    Console.WriteLine("Running Linux VM bash script.");

                    var vmResource = await linuxVm.Update()
                        .DefineNewExtension(linuxCustomScriptExtensionName)
                            .WithPublisher(linuxCustomScriptExtensionPublisherName)
                            .WithType(linuxCustomScriptExtensionTypeName)
                            .WithVersion(linuxCustomScriptExtensionVersionName)
                            .WithMinorVersionAutoUpgrade()
                            .WithPublicSetting("fileUris", new List<string> { InstallScriptUrl })
                            .WithPublicSetting("commandToExecute", bashCommand)
                        .Attach()
                        .ApplyAsync();

                    WriteExecutionTime();
                    return vmResource;
                });

                await ExecuteWithCredentialsAsync(async (azureCredentials, azure) =>
                {
                    Console.WriteLine("Removing existing extension...");

                    var vmResource = await linuxVm.Update()
                        .WithoutExtension(linuxCustomScriptExtensionName)
                        .ApplyAsync();

                    WriteExecutionTime();
                    return vmResource;
                });

                await ExecuteWithCredentialsAsync(async (azureCredentials, azure) =>
                {
                    Console.WriteLine("Restarting VM...");
                    await linuxVm.RestartAsync();
                    WriteExecutionTime();
                    return Task.FromResult(false);
                });

                var isTestWorkflowSuccessful = await VerifyInstallationAsync();

                if (!isTestWorkflowSuccessful)
                {
                    await DeleteResourceGroupIfUserConsentsAsync();
                }
            }
            catch (CloudException cloudException)
            {
                var json = cloudException.Response.Content;
                Console.WriteLine(json);
                Debugger.Break();
                WriteGeneralRetryMessageToConsole();
                await DeleteResourceGroupIfUserConsentsAsync();
            }
            catch (Exception exc)
            {
                Console.WriteLine(exc);
                Debugger.Break();
                WriteGeneralRetryMessageToConsole();
                await DeleteResourceGroupIfUserConsentsAsync();
            }

            Console.WriteLine($"Completed in {mainTimer.Elapsed.TotalMinutes:n1} minutes.");
        }

        private static void ValidateMainIdentifierPrefix(string prefix)
        {
            const int maxLength = 12;

            if (prefix.Any(c => !char.IsLetter(c)))
            {
                throw new ValidationException($"MainIdentifierPrefix must only contain letters.");
            }

            if (prefix.Length > maxLength)
            {
                throw new ValidationException($"MainIdentifierPrefix too long - must be {maxLength} characters or less.");
            }
        }

        private static void ValidateRegionName(string regionName)
        {
            if (string.IsNullOrWhiteSpace(regionName))
            {
                throw new ValidationException("RegionName is required.");
            }

            var invalidRegionsRegex = new Regex("^(germany|china|us)");
            var validRegionNames = Region.Values.Select(r => r.Name).Where(rn => !invalidRegionsRegex.IsMatch(rn));

            if (!validRegionNames.Contains(regionName, StringComparer.OrdinalIgnoreCase))
            {
                throw new ValidationException($"Invalid region name '{regionName}'. Valid names are: {string.Join(", ", validRegionNames)}");
            }
        }

        private static async Task ValidateSubscriptionAsync(string subscriptionId)
        {
            if (string.IsNullOrWhiteSpace(subscriptionId))
            {
                throw new ValidationException($"SubcriptionId is required.");
            }

            var azure = Azure
                .Configure()
                .WithLogLevel(HttpLoggingDelegatingHandler.Level.Basic)
                .Authenticate(await GetAzureCredentialsAsync());

            var subscriptionExists = (await azure.Subscriptions.ListAsync()).Any(sub => sub.Inner.SubscriptionId.Equals(subscriptionId, StringComparison.OrdinalIgnoreCase));

            if (!subscriptionExists)
            {
                throw new ValidationException($"Invalid or inaccessible subcription id '{subscriptionId}'. Make sure that subscription exists and that you have Contributor access to it.", displayExample: false);
            }
        }

        private static async Task<AzureCredentials> GetAzureCredentialsAsync()
        {
            try
            {
                var tokenProvider = new AzureServiceTokenProvider();
                var accessToken = await tokenProvider.GetAccessTokenAsync("https://management.azure.com/");
                return new AzureCredentials(new TokenCredentials(accessToken), null, null, AzureEnvironment.AzureGlobalCloud);
            }
            catch (AzureServiceTokenProviderException)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("No access token found.  Please install the Azure CLI and login with 'az login'");
                Console.ResetColor();
                Console.WriteLine("Link: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli");
                Environment.Exit(1);
                throw;
            }
        }

        private async Task ValidateConfigurationAsync()
        {
            try
            {
                ValidateMainIdentifierPrefix(configuration.MainIdentifierPrefix);
                ValidateRegionName(configuration.RegionName);
                await ValidateSubscriptionAsync(configuration.SubscriptionId);
            }
            catch (ValidationException validationException)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(validationException.Reason);
                Console.ResetColor();

                if (validationException.DisplayExample)
                {
                    Console.WriteLine();
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.Write($"Example: ");
                    Console.ResetColor();
                    Console.WriteLine($"deploy-cromwell-on-azure --subscriptionid {Guid.NewGuid()} --regionname westus2 --mainidentifierprefix coa");
                    Console.ResetColor();
                }

                Environment.Exit(1);
            }
        }

        private async Task DeleteResourceGroupIfUserConsentsAsync()
        {
            Console.WriteLine();
            Console.Write("Delete the resource group?  Type 'yes' and press enter, or, press any key to exit: ");
            var userResponse = Console.ReadLine();

            if (userResponse.Equals("yes", StringComparison.OrdinalIgnoreCase))
            {
                await ExecuteWithCredentialsAsync(async (azureCredentials, azure) =>
                {
                    Console.WriteLine("Deleting resource group...");
                    await azure.ResourceGroups.DeleteByNameAsync(configuration.ResourceGroupName);
                    WriteExecutionTime();
                    return Task.FromResult(false);
                });
            }
        }

        private async Task<bool> VerifyInstallationAsync()
        {
            Console.WriteLine("Running a test workflow...");
            var isTestWorkflowSuccessful = await TestWorkflowAsync();
            WriteExecutionTime();

            if (isTestWorkflowSuccessful)
            {
                Console.WriteLine();
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"Test workflow succeeded.");
                Console.ResetColor();
                Console.WriteLine();
                Console.WriteLine("Learn more about how to use Cromwell on Azure: https://github.com/microsoft/CromwellOnAzure");
                Console.WriteLine();
            }
            else
            {
                Console.WriteLine();
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Test workflow failed.");
                Console.ResetColor();
                Console.WriteLine();
                WriteGeneralRetryMessageToConsole();
                Console.WriteLine();
            }

            return isTestWorkflowSuccessful;
        }

        private void WriteGeneralRetryMessageToConsole()
        {
            Console.WriteLine("Please try deployment again, and create an issue if this continues to fail: https://github.com/microsoft/CromwellOnAzure/issues");
        }

        private async Task<bool> TestWorkflowAsync()
        {
            var id = Guid.NewGuid();
            const string container = "workflows";
            var wdlUrl = await azureStorage.UploadFileTextAsync(File.ReadAllText(Path.Combine(System.AppContext.BaseDirectory, "test.wdl")), "inputs", "test/test.wdl");
            var jsonUrl = await azureStorage.UploadFileTextAsync(File.ReadAllText(Path.Combine(System.AppContext.BaseDirectory, "test.json")), "inputs", "test/test.json");

            var workflow = new Workflow
            {
                WorkflowUrl = wdlUrl,
                WorkflowInputsUrl = jsonUrl
            };

            var json = JsonConvert.SerializeObject(workflow);

            await azureStorage.UploadFileTextAsync(json, container, $"new/{id}.json");
            return await IsSuccessfulAfterLongPollingAsync(id, container);
        }

        private async Task<bool> IsSuccessfulAfterLongPollingAsync(Guid id, string container)
        {
            while (true)
            {
                try
                {
                    var isSucceeded = await azureStorage.IsSingleBlobExistsFromPrefixAsync(container, $"succeeded/{id}");
                    var isFailed = await azureStorage.IsSingleBlobExistsFromPrefixAsync(container, $"failed/{id}");

                    if (isSucceeded || isFailed)
                    {
                        return isSucceeded && !isFailed;
                    }
                }
                catch (Exception exc)
                {
                    // "Server is busy" occasionally can be ignored
                    Console.WriteLine(exc.Message);
                }

                await Task.Delay(5000);
            }
        }

        private async Task<T> ExecuteWithCredentialsAsync<T>(Func<AzureCredentials, IAzure, Task<T>> func)
        {
            const int retryCount = 3;

            for (var i = 0; i < retryCount; i++)
            {
                try
                {
                    return await func.Invoke(azureCredentials, azureFluentApi);
                }
                catch (CloudException cloudException)
                {
                    if (cloudException.ToCloudErrorType() == CloudErrorType.ExpiredAuthenticationToken)
                    {
                        await RefreshAzureTokenAsync();
                        continue;
                    }

                    throw;
                }
            }

            throw new Exception($"Failed after {retryCount} attempts");
        }

        private async Task RefreshAzureTokenAsync()
        {
            Console.WriteLine("Getting Azure credentials...");
            azureCredentials = await GetAzureCredentialsAsync();

            azureFluentApi = Azure
                .Configure()
                .WithLogLevel(HttpLoggingDelegatingHandler.Level.Basic)
                .Authenticate(azureCredentials)
                .WithSubscription(configuration.SubscriptionId);
        }

        private void WriteExecutionTime()
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"Completed in {stepTimer.Elapsed.TotalSeconds:n0}s");
            Console.ResetColor();
            stepTimer.Restart();
        }

        private class ValidationException : Exception
        {
            public string Reason { get; set; }
            public bool DisplayExample { get; set; }

            public ValidationException(string reason, bool displayExample = true)
            {
                Reason = reason;
                DisplayExample = displayExample;
            }
        }
    }
}
