using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Management.CosmosDB.Fluent;
using Microsoft.Azure.Management.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Microsoft.Rest;
using Microsoft.Rest.Azure;
using Newtonsoft.Json;

namespace CromwellOnAzureDeployer
{
    public static class DatabaseAccountsOperations2Extensions
    {
        public static ICosmosDBAccounts2 CosmosDBAccounts2(this IAzure azure)
            => new SqlResources2(azure);

        public static ICosmosDBAccount2 CosmosDBAccount2(this ICosmosDBAccount cosmosDBAccount)
            => new SqlResource2(cosmosDBAccount);
    }

    public interface ICosmosDBAccounts2 : IHasManager<ICosmosDBManager>, IHasInner<IDatabaseAccountsOperations2>
    {
        Task<SqlRoleAssignmentGetResultsInner> CreateUpdateSqlRoleAssignment(string resourceGroupName, string accountName, string roleAssignmentId, SqlRoleAssignmentCreateUpdateParameters createUpdateParameters, CancellationToken cancellationToken = default);
        Task<SqlRoleDefinitionGetResultsInner> CreateUpdateSqlRoleDefinition(string resourceGroupName, string accountName, string roleDefinitionId, SqlRoleDefinitionCreateUpdateParameters createUpdateParameters, CancellationToken cancellationToken = default);
        Task DeleteSqlRoleAssignment(string resourceGroupName, string accountName, string roleAssignmentId, CancellationToken cancellationToken = default);
        Task DeleteSqlRoleDefinition(string resourceGroupName, string accountName, string roleDefinitionId, CancellationToken cancellationToken = default);
        Task<SqlRoleAssignmentGetResultsInner> GetSqlRoleAssignment(string resourceGroupName, string accountName, string roleAssignmentId, CancellationToken cancellationToken = default);
        Task<SqlRoleDefinitionGetResultsInner> GetSqlRoleDefinition(string resourceGroupName, string accountName, string roleDefinitionId, CancellationToken cancellationToken = default);
        Task<IEnumerable<SqlRoleAssignmentGetResultsInner>> ListSqlRoleAssignmenst(string resourceGroupName, string accountName, CancellationToken cancellationToken = default);
        Task<IEnumerable<SqlRoleDefinitionGetResultsInner>> ListSqlRoleDefinitions(string resourceGroupName, string accountName, CancellationToken cancellationToken = default);
    }

    public interface ICosmosDBAccount2 : IHasManager<ICosmosDBManager>, IHasInner<IDatabaseAccountOperations2>
    {
        Task<SqlRoleAssignmentGetResultsInner> CreateUpdateSqlRoleAssignment(string roleAssignmentId, SqlRoleAssignmentCreateUpdateParameters createUpdateParameters, CancellationToken cancellationToken = default);
        Task<SqlRoleDefinitionGetResultsInner> CreateUpdateSqlRoleDefinition(string roleDefinitionId, SqlRoleDefinitionCreateUpdateParameters createUpdateParameters, CancellationToken cancellationToken = default);
        Task DeleteSqlRoleAssignment(string roleAssignmentId, CancellationToken cancellationToken = default);
        Task DeleteSqlRoleDefinition(string roleDefinitionId, CancellationToken cancellationToken = default);
        Task<SqlRoleAssignmentGetResultsInner> GetSqlRoleAssignment(string roleAssignmentId, CancellationToken cancellationToken = default);
        Task<SqlRoleDefinitionGetResultsInner> GetSqlRoleDefinition(string roleDefinitionId, CancellationToken cancellationToken = default);
        Task<IEnumerable<SqlRoleAssignmentGetResultsInner>> ListSqlRoleAssignmenst(CancellationToken cancellationToken = default);
        Task<IEnumerable<SqlRoleDefinitionGetResultsInner>> ListSqlRoleDefinitions(CancellationToken cancellationToken = default);
    }

    internal class SqlResources2 : ICosmosDBAccounts2
    {
        private readonly IDatabaseAccountsOperations2 inner;
        private readonly ICosmosDBManager manager;

        public SqlResources2(IAzure azure)
        {
            manager = azure.CosmosDBAccounts.Manager;
            inner = new SqlResourcesImpl(manager);
        }

        public ICosmosDBManager Manager
            => manager;

        public IDatabaseAccountsOperations2 Inner
            => inner;

        public async Task<SqlRoleAssignmentGetResultsInner> CreateUpdateSqlRoleAssignment(string resourceGroupName, string accountName, string roleAssignmentId, SqlRoleAssignmentCreateUpdateParameters createUpdateParameters, CancellationToken cancellationToken = default)
        {
            using var response = await Inner.CreateUpdateSqlRoleAssignmentWithHttpMessagesAsync(resourceGroupName, accountName, roleAssignmentId, createUpdateParameters, default, cancellationToken);
            return response.Body;
        }

        public async Task<SqlRoleDefinitionGetResultsInner> CreateUpdateSqlRoleDefinition(string resourceGroupName, string accountName, string roleDefinitionId, SqlRoleDefinitionCreateUpdateParameters createUpdateParameters, CancellationToken cancellationToken = default)
        {
            using var response = await Inner.CreateUpdateSqlRoleDefinitionWithHttpMessagesAsync(resourceGroupName, accountName, roleDefinitionId, createUpdateParameters, default, cancellationToken);
            return response.Body;
        }

        public async Task DeleteSqlRoleAssignment(string resourceGroupName, string accountName, string roleAssignmentId, CancellationToken cancellationToken = default)
        {
            using var response = await Inner.DeleteSqlRoleAssignmentWithHttpMessagesAsync(resourceGroupName, accountName, roleAssignmentId, default, cancellationToken);
        }

        public async Task DeleteSqlRoleDefinition(string resourceGroupName, string accountName, string roleDefinitionId, CancellationToken cancellationToken = default)
        {
            using var response = await Inner.DeleteSqlRoleDefinitionWithHttpMessagesAsync(resourceGroupName, accountName, roleDefinitionId, default, cancellationToken);
        }

        public async Task<SqlRoleAssignmentGetResultsInner> GetSqlRoleAssignment(string resourceGroupName, string accountName, string roleAssignmentId, CancellationToken cancellationToken = default)
        {
            using var response = await Inner.GetSqlRoleAssignmentWithHttpMessagesAsync(resourceGroupName, accountName, roleAssignmentId, default, cancellationToken);
            return response.Body;
        }

        public async Task<SqlRoleDefinitionGetResultsInner> GetSqlRoleDefinition(string resourceGroupName, string accountName, string roleDefinitionId, CancellationToken cancellationToken = default)
        {
            using var response = await Inner.GetSqlRoleDefinitionWithHttpMessagesAsync(resourceGroupName, accountName, roleDefinitionId, default, cancellationToken);
            return response.Body;
        }

        public async Task<IEnumerable<SqlRoleAssignmentGetResultsInner>> ListSqlRoleAssignmenst(string resourceGroupName, string accountName, CancellationToken cancellationToken = default)
        {
            using var response = await Inner.ListSqlRoleAssignmenstWithHttpMessagesAsync(resourceGroupName, accountName, default, cancellationToken);
            return response.Body;
        }

        public async Task<IEnumerable<SqlRoleDefinitionGetResultsInner>> ListSqlRoleDefinitions(string resourceGroupName, string accountName, CancellationToken cancellationToken = default)
        {
            using var response = await Inner.ListSqlRoleDefinitionsWithHttpMessagesAsync(resourceGroupName, accountName, default, cancellationToken);
            return response.Body;
        }
    }

    internal class SqlResource2 : ICosmosDBAccount2
    {
        private readonly IDatabaseAccountOperations2 inner;
        private readonly ICosmosDBManager manager;

        public SqlResource2(ICosmosDBAccount cosmosDBAccount)
        {
            manager = cosmosDBAccount.Manager;
            inner = new SqlResourcesWrapper(cosmosDBAccount);
        }

        public ICosmosDBManager Manager
            => manager;

        public IDatabaseAccountOperations2 Inner
            => inner;

        public async Task<SqlRoleAssignmentGetResultsInner> CreateUpdateSqlRoleAssignment(string roleAssignmentId, SqlRoleAssignmentCreateUpdateParameters createUpdateParameters, CancellationToken cancellationToken = default)
        {
            using var response = await Inner.CreateUpdateSqlRoleAssignmentWithHttpMessagesAsync(roleAssignmentId, createUpdateParameters, default, cancellationToken);
            return response.Body;
        }

        public async Task<SqlRoleDefinitionGetResultsInner> CreateUpdateSqlRoleDefinition(string roleDefinitionId, SqlRoleDefinitionCreateUpdateParameters createUpdateParameters, CancellationToken cancellationToken = default)
        {
            using var response = await Inner.CreateUpdateSqlRoleDefinitionWithHttpMessagesAsync(roleDefinitionId, createUpdateParameters, default, cancellationToken);
            return response.Body;
        }

        public async Task DeleteSqlRoleAssignment(string roleAssignmentId, CancellationToken cancellationToken = default)
        {
            using var response = await Inner.DeleteSqlRoleAssignmentWithHttpMessagesAsync(roleAssignmentId, default, cancellationToken);
        }

        public async Task DeleteSqlRoleDefinition(string roleDefinitionId, CancellationToken cancellationToken = default)
        {
            using var response = await Inner.DeleteSqlRoleDefinitionWithHttpMessagesAsync(roleDefinitionId, default, cancellationToken);
        }

        public async Task<SqlRoleAssignmentGetResultsInner> GetSqlRoleAssignment(string roleAssignmentId, CancellationToken cancellationToken = default)
        {
            using var response = await Inner.GetSqlRoleAssignmentWithHttpMessagesAsync(roleAssignmentId, default, cancellationToken);
            return response.Body;
        }

        public async Task<SqlRoleDefinitionGetResultsInner> GetSqlRoleDefinition(string roleDefinitionId, CancellationToken cancellationToken = default)
        {
            using var response = await Inner.GetSqlRoleDefinitionWithHttpMessagesAsync(roleDefinitionId, default, cancellationToken);
            return response.Body;
        }

        public async Task<IEnumerable<SqlRoleAssignmentGetResultsInner>> ListSqlRoleAssignmenst(CancellationToken cancellationToken = default)
        {
            using var response = await Inner.ListSqlRoleAssignmenstWithHttpMessagesAsync(default, cancellationToken);
            return response.Body;
        }

        public async Task<IEnumerable<SqlRoleDefinitionGetResultsInner>> ListSqlRoleDefinitions(CancellationToken cancellationToken = default)
        {
            using var response = await Inner.ListSqlRoleDefinitionsWithHttpMessagesAsync(default, cancellationToken);
            return response.Body;
        }
    }

    public interface IDatabaseAccountsOperations2
    {
        Task<AzureOperationResponse<SqlRoleAssignmentGetResultsInner>> CreateUpdateSqlRoleAssignmentWithHttpMessagesAsync(string resourceGroupName, string accountName, string roleAssignmentId, SqlRoleAssignmentCreateUpdateParameters createUpdateParameters, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default);
        Task<AzureOperationResponse<SqlRoleDefinitionGetResultsInner>> CreateUpdateSqlRoleDefinitionWithHttpMessagesAsync(string resourceGroupName, string accountName, string roleDefinitionId, SqlRoleDefinitionCreateUpdateParameters createUpdateParameters, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default);
        Task<AzureOperationResponse> DeleteSqlRoleAssignmentWithHttpMessagesAsync(string resourceGroupName, string accountName, string roleAssignmentId, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default);
        Task<AzureOperationResponse> DeleteSqlRoleDefinitionWithHttpMessagesAsync(string resourceGroupName, string accountName, string roleDefinitionId, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default);
        Task<AzureOperationResponse<SqlRoleAssignmentGetResultsInner>> GetSqlRoleAssignmentWithHttpMessagesAsync(string resourceGroupName, string accountName, string roleAssignmentId, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default);
        Task<AzureOperationResponse<SqlRoleDefinitionGetResultsInner>> GetSqlRoleDefinitionWithHttpMessagesAsync(string resourceGroupName, string accountName, string roleDefinitionId, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default);
        Task<AzureOperationResponse<IEnumerable<SqlRoleAssignmentGetResultsInner>>> ListSqlRoleAssignmenstWithHttpMessagesAsync(string resourceGroupName, string accountName, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default);
        Task<AzureOperationResponse<IEnumerable<SqlRoleDefinitionGetResultsInner>>> ListSqlRoleDefinitionsWithHttpMessagesAsync(string resourceGroupName, string accountName, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default);
    }

    public interface IDatabaseAccountOperations2
    {
        Task<AzureOperationResponse<SqlRoleAssignmentGetResultsInner>> CreateUpdateSqlRoleAssignmentWithHttpMessagesAsync(string roleAssignmentId, SqlRoleAssignmentCreateUpdateParameters createUpdateParameters, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default);
        Task<AzureOperationResponse<SqlRoleDefinitionGetResultsInner>> CreateUpdateSqlRoleDefinitionWithHttpMessagesAsync(string roleDefinitionId, SqlRoleDefinitionCreateUpdateParameters createUpdateParameters, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default);
        Task<AzureOperationResponse> DeleteSqlRoleAssignmentWithHttpMessagesAsync(string roleAssignmentId, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default);
        Task<AzureOperationResponse> DeleteSqlRoleDefinitionWithHttpMessagesAsync(string roleDefinitionId, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default);
        Task<AzureOperationResponse<SqlRoleAssignmentGetResultsInner>> GetSqlRoleAssignmentWithHttpMessagesAsync(string roleAssignmentId, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default);
        Task<AzureOperationResponse<SqlRoleDefinitionGetResultsInner>> GetSqlRoleDefinitionWithHttpMessagesAsync(string roleDefinitionId, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default);
        Task<AzureOperationResponse<IEnumerable<SqlRoleAssignmentGetResultsInner>>> ListSqlRoleAssignmenstWithHttpMessagesAsync(Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default);
        Task<AzureOperationResponse<IEnumerable<SqlRoleDefinitionGetResultsInner>>> ListSqlRoleDefinitionsWithHttpMessagesAsync(Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default);
    }

    internal class SqlResourcesImpl : IDatabaseAccountsOperations2, IHasManager<ICosmosDBManager>
    {
        public ICosmosDBManager Manager { get; }

        public SqlResourcesImpl(ICosmosDBManager manager)
            => Manager = manager ?? throw new ArgumentNullException(nameof(manager));

        public static void Validate(string subscriptionId, string resourceGroupName, string accountName)
        {
            if (subscriptionId == null)
            {
                throw new ValidationException(ValidationRules.CannotBeNull, "subscriptionId");
            }
            if (resourceGroupName == null)
            {
                throw new ValidationException(ValidationRules.CannotBeNull, "resourceGroupName");
            }
            if (resourceGroupName != null)
            {
                if (resourceGroupName.Length > 90)
                {
                    throw new ValidationException(ValidationRules.MaxLength, "resourceGroupName", 90);
                }
                if (resourceGroupName.Length < 1)
                {
                    throw new ValidationException(ValidationRules.MinLength, "resourceGroupName", 1);
                }
                if (!System.Text.RegularExpressions.Regex.IsMatch(resourceGroupName, "^[-\\w\\._\\(\\)]+$"))
                {
                    throw new ValidationException(ValidationRules.Pattern, "resourceGroupName", "^[-\\w\\._\\(\\)]+$");
                }
            }
            if (accountName == null)
            {
                throw new ValidationException(ValidationRules.CannotBeNull, "accountName");
            }
            if (accountName != null)
            {
                if (accountName.Length > 50)
                {
                    throw new ValidationException(ValidationRules.MaxLength, "accountName", 50);
                }
                if (accountName.Length < 3)
                {
                    throw new ValidationException(ValidationRules.MinLength, "accountName", 3);
                }
                if (!System.Text.RegularExpressions.Regex.IsMatch(accountName, "^[a-z0-9]+(-[a-z0-9]+)*"))
                {
                    throw new ValidationException(ValidationRules.Pattern, "accountName", "^[a-z0-9]+(-[a-z0-9]+)*");
                }
            }
        }

        private static string ValidateIsGuid(string guid, [System.Runtime.CompilerServices.CallerArgumentExpression("guid")] string name = default) // TODO: this should probably be improved
        {
            if (default == guid)
            {
                throw new ValidationException(ValidationRules.CannotBeNull, name);
            }

            if (!Guid.TryParse(guid, out var uuid))
            {
                throw new ValidationException(ValidationRules.Pattern, name, @"^[{]?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}[}]?$");
            }

            if (uuid == Guid.Empty)
            {
                throw new ValidationException(ValidationRules.ExclusiveMinimum, name, Guid.Empty.ToString());
            }

            return guid;
        }
        private class SqlRoleAssignmentCreateUpdate
        {
            [JsonProperty(PropertyName = "properties", NullValueHandling = NullValueHandling.Ignore)]
            public SqlRoleAssignmentCreateUpdateParameters Properties { get; set; }
        }

        private class SqlRoleDefinitionCreateUpdate
        {
            [JsonProperty(PropertyName = "properties", NullValueHandling = NullValueHandling.Ignore)]
            public SqlRoleDefinitionCreateUpdateParameters Properties { get; set; }
        }

        public Task<AzureOperationResponse<SqlRoleAssignmentGetResultsInner>> CreateUpdateSqlRoleAssignmentWithHttpMessagesAsync(string resourceGroupName, string accountName, string roleAssignmentId, SqlRoleAssignmentCreateUpdateParameters createUpdateParameters, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default)
            => WithHttpMessagesAsync(this, new WithHttpMessagesOptions<SqlRoleAssignmentCreateUpdateParameters, SqlRoleAssignmentCreateUpdate, SqlRoleAssignmentGetResultsInner, AzureOperationResponse<SqlRoleAssignmentGetResultsInner>>(
                Manager, "CreateUpdateSqlRoleAssignment", HttpMethod.Put, resourceGroupName, accountName,
                    @"subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.DocumentDB/databaseAccounts/{accountName}/sqlRoleAssignments/" + ValidateIsGuid(roleAssignmentId),
                    new[] { HttpStatusCode.OK, HttpStatusCode.Accepted }, customHeaders, cancellationToken, validateParams: p => { }, prepareRequest: p => new SqlRoleAssignmentCreateUpdate() { Properties = p }, prepareResponse: (b, r) => r.Body = b), createUpdateParameters);

        public Task<AzureOperationResponse<SqlRoleDefinitionGetResultsInner>> CreateUpdateSqlRoleDefinitionWithHttpMessagesAsync(string resourceGroupName, string accountName, string roleDefinitionId, SqlRoleDefinitionCreateUpdateParameters createUpdateParameters, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default)
            => WithHttpMessagesAsync(this, new WithHttpMessagesOptions<SqlRoleDefinitionCreateUpdateParameters, SqlRoleDefinitionCreateUpdate, SqlRoleDefinitionGetResultsInner, AzureOperationResponse<SqlRoleDefinitionGetResultsInner>>(
                Manager, "CreateUpdateSqlRoleDefinition", HttpMethod.Put, resourceGroupName, accountName,
                    @"subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.DocumentDB/databaseAccounts/{accountName}/sqlRoleDefinitions/" + ValidateIsGuid(roleDefinitionId),
                    new[] { HttpStatusCode.OK, HttpStatusCode.Accepted }, customHeaders, cancellationToken, validateParams: p => { }, prepareRequest: p => new SqlRoleDefinitionCreateUpdate() { Properties = p }, prepareResponse: (b, r) => r.Body = b), createUpdateParameters);

        public Task<AzureOperationResponse> DeleteSqlRoleAssignmentWithHttpMessagesAsync(string resourceGroupName, string accountName, string roleAssignmentId, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default)
            => WithHttpMessagesAsync(this, new WithHttpMessagesOptions<object, object, object, AzureOperationResponse>(
                Manager, "DeleteSqlRoleAssignment", HttpMethod.Delete, resourceGroupName, accountName,
                    @"subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.DocumentDB/databaseAccounts/{accountName}/sqlRoleAssignments/" + ValidateIsGuid(roleAssignmentId),
                    new[] { HttpStatusCode.OK, HttpStatusCode.Accepted, HttpStatusCode.NoContent }, customHeaders, cancellationToken));

        public Task<AzureOperationResponse> DeleteSqlRoleDefinitionWithHttpMessagesAsync(string resourceGroupName, string accountName, string roleDefinitionId, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default)
            => WithHttpMessagesAsync(this, new WithHttpMessagesOptions<object, object, object, AzureOperationResponse>(
                Manager, "DeleteSqlRoleDefinition", HttpMethod.Delete, resourceGroupName, accountName,
                    @"subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.DocumentDB/databaseAccounts/{accountName}/sqlRoleDefinitions/" + ValidateIsGuid(roleDefinitionId),
                    new[] { HttpStatusCode.OK, HttpStatusCode.Accepted, HttpStatusCode.NoContent }, customHeaders, cancellationToken));

        public Task<AzureOperationResponse<SqlRoleAssignmentGetResultsInner>> GetSqlRoleAssignmentWithHttpMessagesAsync(string resourceGroupName, string accountName, string roleAssignmentId, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default)
            => WithHttpMessagesAsync(this, new WithHttpMessagesOptions<object, object, SqlRoleAssignmentGetResultsInner, AzureOperationResponse<SqlRoleAssignmentGetResultsInner>>(
                Manager, "GetSqlRoleAssignment", HttpMethod.Get, resourceGroupName, accountName,
                    @"subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.DocumentDB/databaseAccounts/{accountName}/sqlRoleAssignments/" + ValidateIsGuid(roleAssignmentId),
                    new[] { HttpStatusCode.OK }, customHeaders, cancellationToken, prepareResponse: (b, r) => r.Body = b));

        public Task<AzureOperationResponse<SqlRoleDefinitionGetResultsInner>> GetSqlRoleDefinitionWithHttpMessagesAsync(string resourceGroupName, string accountName, string roleDefinitionId, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default)
            => WithHttpMessagesAsync(this, new WithHttpMessagesOptions<object, object, SqlRoleDefinitionGetResultsInner, AzureOperationResponse<SqlRoleDefinitionGetResultsInner>>(
                Manager, "GetSqlRoleDefinition", HttpMethod.Get, resourceGroupName, accountName,
                    @"subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.DocumentDB/databaseAccounts/{accountName}/sqlRoleDefinitions/" + ValidateIsGuid(roleDefinitionId),
                    new[] { HttpStatusCode.OK }, customHeaders, cancellationToken, prepareResponse: (b, r) => r.Body = b));

        public Task<AzureOperationResponse<IEnumerable<SqlRoleAssignmentGetResultsInner>>> ListSqlRoleAssignmenstWithHttpMessagesAsync(string resourceGroupName, string accountName, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default)
            => WithHttpMessagesAsync(this, new WithHttpMessagesOptions<object, object, SqlRoleAssignmentListResultInner, AzureOperationResponse<IEnumerable<SqlRoleAssignmentGetResultsInner>>>(
                Manager, "ListSqlRoleAssignmenst", HttpMethod.Get, resourceGroupName, accountName,
                    @"subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.DocumentDB/databaseAccounts/{accountName}/sqlRoleAssignments",
                    new[] { HttpStatusCode.OK }, customHeaders, cancellationToken, prepareResponse: (b, r) => r.Body = b.Value));

        public Task<AzureOperationResponse<IEnumerable<SqlRoleDefinitionGetResultsInner>>> ListSqlRoleDefinitionsWithHttpMessagesAsync(string resourceGroupName, string accountName, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default)
            => WithHttpMessagesAsync(this, new WithHttpMessagesOptions<object, object, SqlRoleDefinitionListResultInner, AzureOperationResponse<IEnumerable<SqlRoleDefinitionGetResultsInner>>>(
                Manager, "ListSqlRoleDefinitions", HttpMethod.Get, resourceGroupName, accountName,
                    @"subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.DocumentDB/databaseAccounts/{accountName}/sqlRoleDefinitions",
                    new[] { HttpStatusCode.OK }, customHeaders, cancellationToken, prepareResponse: (b, r) => r.Body = b.Value));

        private struct WithHttpMessagesOptions<TParam, TRequest, TResponse, TResult>
            where TResult : IAzureOperationResponse, IHttpOperationResponse, new()
            where TParam : class
            where TRequest : class
        {
            public WithHttpMessagesOptions(ICosmosDBManager manager, string methodName, HttpMethod method, string resourceGroupName, string accountName, string url, IEnumerable<HttpStatusCode> acceptableResults, Dictionary<string, List<string>> customHeaders, CancellationToken cancellationToken, Action<TParam> validateParams = default, Func<TParam, TRequest> prepareRequest = default, Action<TResponse, TResult> prepareResponse = default, IEnumerable<(string, object)> tracingParameters = default)
            {
                ApiVersion = "2021-04-15";
                SubscriptionId = (manager ?? throw new ArgumentNullException(nameof(manager))).SubscriptionId;
                Client = (CosmosDBManagementClient)manager.Inner;

                SqlResourcesImpl.Validate(SubscriptionId, resourceGroupName, accountName);

                Method = method ?? throw new ArgumentNullException(nameof(method));
                Validate = validateParams;
                PrepareRequest = prepareRequest;
                PrepareResponse = prepareResponse;
                MethodName = methodName ?? throw new ArgumentNullException(nameof(methodName));
                ResourceGroupName = resourceGroupName;
                AccountName = accountName;
                CustomHeaders = customHeaders;
                CancellationToken = cancellationToken;
                Url = url ?? throw new ArgumentNullException(nameof(url));
                AcceptableResults = acceptableResults ?? throw new ArgumentNullException(nameof(acceptableResults));
                TracingParameters = tracingParameters ?? Enumerable.Empty<(string, object)>();

                // Validate
                SqlResourcesImpl.Validate(SubscriptionId, ResourceGroupName, AccountName);
            }

            public Action<TParam> Validate { get; }

            public Func<TParam, TRequest> PrepareRequest { get; }

            public Action<TResponse, TResult> PrepareResponse { get; }

            public CosmosDBManagementClient Client { get; }

            public string MethodName { get; }

            public HttpMethod Method { get; }

            public string SubscriptionId { get; }

            public string ResourceGroupName { get; }

            public string AccountName { get; }

            public Dictionary<string, List<string>> CustomHeaders { get; }

            public CancellationToken CancellationToken { get; }

            public string ApiVersion { get; }

            public string Url { get; }

            public IEnumerable<HttpStatusCode> AcceptableResults { get; }

            public IEnumerable<(string, object)> TracingParameters { get; }
        }

        private static async Task<TResult> WithHttpMessagesAsync<TParam, TRequest, TResponse, TResult>(object @this, WithHttpMessagesOptions<TParam, TRequest, TResponse, TResult> options, TParam param = default)
            where TResult : IAzureOperationResponse, IHttpOperationResponse, new()
            where TParam : class
            where TRequest : class
        {
            // Validation
            if (default != options.Validate)
            {
                options.Validate(param);
            }
            else if (default != param)
            {
                throw new InvalidOperationException("Unvalidated parameters");
            }

            TRequest request = default;
            if (default != param)
            {
                if (default != options.PrepareRequest)
                {
                    request = options.PrepareRequest(param);
                }
                else
                {
                    throw new InvalidOperationException("Unprepared parameters");
                }
            }

            // Tracing
            var _shouldTrace = ServiceClientTracing.IsEnabled;
            string _invocationId = null;
            if (_shouldTrace)
            {
                _invocationId = ServiceClientTracing.NextInvocationId.ToString();
                Dictionary<string, object> tracingParameters = new Dictionary<string, object>();
                tracingParameters.Add("resourceGroupName", options.ResourceGroupName);
                tracingParameters.Add("accountName", options.AccountName);
                tracingParameters.Add("apiVersion", options.ApiVersion);
                foreach (var tp in options.TracingParameters)
                {
                    tracingParameters.Add(tp.Item1, tp.Item2);
                }
                tracingParameters.Add("cancellationToken", options.CancellationToken);
                ServiceClientTracing.Enter(_invocationId, @this, options.MethodName, tracingParameters);
            }

            // Construct URL
            var _baseUrl = options.Client.BaseUri.AbsoluteUri;
            var _url = new Uri(new Uri(_baseUrl + (_baseUrl.EndsWith("/") ? string.Empty : "/")), options.Url).ToString();
            _url = _url.Replace("{subscriptionId}", Uri.EscapeDataString(options.SubscriptionId));
            _url = _url.Replace("{resourceGroupName}", Uri.EscapeDataString(options.ResourceGroupName));
            _url = _url.Replace("{accountName}", Uri.EscapeDataString(options.AccountName));
            var _queryParameters = new List<string>();
            if (options.ApiVersion != null)
            {
                _queryParameters.Add(string.Format("api-version={0}", System.Uri.EscapeDataString(options.ApiVersion)));
            }
            if (_queryParameters.Count > 0)
            {
                _url += (_url.Contains("?") ? "&" : "?") + string.Join("&", _queryParameters);
            }

            // Create HTTP transport objects
            var _httpRequest = new HttpRequestMessage();
            HttpResponseMessage _httpResponse;
            _httpRequest.Method = options.Method;
            _httpRequest.RequestUri = new Uri(_url);

            // Set Headers
            if (options.Client.GenerateClientRequestId != null && options.Client.GenerateClientRequestId.Value)
            {
                _httpRequest.Headers.TryAddWithoutValidation("x-ms-client-request-id", System.Guid.NewGuid().ToString());
            }
            if (options.Client.AcceptLanguage != null)
            {
                if (_httpRequest.Headers.Contains("accept-language"))
                {
                    _httpRequest.Headers.Remove("accept-language");
                }
                _httpRequest.Headers.TryAddWithoutValidation("accept-language", options.Client.AcceptLanguage);
            }

            if (options.CustomHeaders != null)
            {
                foreach (var _header in options.CustomHeaders)
                {
                    if (_httpRequest.Headers.Contains(_header.Key))
                    {
                        _httpRequest.Headers.Remove(_header.Key);
                    }
                    _httpRequest.Headers.TryAddWithoutValidation(_header.Key, _header.Value);
                }
            }

            // Serialize Request
            string _requestContent = null;
            if (default != request)
            {
                _requestContent = Microsoft.Rest.Serialization.SafeJsonConvert.SerializeObject(request, options.Client.SerializationSettings);
                _httpRequest.Content = new StringContent(_requestContent, Encoding.UTF8);
                _httpRequest.Content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json; charset=utf-8");
            }

            // Set Credentials
            if (options.Client.Credentials != null)
            {
                options.CancellationToken.ThrowIfCancellationRequested();
                await options.Client.Credentials.ProcessHttpRequestAsync(_httpRequest, options.CancellationToken).ConfigureAwait(false);
            }
            // Send Request
            if (_shouldTrace)
            {
                ServiceClientTracing.SendRequest(_invocationId, _httpRequest);
            }
            options.CancellationToken.ThrowIfCancellationRequested();
            _httpResponse = await options.Client.HttpClient.SendAsync(_httpRequest, options.CancellationToken).ConfigureAwait(false);
            if (_shouldTrace)
            {
                ServiceClientTracing.ReceiveResponse(_invocationId, _httpResponse);
            }
            var _statusCode = _httpResponse.StatusCode;
            options.CancellationToken.ThrowIfCancellationRequested();
            string _responseContent = null;
            if (!options.AcceptableResults.Contains(_statusCode))
            {
                var ex = new CloudException(string.Format("Operation returned an invalid status code '{0}'", _statusCode));
                try
                {
                    _responseContent = await _httpResponse.Content.ReadAsStringAsync().ConfigureAwait(false);
                    var _errorBody = Microsoft.Rest.Serialization.SafeJsonConvert.DeserializeObject<CloudError>(_responseContent, options.Client.DeserializationSettings);
                    if (_errorBody != null)
                    {
                        ex = new CloudException(_errorBody.Message)
                        {
                            Body = _errorBody
                        };
                    }
                }
                catch (JsonException)
                {
                    // Ignore the exception
                }
                ex.Request = new HttpRequestMessageWrapper(_httpRequest, _requestContent);
                ex.Response = new HttpResponseMessageWrapper(_httpResponse, _responseContent);
                if (_httpResponse.Headers.Contains("x-ms-request-id"))
                {
                    ex.RequestId = _httpResponse.Headers.GetValues("x-ms-request-id").FirstOrDefault();
                }
                if (_shouldTrace)
                {
                    ServiceClientTracing.Error(_invocationId, ex);
                }
                _httpRequest.Dispose();
                if (_httpResponse != null)
                {
                    _httpResponse.Dispose();
                }
                throw ex;
            }

            // Create Result
            var _result = new TResult
            {
                Request = _httpRequest,
                Response = _httpResponse
            };
            if (_httpResponse.Headers.Contains("x-ms-request-id"))
            {
                _result.RequestId = _httpResponse.Headers.GetValues("x-ms-request-id").FirstOrDefault();
            }

            // Deserialize Response
            if (default != options.PrepareResponse && (int)_statusCode == 200)
            {
                _responseContent = await _httpResponse.Content.ReadAsStringAsync().ConfigureAwait(false);
                try
                {
                    options.PrepareResponse(Microsoft.Rest.Serialization.SafeJsonConvert.DeserializeObject<TResponse>(_responseContent, options.Client.DeserializationSettings), _result);
                }
                catch (JsonException ex)
                {
                    _httpRequest.Dispose();
                    if (_httpResponse != null)
                    {
                        _httpResponse.Dispose();
                    }
                    throw new SerializationException("Unable to deserialize the response.", _responseContent, ex);
                }
            }

            if (_shouldTrace)
            {
                ServiceClientTracing.Exit(_invocationId, _result);
            }

            return _result;
        }
    }

    internal class SqlResourcesWrapper : IDatabaseAccountOperations2, IHasManager<ICosmosDBManager>
    {
        private readonly SqlResourcesImpl inner;
        private readonly string resourceGroupName;
        private readonly string accountName;

        public SqlResourcesWrapper(ICosmosDBAccount cosmosDBAccount)
        {
            inner = new SqlResourcesImpl((cosmosDBAccount ?? throw new ArgumentNullException(nameof(cosmosDBAccount))).Manager);
            accountName = cosmosDBAccount.Name;
            resourceGroupName = cosmosDBAccount.ResourceGroupName;

            // Validate
            SqlResourcesImpl.Validate(cosmosDBAccount.Manager.SubscriptionId, resourceGroupName, accountName);
        }

        public ICosmosDBManager Manager
            => inner.Manager;

        public Task<AzureOperationResponse<SqlRoleAssignmentGetResultsInner>> CreateUpdateSqlRoleAssignmentWithHttpMessagesAsync(string roleAssignmentId, SqlRoleAssignmentCreateUpdateParameters createUpdateParameters, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default)
            => inner.CreateUpdateSqlRoleAssignmentWithHttpMessagesAsync(resourceGroupName, accountName, roleAssignmentId, createUpdateParameters, customHeaders, cancellationToken);

        public Task<AzureOperationResponse<SqlRoleDefinitionGetResultsInner>> CreateUpdateSqlRoleDefinitionWithHttpMessagesAsync(string roleDefinitionId, SqlRoleDefinitionCreateUpdateParameters createUpdateParameters, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default)
            => inner.CreateUpdateSqlRoleDefinitionWithHttpMessagesAsync(resourceGroupName, accountName, roleDefinitionId, createUpdateParameters, customHeaders, cancellationToken);

        public Task<AzureOperationResponse> DeleteSqlRoleAssignmentWithHttpMessagesAsync(string roleAssignmentId, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default)
            => inner.DeleteSqlRoleAssignmentWithHttpMessagesAsync(resourceGroupName, accountName, roleAssignmentId, customHeaders, cancellationToken);

        public Task<AzureOperationResponse> DeleteSqlRoleDefinitionWithHttpMessagesAsync(string roleDefinitionId, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default)
            => inner.DeleteSqlRoleDefinitionWithHttpMessagesAsync(resourceGroupName, accountName, roleDefinitionId, customHeaders, cancellationToken);

        public Task<AzureOperationResponse<SqlRoleAssignmentGetResultsInner>> GetSqlRoleAssignmentWithHttpMessagesAsync(string roleAssignmentId, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default)
            => inner.GetSqlRoleAssignmentWithHttpMessagesAsync(resourceGroupName, accountName, roleAssignmentId, customHeaders, cancellationToken);

        public Task<AzureOperationResponse<SqlRoleDefinitionGetResultsInner>> GetSqlRoleDefinitionWithHttpMessagesAsync(string roleDefinitionId, Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default)
            => inner.GetSqlRoleDefinitionWithHttpMessagesAsync(resourceGroupName, accountName, roleDefinitionId, customHeaders, cancellationToken);

        public Task<AzureOperationResponse<IEnumerable<SqlRoleAssignmentGetResultsInner>>> ListSqlRoleAssignmenstWithHttpMessagesAsync(Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default)
            => inner.ListSqlRoleAssignmenstWithHttpMessagesAsync(resourceGroupName, accountName, customHeaders, cancellationToken);

        public Task<AzureOperationResponse<IEnumerable<SqlRoleDefinitionGetResultsInner>>> ListSqlRoleDefinitionsWithHttpMessagesAsync(Dictionary<string, List<string>> customHeaders = null, CancellationToken cancellationToken = default)
            => inner.ListSqlRoleDefinitionsWithHttpMessagesAsync(resourceGroupName, accountName, customHeaders, cancellationToken);
    }

    public class SqlRoleAssignmentCreateUpdateParameters
    {
        [JsonProperty(PropertyName = "principalId", NullValueHandling = NullValueHandling.Ignore)]
        public string PrincipalId { get; set; }

        [JsonProperty(PropertyName = "roleDefinitionId", NullValueHandling = NullValueHandling.Ignore)]
        public string RoleDefinitionId { get; set; }

        [JsonProperty(PropertyName = "scope", NullValueHandling = NullValueHandling.Ignore)]
        public string Scope { get; set; }
    }

    //public class SqlRoleAssignmentGetResults
    //{ }

    public class SqlRoleAssignmentGetResultsInner
    {
        [JsonProperty(PropertyName = "id", NullValueHandling = NullValueHandling.Ignore)]
        public string Id { get; set; }

        [JsonProperty(PropertyName = "name", NullValueHandling = NullValueHandling.Ignore)]
        public string Name { get; set; }

        [JsonProperty(PropertyName = "properties", NullValueHandling = NullValueHandling.Ignore)]
        public SqlRoleAssignmentCreateUpdateParameters Properties { get; set; }

        [JsonProperty(PropertyName = "type", NullValueHandling = NullValueHandling.Ignore)]
        public string Type { get; set; }
    }

    public class SqlRoleAssignmentListResultInner
    {
        [JsonProperty(PropertyName = "value", NullValueHandling = NullValueHandling.Ignore)]
        public SqlRoleAssignmentGetResultsInner[] Value { get; set; }
    }

    public class SqlRoleDefinitionCreateUpdateParameters
    {
        [JsonProperty(PropertyName = "assignableScopes", NullValueHandling = NullValueHandling.Ignore)]
        public string[] AssignableScopes { get; set; }

        [JsonProperty(PropertyName = "permissions", NullValueHandling = NullValueHandling.Ignore)]
        public SqlRoleDefinitionPermission[] Permissions { get; set; }

        [JsonProperty(PropertyName = "roleName", NullValueHandling = NullValueHandling.Ignore)]
        public string RoleName { get; set; }

        [JsonProperty(PropertyName = "type", NullValueHandling = NullValueHandling.Ignore)]
        public string RoleDefinitionType { get; set; } // TODO: replace with enum RoleDefinitionType {BuiltInRole, CustomRole}
    }

    public class SqlRoleDefinitionGetResultsInner
    {
        [JsonProperty(PropertyName = "id", NullValueHandling = NullValueHandling.Ignore)]
        public string Id { get; set; }

        [JsonProperty(PropertyName = "name", NullValueHandling = NullValueHandling.Ignore)]
        public string Name { get; set; }

        [JsonProperty(PropertyName = "properties", NullValueHandling = NullValueHandling.Ignore)]
        public SqlRoleDefinitionCreateUpdateParameters Properties { get; set; }

        [JsonProperty(PropertyName = "type", NullValueHandling = NullValueHandling.Ignore)]
        public string Type { get; set; }
    }

    public class SqlRoleDefinitionPermission
    {
        [JsonProperty(PropertyName = "dataActions", NullValueHandling = NullValueHandling.Ignore)]
        public string[] DataActionsGranted { get; set; }

        [JsonProperty(PropertyName = "notDataActions", NullValueHandling = NullValueHandling.Ignore)]
        public string[] DataActionsDenied { get; set; }
    }

    //public class SqlRoleDefinitionGetResults
    //{ }

    public class SqlRoleDefinitionListResultInner
    {
        [JsonProperty(PropertyName = "value", NullValueHandling = NullValueHandling.Ignore)]
        public SqlRoleDefinitionGetResultsInner[] Value { get; set; }
    }
}
