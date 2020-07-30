// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web
{
    using System;
    using System.Collections.Generic;
    using System.Dynamic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading.Tasks;
    using Microsoft.ApplicationInsights.Extensibility.Implementation;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;
    using Newtonsoft.Json;
    using TesApi.Models;

    /// <summary>
    /// A repository for interacting with an Azure Cosmos DB instance
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class CosmosDbRepository<T> : IRepository<T> where T : class
    {
        private const string PartitionKeyFieldName = "_partitionKey";
        private readonly DocumentClient client;
        private readonly Microsoft.Azure.Cosmos.CosmosClient cosmosClient;
        private readonly string databaseId;
        private readonly string collectionId;
        private readonly PartitionKey partitionKeyObjectForRequestOptions;
        private readonly Uri databaseUri;
        private readonly Uri documentCollectionUri;
        private readonly Func<string, Uri> documentUriFactory;
        private readonly string partitionKeyValue;

        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="endpoint">Azure Cosmos DB endpoint</param>
        /// <param name="key">Azure Cosmos DB authentication key</param>
        /// <param name="databaseId">Azure Cosmos DB database ID</param>
        /// <param name="collectionId">Azure Cosmos DB collection ID</param>
        /// <param name="partitionKeyValue">Partition key value. Only the items matching this value will be visible.</param>
        /// <param name="useAutoscaling">If true, CosmosDB database autoscaling will be enabled</param>
        public CosmosDbRepository(Uri endpoint, string key, string databaseId, string collectionId, string partitionKeyValue, bool useAutoscaling, int cosmosDbAutoscalingMaxThroughput)
        {
            client = new DocumentClient(endpoint, key);
            cosmosClient = new Microsoft.Azure.Cosmos.CosmosClient(endpoint.ToString(), key);
            this.databaseId = databaseId;
            this.collectionId = collectionId;
            databaseUri = UriFactory.CreateDatabaseUri(databaseId);
            documentCollectionUri = UriFactory.CreateDocumentCollectionUri(databaseId, this.collectionId);
            documentUriFactory = documentId => UriFactory.CreateDocumentUri(databaseId, this.collectionId, documentId);
            this.partitionKeyValue = partitionKeyValue;
            partitionKeyObjectForRequestOptions = new PartitionKey(this.partitionKeyValue);
            CreateDatabaseIfNotExistsAsync(useAutoscaling, cosmosDbAutoscalingMaxThroughput).Wait();
            CreateCollectionIfNotExistsAsync().Wait();
        }

        /// <summary>
        /// Reads a document from the database
        /// </summary>
        /// <param name="id">The document ID</param>
        /// <param name="onSuccess"></param>
        /// <returns>An etag and object of type T as a RepositoryItem</returns>
        public async Task<bool> TryGetItemAsync(string id, Action<RepositoryItem<T>> onSuccess)
        {
            try
            {
                var requestOptions = new RequestOptions { PartitionKey = partitionKeyObjectForRequestOptions };
                Document document = await client.ReadDocumentAsync(documentUriFactory(id), requestOptions);
                var item = new RepositoryItem<T> { ETag = document.ETag, Value = (T)(dynamic)document };

                if (item != null)
                {
                    onSuccess?.Invoke(item);
                    return true;
                }
            }
            catch (DocumentClientException e)
            {
                if (e.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    return false;
                }
                else
                {
                    throw;
                }
            }

            return false;
        }

        public async Task<IEnumerable<RepositoryItem<T>>> GetItemsAsync(Expression<Func<T, bool>> predicate)
        {
            string continuationToken = null;
            var listOfRepositoryItems = new List<RepositoryItem<T>>();

            do
            {
                IEnumerable<RepositoryItem<T>> repositoryItems;

                (continuationToken, repositoryItems) = await GetItemsAsync(
                    predicate,
                    pageSize: 256,
                    continuationToken: continuationToken);

                listOfRepositoryItems.AddRange(repositoryItems);
            }
            while (continuationToken != null);

            return listOfRepositoryItems;
        }

        /// <summary>
        /// Reads a collection of documents from the database
        /// </summary>
        /// <param name="predicate">The 'where' clause</param>
        /// <param name="pageSize">The max number of documents to retrieve</param>
        /// <param name="continuationToken">A token to continue retrieving documents if the max is returned</param>
        /// <returns>A continuation token string, and the collection of retrieved RepositoryItem (etag and object of type T class wrapper)</returns>
        public async Task<(string, IEnumerable<RepositoryItem<T>>)> GetItemsAsync(Expression<Func<T, bool>> predicate, int pageSize, string continuationToken)
        {
            var feedOptions = new FeedOptions { MaxItemCount = pageSize, EnableCrossPartitionQuery = false, RequestContinuation = continuationToken, PartitionKey = partitionKeyObjectForRequestOptions };

            var query = client
                .CreateDocumentQuery<T>(documentCollectionUri, feedOptions)
                .Where(predicate)
                .AsDocumentQuery();

            var queryResult = await query.ExecuteNextAsync<Document>();
            var result = queryResult.Select(r => new RepositoryItem<T> { ETag = r.ETag, Value = (T)(dynamic)r });

            return (queryResult.ResponseContinuation, result);
        }

        /// <summary>
        /// Creates a new document in the database
        /// </summary>
        /// <param name="item">The object to persist</param>
        /// <returns>The created RepositoryItem (etag and object of type T class wrapper)</returns>
        public async Task<RepositoryItem<T>> CreateItemAsync(T item)
        {
            var storedItem = AppendPartitionKey(item);
            var response = await client.CreateDocumentAsync(documentCollectionUri, storedItem);
            return new RepositoryItem<T> { ETag = response.Resource.ETag, Value = (T)(dynamic)response.Resource };
        }

        /// <summary>
        /// Updates a document in the database.  Does NOT currently use native CosmosDB partial updates.
        /// </summary>
        /// <param name="id">The ID of the document to update</param>
        /// <param name="item">The item to update</param>
        /// <returns>The repository item with an etag and the updated object</returns>
        public async Task<RepositoryItem<T>> UpdateItemAsync(string id, RepositoryItem<T> item)
        {
            // Note: Refactor to use native CosmosDB partial updates when available. 
            // See https://feedback.azure.com/forums/263030-azure-cosmos-db/suggestions/6693091-be-able-to-do-partial-updates-on-document?page=1&per_page=20

            var requestOptions = new RequestOptions { AccessCondition = new AccessCondition { Type = AccessConditionType.IfMatch, Condition = item.ETag }, PartitionKey = partitionKeyObjectForRequestOptions };
            var storedItem = AppendPartitionKey(item.Value);
            var response = await client.ReplaceDocumentAsync(documentUriFactory(id), storedItem, requestOptions);
            return new RepositoryItem<T> { ETag = response.Resource.ETag, Value = (T)(dynamic)response.Resource };
        }

        /// <summary>
        /// Delete an item from the database 
        /// </summary>
        /// <param name="id">The ID of the item to delete</param>
        public async Task DeleteItemAsync(string id)
        {
            var requestOptions = new RequestOptions { PartitionKey = partitionKeyObjectForRequestOptions };
            await client.DeleteDocumentAsync(documentUriFactory(id), requestOptions);
        }

        private async Task CreateDatabaseIfNotExistsAsync(bool setCosmosDbAutoscalingOnStartup, int cosmosDbAutoscalingMaxThroughput)
        {
            var throughputProperties = Microsoft.Azure.Cosmos.ThroughputProperties.CreateAutoscaleThroughput(cosmosDbAutoscalingMaxThroughput);
            var response = await cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId, throughputProperties);

            if (setCosmosDbAutoscalingOnStartup && response.StatusCode == System.Net.HttpStatusCode.Accepted)
            {
                // Database already exists

                // 7/30/2020 - there is no programmatic way to check if autoscale is enabled
                // as a result, enabling autoscaling AND setting max value is done here
                await response.Database.ReplaceThroughputAsync(throughputProperties);
            }
        }

        /// <summary>
        /// Creates a new collection
        /// </summary>
        private async Task CreateCollectionIfNotExistsAsync()
        {
            try
            {
                var documentCollection = await client.ReadDocumentCollectionAsync(documentCollectionUri);

                var existingPartitionKeyPath = documentCollection.Resource.PartitionKey.Paths.FirstOrDefault();

                if (existingPartitionKeyPath == null)
                {
                    throw new Exception($"Existing collection {collectionId} does not have partition key path defined. Recreate the collection.");
                }
                else if (existingPartitionKeyPath != $"/{PartitionKeyFieldName}")
                {
                    throw new Exception($"Existing collection {collectionId} partition key path ({existingPartitionKeyPath}) differs from the requested one (/{PartitionKeyFieldName}). Recreate the collection.");
                }
                // TODO: Check that the existing collection has correct partition key path
            }
            catch (DocumentClientException e)
            {
                if (e.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    var documentCollection = new DocumentCollection { Id = collectionId };
                    documentCollection.PartitionKey.Paths.Add($"/{PartitionKeyFieldName}");
                    await client.CreateDocumentCollectionAsync(databaseUri, documentCollection, new RequestOptions { OfferThroughput = 400 });
                }
                else
                {
                    throw;
                }
            }
        }

        private ExpandoObject AppendPartitionKey(T item)
        {
            var expandoObject = JsonConvert.DeserializeObject<ExpandoObject>(JsonConvert.SerializeObject(item));
            ((IDictionary<string, object>)expandoObject)[PartitionKeyFieldName] = partitionKeyValue;
            return expandoObject;
        }
    }
}
