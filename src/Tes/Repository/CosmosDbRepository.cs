// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Repository
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos;
    using Microsoft.Azure.Cosmos.Linq;

    /// <summary>
    /// A repository for interacting with an Azure Cosmos DB instance
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class CosmosDbRepository<T> : IRepository<T> where T : RepositoryItem<T>
    {
        private const int MaxAutoScaleThroughput = 4000;
        private readonly CosmosClient cosmosClient;
        private readonly Container container;
        private readonly PartitionKey partitionKey;
        private readonly string partitionKeyValue;

        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="endpoint">Azure Cosmos DB endpoint</param>
        /// <param name="key">Azure Cosmos DB authentication key</param>
        /// <param name="databaseId">Azure Cosmos DB database ID</param>
        /// <param name="containerId">Azure Cosmos DB container ID</param>
        /// <param name="partitionKeyValue">Partition key value. Only the items matching this value will be visible.</param>
        public CosmosDbRepository(string endpoint, string key, string databaseId, string containerId, string partitionKeyValue)
        {
            Common.NewtonsoftJsonSafeInit.SetDefaultSettings();
            this.cosmosClient = new CosmosClient(endpoint, key);
            this.container = cosmosClient.GetContainer(databaseId, containerId);
            this.partitionKeyValue = partitionKeyValue;
            this.partitionKey = new PartitionKey(partitionKeyValue);
            CreateDatabaseIfNotExistsAsync().Wait();
            CreateContainerIfNotExistsAsync().Wait();
        }

        /// <summary>
        /// Reads a document from the database
        /// </summary>
        /// <param name="id">The document ID</param>
        /// <param name="onSuccess"></param>
        /// <returns>An etag and object of type T as a RepositoryItem</returns>
        public async Task<bool> TryGetItemAsync(string id, Action<T> onSuccess)
        {
            try
            {
                var response = await this.container.ReadItemAsync<T>(id, partitionKey);
                var item = response.Resource;

                if (item is not null)
                {
                    onSuccess?.Invoke(item);
                    return true;
                }
            }
            catch (CosmosException e)
            {
                if (e.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    return false;
                }

                throw;
            }

            return false;
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<T>> GetItemsAsync(Expression<Func<T, bool>> predicate)
        {
            string continuationToken = null;
            var listOfRepositoryItems = new List<T>();

            do
            {
                IEnumerable<T> repositoryItems;
                (continuationToken, repositoryItems) = await GetItemsAsync(predicate, pageSize: 256, continuationToken);
                listOfRepositoryItems.AddRange(repositoryItems);
            }
            while (continuationToken is not null);

            return listOfRepositoryItems;
        }

        /// <inheritdoc/>
        public async Task<(string, IEnumerable<T>)> GetItemsAsync(Expression<Func<T, bool>> predicate, int pageSize, string continuationToken)
        {
            var requestOptions = new QueryRequestOptions { MaxItemCount = pageSize, PartitionKey = partitionKey };
            var feedIterator = container.GetItemLinqQueryable<T>(false, continuationToken, requestOptions).Where(predicate).ToFeedIterator();
            var response = await feedIterator.ReadNextAsync();

            return (response.ContinuationToken, response.Resource);
        }

        /// <inheritdoc/>
        public async Task<T> CreateItemAsync(T item)
        {
            item.PartitionKey = this.partitionKeyValue;
            var response = await container.CreateItemAsync(item);

            return response.Resource;
        }

        /// <inheritdoc/>
        public async Task<T> UpdateItemAsync(T item)
        {
            // Note: Refactor to use native CosmosDB partial updates when available. 
            // See https://feedback.azure.com/forums/263030-azure-cosmos-db/suggestions/6693091-be-able-to-do-partial-updates-on-document?page=1&per_page=20
            var requestOptions = new ItemRequestOptions { IfMatchEtag = item.ETag };
            item.PartitionKey = this.partitionKeyValue;
            var response = await container.ReplaceItemAsync(item, item.GetId(), null, requestOptions);

            return response.Resource;
        }

        /// <inheritdoc/>
        public async Task DeleteItemAsync(string id)
            => await container.DeleteItemAsync<T>(id, partitionKey);

        /// <summary>
        /// Creates a new database
        /// </summary>
        private async Task CreateDatabaseIfNotExistsAsync()
        {
            var throughputProperties = ThroughputProperties.CreateAutoscaleThroughput(MaxAutoScaleThroughput);
            await cosmosClient.CreateDatabaseIfNotExistsAsync(container.Database.Id, throughputProperties);
        }

        /// <summary>
        /// Creates a new container
        /// </summary>
        private async Task CreateContainerIfNotExistsAsync()
        {
            var database = this.cosmosClient.GetDatabase(container.Database.Id);
            await database.CreateContainerIfNotExistsAsync(new ContainerProperties { Id = container.Id, PartitionKeyPath = $"/{RepositoryItem<T>.PartitionKeyFieldName}" });

            var existingPartitionKeyPath = (await container.ReadContainerAsync()).Resource.PartitionKeyPath;

            if (existingPartitionKeyPath is null)
            {
                throw new Exception($"Existing CosmosDb container {container.Id} does not have partition key path defined. Recreate the container.");
            }
            else if (existingPartitionKeyPath != $"/{RepositoryItem<T>.PartitionKeyFieldName}")
            {
                throw new Exception($"Existing CosmosDb container {container.Id} partition key path ({existingPartitionKeyPath}) differs from the requested one (/{RepositoryItem<T>.PartitionKeyFieldName}). Recreate the container.");
            }
        }
    }
}
