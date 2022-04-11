// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using LazyCache;
using Microsoft.Azure.Batch;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace TesApi.Web
{
    /// <summary>
    /// Factory to create BatchPool instances.
    /// </summary>
    public sealed class BatchPoolFactory
    {
        private readonly Func<PoolInformation, string, IBatchPools, IBatchPool> _batchPoolCreator;
        private readonly Func<string, string, IBatchPools, IBatchPool> _batchPoolRequester;

        /// <summary>
        /// Constructor for <see cref="BatchPoolFactory"/>.
        /// </summary>
        /// <param name="serviceProvider"></param>
        public BatchPoolFactory(IServiceProvider serviceProvider)
        {
            _batchPoolCreator = (pool, key, pools) => (IBatchPool)ActivatorUtilities.CreateFactory(typeof(BatchPool), new Type[] { typeof(PoolInformation), typeof(string), typeof(IBatchPools) })(serviceProvider, new object[] { pool, key, pools });
            _batchPoolRequester = (pool, key, pools) => (IBatchPool)ActivatorUtilities.CreateFactory(typeof(BatchPool), new Type[] { typeof(string), typeof(string), typeof(IBatchPools) })(serviceProvider, new object[] { pool, key, pools });
        }

        /// <summary>
        /// Creates <see cref="BatchPool"/> instances.
        /// </summary>
        /// <param name="poolInformation"></param>
        /// <param name="key"></param>
        /// <param name="batchPools"></param>
        /// <returns></returns>
        public IBatchPool CreateNew(PoolInformation poolInformation, string key, IBatchPools batchPools)
            => _batchPoolCreator(poolInformation, key, batchPools);

        /// <summary>
        /// Retrieves <see cref="BatchPool"/> instances.
        /// </summary>
        /// <param name="poolId"></param>
        /// <param name="key"></param>
        /// <param name="batchPools"></param>
        /// <returns></returns>
        public IBatchPool Retrieve(string poolId, string key, IBatchPools batchPools)
            => _batchPoolRequester(poolId, key, batchPools);
    }

    /// <summary>
    /// Factory to create factory instances of <seealso cref="Tes.Repository.CosmosDbRepository{T}"/> which segment via "partitionKeyValue".
    /// </summary>
    public sealed class PoolRepositoryFactory
    {
        private readonly Func<string, PoolDataRepositoryFactory> _poolDataRepoFactory;
        private readonly Func<string, PoolPendingReservationRepositoryFactory> _poolPendingReservationFactory;
        private readonly IAppCache _cache;

        private static string MakePoolDataRepositoryCacheKey(string key)
            => nameof(PoolDataRepositoryFactory) + "_" + key;
        private static string MakePendingReservationCacheKey(string poolId)
            => nameof(PoolPendingReservationRepositoryFactory) + "_" + poolId;

        /// <summary>
        /// Constructor for <see cref="PoolRepositoryFactory"/>.
        /// </summary>
        /// <param name="serviceProvider"></param>
        /// <param name="configuration"></param>
        /// <param name="azureProxy"></param>
        /// <param name="cache"></param>
        /// <param name="poolDataType">Override for testing.</param>
        /// <param name="pendingReservationItemType">Override for testing.</param>
        public PoolRepositoryFactory(IServiceProvider serviceProvider, IConfiguration configuration, IAzureProxy azureProxy, IAppCache cache, Type poolDataType = default, Type pendingReservationItemType = default)
        {
            poolDataType ??= typeof(Tes.Repository.CosmosDbRepository<BatchPool.PoolData>);
            pendingReservationItemType ??= typeof(Tes.Repository.CosmosDbRepository<BatchPool.PendingReservationItem>);
            _cache = cache;

            (var cosmosDbEndpoint, var cosmosDbKey) = azureProxy.GetCosmosDbEndpointAndKeyAsync(configuration["CosmosDbAccountName"]).Result;
            _poolDataRepoFactory = id => (PoolDataRepositoryFactory)ActivatorUtilities.CreateFactory(typeof(PoolDataRepositoryFactory), new Type[] { typeof(string), typeof(string), typeof(string), typeof(string), typeof(string), typeof(Type) })(serviceProvider, new object[] { cosmosDbEndpoint, cosmosDbKey, Common.Constants.CosmosDbDatabaseId, BatchPools.CosmosDbContainerId, id, poolDataType});
            _poolPendingReservationFactory = id => (PoolPendingReservationRepositoryFactory)ActivatorUtilities.CreateFactory(typeof(PoolPendingReservationRepositoryFactory), new Type[] { typeof(string), typeof(string), typeof(string), typeof(string), typeof(string), typeof(Type) })(serviceProvider, new object[] { cosmosDbEndpoint, cosmosDbKey, Common.Constants.CosmosDbDatabaseId, BatchPools.CosmosDbContainerId, id, pendingReservationItemType });
        }

        /// <summary>
        /// Obtains a <see cref="PoolDataRepositoryFactory"/> for a specific batch pools key.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public PoolDataRepositoryFactory GetPoolDataRepositoryFactory(string key)
            => _cache.GetOrAdd(MakePoolDataRepositoryCacheKey(key), () => _poolDataRepoFactory(key));

        /// <summary>
        /// Obtains a <see cref="PoolPendingReservationRepositoryFactory"/> for a specific <see cref="IBatchPool"/>.
        /// </summary>
        /// <param name="poolId"></param>
        /// <returns></returns>
        public PoolPendingReservationRepositoryFactory GetPoolPendingReservationRepositoryFactory(string poolId)
            => _cache.GetOrAdd(MakePendingReservationCacheKey(poolId), () => _poolPendingReservationFactory(poolId));

        /// <summary>
        /// Factory to create <see cref="Tes.Repository.CosmosDbRepository{T}"/> instances which query for only one <see cref="IBatchPool"/>.
        /// </summary>
        public sealed class PoolDataRepositoryFactory
        {
            private readonly Func<Tes.Repository.IRepository<BatchPool.PoolData>> _repositoryCreator;

            /// <summary>
            /// Constructor for <see cref="PoolDataRepositoryFactory"/>.
            /// </summary>
            /// <param name="serviceProvider"></param>
            /// <param name="endpoint"></param>
            /// <param name="key"></param>
            /// <param name="databaseId"></param>
            /// <param name="containerId"></param>
            /// <param name="partitionKeyValue"></param>
            /// <param name="poolDataType"></param>
            public PoolDataRepositoryFactory(IServiceProvider serviceProvider, string endpoint, string key, string databaseId, string containerId, string partitionKeyValue, Type poolDataType)
                => _repositoryCreator = () => (Tes.Repository.IRepository<BatchPool.PoolData>)ActivatorUtilities.CreateFactory(poolDataType, new Type[] { typeof(string), typeof(string), typeof(string), typeof(string), typeof(string) })(serviceProvider, new object[] { endpoint, key, databaseId, containerId, partitionKeyValue });

            /// <summary>
            /// Creates <see cref="Tes.Repository.CosmosDbRepository{T}"/> where T : Models.PoolData instances.
            /// </summary>
            /// <returns></returns>
            public Tes.Repository.IRepository<BatchPool.PoolData> CreateRepository()
                => _repositoryCreator();
        }

        /// <summary>
        /// Factory to create <see cref="Tes.Repository.CosmosDbRepository{T}"/> instances which query for only one <see cref="IBatchPool"/>.
        /// </summary>
        public sealed class PoolPendingReservationRepositoryFactory
        {
            private readonly Func<Tes.Repository.IRepository<BatchPool.PendingReservationItem>> _repositoryCreator;

            /// <summary>
            /// Constructor for <see cref="PoolPendingReservationRepositoryFactory"/>.
            /// </summary>
            /// <param name="serviceProvider"></param>
            /// <param name="endpoint"></param>
            /// <param name="key"></param>
            /// <param name="databaseId"></param>
            /// <param name="containerId"></param>
            /// <param name="partitionKeyValue"></param>
            /// <param name="pendingReservationItemType"></param>
            public PoolPendingReservationRepositoryFactory(IServiceProvider serviceProvider, string endpoint, string key, string databaseId, string containerId, string partitionKeyValue, Type pendingReservationItemType)
                => _repositoryCreator = () => (Tes.Repository.IRepository<BatchPool.PendingReservationItem>)ActivatorUtilities.CreateFactory(pendingReservationItemType, new Type[] { typeof(string), typeof(string), typeof(string), typeof(string), typeof(string) })(serviceProvider, new object[] { endpoint, key, databaseId, containerId, partitionKeyValue });

            /// <summary>
            /// Creates <see cref="Tes.Repository.CosmosDbRepository{T}"/> where T : Models.PendingReservation instances.
            /// </summary>
            /// <returns></returns>
            public Tes.Repository.IRepository<BatchPool.PendingReservationItem> CreateRepository()
                => _repositoryCreator();
        }
    }
}
