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
        private readonly Func<PoolInformation, IBatchPools, IBatchPool> _batchPoolCreator;
        private readonly Func<string, IBatchPools, IBatchPool> _batchPoolRequester;

        /// <summary>
        /// Constructor for <see cref="BatchPoolFactory"/>.
        /// </summary>
        /// <param name="serviceProvider"></param>
        public BatchPoolFactory(IServiceProvider serviceProvider)
        {
            _batchPoolCreator = (pool, pools) => (IBatchPool)ActivatorUtilities.CreateFactory(typeof(BatchPool), new Type[] { typeof(PoolInformation), typeof(IBatchPools) })(serviceProvider, new object[] { pool, pools });
            _batchPoolRequester = (pool, pools) => (IBatchPool)ActivatorUtilities.CreateFactory(typeof(BatchPool), new Type[] { typeof(string), typeof(IBatchPools) })(serviceProvider, new object[] { pool, pools });
        }

        /// <summary>
        /// Creates <see cref="BatchPool"/> instances.
        /// </summary>
        /// <param name="poolInformation"></param>
        /// <param name="batchPools"></param>
        /// <returns></returns>
        public IBatchPool CreateNew(PoolInformation poolInformation, IBatchPools batchPools)
            => _batchPoolCreator(poolInformation, batchPools);

        /// <summary>
        /// Retrieves <see cref="BatchPool"/> instances.
        /// </summary>
        /// <param name="poolId"></param>
        /// <param name="batchPools"></param>
        /// <returns></returns>
        public IBatchPool Retrieve(string poolId, IBatchPools batchPools)
            => _batchPoolRequester(poolId, batchPools);
    }

    /// <summary>
    /// Factory to create <see cref="PoolRepositoryFactory"/> instances which query for only one <see cref="IBatchPool"/>.
    /// </summary>
    public sealed class PoolRepositoryFactoryFactory
    {
        private readonly Func<string, PoolRepositoryFactory> _poolRepoFactory;
        private readonly IAppCache _cache;

        private static string MakeCacheKey(string poolId)
            => nameof(PoolRepositoryFactory) + "_" + poolId;

        /// <summary>
        /// Constructor for <see cref="PoolRepositoryFactoryFactory"/>.
        /// </summary>
        /// <param name="serviceProvider"></param>
        /// <param name="configuration"></param>
        /// <param name="azureProxy"></param>
        /// <param name="cache"></param>
        /// <param name="poolDataType"></param>
        /// <param name="pendingReservationItemType"></param>
        public PoolRepositoryFactoryFactory(IServiceProvider serviceProvider, IConfiguration configuration, IAzureProxy azureProxy, IAppCache cache, Type poolDataType = default, Type pendingReservationItemType = default)
        {
            poolDataType ??= typeof(Tes.Repository.CosmosDbRepository<BatchPool.PoolData>);
            pendingReservationItemType ??= typeof(Tes.Repository.CosmosDbRepository<BatchPool.PendingReservationItem>);
            _cache = cache;

            (var cosmosDbEndpoint, var cosmosDbKey) = azureProxy.GetCosmosDbEndpointAndKeyAsync(configuration["CosmosDbAccountName"]).Result;
            _poolRepoFactory = id => (PoolRepositoryFactory)ActivatorUtilities.CreateFactory(typeof(PoolRepositoryFactory), new Type[] { typeof(string), typeof(string), typeof(string), typeof(string), typeof(string), typeof(Type), typeof(Type) })(serviceProvider, new object[] { cosmosDbEndpoint, cosmosDbKey, Common.Constants.CosmosDbDatabaseId, BatchPools.CosmosDbContainerId, id, poolDataType, pendingReservationItemType });
        }

        /// <summary>
        /// Obtains a <see cref="PoolRepositoryFactory"/> for a specific <see cref="IBatchPool"/>.
        /// </summary>
        /// <param name="poolId"></param>
        /// <returns></returns>
        public PoolRepositoryFactory GetFactory(string poolId)
            => _cache.GetOrAdd(MakeCacheKey(poolId), () => _poolRepoFactory(poolId));
    }

    /// <summary>
    /// Factory to create <see cref="Tes.Repository.CosmosDbRepository{T}"/> instances which query for only one <see cref="IBatchPool"/>.
    /// </summary>
    public sealed class PoolRepositoryFactory
    {
        private readonly Func<Tes.Repository.IRepository<BatchPool.PoolData>> _poolDataCreator;
        private readonly Func<Tes.Repository.IRepository<BatchPool.PendingReservationItem>> _poolReservationCreator;

        /// <summary>
        /// Constructor for <see cref="PoolRepositoryFactory"/>.
        /// </summary>
        /// <param name="serviceProvider"></param>
        /// <param name="endpoint"></param>
        /// <param name="key"></param>
        /// <param name="databaseId"></param>
        /// <param name="containerId"></param>
        /// <param name="partitionKeyValue"></param>
        /// <param name="poolDataType"></param>
        /// <param name="pendingReservationItemType"></param>
        public PoolRepositoryFactory(IServiceProvider serviceProvider, string endpoint, string key, string databaseId, string containerId, string partitionKeyValue, Type poolDataType, Type pendingReservationItemType)
        {
            _poolDataCreator = () => (Tes.Repository.IRepository<BatchPool.PoolData>)ActivatorUtilities.CreateFactory(poolDataType, new Type[] { typeof(string), typeof(string), typeof(string), typeof(string), typeof(string) })(serviceProvider, new object[] { endpoint, key, databaseId, containerId, partitionKeyValue });
            _poolReservationCreator = () => (Tes.Repository.IRepository<BatchPool.PendingReservationItem>)ActivatorUtilities.CreateFactory(pendingReservationItemType, new Type[] { typeof(string), typeof(string), typeof(string), typeof(string), typeof(string) })(serviceProvider, new object[] { endpoint, key, databaseId, containerId, partitionKeyValue });
        }

        /// <summary>
        /// Creates <see cref="Tes.Repository.CosmosDbRepository{T}"/> where T : Models.PoolData instances.
        /// </summary>
        /// <returns></returns>
        public Tes.Repository.IRepository<BatchPool.PoolData> CreatePoolDataRepository()
            => _poolDataCreator();

        /// <summary>
        /// Creates <see cref="Tes.Repository.CosmosDbRepository{T}"/> where T : Models.PendingReservation instances.
        /// </summary>
        /// <returns></returns>
        public Tes.Repository.IRepository<BatchPool.PendingReservationItem> CreatePendingReservationRepository()
            => _poolReservationCreator();
    }
}
