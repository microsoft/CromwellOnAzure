﻿// Copyright (c) Microsoft Corporation.
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
        private readonly Func<PoolInformation, string, IBatchScheduler, IBatchPool> _batchPoolCreator;
        private readonly Func<BatchPool.PoolData, string, IBatchScheduler, IBatchPool> _batchPoolRequester;

        /// <summary>
        /// Constructor for <see cref="BatchPoolFactory"/>.
        /// </summary>
        /// <param name="serviceProvider"></param>
        public BatchPoolFactory(IServiceProvider serviceProvider)
        {
            _batchPoolCreator = (pool, key, batchScheduler) => (IBatchPool)ActivatorUtilities.CreateFactory(typeof(BatchPool), new Type[] { typeof(PoolInformation), typeof(string), typeof(IBatchScheduler) })(serviceProvider, new object[] { pool, key, batchScheduler });
            _batchPoolRequester = (pool, key, batchScheduler) => (IBatchPool)ActivatorUtilities.CreateFactory(typeof(BatchPool), new Type[] { typeof(BatchPool.PoolData), typeof(string), typeof(IBatchScheduler) })(serviceProvider, new object[] { pool, key, batchScheduler });
        }

        /// <summary>
        /// Creates <see cref="BatchPool"/> instances.
        /// </summary>
        /// <param name="poolInformation"></param>
        /// <param name="key"></param>
        /// <param name="batchScheduler"></param>
        /// <returns></returns>
        public IBatchPool CreateNew(PoolInformation poolInformation, string key, IBatchScheduler batchScheduler)
            => _batchPoolCreator(poolInformation, key, batchScheduler);

        /// <summary>
        /// Retrieves <see cref="BatchPool"/> instances.
        /// </summary>
        /// <param name="poolData"></param>
        /// <param name="key"></param>
        /// <param name="batchScheduler"></param>
        /// <returns></returns>
        public IBatchPool Retrieve(BatchPool.PoolData poolData, string key, IBatchScheduler batchScheduler)
            => _batchPoolRequester(poolData, key, batchScheduler);
    }

    ///// <summary>
    ///// Factory to create factory instances of <seealso cref="Tes.Repository.CosmosDbRepository{T}"/> which segment via "partitionKeyValue".
    ///// </summary>
    //public sealed class PoolRepositoryFactory
    //{
    //    private readonly Func<string, PoolDataRepositoryFactory> _poolDataRepoFactory;
    //    private readonly Func<string, PoolPendingReservationRepositoryFactory> _poolPendingReservationFactory;
    //    private readonly IAppCache _cache;

    //    private static string MakePoolDataRepositoryCacheKey(string key)
    //        => nameof(PoolDataRepositoryFactory) + "_" + key;
    //    private static string MakePendingReservationCacheKey(string poolId)
    //        => nameof(PoolPendingReservationRepositoryFactory) + "_" + poolId;

    //    /// <summary>
    //    /// Constructor for <see cref="PoolRepositoryFactory"/>.
    //    /// </summary>
    //    /// <param name="serviceProvider"></param>
    //    /// <param name="cosmosCredentials"></param>
    //    /// <param name="cache"></param>
    //    /// <param name="poolDataType">Override for testing.</param>
    //    /// <param name="pendingReservationItemType">Override for testing.</param>
    //    public PoolRepositoryFactory(IServiceProvider serviceProvider, CosmosCredentials cosmosCredentials, IAppCache cache, Type poolDataType = default, Type pendingReservationItemType = default)
    //    {
    //        poolDataType ??= typeof(Tes.Repository.CosmosDbRepository<BatchPool.PoolData>);
    //        pendingReservationItemType ??= typeof(Tes.Repository.CosmosDbRepository<BatchPool.PendingReservationItem>);
    //        _cache = cache;

    //        _poolDataRepoFactory = id => (PoolDataRepositoryFactory)ActivatorUtilities.CreateFactory(typeof(PoolDataRepositoryFactory), new Type[] { typeof(string), typeof(string), typeof(string), typeof(string), typeof(string), typeof(Type) })(serviceProvider, new object[] { cosmosCredentials.CosmosDbEndpoint, cosmosCredentials.CosmosDbKey, Common.Constants.CosmosDbDatabaseId, BatchScheduler.CosmosDbContainerId, id, poolDataType});
    //        _poolPendingReservationFactory = id => (PoolPendingReservationRepositoryFactory)ActivatorUtilities.CreateFactory(typeof(PoolPendingReservationRepositoryFactory), new Type[] { typeof(string), typeof(string), typeof(string), typeof(string), typeof(string), typeof(Type) })(serviceProvider, new object[] { cosmosCredentials.CosmosDbEndpoint, cosmosCredentials.CosmosDbKey, Common.Constants.CosmosDbDatabaseId, BatchScheduler.CosmosDbContainerId, id, pendingReservationItemType });
    //    }

    //    /// <summary>
    //    /// Obtains a <see cref="PoolDataRepositoryFactory"/> for a specific batch pools key.
    //    /// </summary>
    //    /// <param name="key"></param>
    //    /// <returns></returns>
    //    public PoolDataRepositoryFactory GetPoolDataRepositoryFactory(string key)
    //        => _cache.GetOrAdd(MakePoolDataRepositoryCacheKey(key), () => _poolDataRepoFactory(key));

    //    /// <summary>
    //    /// Obtains a <see cref="PoolPendingReservationRepositoryFactory"/> for a specific <see cref="IBatchPool"/>.
    //    /// </summary>
    //    /// <param name="poolId"></param>
    //    /// <returns></returns>
    //    public PoolPendingReservationRepositoryFactory GetPoolPendingReservationRepositoryFactory(string poolId)
    //        => _cache.GetOrAdd(MakePendingReservationCacheKey(poolId), () => _poolPendingReservationFactory(poolId));

    //    /// <summary>
    //    /// Factory to create <see cref="Tes.Repository.CosmosDbRepository{T}"/> instances which query for only one <see cref="IBatchPool"/>.
    //    /// </summary>
    //    public sealed class PoolDataRepositoryFactory
    //    {
    //        private readonly Func<Tes.Repository.IRepository<BatchPool.PoolData>> _repositoryCreator;

    //        /// <summary>
    //        /// Constructor for <see cref="PoolDataRepositoryFactory"/>.
    //        /// </summary>
    //        /// <param name="serviceProvider"></param>
    //        /// <param name="endpoint"></param>
    //        /// <param name="key"></param>
    //        /// <param name="databaseId"></param>
    //        /// <param name="containerId"></param>
    //        /// <param name="partitionKeyValue"></param>
    //        /// <param name="poolDataType"></param>
    //        public PoolDataRepositoryFactory(IServiceProvider serviceProvider, string endpoint, string key, string databaseId, string containerId, string partitionKeyValue, Type poolDataType)
    //            => _repositoryCreator = () => (Tes.Repository.IRepository<BatchPool.PoolData>)ActivatorUtilities.CreateFactory(poolDataType, new Type[] { typeof(string), typeof(string), typeof(string), typeof(string), typeof(string) })(serviceProvider, new object[] { endpoint, key, databaseId, containerId, partitionKeyValue });

    //        /// <summary>
    //        /// Creates <see cref="Tes.Repository.CosmosDbRepository{T}"/> where T : Models.PoolData instances.
    //        /// </summary>
    //        /// <returns></returns>
    //        public Tes.Repository.IRepository<BatchPool.PoolData> CreateRepository()
    //            => _repositoryCreator();
    //    }

    //    /// <summary>
    //    /// Factory to create <see cref="Tes.Repository.CosmosDbRepository{T}"/> instances which query for only one <see cref="IBatchPool"/>.
    //    /// </summary>
    //    public sealed class PoolPendingReservationRepositoryFactory
    //    {
    //        private readonly Func<Tes.Repository.IRepository<BatchPool.PendingReservationItem>> _repositoryCreator;

    //        /// <summary>
    //        /// Constructor for <see cref="PoolPendingReservationRepositoryFactory"/>.
    //        /// </summary>
    //        /// <param name="serviceProvider"></param>
    //        /// <param name="endpoint"></param>
    //        /// <param name="key"></param>
    //        /// <param name="databaseId"></param>
    //        /// <param name="containerId"></param>
    //        /// <param name="partitionKeyValue"></param>
    //        /// <param name="pendingReservationItemType"></param>
    //        public PoolPendingReservationRepositoryFactory(IServiceProvider serviceProvider, string endpoint, string key, string databaseId, string containerId, string partitionKeyValue, Type pendingReservationItemType)
    //            => _repositoryCreator = () => (Tes.Repository.IRepository<BatchPool.PendingReservationItem>)ActivatorUtilities.CreateFactory(pendingReservationItemType, new Type[] { typeof(string), typeof(string), typeof(string), typeof(string), typeof(string) })(serviceProvider, new object[] { endpoint, key, databaseId, containerId, partitionKeyValue });

    //        /// <summary>
    //        /// Creates <see cref="Tes.Repository.CosmosDbRepository{T}"/> where T : Models.PendingReservation instances.
    //        /// </summary>
    //        /// <returns></returns>
    //        public Tes.Repository.IRepository<BatchPool.PendingReservationItem> CreateRepository()
    //            => _repositoryCreator();
    //    }
    //}
}
