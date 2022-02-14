// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Extensions.Logging;

namespace TesApi.Web
{
    internal interface IBatchPoolsImpl
    {
        ConcurrentDictionary<string, ConcurrentQueue<IBatchPool>> ManagedBatchPools { get; }
        object syncObject { get; }
        IAzureProxy azureProxy { get; }
    }

    /// <summary>
    /// Managed Azure Batch Pools service
    /// </summary>
    public sealed class BatchPools : IBatchPools, IBatchPoolsImpl
    {
        private readonly ILogger _logger;
        private readonly IAzureProxy _azureProxy;

        /// <summary>
        /// Constructor for the Managed Azure Batch Pools service
        /// </summary>
        /// <param name="azureProxy"></param>
        /// <param name="logger"></param>
        public BatchPools(IAzureProxy azureProxy, ILogger<BatchPools> logger)
        {
            _azureProxy = azureProxy;
            _logger = logger;
        }

        /// <summary>
        /// True if the service has no active pools
        /// </summary>
        public bool IsEmpty
            => ManagedBatchPools.IsEmpty;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="poolId">The <see cref="PoolInformation.PoolId"/> of the requested <paramref name="batchPool"/>.</param>
        /// <param name="batchPool">Returns the requested <see cref="IBatchPool"/>.</param>
        /// <returns>True if the requested <paramref name="batchPool"/> was found, False otherwise.</returns>
        public bool TryGet(string poolId, out IBatchPool batchPool)
        {
            batchPool = ManagedBatchPools.Values.SelectMany(q => q).FirstOrDefault(p => p.Pool.PoolId.Equals(poolId, StringComparison.OrdinalIgnoreCase));
            return batchPool is not null;
        }

        /// <summary>
        /// Retrieves a pool that manages compute nodes of the related vmSize, creating the pool if the key doesn't exist.
        /// </summary>
        /// <param name="key">The key to locate the configured pool.</param>
        /// <param name="valueFactory">A delegate to create the pool.</param>
        /// <returns></returns>
        public async Task<IBatchPool> GetOrAdd(string key, Func<string, Task<PoolInformation>> valueFactory)
        {
            _ = valueFactory ?? throw new ArgumentNullException(nameof(valueFactory));
            var queue = ManagedBatchPools.GetOrAdd(key, k => new());
            var result = queue.LastOrDefault(Available);
            if (result is null)
            {
                await Task.Run(() =>
                {
                    lock (syncObject)
                    {
                        result = queue.LastOrDefault(Available);
                        if (result is null)
                        {
                            result = BatchPool.Create(valueFactory($"{key}-{Guid.NewGuid()}").Result, this);
                            queue.Enqueue(result);
                        }
                    }
                });
            }
            return result;

            static bool Available(IBatchPool pool)
                => pool.IsAvailable;
        }

        private readonly object syncObject = new();
        private ConcurrentDictionary<string, ConcurrentQueue<IBatchPool>> ManagedBatchPools { get; } = new();

        #region IBatchPoolsImpl
        ConcurrentDictionary<string, ConcurrentQueue<IBatchPool>> IBatchPoolsImpl.ManagedBatchPools => ManagedBatchPools;
        object IBatchPoolsImpl.syncObject => syncObject;
        IAzureProxy IBatchPoolsImpl.azureProxy => _azureProxy;
        #endregion
    }
}
