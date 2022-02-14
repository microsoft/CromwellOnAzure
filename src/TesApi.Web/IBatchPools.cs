// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;

namespace TesApi.Web
{
    /// <summary>
    /// Managed Azure Batch Pools service
    /// </summary>
    public interface IBatchPools
    {
        /// <summary>
        /// True if the service has no active pools
        /// </summary>
        bool IsEmpty { get; }

        /// <summary>
        /// Retrieves a pool that manages compute nodes of the related vmSize, creating the pool if the key doesn't exist.
        /// </summary>
        /// <param name="key">The key to locate the configured pool.</param>
        /// <param name="valueFactory">A delegate to create the pool.</param>
        /// <returns></returns>
        Task<IBatchPool> GetOrAdd(string key, Func<string, Task<PoolInformation>> valueFactory);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="poolId">The <see cref="PoolInformation.PoolId"/> of the requested <paramref name="batchPool"/>.</param>
        /// <param name="batchPool">Returns the requested <see cref="IBatchPool"/>.</param>
        /// <returns>True if the requested <paramref name="batchPool"/> was found, False otherwise.</returns>
        bool TryGet(string poolId, out IBatchPool batchPool);
    }
}
