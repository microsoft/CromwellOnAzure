// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using BatchModels = Microsoft.Azure.Management.Batch.Models;

namespace TesApi.Web
{
    /// <summary>
    /// Managed Azure Batch Pools service
    /// </summary>
    public interface IBatchPools
    {
        /// <summary>
        /// Enumerates all the managed batch pools.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        IAsyncEnumerable<IBatchPool> GetPoolsAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Indicates that a pool for the key value is available to service tasks.
        /// </summary>
        /// <param name="key">The key to locate the configured pool.</param>
        /// <returns>True if pool is available, otherwise a new pool would need to be created to service the indicated configuration.</returns>
        bool IsPoolAvailable(string key);

        /// <summary>
        /// Retrieves a pool that manages compute nodes of the related vmSize, creating the pool if the key doesn't exist.
        /// </summary>
        /// <param name="key">The key to locate the configured pool. Must be between 1 and 50 chars in length.</param>
        /// <param name="valueFactory">A delegate to create the pool. Only called (once, even if called by multiple threads) if a pool isn't available.</param>
        /// <remarks>The argument to <paramref name="valueFactory"/> needs to be the Name argument of its construtor. It's recommended to set the <see cref="BatchModels.Pool.DisplayName"/> to the <paramref name="key"/> value.</remarks>
        /// <returns></returns>
        Task<IBatchPool> GetOrAddAsync(string key, Func<string, ValueTask<BatchModels.Pool>> valueFactory);

        /// <summary>
        /// Retrieves the requested batch pool.
        /// </summary>
        /// <param name="poolId">The <see cref="PoolInformation.PoolId"/> of the requested <paramref name="batchPool"/>.</param>
        /// <param name="batchPool">Returns the requested <see cref="IBatchPool"/>.</param>
        /// <returns>True if the requested <paramref name="batchPool"/> was found, False otherwise.</returns>
        bool TryGet(string poolId, out IBatchPool batchPool);

        /// <summary>
        /// Schedules reimaging of the compute node.
        /// </summary>
        /// <param name="nodeInformation">Descriptor of the compute node to reimage.</param>
        /// <param name="taskState"></param>
        /// <returns></returns>
        /// <remarks>This needs to be called as soon as possible after the compute node enters the 'Running' state. It's safe to call at any time as well as repeatedly.</remarks>
        Task ScheduleReimage(ComputeNodeInformation nodeInformation, BatchTaskState taskState);
    }
}
