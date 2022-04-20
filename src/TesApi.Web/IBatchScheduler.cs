// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Tes.Models;

namespace TesApi.Web
{
    /// <summary>
    /// An interface for scheduling <see cref="TesTask"/>s on a batch processing system
    /// </summary>
    public interface IBatchScheduler
    {
        /// <summary>
        /// Iteratively schedule a <see cref="TesTask"/> on a batch system until completion or failure
        /// </summary>
        /// <param name="tesTask"><see cref="TesTask"/> to schedule on the batch system</param>
        /// <returns>Whether the <see cref="TesTask"/> was modified.</returns>
        ValueTask<bool> ProcessTesTaskAsync(TesTask tesTask);

        /// <summary>
        /// Enumerates all the managed batch pools.
        /// </summary>
        /// <returns></returns>
        IAsyncEnumerable<IBatchPool> GetPoolsAsync();

        /// <summary>
        /// Synchronizes the CosmosDB storage of batch pool metadata
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        ValueTask<bool> UpdateBatchPools(CancellationToken cancellationToken);

        /// <summary>
        /// Retrieves the requested batch pool.
        /// </summary>
        /// <param name="poolId">The <see cref="Microsoft.Azure.Batch.PoolInformation.PoolId"/> of the requested <paramref name="batchPool"/>.</param>
        /// <param name="batchPool">Returns the requested <see cref="IBatchPool"/>.</param>
        /// <returns>True if the requested <paramref name="batchPool"/> was found, False otherwise.</returns>
        delegate bool TryGetBatchPool(string poolId, out IBatchPool batchPool);
    }
}
