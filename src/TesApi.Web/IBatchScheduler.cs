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
        IEnumerable<IBatchPool> GetPools();

        /// <summary>
        /// Provides a list of pools that can safely be disposed of.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        ValueTask<IEnumerable<Task>> GetShutdownCandidatePools(CancellationToken cancellationToken);
    }
}
