// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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
        Task<bool> ProcessTesTaskAsync(TesTask tesTask);

        /// <summary>
        /// Garbage collects the old batch task state log hashset
        /// </summary>
        void ClearBatchLogState();
    }
}
