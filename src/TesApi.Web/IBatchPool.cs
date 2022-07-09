// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;

namespace TesApi.Web
{
    /// <summary>
    /// Represents a pool in an Azure Batch Account.
    /// </summary>
    public interface IBatchPool
    {
        /// <summary>
        /// Types of maintenance calls offered by the <see cref="IBatchPool.ServicePoolAsync(ServiceKind, CancellationToken)"/> service method.
        /// </summary>
        enum ServiceKind
        {
            /// <summary>
            /// Queues resize errors (if available).
            /// </summary>
            GetResizeErrors,

            /// <summary>
            /// Removes idle compute nodes from the pool.
            /// </summary>
            RemoveNodeIfIdle,

            /// <summary>
            /// Removes <see cref="CloudPool"/> if it's retired and empty.
            /// </summary>
            RemovePoolIfEmpty,

            /// <summary>
            /// Updates the targeted numbers of dedicated and low priority compute nodes in the pool.
            /// </summary>
            Resize,

            /// <summary>
            /// Stages rotating or retiring this <see cref="CloudPool"/> if needed.
            /// </summary>
            Rotate,

            /// <summary>
            /// Save the pool as update
            /// </summary>
            Update,
        }

        /// <summary>
        /// Indicates that the pool is available for new jobs/tasks.
        /// </summary>
        bool IsAvailable { get; }

        /// <summary>
        /// Indicates that the pool contains only preemptable nodes. If false, indicates that the pool contains only dedicated nodes.
        /// </summary>
        bool IsPreemptable { get; }

        /// <summary>
        /// Provides the <see cref="PoolInformation"/> for the pool.
        /// </summary>
        PoolInformation Pool { get; }

        /// <summary>
        /// Indicates that the pool is not scheduled to run tasks nor running tasks.
        /// </summary>
        /// <param name="cancellationToken"></param>
        ValueTask<bool> CanBeDeleted(CancellationToken cancellationToken = default);

        /// <summary>
        /// Removes and returns the next available resize error.
        /// </summary>
        /// <returns>The first <see cref="ResizeError"/> in the list, or null if the list is empty.</returns>
        ResizeError PopNextResizeError();

        /// <summary>
        /// Removes and returns the next available start task failure.
        /// </summary>
        /// <returns>The first <see cref="TaskFailureInformation"/> in the list, or null if the list is empty.</returns>
        TaskFailureInformation PopNextStartTaskFailure();

        /// <summary>
        /// Updates this instance based on changes to its environment.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <remarks>Calls each internal servicing method in order. Throws all exceptions gathered from all methods.</remarks>
        ValueTask ServicePoolAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Service methods dispatcher.
        /// </summary>
        /// <param name="serviceKind">The type of <see cref="ServiceKind"/> service call.</param>
        /// <param name="cancellationToken"></param>
        ValueTask ServicePoolAsync(ServiceKind serviceKind, CancellationToken cancellationToken = default);
    }
}
