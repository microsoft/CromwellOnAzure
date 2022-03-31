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
        /// Indicates that the pool is available for new jobs/tasks.
        /// </summary>
        bool IsAvailable { get; }

        /// <summary>
        /// Provides the <see cref="PoolInformation"/> for the pool.
        /// </summary>
        PoolInformation Pool { get; }

        /// <summary>
        /// Provides the VmSize of the compute nodes this pool can manage
        /// </summary>
        string VmSize { get; }

        /// <summary>
        /// Either reserves an idle compute node in the pool, or requests an additional compute node.
        /// </summary>
        /// <param name="jobId">The <see cref="CloudJob.Id"/> to be assigned a node</param>
        /// <param name="isLowPriority">True if the task is low priority, False if dedicated.</param>
        /// <param name="cancellationToken"></param>
        /// <returns>An <see cref="AffinityInformation"/> describing the reserved compute node, or null if a new node is requested.</returns>
        Task<AffinityInformation> PrepareNodeAsync(string jobId, bool isLowPriority, CancellationToken cancellationToken = default);

        /// <summary>
        /// Releases a compute node reservation.
        /// </summary>
        /// <param name="affinityInformation">The <see cref="AffinityInformation"/> of the compute node to release.</param>
        void ReleaseNode(AffinityInformation affinityInformation);

        /// <summary>
        /// Releases a compute node reservation.
        /// </summary>
        /// <param name="jobId">The <see cref="CloudJob.Id"/> to be removed from the pending reservation list.</param>
        void ReleaseNode(string jobId);

        /// <summary>
        /// Method used by the {BatchPoolService} to maintain this <see cref="IBatchPool"/> in the <see cref="IBatchPools"/> service.
        /// </summary>
        /// <param name="serviceKind">The type of <see cref="ServiceKind"/> service call.</param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task ServicePoolAsync(ServiceKind serviceKind, CancellationToken cancellationToken = default);

        /// <summary>
        /// Schedules reimaging of the compute node.
        /// </summary>
        /// <param name="nodeInformation">Descriptor of the compute node to reimage.</param>
        /// <param name="taskState"></param>
        /// <returns></returns>
        /// <remarks>This needs to be called as soon as possible after the compute node enters the 'Running' state. It's safe to call at any time as well as repeatedly.</remarks>
        Task ScheduleReimage(ComputeNodeInformation nodeInformation, BatchTaskState taskState);

        /// <summary>
        /// Types of maintenance calls offered by the <see cref="IBatchPool.ServicePoolAsync(ServiceKind, CancellationToken)"/> service method.
        /// </summary>
        enum ServiceKind
        {
            /// <summary>
            /// Syncs the locally stored target values to the pool's target values.
            /// </summary>
            SyncSize,

            /// <summary>
            /// Updates the targeted numbers of dedicated and low priority compute nodes in the pool.
            /// </summary>
            Resize,

            /// <summary>
            /// Removes idle compute nodes from the pool.
            /// </summary>
            RemoveNodeIfIdle,

            /// <summary>
            /// Stages rotating or retiring this <see cref="CloudPool"/> if needed.
            /// </summary>
            Rotate,

            /// <summary>
            /// Removes <see cref="CloudPool"/> if it's retired and empty.
            /// </summary>
            RemovePoolIfEmpty,
        }
    }
}
