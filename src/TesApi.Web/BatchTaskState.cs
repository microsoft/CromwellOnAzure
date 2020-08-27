// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web
{
    /// <summary>
    /// A superset of Azure Batch task states
    /// </summary>
    public enum BatchTaskState
    {
        /// <summary>
        /// The task has been assigned to a compute node, but is waiting for a
        /// required Job Preparation task to complete on the node.
        /// </summary>
        Initializing,
        /// <summary>
        /// The task is running on a compute node.
        /// </summary>
        Running,
        /// <summary>
        /// The task is no longer eligible to run, usually because the task has
        /// finished successfully, or the task has finished unsuccessfully and
        /// has exhausted its retry limit.  A task is also marked as completed
        /// if an error occurred launching the task, or when the task has been
        /// terminated.
        /// </summary>
        CompletedSuccessfully,

        /// <summary>
        /// The task has completed, but it finished unsuccessfully or the executor had an error
        /// </summary>
        CompletedWithErrors,

        /// <summary>
        /// Batch job is active and has non-null ExecutionInformation.PoolId but pool does not exist
        /// </summary>
        ActiveJobWithMissingAutoPool,

        /// <summary>
        /// Batch job is either not found or is being deleted
        /// </summary>
        JobNotFound,

        /// <summary>
        /// An error occurred while attempting to retrieve jobs from the Azure Batch API.  This can occur due to a network or service issue
        /// </summary>
        ErrorRetrievingJobs,

        /// <summary>
        /// More than one active job is associated with a given task.  This may indicate an implementation defect due to an unforeseen edge case
        /// </summary>
        MoreThanOneActiveJobFound,

        /// <summary>
        /// Azure Batch was unable to allocate a machine for the job.  This could be due to either a temporary or permanent unavailability of the given VM SKU
        /// </summary>
        NodeAllocationFailed,

        /// <summary>
        /// Azure Batch pre-empted the execution of this task while running on a low-priority node
        /// </summary>
        NodePreempted,

        /// <summary>
        /// node in an Unusable state detected
        /// </summary>
        NodeUnusable,

        /// <summary>
        /// Batch job exists but task is missing. This can happen if scheduler goes down after creating the job but before creating the task.
        /// </summary>
        MissingBatchTask,

        /// <summary>
        /// Node failed during startup or task execution (for example, ContainerInvalidImage, DiskFull)
        /// </summary>
        NodeFailedDuringStartupOrExecution
    }
}
