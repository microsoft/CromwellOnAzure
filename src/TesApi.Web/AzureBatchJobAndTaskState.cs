// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;

namespace TesApi.Web
{
    /// <summary>
    /// Gets the combined state of Azure Batch job, task and pool that corresponds to the given TES task
    /// </summary>
    public struct AzureBatchJobAndTaskState
    {
        /// <summary>
        /// More than one active Azure Batch job was found in the active state for this task. No other members are set.
        /// </summary>
        public bool MoreThanOneActiveJobFound { get; set; }
        /// <summary>
        /// Job is active but has not had a pool assigned within a configured time limit.
        /// </summary>
        public bool ActiveJobWithMissingAutoPool { get; set; }
        /// <summary>
        /// Attempt number for this task. The other members are from this attempt.
        /// </summary>
        public int AttemptNumber { get; set; }
        /// <summary>
        /// The Batch service encountered an error while resizing
        ///     the pool or the pool's Microsoft.Azure.Batch.CloudPool.AllocationState
        ///     was Steady.
        /// </summary>
        public bool NodeAllocationFailed { get; set; }
        /// <summary>
        /// Gets a code for the first error encountered by the compute node in this attempt. See Microsoft.Azure.Batch.Common.BatchErrorCodeStrings
        ///     for possible values.
        /// </summary>
        public string NodeErrorCode { get; set; }
        /// <summary>
        /// Gets a list of additional error details related to the first error encountered by the compute node.
        /// </summary>
        public IEnumerable<string> NodeErrorDetails { get; set; }
        /// <summary>
        /// Gets the current state of Azure Batch job. Is `null` if no batch jobs corresponding to the TES task was found.
        /// </summary>
        public JobState? JobState { get; set; }
        /// <summary>
        /// Gets the creation time of the Azure Batch job.
        /// </summary>
        public DateTime? JobStartTime { get; set; }
        /// <summary>
        /// Gets the completion time of Azure Batch job.
        /// </summary>
        public DateTime? JobEndTime { get; set; }
        /// <summary>
        /// Gets the error encountered by the Batch service in scheduling the Azure Batch job.
        /// </summary>
        public JobSchedulingError JobSchedulingError { get; set; }
        /// <summary>
        /// Gets the current state of the compute node.
        /// </summary>
        public ComputeNodeState? NodeState { get; set; }
        /// <summary>
        /// Gets the current state of the Azure Batch task.
        /// </summary>
        public TaskState? TaskState { get; set; }
        /// <summary>
        /// Gets the exit code of the program specified on the task command line.
        /// </summary>
        /// <remarks>
        /// This property is only returned if the task is in the Microsoft.Azure.Batch.Common.TaskState.Completed
        ///     state. The exit code for a process reflects the specific convention implemented
        ///     by the application developer for that process. If you use the exit code value
        ///     to make decisions in your code, be sure that you know the exit code convention
        ///     used by the application process. Note that the exit code may also be generated
        ///     by the compute node operating system, such as when a process is forcibly terminated.
        /// </remarks>
        public int? TaskExitCode { get; set; }
        /// <summary>
        /// Gets the result of the task execution.
        /// </summary>
        /// <remarks>
        /// If the value is Microsoft.Azure.Batch.Common.TaskExecutionResult.Failure, then
        ///     the details of the failure can be found in the TaskFailureInformation
        ///     property.
        /// </remarks>
        public TaskExecutionResult? TaskExecutionResult { get; set; }
        /// <summary>
        /// Gets the time at which the task started running.
        /// </summary>
        public DateTime? TaskStartTime { get; set; }
        /// <summary>
        /// Gets the time at which the task completed.
        /// </summary>
        public DateTime? TaskEndTime { get; set; }
        /// <summary>
        /// Gets information describing the task failure, if any.
        /// </summary>
        /// <remarks>
        /// This property is set only if the task is in the Microsoft.Azure.Batch.Common.TaskState.Completed
        ///     state and encountered a failure.
        /// </remarks>
        public TaskFailureInformation TaskFailureInformation { get; set; }
        /// <summary>
        /// Gets the state of the container under which the task is executing.
        /// </summary>
        /// <remarks>
        /// This is the state of the container according to the Docker service. It is equivilant
        ///     to the status field returned by "docker inspect".
        /// </remarks>
        public string TaskContainerState { get; set; }
        /// <summary>
        /// Gets detailed error information about the container under which the task is executing.
        /// </summary>
        /// <remarks>
        /// This is the detailed error string from the Docker service, if available. It is
        ///     equivilant to the error field returned by "docker inspect".
        /// </remarks>
        public string TaskContainerError { get; set; }
    }
}
