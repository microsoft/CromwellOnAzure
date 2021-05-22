// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;

namespace TesApi.Web
{
    /// <summary>
    /// AzureBatchJobAndTaskState
    /// </summary>
    public struct AzureBatchJobAndTaskState
    {
        /// <summary>
        /// MoreThanOneActiveJobFound
        /// </summary>
        public bool MoreThanOneActiveJobFound { get; set; }

        /// <summary>
        /// ActiveJobWithMissingAutoPool
        /// </summary>
        public bool ActiveJobWithMissingAutoPool { get; set; }

        /// <summary>
        /// AttemptNumber
        /// </summary>
        public int AttemptNumber { get; set; }

        /// <summary>
        /// NodeAllocationFailed
        /// </summary>
        public bool NodeAllocationFailed { get; set; }

        /// <summary>
        /// NodeErrorCode
        /// </summary>
        public string NodeErrorCode { get; set; }

        /// <summary>
        /// NodeErrorDetails
        /// </summary>
        public IEnumerable<string> NodeErrorDetails { get; set; }

        /// <summary>
        /// JobState
        /// </summary>
        public JobState? JobState { get; set; }

        /// <summary>
        /// JobStartTime
        /// </summary>
        public DateTime? JobStartTime { get; set; }

        /// <summary>
        /// JobEndTime
        /// </summary>
        public DateTime? JobEndTime { get; set; }

        /// <summary>
        /// JobSchedulingError
        /// </summary>
        public JobSchedulingError JobSchedulingError { get; set; }

        /// <summary>
        /// NodeState
        /// </summary>
        public ComputeNodeState? NodeState { get; set; }

        /// <summary>
        /// TaskState
        /// </summary>
        public TaskState? TaskState { get; set; }

        /// <summary>
        /// TaskExitCode
        /// </summary>
        public int? TaskExitCode { get; set; }

        /// <summary>
        /// TaskExecutionResult
        /// </summary>
        public TaskExecutionResult? TaskExecutionResult { get; set; }

        /// <summary>
        /// TaskStartTime
        /// </summary>
        public DateTime? TaskStartTime { get; set; }

        /// <summary>
        /// TaskEndTime
        /// </summary>
        public DateTime? TaskEndTime { get; set; }

        /// <summary>
        /// TaskFailureInformation
        /// </summary>
        public TaskFailureInformation TaskFailureInformation { get; set; }

        /// <summary>
        /// TaskContainerState
        /// </summary>
        public string TaskContainerState { get; set; }

        /// <summary>
        /// TaskContainerError
        /// </summary>
        public string TaskContainerError { get; set; }
    }
}
