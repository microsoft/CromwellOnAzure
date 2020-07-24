// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;

namespace TesApi.Web
{
    public struct AzureBatchJobAndTaskState
    {
        public bool MoreThanOneActiveJobFound { get; set; }
        public bool ActiveJobWithMissingAutoPool { get; set; }
        public int AttemptNumber { get; set; }
        public bool NodeAllocationFailed { get; set; }
        public string NodeErrorCode { get; set; }
        public IEnumerable<string> NodeErrorDetails { get; set; }
        public JobState? JobState { get; set; }
        public DateTime? JobStartTime { get; set; }
        public DateTime? JobEndTime { get; set; }
        public JobSchedulingError JobSchedulingError { get; set; }
        public TaskState? TaskState { get; set; }
        public int? TaskExitCode { get; set; }
        public TaskExecutionResult? TaskExecutionResult { get; set; }
        public DateTime? TaskStartTime { get; set; }
        public DateTime? TaskEndTime { get; set; }
        public TaskFailureInformation TaskFailureInformation { get; set; }
        public string TaskContainerState { get; set; }
        public string TaskContainerError { get; set; }
    }
}
