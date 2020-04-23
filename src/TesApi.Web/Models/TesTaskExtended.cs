// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Runtime.Serialization;

namespace TesApi.Models
{
    public partial class TesTask
    {
        /// <summary>
        /// Number of retries attempted
        /// </summary>
        [DataMember(Name = "error_count")]
        public int ErrorCount { get; set; }

        /// <summary>
        /// Boolean of whether cancellation was requested
        /// </summary>
        [DataMember(Name = "is_cancel_requested")]
        public bool IsCancelRequested { get; set; }

        /// <summary>
        /// Date + time the task was completed, in RFC 3339 format. This is set by the system, not the client.
        /// </summary>
        /// <value>Date + time the task was completed, in RFC 3339 format. This is set by the system, not the client.</value>
        [DataMember(Name = "end_time")]
        public string EndTime { get; set; }


        /// <summary>
        /// Top-most parent workflow ID from the workflow engine
        /// </summary>
        [DataMember(Name = "workflow_id")]
        public string WorkflowId { get; set; }
    }
}
