// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace Common
{
    [Serializable]
    [JsonObject(MemberSerialization =MemberSerialization.OptIn)]
    public class WorkflowFailureInfo
    {
        [JsonProperty]
        public string WorkflowFailureReason { get; set; }
        public bool ShouldSerialzeWorkflowFailureReason() { return !(this.WorkflowFailureReason is null); }

        [JsonProperty]
        public string WorkflowFailureReasonDetail { get; set; }

        public bool ShouldSerializeWorkflowFailureReasonDetail() { return !(this.WorkflowFailureReasonDetail is null); }

        [JsonProperty]
        public List<FailedTaskInfo> FailedTaskDetails { get; set; }

        public bool ShouldSerializeFailedTaskDetails() { return !(this.FailedTaskDetails is null); }
    }
}
