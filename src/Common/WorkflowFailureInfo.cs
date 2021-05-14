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
        public bool ShouldSerialzeWorkflowFailureReason() { return this.WorkflowFailureReason != null; }

        [JsonProperty]
        public string WorkflowFailureReasonDetail { get; set; }

        public bool ShouldSerializeWorkflowFailureReasonDetail() { return this.WorkflowFailureReasonDetail != null; }

        [JsonProperty]
        public List<FailedTaskInfo> FailedTaskDetails { get; set; }

        public bool ShouldSerializeFailedTaskDetails() { return this.FailedTaskDetails != null; }
    }
}
