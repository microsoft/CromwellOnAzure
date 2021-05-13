using System.Collections.Generic;

namespace Common
{
    public class WorkflowFailureInfo
    {
        [DoNotSerializeIfNull]
        public string WorkflowFailureReason { get; set; }
        [DoNotSerializeIfNull]
        public string WorkflowFailureReasonDetail { get; set; }
        [DoNotSerializeIfNull]
        public List<FailedTaskInfo> FailedTaskDetails { get; set; }
    }
}
