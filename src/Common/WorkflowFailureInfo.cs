// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;

namespace Common
{
    public class WorkflowFailureInfo
    {
        public string WorkflowFailureReason { get; set; }
        public string WorkflowFailureReasonDetail { get; set; }
        public List<FailedTaskInfo> FailedTasks { get; set; }

        public bool ShouldSerializeWorkflowFailureReason() => WorkflowFailureReason is object;
        public bool ShouldSerializeWorkflowFailureReasonDetail() => WorkflowFailureReasonDetail is object;
        public bool ShouldSerializeFailedTasks() => FailedTasks is object && FailedTasks.Count > 0;
    }
}
