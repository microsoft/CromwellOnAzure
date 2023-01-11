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

        public bool ShouldSerializeWorkflowFailureReason() => WorkflowFailureReason is not null;
        public bool ShouldSerializeWorkflowFailureReasonDetail() => WorkflowFailureReasonDetail is not null;
        public bool ShouldSerializeFailedTasks() => FailedTasks is not null && FailedTasks.Count > 0;
    }
}
