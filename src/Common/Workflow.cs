// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;

namespace Common
{
    public class Workflow
    {
        public string WorkflowUrl { get; set; }
        public string WorkflowInputsUrl { get; set; }
        public List<string> WorkflowInputsUrls { get; set; }
        public string WorkflowOptionsUrl { get; set; }
        public string WorkflowDependenciesUrl { get; set; }
        public WorkflowFailureInfo WorkflowFailureInfo { get; set; }
        public List<TaskWarning> TaskWarnings { get; set; }

        public bool ShouldSerializeWorkflowFailureInfo() => WorkflowFailureInfo is object;
        public bool ShouldSerializeTaskWarnings() => TaskWarnings?.Count > 0;
    }
}
