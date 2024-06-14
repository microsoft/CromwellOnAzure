// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;

namespace CromwellApiClient
{
    public class ProcessedTriggerInfo
    {
        public string WorkflowUrl { get; private set; }
        public List<ProcessedWorkflowItem> WorkflowInputs { get; private set; }
        public ProcessedWorkflowItem WorkflowOptions { get; private set; }
        public ProcessedWorkflowItem WorkflowDependencies { get; private set; }

        public ProcessedTriggerInfo(string workflowUrl, List<ProcessedWorkflowItem> workflowInputs,
            ProcessedWorkflowItem workflowOptions, ProcessedWorkflowItem workflowDependencies)
        {
            this.WorkflowUrl = workflowUrl;
            this.WorkflowInputs = workflowInputs;
            this.WorkflowOptions = workflowOptions;
            this.WorkflowDependencies = workflowDependencies;
        }
    }

    public class ProcessedWorkflowItem
    {
        public string Filename { get; private set; }
        public byte[] Data { get; private set; }

        public ProcessedWorkflowItem(string filename, byte[] data)
        {
            this.Filename = filename;
            this.Data = data;
        }
    }
}
