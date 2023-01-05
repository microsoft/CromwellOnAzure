// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;

namespace CromwellApiClient
{
    public class ProcessedTriggerInfo
    {
        public ProcessedWorkflowItem WorkflowSource { get; private set; }
        public List<ProcessedWorkflowItem> WorkflowInputs { get; private set; }
        public ProcessedWorkflowItem WorkflowOptions { get; private set; }
        public ProcessedWorkflowItem WorkflowDependencies { get; private set; }

        public ProcessedWorkflowItem Labels { get; private set; }

        public ProcessedTriggerInfo(ProcessedWorkflowItem workflowSource, List<ProcessedWorkflowItem> workflowInputs,
            ProcessedWorkflowItem workflowOptions, ProcessedWorkflowItem workflowDependencies, ProcessedWorkflowItem labels)
        {
            this.WorkflowSource = workflowSource;
            this.WorkflowInputs = workflowInputs;
            this.WorkflowOptions = workflowOptions;
            this.WorkflowDependencies = workflowDependencies;
            this.Labels = labels;
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
