// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;

namespace TriggerService
{
    public sealed record class ProcessedTriggerInfo(ProcessedWorkflowItem WorkflowSource, List<ProcessedWorkflowItem> WorkflowInputs,
        ProcessedWorkflowItem WorkflowOptions, ProcessedWorkflowItem WorkflowDependencies, ProcessedWorkflowItem WorkflowLabels);

    public sealed record class ProcessedWorkflowItem(string Filename, byte[] Data);
}
