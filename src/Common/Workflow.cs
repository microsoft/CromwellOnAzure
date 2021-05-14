// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace Common
{
    [Serializable]
    [JsonObject(MemberSerialization = MemberSerialization.OptIn)]
    public class Workflow
    {
        [JsonProperty]
        public string WorkflowUrl { get; set; }
        public static bool ShouldSerializeWorkflowUrl() { return true; }

        [JsonProperty]
        public string WorkflowInputsUrl { get; set; }
        public static bool ShouldSerializeWorkflowInputsUrl() { return true; }

        [JsonProperty]
        public List<string> WorkflowInputsUrls { get; set; }
        public static bool ShouldSerializeWorkflowInputsUrls() { return true; }

        [JsonProperty]
        public string WorkflowOptionsUrl { get; set; }
        public static bool ShouldSerializeWorkflowOptionsUrl() { return true; }

        [JsonProperty]
        public string WorkflowDependenciesUrl { get; set; }
        public static bool ShouldSerializeWorkflowDependencyUrl() { return true; }

        [JsonProperty]
        public WorkflowFailureInfo WorkflowFailureDetails { get; set; }
        public bool ShouldSerializeWorkflowFailureDetails() { return !(this.WorkflowFailureDetails is null); }
    }
}
