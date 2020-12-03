// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;

namespace Common
{
    public class Workflow
    {
        public string WorkflowUrl { get; set; }
        [Obsolete("For backwards compatibility only; please use WorkflowInputsUrls instead")]
        [Newtonsoft.Json.JsonIgnore]
        public string WorkflowInputsUrl { get; set; }
        public List<string> WorkflowInputsUrls { get; set; }
        public string WorkflowOptionsUrl { get; set; }
        public string WorkflowDependenciesUrl { get; set; }
    }
}
