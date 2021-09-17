// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;

namespace Common
{
    public class TaskWarning
    {
        public string TaskId { get; set; }
        public string TaskName { get; set; }
        public string Warning { get; set; }
        public List<string> WarningDetails { get; set; }

        public bool ShouldSerializeWarningDetails() => WarningDetails?.Count > 0;
    }
}
