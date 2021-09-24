// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;

namespace Common
{
    public class FailedTaskInfo
    {
        public string TaskId { get; set; }
        public string TaskName { get; set; }
        public string FailureReason { get; set; }
        public List<string> SystemLogs { get; set; }
        public string StdOut { get; set; }
        public string StdErr { get; set; }
        public int? CromwellResultCode { get; set; }

        public bool ShouldSerializeSystemLogs() => SystemLogs?.Count > 0;
        public bool ShouldSerializeStdOut() => StdOut is object;
        public bool ShouldSerializeStdErr() => StdErr is object;
        public bool ShouldSerializeCromwellResultCode() => CromwellResultCode is object;
    }
}
