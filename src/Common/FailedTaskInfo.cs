// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace Common
{
    [Serializable]
    [JsonObject(MemberSerialization = MemberSerialization.OptIn)]
    public class FailedTaskInfo
    {
        [JsonProperty]
        public string TaskId { get; set; }
        public bool ShouldSerializeTaskId() { return !(this.TaskId is null); }

        [JsonProperty]
        public string FailureReason { get; set; }
        public bool ShouldSerializeFailureReason() { return !(this.FailureReason is null); }

        [JsonProperty]
        public List<string> SystemLogs { get; set; }
        public bool ShouldSerializeSystemLogs() { return !(this.SystemLogs is null); }

        [JsonProperty]
        public string StdOut { get; set; }
        public bool ShouldSerializeStdOut() { return !(this.StdOut is null); }

        [JsonProperty]
        public string StdErr { get; set; }
        public bool ShouldSerializeStdErr() { return !(this.StdErr is null); }

        [JsonProperty]
        public int? CromwellResultCode { get; set; }
        public bool ShouldSerializeCromwellResultCode() { return this.CromwellResultCode.HasValue; }
    }
}
