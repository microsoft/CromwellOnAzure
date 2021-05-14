using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Common
{
    [Serializable]
    [JsonObject(MemberSerialization = MemberSerialization.OptIn)]
    public class FailedTaskInfo
    {
        [JsonProperty]
        public string TaskId { get; set; }
        public bool ShouldSerializeTaskId() { return this.TaskId != null; }

        [JsonProperty]
        public string FailureReason { get; set; }

        public bool ShouldSerializeFailureReason() { return this.FailureReason != null; }

        [JsonProperty]
        public List<string> SystemLogs { get; set; }
        public bool ShouldSerializeSystemLogs() { return this.SystemLogs != null; }

        [JsonProperty]
        public string StdOut { get; set; }
        public bool ShouldSerializeStdOut() { return this.StdOut != null; }

        [JsonProperty]
        public string StdErr { get; set; }
        public bool ShouldSerializeStdErr() { return this.StdErr != null; }

        [JsonProperty]
        public int? CromwellResultCode { get; set; }

        public bool ShouldSerializeCromwellResultCode() { return this.CromwellResultCode.HasValue; }
    }
}
