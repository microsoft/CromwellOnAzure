using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Common
{
    public class FailedTaskInfo
    {
        public string TaskId { get; set; }
        public string FailureReason { get; set; }
        public List<string> SystemLogs { get; set; } 
        public string StdOut { get; set; }
        public string StdErr { get; set; }
        public int CromwellResultCode { get; set; }
    }
}
