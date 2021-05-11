using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Common
{
    public class WorkflowFailureInfo
    {
        public string WorkflowFailureReason { get; set; }
        public string WorkflowFailureReasonDetail { get; set; }
        public List<FailedTaskInfo> FailedTaskDetails { get; set; }
    }
}
