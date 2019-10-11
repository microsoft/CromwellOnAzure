// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Runtime.Serialization;

namespace TesApi.Models
{
    public partial class TesResources
    {
        /// <summary>
        /// Information about the VM running that's running the task.
        /// </summary>
        [DataMember(Name = "vm_info")]
        public VirtualMachineInfo VmInfo { get; set; }
    }
}
