// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Runtime.Serialization;


namespace TesApi.Models
{
    /// <summary>
    /// VirtualMachineInfo contains the runtime specs for a VM
    /// </summary>
    [DataContract]
    public class VirtualMachineInfo
    {
        /// <summary>
        /// Size of the VM
        /// </summary>
        [DataMember(Name = "vm_size")]
        public string VmSize { get; set; }

        /// <summary>
        /// VM Series
        /// </summary>
        [DataMember(Name = "vm_series")]
        public string VmSeries { get; set; }

        /// <summary>
        /// True if this is a low-pri VM, otherwise false
        /// </summary>
        [DataMember(Name = "vm_low_priority")]
        public bool LowPriority { get; set; }

        /// <summary>
        /// VM price per hour
        /// </summary>
        [DataMember(Name = "vm_price_per_hour")]
        public decimal PricePerHour { get; set; }

        /// <summary>
        /// VM memory size, in GB
        /// </summary>
        [DataMember(Name = "vm_memory_in_gb")]
        public double MemoryInGB { get; set; }

        /// <summary>
        /// Number of cores for this VM
        /// </summary>
        [DataMember(Name = "vm_number_of_cores")]
        public int NumberOfCores { get; set; }

        /// <summary>
        /// The resources disk size, in GB
        /// </summary>
        [DataMember(Name = "vm_resource_disk_size_in_gb")]
        public double ResourceDiskSizeInGB { get; set; }

        /// <summary>
        /// The max number of data disks for this VM
        /// </summary>
        [DataMember(Name = "vm_max_data_disk_count")]
        public int MaxDataDiskCount { get; set; }
    }
}
