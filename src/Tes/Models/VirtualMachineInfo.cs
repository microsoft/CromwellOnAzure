// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Models
{
    /// <summary>
    /// VirtualMachineInformation contains the runtime specs for a VM
    /// </summary>
    public class VirtualMachineInformation
    {
        /// <summary>
        /// Size of the VM
        /// </summary>
        [TesTaskLogMetadataKey("vm_size")]
        public string VmSize { get; set; }

        /// <summary>
        /// VM Family
        /// </summary>
        [TesTaskLogMetadataKey("vm_family")]
        public string VmFamily { get; set; }

        /// <summary>
        /// True if this is a low-pri VM, otherwise false
        /// </summary>
        [TesTaskLogMetadataKey("vm_low_priority")]
        public bool LowPriority { get; set; }

        /// <summary>
        /// VM price per hour
        /// </summary>
        [TesTaskLogMetadataKey("vm_price_per_hour_usd")]
        public decimal? PricePerHour { get; set; }

        /// <summary>
        /// VM memory size, in GB
        /// </summary>
        [TesTaskLogMetadataKey("vm_memory_in_gb")]
        public double? MemoryInGB { get; set; }

        /// <summary>
        /// Number of cores for this VM
        /// </summary>
        [TesTaskLogMetadataKey("vm_number_of_cores")]
        public int? NumberOfCores { get; set; }

        /// <summary>
        /// The resources disk size, in GB
        /// </summary>
        [TesTaskLogMetadataKey("vm_resource_disk_size_in_gb")]
        public double? ResourceDiskSizeInGB { get; set; }

        /// <summary>
        /// The max number of data disks for this VM
        /// </summary>
        [TesTaskLogMetadataKey("vm_max_data_disk_count")]
        public int? MaxDataDiskCount { get; set; }
    }
}
