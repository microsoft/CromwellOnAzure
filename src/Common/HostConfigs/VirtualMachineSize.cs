// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#nullable enable

using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Common.HostConfigs
{
    /// <summary>
    /// Collection of <see cref="VirtualMachineSize"/>.
    /// </summary>
    public class VirtualMachineSizes : List<VirtualMachineSize>
    {
        public static VirtualMachineSizes Empty => new();
    }

    /// <summary>
    /// Describes the properties of relevant VM sizes.
    /// </summary>
    /// <remarks>TODO</remarks>
    public class VirtualMachineSize
    {
        /// <summary>
        /// Name of the container image, for example: ubuntu quay.io/aptible/ubuntu gcr.io/my-org/my-image etc.
        /// </summary>
        /// <remarks>TODO</remarks>
        [DataMember(Name = "container")]
        public string? Container { get; set; }

        /// <summary>
        /// The family name of the virtual machine size from which to select.
        /// </summary>
        /// <remarks>TODO</remarks>
        [DataMember(Name = "familyName")]
        public string? FamilyName { get; set; }

        /// <summary>
        /// The name of the virtual machine size to use.
        /// </summary>
        /// <remarks>TODO</remarks>
        [DataMember(Name = "vmSize")]
        public string? VmSize { get; set; }

        /// <summary>
        /// The name of the virtual machine size to use.
        /// </summary>
        /// <remarks>TODO</remarks>
        [DataMember(Name = "minVmSize")]
        public string? MinVmSize { get; set; }
    }
}
