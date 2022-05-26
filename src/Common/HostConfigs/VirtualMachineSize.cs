// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#nullable enable

using System.Runtime.Serialization;

namespace Common.HostConfigs
{
    /// <summary>
    /// Describes the properties of relevant VM sizes.
    /// </summary>
    /// <remarks>
    /// Either <see cref="Name"/> or <see cref="Family"/> should be set, or <see cref="HostConfiguration.VmSize"/> should not be provided. It is unexpected that both properties would be provided.
    /// </remarks>
    public class VirtualMachineSize
    {
        /// <summary>
        /// The family name of the virtual machine size from which to select.
        /// </summary>
        /// <remarks>
        /// Either <see cref="Name"/> or <see cref="Family"/> should be set, or <see cref="HostConfiguration.VmSize"/> should not be provided. It is unexpected that both properties would be provided.
        /// </remarks>
        [DataMember(Name = "family")]
        public string? Family { get; set; }

        /// <summary>
        /// The name of the virtual machine size to use.
        /// </summary>
        /// <remarks>
        /// Either <see cref="Name"/> or <see cref="Family"/> should be set, or <see cref="HostConfiguration.VmSize"/> should not be provided. It is unexpected that both properties would be provided.
        /// </remarks>
        [DataMember(Name = "name")]
        public string? Name { get; set; }
    }
}
