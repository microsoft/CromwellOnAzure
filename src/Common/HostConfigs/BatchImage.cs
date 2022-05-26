// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Runtime.Serialization;

#nullable enable

namespace Common.HostConfigs
{
    /// <summary>
    /// Configuration for <see cref="BatchNodeInfo"/>. <seealso cref="VirtualMachineConfiguration"/>
    /// </summary>
    [DataContract]
    public class BatchImage
    {
        /// <summary>
        /// A reference to an Azure Virtual Machines Marketplace Image.
        /// </summary>
        [DataMember(Name = "imageReference")]
        public ImageReference? ImageReference { get; set; }

        /// <summary>
        /// Configures the SKU of Batch Node Agent to be provisioned on the compute node.
        /// </summary>
        [DataMember(Name = "nodeAgentSKUId")]
        public string? NodeAgentSkuId { get; set; }
    }
}
