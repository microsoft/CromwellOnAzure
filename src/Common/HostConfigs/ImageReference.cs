// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Runtime.Serialization;

#nullable enable

namespace Common.HostConfigs
{
    /// <summary>
    /// A reference to an Azure Virtual Machines Marketplace Image.
    /// </summary>
    [DataContract]
    public class ImageReference
    {
        /// <summary>
        /// Configures the offer type of the Azure Virtual Machines Marketplace Image.
        /// </summary>
        [DataMember(Name = "offer")]
        public string? Offer { get; set; }

        /// <summary>
        /// Configures the publisher of the Azure Virtual Machines Marketplace Image.
        /// </summary>
        [DataMember(Name = "publisher")]
        public string? Publisher { get; set; }

        /// <summary>
        /// Configures the SKU of the Azure Virtual Machines Marketplace Image.
        /// </summary>
        [DataMember(Name = "sku")]
        public string? Sku { get; set; }

        /// <summary>
        /// Configures the version of the Azure Virtual Machines Marketplace Image.
        /// </summary>
        [DataMember(Name = "version")]
        public string? Version { get; set; }
    }
}
