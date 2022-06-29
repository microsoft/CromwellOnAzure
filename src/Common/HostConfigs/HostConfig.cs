// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Runtime.Serialization;

#nullable enable

namespace Common.HostConfigs
{
    /// <summary>
    /// Provides metadata for TES for all deployed HostConfigs
    /// </summary>
    [DataContract]
    public class HostConfig
    {
        /// <summary>
        /// Deployed Batch Applications
        /// </summary>
        [DataMember(Name = "applicationVersions")]
        public ApplicationVersions ApplicationVersions { get; set; } = ApplicationVersions.Empty;

        /// <summary>
        /// Batch Application zip file Hashes
        /// </summary>
        [DataMember(Name = "packageHashes")]
        public PackageHashes PackageHashes { get; set; } = PackageHashes.Empty;

        /// <summary>
        /// Metadata for each deployed <see cref="HostConfiguration"/>.
        /// </summary>
        [DataMember(Name = "hostConfigurations")]
        public HostConfigurations HostConfigurations { get; set; } = HostConfigurations.Empty;
    }
}
