// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;
using System.Runtime.Serialization;

#nullable enable

namespace Common.HostConfigs
{
    /// <summary>
    /// <see cref="ApplicationVersion"/> selected by <code>host-config-id-type</code> defined in the HostConfig.
    /// </summary>
    /// <remarks>
    /// Key is the version of the application to deploy.
    /// </remarks>
    public class ApplicationVersions : Dictionary<string, ApplicationVersion>
    {
        public static ApplicationVersions Empty
            => new();
    }

    /// <summary>
    /// Defines a batch application.
    /// </summary>
    /// <remarks>
    /// Each batch account has a limit of 10 batch applications (represented by <see cref="Package"/>) across all host-configs selected for deployment.
    /// </remarks>
    [DataContract]
    public class ApplicationVersion
    {
        /// <summary>
        /// The ID of the application to deploy.
        /// </summary>
        /// <remarks>
        /// This value is calculated from the <code>host-config-id-type</code> key <seealso cref="ApplicationVersions"/>.
        /// </remarks>
        [DataMember(Name = "applicationId")]
        public string? ApplicationId { get; set; }

        /// <summary>
        /// Zip files for each version of the application.
        /// </summary>
        [DataMember(Name = "packages")]
        public Packages Packages { get; set; } = Packages.Empty;
    }
}
