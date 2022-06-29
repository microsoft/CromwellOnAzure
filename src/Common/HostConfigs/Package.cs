// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

#nullable enable

namespace Common.HostConfigs
{
    /// <summary>
    /// Zip files for each version of the application.
    /// </summary>
    /// <remarks>
    /// Key is <code>version</code> (aka hash).
    /// </remarks>
    public class Packages : Dictionary<string, Package>
    {
        public static Packages Empty
            => new();
    }

    /// <summary>
    /// Metadata related to the Zip file for each batch application.
    /// </summary>
    [DataContract]
    public class Package
    {
        /// <summary>
        /// Names of (potentially) docker loadable tar files included in the Zip file.
        /// </summary>
        [DataMember(Name = "dockerLoadables")]
        public string[] DockerLoadables { get; set; } = Array.Empty<string>();

        /// <summary>
        /// Indicates that the Zip file contains a task start script file.
        /// </summary>
        [DataMember(Name = "containsTaskScript")]
        public bool ContainsTaskScript { get; set; }
    }
}
