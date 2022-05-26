// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Runtime.Serialization;

#nullable enable

namespace Common.HostConfigs
{
    /// <summary>
    /// Configures <see cref="ResourceFile"/>
    /// </summary>
    [DataContract]
    public class ResourceFile
    {
        /// <summary>
        /// The URL of the file to download.
        /// </summary>
        [DataMember(Name = "httpUrl")]
        public string? HttpUrl { get; set; }

        /// <summary>
        /// The location on the Compute Node to which to download the file(s), relative to the Task's working directory.
        /// </summary>
        /// <remarks>Must be set, and must include the filename</remarks>
        [DataMember(Name = "filePath")]
        public string? FilePath { get; set; }

        /// <summary>
        /// The file permission mode attribute in octal format.
        /// </summary>
        /// <remarks>The default value is 0770 (aka ug+rw).</remarks>
        [DataMember(Name = "fileMode")]
        public string? FileMode { get; set; }
    }
}
