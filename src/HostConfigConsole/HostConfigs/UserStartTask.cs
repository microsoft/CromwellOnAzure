// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Runtime.Serialization;
using Common.HostConfigs;

namespace HostConfigConsole.HostConfigs
{
    /// <summary>
    /// Configures <seealso cref="Microsoft.Azure.Batch.StartTask"/> via <see cref="StartTask"/>.
    /// </summary>
    [DataContract]
    public class UserStartTask
    {
        /// <summary>
        /// A list of files that the Batch service will download to the Compute Node before running the command line.
        /// </summary>
        /// <remarks>
        /// There is a maximum size for the list of resource files. See the API docs for details.
        /// </remarks>
        [DataMember(Name = "resourceFiles")]
        public ResourceFile[]? ResourceFiles { get; set; }
    }
}
