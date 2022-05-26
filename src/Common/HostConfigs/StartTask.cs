// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Runtime.Serialization;

#nullable enable

namespace Common.HostConfigs
{
    /// <summary>
    /// A Task which is run when a Node joins a Pool in the Azure Batch service, or when the Compute Node is rebooted or reimaged.
    /// </summary>
    [DataContract]
    public class StartTask
    {
        /// <summary>
        /// Hash covering the start task metadata and the start task script file. Will be null if the HostConfig directory does not contain the task script file.
        /// </summary>
        [DataMember(Name = "startTaskHash")]
        public string? StartTaskHash { get; set; }

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
