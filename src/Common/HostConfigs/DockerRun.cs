// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Runtime.Serialization;

#nullable enable

namespace Common.HostConfigs
{
    /// <summary>
    /// Configures the built-in task command builder for each task in the pool.
    /// </summary>
    [DataContract]
    public class DockerRun
    {
        /// <summary>
        /// Provides additional parameters to the &quot;docker run&quot; that runs each task's command.
        /// </summary>
        [DataMember(Name = "parameters")]
        public string? Parameters { get; set; }

        /// <summary>
        /// Provides an additional command run in the same docker container as the task's command, in a separate invocation, called right before the task's own script.
        /// </summary>
        /// <remarks>This does not supply a shell nor does it insert a &apos;-c&apos; between the first and additional elements. It does, however, quote all except the first element.</remarks>
        [DataMember(Name = "pretaskCommand")]
        public string[]? PreTaskCmd { get; set; }
    }
}
