// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;
using System.Runtime.Serialization;

#nullable enable

namespace Common.HostConfigs
{
    /// <summary>
    /// Host pool configurations for all deployed HostConfigs
    /// </summary>
    public class HostConfigurations : Dictionary<string, HostConfiguration>
    {
        public static HostConfigurations Empty
            => new();
    }

    /// <summary>
    /// Pool host configuration for <see cref="TesResources.SupportedBackendParameters.docker_host_configuration"/>
    /// </summary>
    [DataContract]
    public class HostConfiguration
    {
        /// <summary>
        /// Configuration for <see cref="BatchNodeInfo"/>.
        /// </summary>
        [DataMember(Name = "batchImage")]
        public BatchImage? BatchImage { get; set; }

        /// <summary>
        /// HostConfig specified vmSize information.
        /// </summary>
        [DataMember(Name = "vmSize")]
        public VirtualMachineSize? VmSize { get; set; }

        /// <summary>
        /// A Task which is run when a Node joins a Pool in the Azure Batch service, or when the Compute Node is rebooted or reimaged.
        /// </summary>
        [DataMember(Name = "startTask")]
        public StartTask? StartTask { get; set; }

        /// <summary>
        /// Configures the built-in task command builder for each task in the pool.
        /// </summary>
        [DataMember(Name = "dockerRun")]
        public DockerRun? DockerRun { get; set; }
    }
}
