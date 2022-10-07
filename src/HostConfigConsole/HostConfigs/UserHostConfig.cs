﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;
using System.Runtime.Serialization;
using Common.HostConfigs;

namespace HostConfigConsole.HostConfigs
{
    /// <summary>
    /// All HostConfigs as stored in the CoA project's source repository.
    /// </summary>
    /// <remarks>
    /// Key is directory name under the "HostConfigs" source code root directory. Each directory must contain a config.json file which matches the schema of <see cref="UserHostConfig"/>. That directory may optionally contain a start-task.sh script file, a start.zip file, and a task.zip file.
    /// </remarks>
    public class UserHostConfigs : Dictionary<string, HostConfiguration>
    {
        public static UserHostConfigs Empty
            => new();
    }

    /// <summary>
    /// Pool host configuration for <see cref="TesResources.SupportedBackendParameters.docker_host_configuration"/>
    /// </summary>
    [DataContract]
    public class UserHostConfig
    {
        /// <summary>
        /// Configuration for <see cref="BatchNodeInfo"/>.
        /// </summary>
        [DataMember(Name = "batchImage")]
        public BatchImage? BatchImage { get; set; }

        /// <summary>
        /// HostConfig specified vmSize information.
        /// </summary>
        [DataMember(Name = "virtualMachineSizes")]
        public VirtualMachineSizes VirtualMachineSizes { get; set; } = VirtualMachineSizes.Empty;

        /// <summary>
        /// A Task which is run when a Node joins a Pool in the Azure Batch service, or when the Compute Node is rebooted or reimaged.
        /// </summary>
        [DataMember(Name = "startTask")]
        public UserStartTask? StartTask { get; set; }

        /// <summary>
        /// Configures the built-in task command builder for each task in the pool.
        /// </summary>
        [DataMember(Name = "dockerRun")]
        public DockerRun? DockerRun { get; set; }
    }
}