// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.IO;
using System.Linq;
using Microsoft.Azure.Batch;

namespace TesApi.Web
{
    /// <summary>
    /// Util class for Azure Batch related helper functions.
    /// </summary>
    public class BatchUtils
    {
        /// <summary>
        /// Readonly variable for the command line string so we're only reading from the file once.
        /// </summary>
        public static readonly string StartTaskScript = GetStartTaskScript();

        /// <summary>
        /// Converts the install-docker.sh shell script to a string.
        /// </summary>
        /// <returns>The string version of the shell script.</returns>
        private static string GetStartTaskScript()
        {
            return File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Scripts/start-task.sh"));
        }

        /// <summary>
        /// Returns the host config file.
        /// </summary>
        /// <param name="host">The <seealso cref="Tes.Models.TesResources.SupportedBackendParameters.docker_host_configuration"/> <see cref="Tes.Models.TesResources.BackendParameters"/> value</param>
        /// <returns><see cref="Stream"/></returns>
        public static Stream GetHostConfig(string host)
            => GetHostConfigFile(host, "config.json")?.OpenRead();

        /// <summary>
        /// Returns a file from the Config section of the indicated HostConfigs
        /// </summary>
        /// <param name="parts">Path directories. First directory name is the docker_host_configuration label value.</param>
        /// <returns><see cref="FileInfo"/></returns>
        public static FileInfo GetHostConfigFile(params string[] parts)
            => parts?.Length <= 1 ? default : new(Path.Combine(AppContext.BaseDirectory, $"HostConfigs/{parts[0]}/Config/{string.Join('/', parts.Skip(1))}"));
    }
}
