// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections;
using System.Collections.Generic;
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
        /// Returns the host config file.
        /// </summary>
        /// <param name="host">The <seealso cref="Tes.Models.TesResources.BackendParameters"/> <see cref="Tes.Models.TesResources.SupportedBackendParameters.docker_host_configuration"/> value</param>
        /// <returns><see cref="Stream"/></returns>
        public static Stream GetHostConfig(string host)
            => GetHostConfigFile(host, "config.json")?.OpenRead();

        /// <summary>
        /// Parses the Hashes.txt file
        /// </summary>
        /// <param name="hashFileContent">Content of the Hashes.txt file to parse. If is 'null' then reads the file in the container.</param>
        /// <returns>Dictionary of hashes where they keys are file paths starting with 'HostConfigs/'.</returns>
        public static IReadOnlyDictionary<string, byte[]> GetBlobHashes(string hashFileContent = null)
            => new Dictionary<string, byte[]>((hashFileContent ?? GetBlobHashFileContent())?.Split('\n', StringSplitOptions.RemoveEmptyEntries).Select(l => l.Split(':', 2)).Select(p => new KeyValuePair<string, byte[]>(p[0].Trim(), Convert.FromHexString(p[1].Trim()))) ?? Enumerable.Empty<KeyValuePair<string, byte[]>>());

        /// <summary>
        /// Gets the content of the 'HostConfigs/Hashes.txt' file.
        /// </summary>
        /// <returns>File content as text.</returns>
        public static string GetBlobHashFileContent()
            => File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "HostConfigs/Hashes.txt")).Replace('\\', '/').Replace("\r\n", "\n");

        /// <summary>
        /// Returns a file from the Config section of the indicated HostConfigs
        /// </summary>
        /// <param name="parts">Path directories. First directory name is the docker_host_configuration label value.</param>
        /// <returns><see cref="FileInfo"/></returns>
        public static FileInfo GetHostConfigFile(params string[] parts)
            => parts?.Length <= 1 ? default : new(Path.Combine(AppContext.BaseDirectory, $"HostConfigs/{parts[0]}/Config/{string.Join('/', parts.Skip(1))}"));
    }
}
