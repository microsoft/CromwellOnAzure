// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Microsoft.AspNetCore.Mvc.ApplicationModels;
using Microsoft.AspNetCore.Mvc.Formatters;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Cosmos.Core;
using Microsoft.Azure.Management.Compute.Models;
using Newtonsoft.Json;

namespace TesApi.Web
{
    /// <summary>
    /// Util class for Azure Batch related helper functions.
    /// </summary>
    public class BatchUtils
    {
        private static readonly string HostConfigsDirectory = AppContext.BaseDirectory + @"/HostConfigs";

        /// <summary>
        /// Returns the host config file.
        /// </summary>
        /// <param name="host">The <seealso cref="Tes.Models.TesResources.BackendParameters"/> <see cref="Tes.Models.TesResources.SupportedBackendParameters.docker_host_configuration"/> value</param>
        /// <returns><see cref="Stream"/></returns>
        public static Stream GetHostConfig(string host)
        {
            try
            {
                return File.OpenRead(Path.Combine(HostConfigsDirectory, host, @"config.json"));
            }
            catch (FileNotFoundException) { }
            catch (DirectoryNotFoundException) { }
            return default;
        }

        /// <summary>
        /// Returns the batch application payload
        /// </summary>
        /// <param name="name">Application name</param>
        /// <returns></returns>
        public static Stream GetApplicationPayload(string name)
            => File.OpenRead(Path.Combine(HostConfigsDirectory, $"{FilenameFromBatchAppName(name)}.zip"));

        private static string FilenameFromBatchAppName(string name)
        {
            var idx = name.LastIndexOf('_');
            return $"{name[..idx]}/{name[(idx+1)..]}";
        }

        /// <summary>
        /// Parses the Hashes.txt file
        /// </summary>
        /// <returns>Dictionary of hashes where the keys are the batch application names.</returns>
        public static IReadOnlyDictionary<string, byte[]> GetApplicationPayloadHashes()
        {
            var hashes = new FileInfo(Path.Combine(HostConfigsDirectory, @"Hashes.txt"));
            using var reader = hashes.Exists ? hashes.OpenText() : default;
            return hashes.Exists ? new Dictionary<string, byte[]>(
                           reader.ReadToEnd()
                               .Replace('\\', '/').Replace("\r\n", "\n")
                               .Split('\n', StringSplitOptions.RemoveEmptyEntries)
                               .Select(l => l.Split(':', 2, StringSplitOptions.TrimEntries))
                               .Select(p => new KeyValuePair<string, byte[]>(BatchAppNameFromFilename(p[0]), Convert.FromHexString(p[1]))))
                : new Dictionary<string, byte[]>();
        }

        private static string BatchAppNameFromFilename(string path)
        {
            var parts = path.Split('/', 3, StringSplitOptions.TrimEntries);
            return $"{parts[1]}_{Path.GetFileNameWithoutExtension(parts[2])}";
        }

        /// <summary>
        /// Indicates if the well-known named task script is included in the application content.
        /// </summary>
        /// <param name="host">The <seealso cref="Tes.Models.TesResources.BackendParameters"/> <see cref="Tes.Models.TesResources.SupportedBackendParameters.docker_host_configuration"/> value</param>
        /// <param name="task">The task type where the application is first consumed.</param>
        /// <returns></returns>
        public static bool DoesHostConfigTaskIncludeTaskScript(string host, string task)
            => FindMetadata($"{host}_{task}").Item2;

        /// <summary>
        /// Gets the environment variable on the compute node where the application's contents will be provided.
        /// </summary>
        /// <param name="host">The <seealso cref="Tes.Models.TesResources.BackendParameters"/> <see cref="Tes.Models.TesResources.SupportedBackendParameters.docker_host_configuration"/> value</param>
        /// <param name="task">The task type where the application is first consumed.</param>
        /// <returns></returns>
        public static string GetApplicationDirectoryForHostConfigTask(string host, string task)
        {
            var name = $"{host}_{task}";
            var version = FindMetadata(name).Item1;
            if (version == -1) { throw new KeyNotFoundException(); }
            var variable = $"AZ_BATCH_APP_PACKAGE_{name}#{version:G}";
            return OperatingSystem.IsWindows() ? variable.ToUpperInvariant() : variable.Replace('.', '_').Replace('-', '_').Replace('#', '_');
        }

        private static (int, bool) FindMetadata(string host)
            => ReadApplicationVersions()
                .TryGetValue(host, out var hashes)
                    ? hashes.TryGetValue(GetApplicationPayloadHashes()
                        .TryGetValue(host, out var hashVal)
                            ? Convert.ToHexString(hashVal)
                            : string.Empty, out var value)
                        ? value
                        : (-1, false)
                    : (-1, false);

        /// <summary>
        /// Retrieves the current registered batch application versions
        /// </summary>
        /// <returns>Application version values. host -> hash -> version.</returns>
        public static Dictionary<string, Dictionary<string, (int, bool)>> ReadApplicationVersions()
        {
            var versionsFile = new FileInfo(Path.Combine(HostConfigsDirectory, @"Versions.json"));
            return versionsFile.Exists
                ? ReadFile()
                : new Dictionary<string, Dictionary<string, (int, bool)>>();

            Dictionary<string, Dictionary<string, (int, bool)>> ReadFile()
            {
                using var reader = new JsonTextReader(versionsFile.OpenText());
                return JsonSerializer.CreateDefault().Deserialize<Dictionary<string, Dictionary<string, (int, bool)>>>(reader);
            }
        }

        /// <summary>
        /// Stores the current registered batch application versions
        /// </summary>
        /// <param name="versions">Application version values. host -> hash -> version.</param>
        public static void WriteApplicationVersions(Dictionary<string, Dictionary<string, (int, bool)>> versions)
        {
            using var writer = new JsonTextWriter(File.CreateText(Path.Combine(HostConfigsDirectory, @"Versions.json")));
            JsonSerializer.CreateDefault().Serialize(writer, versions);
        }
    }
}
