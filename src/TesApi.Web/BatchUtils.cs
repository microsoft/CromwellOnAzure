// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Common.HostConfigs;
using Newtonsoft.Json;

namespace TesApi.Web
{
    /// <summary>
    /// Util class for Azure Batch related helper functions.
    /// </summary>
    public static class BatchUtils
    {
        private static readonly string HostConfigsDirectory = AppContext.BaseDirectory + @"/HostConfigs";
        private static readonly string PackageHashesFile = @"Hashes.json";
        private static readonly string ApplicationVersionsFile = @"Versions.json";
        private static readonly string ContainerImagesFile = @"Images.json";

        /// <summary>
        /// Returns the host configuration associated with the docker container
        /// </summary>
        /// <param name="container"></param>
        /// <returns>The value to pass to <see cref="GetHostConfig(string)"/>.</returns>
        public static string GetHostConfigForContainer(string container)
        {
            var file = new FileInfo(Path.Combine(HostConfigsDirectory, ContainerImagesFile));
            var images = ReadJson<Dictionary<string, string>>(file.Exists ? file.OpenText() : default, () => default);
            return images?.TryGetValue(container, out var value) ?? false ? value : default;
        }

        /// <summary>
        /// Returns the host config file.
        /// </summary>
        /// <param name="host">The <seealso cref="Tes.Models.TesResources.BackendParameters"/> <see cref="Tes.Models.TesResources.SupportedBackendParameters.docker_host_configuration"/> value</param>
        /// <returns><see cref="Stream"/></returns>
        public static HostConfiguration GetHostConfig(string host)
            => ReadJson<HostConfiguration>(File.OpenText(Path.Combine(HostConfigsDirectory, $"{host}.json")), () => throw new ArgumentException("File is not a HostConfig configuration.", nameof(host)));

        /// <summary>
        /// Parses the Hashes.txt file
        /// </summary>
        /// <returns>Dictionary of hashes where the keys are the batch application names.</returns>
        public static PackageHashes GetApplicationPayloadHashes()
        {
            var hashes = new FileInfo(Path.Combine(HostConfigsDirectory, PackageHashesFile));
            return ReadJson<PackageHashes>(hashes.Exists ? hashes.OpenText() : default, () => default);
        }

        /// <summary>
        /// Indicates if the well-known named task script is included in the application content.
        /// </summary>
        /// <param name="host">The <seealso cref="Tes.Models.TesResources.BackendParameters"/> <see cref="Tes.Models.TesResources.SupportedBackendParameters.docker_host_configuration"/> value</param>
        /// <param name="task">The task type where the application is first consumed.</param>
        /// <returns></returns>
        public static bool DoesHostConfigTaskIncludeTaskScript(string host, string task)
            => FindMetadata($"{host}_{task}").ContainsTaskScript;

        /// <summary>
        /// Determines if compute node will be running Windows
        /// </summary>
        /// <param name="nodeAgentSKUId">Node agent SKU Id</param>
        /// <returns>True if running Windows, false otherwise.</returns>
        public static bool IsComputeNodeWindows(string nodeAgentSKUId)
        {
            if (string.IsNullOrWhiteSpace(nodeAgentSKUId))
            {
                return false;
            }

            var regex = new Regex(@"^batch\.node\.(\w+)", RegexOptions.CultureInvariant);
            return regex.IsMatch(nodeAgentSKUId) && regex.Match(nodeAgentSKUId).Groups["1"].Value.Equals("windows");
        }

        /// <summary>
        /// Gets the environment variable on the compute node where the application's contents will be provided.
        /// </summary>
        /// <param name="host">The <seealso cref="Tes.Models.TesResources.BackendParameters"/> <see cref="Tes.Models.TesResources.SupportedBackendParameters.docker_host_configuration"/> value</param>
        /// <param name="task">The task type where the application is first consumed.</param>
        /// <param name="isWindows">True if the variable will be used on a compute node running Windows. Defaults to False</param>
        /// <returns></returns>
        public static (string Id, string Version, string Variable) GetBatchApplicationForHostConfigTask(string host, string task, bool isWindows = false)
        {
            var metadata = FindMetadata($"{host}_{task}");
            var version = metadata.Version ?? throw new KeyNotFoundException("Batch Application Version not found.");
            var variable = $"AZ_BATCH_APP_PACKAGE_{metadata.ServerAppName}#{version}";
            return (metadata.ServerAppName, version, isWindows ? variable.ToUpperInvariant() : variable.Replace('.', '_').Replace('-', '_').Replace('#', '_'));
        }

        private static (string Version, bool ContainsTaskScript, string ServerAppName) FindMetadata(string name)
        {
            var version = GetApplicationPayloadHashes()?.TryGetValue(name, out var hashVal) ?? false
                ? hashVal
                : null;
            return version is null
                ? default
                : GetBatchApplicationVersions()?.TryGetValue(name, out var appVersions) ?? false
                    ? appVersions.Packages?.TryGetValue(version, out var package) ?? false
                        ? (version, package.ContainsTaskScript, appVersions.ApplicationId)
                        : default
                    : default;
        }

        /// <summary>
        /// TODO
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="textReader"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public static T ReadJson<T>(TextReader textReader, Func<T> defaultValue)
        {
            return textReader is null
                ? defaultValue()
                : ReadJsonFile();

            T ReadJsonFile()
            {
                using var reader = new JsonTextReader(textReader);
                return JsonSerializer.CreateDefault().Deserialize<T>(reader) ?? defaultValue();
            }
        }

        /// <summary>
        /// TODO
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string WriteJson<T>(T value)
        {
            using var result = new StringWriter();
            using var writer = new JsonTextWriter(result);
            JsonSerializer.CreateDefault(new() { Error = (o, e) => throw e.ErrorContext.Error }).Serialize(writer, value);
            return result.ToString();
        }

        /// <summary>
        /// Retrieves the current registered batch application versions
        /// </summary>
        /// <returns>Application version values. host -> hash -> metadata.</returns>
        public static ApplicationVersions GetBatchApplicationVersions()
        {
            var versionsFile = new FileInfo(Path.Combine(HostConfigsDirectory, ApplicationVersionsFile));
            return ReadJson<ApplicationVersions>(versionsFile.Exists ? versionsFile.OpenText() : default, () => default);
        }

        /// <summary>
        /// Stores the current registered batch application versions
        /// </summary>
        public static void WriteHostConfiguration(HostConfig configuration)
        {
            var images = new Dictionary<string, string>();
            var hostConfigsDir = Directory.CreateDirectory(HostConfigsDirectory);

            File.WriteAllText(Path.Combine(hostConfigsDir.FullName, ApplicationVersionsFile), WriteJson(configuration.ApplicationVersions));
            File.WriteAllText(Path.Combine(hostConfigsDir.FullName, PackageHashesFile), WriteJson(configuration.PackageHashes));

            foreach (var config in configuration.HostConfigurations)
            {
                foreach (var vmSize in config.Value.VmSizes)
                {
                    var container = vmSize.Container;

                    if (!string.IsNullOrWhiteSpace(container))
                    {
                        images.Add(container, config.Key);
                    }
                }
                File.WriteAllText(Path.Combine(hostConfigsDir.FullName, $"{config.Key}.json"), WriteJson(config.Value));
            }

            File.WriteAllText(Path.Combine(hostConfigsDir.FullName, ContainerImagesFile), WriteJson(images));
        }
    }
}
