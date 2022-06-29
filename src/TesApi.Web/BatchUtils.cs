// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
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
        /// Gets the environment variable on the compute node where the application's contents will be provided.
        /// </summary>
        /// <param name="host">The <seealso cref="Tes.Models.TesResources.BackendParameters"/> <see cref="Tes.Models.TesResources.SupportedBackendParameters.docker_host_configuration"/> value</param>
        /// <param name="task">The task type where the application is first consumed.</param>
        /// <param name="isWindows">True if the variable will be used on a compute node running Windows. Defaults to False</param>
        /// <returns></returns>
        public static (string Id, string Version, string Variable) GetBatchApplicationForHostConfigTask(string host, string task, bool isWindows = false)
        {
            var metadata = FindMetadata($"{host}_{task}");
            var version = metadata.Version ?? throw new KeyNotFoundException();
            var variable = $"AZ_BATCH_APP_PACKAGE_{metadata.ServerAppName}#{version}";
            return (metadata.ServerAppName, version, isWindows ? variable.ToUpperInvariant() : variable.Replace('.', '_').Replace('-', '_').Replace('#', '_'));
        }

        private static (string Version, bool ContainsTaskScript, string ServerAppName) FindMetadata(string name)
        {
            string hashVal = default;
            ApplicationVersion appVersions = default;
            Package package = default;
            var version = GetApplicationPayloadHashes()?.TryGetValue(name, out hashVal) ?? false
                ? hashVal
                : null;
            return version is null
                ? default
                : GetBatchApplicationVersions()?.TryGetValue(name, out appVersions) ?? false
                    ? appVersions.Packages?.TryGetValue(version, out package) ?? false
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
            var hostConfigsDir = Directory.CreateDirectory(HostConfigsDirectory);
            File.WriteAllText(Path.Combine(hostConfigsDir.FullName, ApplicationVersionsFile), WriteJson(configuration.ApplicationVersions));
            File.WriteAllText(Path.Combine(hostConfigsDir.FullName, PackageHashesFile), WriteJson(configuration.PackageHashes));
            foreach (var config in configuration.HostConfigurations)
            {
                File.WriteAllText(Path.Combine(hostConfigsDir.FullName, $"{config.Key}.json"), WriteJson(config.Value));
            }
        }
    }
}
