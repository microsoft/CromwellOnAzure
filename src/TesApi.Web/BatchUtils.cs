// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
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
        /// Makes an acceptable batch application name out of the internal calculated name
        /// </summary>
        /// <param name="host"></param>
        /// <returns></returns>
        public static string BatchAppFromHostConfigName(string host)
        {
            const char dash = '-';
            return new string(host.Normalize().Select(Mangle).ToArray());

            static char Mangle(char c)
                => char.GetUnicodeCategory(c) switch
                {
                    UnicodeCategory.UppercaseLetter => MangleLetter(c),
                    UnicodeCategory.LowercaseLetter => MangleLetter(c),
                    UnicodeCategory.TitlecaseLetter => MangleLetter(c),
                    UnicodeCategory.DecimalDigitNumber => c,
                    UnicodeCategory.LetterNumber => MangleNumber(c),
                    UnicodeCategory.OtherNumber => MangleNumber(c),
                    UnicodeCategory.PrivateUse => dash,
                    UnicodeCategory.ConnectorPunctuation => dash,
                    UnicodeCategory.DashPunctuation => dash,
                    UnicodeCategory.MathSymbol => dash,
                    UnicodeCategory.CurrencySymbol => dash,
                    _ => '_',
                };

            static char MangleLetter(char c)
                => char.IsLetterOrDigit(c) && /*char.IsAscii(c)*/(c >= 0x0000 && c <= 0x007f) ? c : dash;

            static char MangleNumber(char c)
                => char.GetNumericValue(c) switch
                {
                    (>= 0) and (<= 9) => (char)('0' + char.GetNumericValue(c)),
                    _ => dash,
                };
        }

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
            => FindMetadata($"{host}_{task}").ContainsTaskScript;

        /// <summary>
        /// Gets the environment variable on the compute node where the application's contents will be provided.
        /// </summary>
        /// <param name="host">The <seealso cref="Tes.Models.TesResources.BackendParameters"/> <see cref="Tes.Models.TesResources.SupportedBackendParameters.docker_host_configuration"/> value</param>
        /// <param name="task">The task type where the application is first consumed.</param>
        /// <returns></returns>
        public static string GetApplicationDirectoryForHostConfigTask(string host, string task)
        {
            var metadata = FindMetadata($"{host}_{task}");
            var version = metadata.Version;
            if (version == -1) { throw new KeyNotFoundException(); }
            var variable = $"AZ_BATCH_APP_PACKAGE_{metadata.ServerAppName}#{version:G}";
            return OperatingSystem.IsWindows() ? variable.ToUpperInvariant() : variable.Replace('.', '_').Replace('-', '_').Replace('#', '_');
        }

        private static (int Version, bool ContainsTaskScript, string ServerAppName) FindMetadata(string host)
            => ReadApplicationVersions()
                .TryGetValue(host, out var hashes)
                    ? hashes.TryGetValue(GetApplicationPayloadHashes()
                        .TryGetValue(host, out var hashVal)
                            ? Convert.ToHexString(hashVal)
                            : string.Empty, out var value)
                        ? value
                        : (-1, false, default)
                    : (-1, false, default);

        /// <summary>
        /// Retrieves the current registered batch application versions
        /// </summary>
        /// <returns>Application version values. host -> hash -> metadata.</returns>
        public static Dictionary<string, Dictionary<string, (int Version, bool ContainsTaskScript, string ServerAppName)>> ReadApplicationVersions()
        {
            var versionsFile = new FileInfo(Path.Combine(HostConfigsDirectory, @"Versions.json"));
            return versionsFile.Exists
                ? ReadFile()
                : new Dictionary<string, Dictionary<string, (int, bool, string)>>();

            Dictionary<string, Dictionary<string, (int, bool, string)>> ReadFile()
            {
                using var reader = new JsonTextReader(versionsFile.OpenText());
                return JsonSerializer.CreateDefault().Deserialize<Dictionary<string, Dictionary<string, (int Version, bool ContainsTaskScript, string ServerAppName)>>>(reader);
            }
        }

        /// <summary>
        /// Stores the current registered batch application versions
        /// </summary>
        /// <param name="versions">Application version values. host -> hash -> version.</param>
        public static void WriteApplicationVersions(Dictionary<string, Dictionary<string, (int Version, bool ContainsTaskScript, string ServerAppName)>> versions)
        {
            using var writer = new JsonTextWriter(File.CreateText(Path.Combine(HostConfigsDirectory, @"Versions.json")));
            JsonSerializer.CreateDefault().Serialize(writer, versions);
        }
    }
}
