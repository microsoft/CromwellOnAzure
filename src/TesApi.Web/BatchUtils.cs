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
        public static (string Id, string Version, string Variable) GetApplicationDirectoryForHostConfigTask(string host, string task)
        {
            var metadata = FindMetadata($"{host}_{task}");
            var version = metadata.Version ?? throw new KeyNotFoundException();
            var variable = $"AZ_BATCH_APP_PACKAGE_{metadata.ServerAppName}#{version}";
            return (metadata.Id, version, OperatingSystem.IsWindows() ? variable.ToUpperInvariant() : variable.Replace('.', '_').Replace('-', '_').Replace('#', '_'));
        }

        private static (string Id, string Version, bool ContainsTaskScript, string ServerAppName) FindMetadata(string name)
        {
            var version = GetApplicationPayloadHashes()
                                   .TryGetValue(name, out var hashVal)
                                       ? Convert.ToHexString(hashVal)
                                       : null;
            return version is null ? default : ReadApplicationVersions()
                           .TryGetValue(name, out var hashes)
                               ? hashes.Packages.TryGetValue(version, out var value)
                                   ? (hashes.Id, version, value.ContainsTaskScript, hashes.ServerAppName)
                                   : default
                               : default;
        }

        /// <summary>
        /// Retrieves the current registered batch application versions
        /// </summary>
        /// <returns>Application version values. host -> hash -> metadata.</returns>
        public static IDictionary<string, (string Id, string ServerAppName, IDictionary<string, (int Reserved, bool ContainsTaskScript)> Packages)> ReadApplicationVersions()
        {
            var versionsFile = new FileInfo(Path.Combine(HostConfigsDirectory, @"Versions.json"));
            return versionsFile.Exists
                ? ReadFile()
                : new Dictionary<string, (string, string, IDictionary<string, (int, bool)>)>();

            IDictionary<string, (string, string, IDictionary<string, (int, bool)>)> ReadFile()
            {
                using var reader = new JsonTextReader(versionsFile.OpenText());
                var serializer = JsonSerializer.CreateDefault();
                serializer.Converters.Add(new ApplicationVersionsDictionaryConverter());
                return serializer.Deserialize<IDictionary<string, (string, string, IDictionary<string, (int, bool)>)>>(reader);
            }
        }

        /// <summary>
        /// Stores the current registered batch application versions
        /// </summary>
        /// <param name="versions">Application version values. host -> hash -> version.</param>
        public static void WriteApplicationVersions(IDictionary<string, (string Id, string ServerAppName, IDictionary<string, (int Reserved, bool ContainsTaskScript)> Packages)> versions)
        {
            using var writer = new JsonTextWriter(File.CreateText(Path.Combine(HostConfigsDirectory, @"Versions.json")));
            JsonSerializer.CreateDefault().Serialize(writer, versions);
        }

        private class ApplicationVersionsDictionaryConverter : JsonConverter
        {
            public override bool CanWrite => false;

            public override bool CanConvert(Type objectType)
                => objectType switch
                {
                    var x when x == typeof(IDictionary<string, (string, string, IDictionary<string, (int, bool)>)>) => true,
                    var x when x == typeof(IDictionary<string, (int, bool)>) => true,
                    _ => false,
                };

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
                => objectType switch
                {
                    var x when x == typeof(IDictionary<string, (string, string, IDictionary<string, (int, bool)>)>) => serializer.Deserialize<Dictionary<string, (string, string, IDictionary<string, (int, bool)>)>>(reader),
                    var x when x == typeof(IDictionary<string, (int, bool)>) => serializer.Deserialize<Dictionary<string, (int, bool)>>(reader),
                    _ => throw new ArgumentException(default, nameof(objectType)),
                };

            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
                => throw new NotImplementedException();
        }
    }
}
