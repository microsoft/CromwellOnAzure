﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace CromwellOnAzureDeployer
{
    public static class Utility
    {
        public static Dictionary<string, string> DelimitedTextToDictionary(string text, string fieldDelimiter = "=", string rowDelimiter = "\n")
            => text.Trim().Split(rowDelimiter)
                .Select(r => r.Trim().Split(fieldDelimiter))
                .ToDictionary(f => f[0].Trim(), f => f[1].Trim());

        public static string PersonalizeContent(IEnumerable<ConfigReplaceTextItemBase> replacements, params string[] pathComponentsRelativeToAppBase)
            => PersonalizeContent(replacements, GetFileContent(pathComponentsRelativeToAppBase));

        public static string PersonalizeContent(IEnumerable<ConfigReplaceTextItemBase> replacements, string source)
        {
            foreach (var replacement in replacements)
            {
                source = replacement.Replace(source);
            }

            return source;
        }

        public abstract class ConfigReplaceTextItemBase
        {
            public abstract string Replace(string input);

            public bool Skip { get; set; }
        }

        public sealed class ConfigReplaceTextItem : ConfigReplaceTextItemBase
        {
            private readonly string _match;
            private readonly string _replacement;

            public ConfigReplaceTextItem(string match, string replacement)
            {
                _match = match ?? throw new ArgumentNullException(nameof(match));
                _replacement = replacement ?? throw new ArgumentNullException(nameof(replacement));
            }

            public override string Replace(string input) => Skip ? input : input.Replace(_match, _replacement);
        }

        public sealed class ConfigReplaceRegExItemText : ConfigReplaceTextItemBase
        {
            private readonly string _match;
            private readonly string _replacement;
            private readonly RegexOptions _options;

            public ConfigReplaceRegExItemText(string match, string replacement, RegexOptions options)
            {
                _match = match ?? throw new ArgumentNullException(nameof(match));
                _replacement = replacement ?? throw new ArgumentNullException(nameof(replacement));
                _options = options;
            }

            public override string Replace(string input) => Skip ? input : Regex.Replace(input, _match, _replacement, _options);
        }

        public sealed class ConfigReplaceRegExItemEvaluator : ConfigReplaceTextItemBase
        {
            private readonly string _match;
            private readonly MatchEvaluator _replacement;
            private readonly RegexOptions _options;

            public ConfigReplaceRegExItemEvaluator(string match, MatchEvaluator replacement, RegexOptions options)
            {
                _match = match ?? throw new ArgumentNullException(nameof(match));
                _replacement = replacement ?? throw new ArgumentNullException(nameof(replacement));
                _options = options;
            }

            public override string Replace(string input) => Skip ? input : Regex.Replace(input, _match, _replacement, _options);
        }

        public static string DictionaryToDelimitedText(Dictionary<string, string> dictionary, string fieldDelimiter = "=", string rowDelimiter = "\n")
            => string.Join(rowDelimiter, dictionary.Select(kv => $"{kv.Key}{fieldDelimiter}{kv.Value}"));

        /// <summary>
        /// Writes all embedded resource files that start with pathComponentsRelativeToAppBase to the output base path,
        /// and creates subdirectories
        /// </summary>
        /// <param name="outputBasePath">The base path to create the subdirectories and write the files</param>
        /// <param name="pathComponentsRelativeToAppBase">The path components relative to the app base to write</param>
        /// <returns></returns>
        public static async Task WriteEmbeddedFilesAsync(string outputBasePath, params string[] pathComponentsRelativeToAppBase)
        {
            var assembly = typeof(Deployer).Assembly;
            var assemblyName = assembly.GetName().Name;
            var resourceNames = assembly.GetManifestResourceNames();
            var componentSubstring = $"{assemblyName}.{string.Join(".", pathComponentsRelativeToAppBase)}";

            foreach (var file in resourceNames.Where(r => r.StartsWith(componentSubstring)))
            {
                var content = (await new StreamReader(assembly.GetManifestResourceStream(file)).ReadToEndAsync()).Replace("\r\n", "\n");
                var componentsAsPath = string.Join(Path.DirectorySeparatorChar, pathComponentsRelativeToAppBase);
                var path = file.Replace(componentSubstring, "").TrimStart('.');
                var outputPath = Path.Join(outputBasePath, path);
                var lastPeriodBeforeFilename = path.LastIndexOf('.', path.LastIndexOf('.') - 1);

                if (lastPeriodBeforeFilename > 0)
                {
                    var subs = path.Substring(0, lastPeriodBeforeFilename).Replace('.', Path.PathSeparator);
                    var filename = path.Substring(lastPeriodBeforeFilename + 1);
                    outputPath = Path.Join(outputBasePath, subs, filename);
                }

                Directory.CreateDirectory(Path.GetDirectoryName(outputPath));
                await File.WriteAllTextAsync(outputPath, content);
            }
        }

        public static string GetFileContent(params string[] pathComponentsRelativeToAppBase)
        {
            using var embeddedResourceStream = GetBinaryFileContent(pathComponentsRelativeToAppBase);
            using var reader = new StreamReader(embeddedResourceStream);
            return reader.ReadToEnd().Replace("\r\n", "\n");
        }

        private static Stream GetBinaryFileContent(params string[] pathComponentsRelativeToAppBase)
            => typeof(Deployer).Assembly.GetManifestResourceStream($"deploy-cromwell-on-azure.{string.Join(".", pathComponentsRelativeToAppBase)}");

        /// <summary>
        /// Generates a secure password with one lowercase letter, one uppercase letter, and one number
        /// </summary>
        /// <param name="length">Length of the password</param>
        /// <returns>The password</returns>
        public static string GeneratePassword(int length = 16)
        {
            // one lower, one upper, one number, min length
            var regex = new Regex(@"^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)[a-zA-Z\d]{" + length.ToString() + ",}$");

            while (true)
            {
                var buffer = RandomNumberGenerator.GetBytes(length);

                var password = Convert.ToBase64String(buffer)
                    .Replace("+", "-")
                    .Replace("/", "_")
                    .Substring(0, length);

                if (regex.IsMatch(password))
                {
                    return password;
                }
            }
        }
    }
}
