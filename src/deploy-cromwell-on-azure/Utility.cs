// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text.RegularExpressions;
using Common.HostConfigs;
using Newtonsoft.Json;

namespace CromwellOnAzureDeployer
{
    public static class Utility
    {
        public static HostConfiguration ParseConfig(TextReader textReader)
        {
            using var reader = new JsonTextReader(textReader);
            return JsonSerializer.CreateDefault().Deserialize<HostConfiguration>(reader) ?? throw new ArgumentException("File is not a HostConfig configuration.", nameof(textReader));
        }

        public static T ReadJson<T>(TextReader textReader, Func<T> defaultValue)
        {
            return textReader is null
                ? defaultValue()
                : ReadFile();

            T ReadFile()
            {
                using var reader = new JsonTextReader(textReader);
                return JsonSerializer.CreateDefault().Deserialize<T>(reader) ?? defaultValue();
            }
        }

        public static string WriteJson<T>(T value)
        {
            using var result = new StringWriter();
            using var writer = new JsonTextWriter(result);
            JsonSerializer.CreateDefault().Serialize(writer, value);
            return result.ToString();
        }

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

        public static string GetFileContent(params string[] pathComponentsRelativeToAppBase)
        {
            using var embeddedResourceStream = GetBinaryFileContent(pathComponentsRelativeToAppBase);
            using var reader = new StreamReader(embeddedResourceStream);
            return reader.ReadToEnd().Replace("\r\n", "\n");
        }

        private static Stream GetBinaryFileContent(params string[] pathComponentsRelativeToAppBase)
            => typeof(Deployer).Assembly.GetManifestResourceStream($"deploy-cromwell-on-azure.{string.Join(".", pathComponentsRelativeToAppBase)}");

        public struct EmbeddedResourceName
        {
            public string Name { get; }
            public string ManifestName { get; }

            public EmbeddedResourceName(string manifestName, string name)
            {
                Name = name;
                ManifestName = manifestName;
            }
        }

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
                    [..length];

                if (regex.IsMatch(password))
                {
                    return password;
                }
            }
        }
    }
}
