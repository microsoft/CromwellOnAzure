// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace CromwellOnAzureDeployer
{
    public static class Utility
    {
        /// <summary>
        /// Generates a random resource names with the prefix.
        /// </summary>
        /// <param name="prefix">the prefix to be used if possible</param>
        /// <param name="maxLength">the maximum length for the random generated name</param>
        /// <returns>random name</returns>
        /// <remarks>Implementation of <c>Microsoft.Azure.Management.ResourceManager.Fluent.SdkContext.RandomResourceName</c></remarks>
        public static string RandomResourceName(string prefix, int maxLength)
            => new ResourceNamer(string.Empty).RandomName(prefix, maxLength);

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

            protected static string ValidateIsNotNullOrEmpty(string value, string name)
            {
                ArgumentException.ThrowIfNullOrEmpty(value, name);
                return value;
            }
        }

        public sealed class ConfigReplaceTextItem(string match, string replacement) : ConfigReplaceTextItemBase
        {
            private readonly string _match = ValidateIsNotNullOrEmpty(match, nameof(match));
            private readonly string _replacement = ValidateIsNotNullOrEmpty(replacement, nameof(replacement));

            public override string Replace(string input) => Skip ? input : input.Replace(_match, _replacement);
        }

        public sealed class ConfigReplaceRegExItemText(string match, string replacement, RegexOptions options) : ConfigReplaceTextItemBase
        {
            private readonly string _match = ValidateIsNotNullOrEmpty(match, nameof(match));
            private readonly string _replacement = ValidateIsNotNullOrEmpty(replacement, nameof(replacement));
            private readonly RegexOptions _options = options;

            public override string Replace(string input) => Skip ? input : Regex.Replace(input, _match, _replacement, _options);
        }

        public sealed class ConfigReplaceRegExItemEvaluator(string match, MatchEvaluator replacement, RegexOptions options) : ConfigReplaceTextItemBase
        {
            private readonly string _match = ValidateIsNotNullOrEmpty(match, nameof(match));
            private readonly MatchEvaluator _replacement = replacement ?? throw new ArgumentNullException(nameof(replacement));
            private readonly RegexOptions _options = options;

            public override string Replace(string input) => Skip ? input : Regex.Replace(input, _match, _replacement, _options);
        }

        public sealed class ConfigNamedConditional(string name, bool condition, string lineSeparator = "\n") : ConfigReplaceTextItemBase
        {
            private readonly string _name = ValidateIsNotNullOrEmpty(name, nameof(name));
            private readonly bool _condition = condition;
            private readonly string _lineSeparator = lineSeparator;

            public override string Replace(string input) => Skip ? input : PerformReplace(input);

            private string PerformReplace(string input)
            {
                IList<string> lines = [];
                var include = true;

                foreach (var line in input.Split(_lineSeparator))
                {
                    if (LineIs(line, $"!if({_name})"))
                    {
                        include = _condition;
                        continue;
                    }

                    if (LineIs(line, $"!else({_name})"))
                    {
                        include = !_condition;
                        continue;
                    }

                    if (LineIs(line, $"!endif({_name})"))
                    {
                        include = true;
                        continue;
                    }

                    if (include)
                    {
                        lines.Add(line);
                    }
                }

                return string.Join(_lineSeparator, lines);

                static bool LineIs(string line, string match) => line.TrimStart().StartsWith(match) && string.IsNullOrWhiteSpace(line.TrimStart()[match.Length..]);
            }
        }

        public static string DictionaryToDelimitedText(Dictionary<string, string> dictionary, string fieldDelimiter = "=", string rowDelimiter = "\n")
            => string.Join(rowDelimiter, dictionary.Select(kv => $"{kv.Key}{fieldDelimiter}{kv.Value}"));

        /// <summary>
        /// Writes all embedded resource files that start with pathComponentsRelativeToAppBase to the output base path,
        /// and creates subdirectories
        /// </summary>
        /// <param name="outputBasePath">The base path to create the subdirectories and write the files</param>
        /// <param name="cancellationToken"></param>
        /// <param name="pathComponentsRelativeToAppBase">The path components relative to the app base to write</param>
        /// <returns></returns>
        public static async Task WriteEmbeddedFilesAsync(string outputBasePath, CancellationToken cancellationToken, params string[] pathComponentsRelativeToAppBase)
        {
            var assembly = typeof(Deployer).Assembly;
            var resourceNames = assembly.GetManifestResourceNames();

            // Assembly is renamed by the build process, so get it from the first resource name
            var firstResourceName = resourceNames.First();
            var assemblyName = firstResourceName.Substring(0, firstResourceName.IndexOf('.'));
            var componentSubstring = $"{assemblyName}.{string.Join(".", pathComponentsRelativeToAppBase)}";

            foreach (var file in resourceNames.Where(r => r.StartsWith(componentSubstring)))
            {
                var content = (await new StreamReader(assembly.GetManifestResourceStream(file)).ReadToEndAsync(cancellationToken)).Replace("\r\n", "\n");
                var pathSeparatedByPeriods = file.Replace(componentSubstring, "").TrimStart('.');
                var outputPath = Path.Join(outputBasePath, pathSeparatedByPeriods);
                var lastPeriodBeforeFilename = pathSeparatedByPeriods.LastIndexOf('.', pathSeparatedByPeriods.LastIndexOf('.') - 1);

                if (lastPeriodBeforeFilename > 0)
                {
                    // There are subdirectories present
                    var subdirectories = pathSeparatedByPeriods.Substring(0, lastPeriodBeforeFilename).Replace('.', Path.DirectorySeparatorChar);
                    var filename = pathSeparatedByPeriods.Substring(lastPeriodBeforeFilename + 1);
                    outputPath = Path.Join(outputBasePath, subdirectories, filename);
                }

                Directory.CreateDirectory(Path.GetDirectoryName(outputPath));
                await File.WriteAllTextAsync(outputPath, content, cancellationToken);
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

        public static void ForEach<T>(this IEnumerable<T> values, Action<T> action)
        {
            foreach (var item in values)
            {
                action(item);
            }
        }

        // borrowed from https://github.com/Azure/azure-libraries-for-net/blob/7d85e294e4e7280c3f74b1c41438e2f20bce2052/src/ResourceManagement/ResourceManager/ResourceNamer.cs
        private class ResourceNamer
        {
            private readonly string randName;
            private static readonly Random random = new();

            public ResourceNamer(string name)
            {
                lock (random)
                {
                    this.randName = name.ToLower() + Guid.NewGuid().ToString("N")[..3].ToLower();
                }
            }

            public string RandomName(string prefix, int maxLen)
            {
                lock (random)
                {
                    prefix = prefix.ToLower();
                    var minRandomnessLength = 5;
                    var minRandomString = random.Next(0, 100000).ToString("D5");

                    if (maxLen < (prefix.Length + randName.Length + minRandomnessLength))
                    {
                        var str1 = prefix + minRandomString;
                        return str1 + RandomString((maxLen - str1.Length) / 2);
                    }

                    var str = prefix + randName + minRandomString;
                    return str + RandomString((maxLen - str.Length) / 2);
                }
            }

            private static string RandomString(int length)
            {
                var str = "";
                while (str.Length < length)
                {
                    str += Guid.NewGuid().ToString("N")[..Math.Min(32, length)].ToLower();
                }
                return str;
            }
        }
    }
}
