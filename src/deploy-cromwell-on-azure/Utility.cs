// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text.RegularExpressions;
using System.Threading;
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
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation</param>
        /// <param name="pathComponentsRelativeToAppBase">The path components relative to the app base to write</param>
        /// <returns>A <see cref="Task"/> that represents the lifetime of the asynchronous operation</returns>
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
                var pathSeparatedByPeriods = file.Replace(componentSubstring, string.Empty).TrimStart('.');
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

        private static readonly char[] Rfc4648Base32 = new[] { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '2', '3', '4', '5', '6', '7' };

        /// <summary>
        /// Converts binary to Base32
        /// </summary>
        /// <param name="bytes">Data to convert.</param>
        /// <returns>RFC 4648 Base32 representation</returns>
        /// <exception cref="InvalidOperationException"></exception>
        public static string ConvertToBase32(byte[] bytes) // https://datatracker.ietf.org/doc/html/rfc4648#section-6
        {
            const int groupBitlength = 5;

            if (BitConverter.IsLittleEndian)
            {
                bytes = bytes.Select(FlipByte).ToArray();
            }

            return new string(new BitArray(bytes)
                    .Cast<bool>()
                    .Select((b, i) => (Index: i, Value: b ? 1 << (groupBitlength - 1 - (i % groupBitlength)) : 0))
                    .GroupBy(t => t.Index / groupBitlength)
                    .Select(g => Rfc4648Base32[g.Sum(t => t.Value)])
                    .ToArray())
                + (bytes.Length % groupBitlength) switch
                {
                    0 => string.Empty,
                    1 => @"======",
                    2 => @"====",
                    3 => @"===",
                    4 => @"=",
                    _ => throw new InvalidOperationException(), // Keeps the compiler happy.
                };

            static byte FlipByte(byte data)
                => (byte)(
                    (((data & 0x01) == 0) ? 0 : 0x80) |
                    (((data & 0x02) == 0) ? 0 : 0x40) |
                    (((data & 0x04) == 0) ? 0 : 0x20) |
                    (((data & 0x08) == 0) ? 0 : 0x10) |
                    (((data & 0x10) == 0) ? 0 : 0x08) |
                    (((data & 0x20) == 0) ? 0 : 0x04) |
                    (((data & 0x40) == 0) ? 0 : 0x02) |
                    (((data & 0x80) == 0) ? 0 : 0x01));
        }
    }
}
