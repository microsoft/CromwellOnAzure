// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text.RegularExpressions;

namespace CromwellOnAzureDeployer
{
    public static class Utility
    {
        private static Dictionary<string, string> DelimitedTextToDictionary(string text, string fieldDelimiter = "=", string rowDelimiter = "\n")
            => text.Trim().Split(rowDelimiter)
                .Select(r => r.Trim().Split(fieldDelimiter))
                .ToDictionary(f => f[0].Trim(), f => f[1].Trim());


        private static string DictionaryToDelimitedText(Dictionary<string, string> dictionary, string fieldDelimiter = "=", string rowDelimiter = "\n")
            => string.Join(rowDelimiter, dictionary.Select(kv => $"{kv.Key}{fieldDelimiter}{kv.Value}"));

        public static string GetFileContent(params string[] pathComponentsRelativeToAppBase)
        {
            var embeddedResourceName = $"deploy-cromwell-on-azure.{string.Join(".", pathComponentsRelativeToAppBase)}";
            var embeddedResourceStream = typeof(Deployer).Assembly.GetManifestResourceStream(embeddedResourceName);

            using var reader = new StreamReader(embeddedResourceStream);
            return reader.ReadToEnd().Replace("\r\n", "\n");
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
                using (var rng = new RNGCryptoServiceProvider())
                {
                    var buffer = new byte[length];
                    rng.GetBytes(buffer);

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
}
