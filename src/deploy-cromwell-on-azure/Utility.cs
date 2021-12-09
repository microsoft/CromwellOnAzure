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
        public static string GetFileContent(params string[] pathComponentsRelativeToAppBase)
        {
            var embeddedResourceName = $"deploy-cromwell-on-azure.{string.Join(".", pathComponentsRelativeToAppBase)}";
            var embeddedResourceStream = typeof(Deployer).Assembly.GetManifestResourceStream(embeddedResourceName);

            using var reader = new StreamReader(embeddedResourceStream);
            return reader.ReadToEnd().Replace("\r\n", "\n");
        }

        public static string UpdateExistingSettingsFileContentV300(string existingSettingsFileContent)
        {
            var existingSettingsFileContentLines = existingSettingsFileContent.Replace("\r\n", "\n").Split('\n', StringSplitOptions.RemoveEmptyEntries);
            var settingsFileContent = GetFileContent("scripts", "env-04-settings.txt");

            var newSettingsKeys = new List<string>
                {
                    "BatchImageOffer",
                    "BatchImagePublisher",
                    "BatchImageSku",
                    "BatchImageVersion",
                    "BatchNodeAgentSkuId",
                    "XilinxFpgaVmSizePrefixes",
                    "XilinxFpgaBatchImageOffer",
                    "XilinxFpgaBatchImagePublisher",
                    "XilinxFpgaBatchImageSku",
                    "XilinxFpgaBatchImageVersion",
                    "XilinxFpgaBatchNodeAgentSkuId",
                    "MarthaUrl",
                    "MarthaKeyVaultName",
                    "MarthaSecretName"
                };

            var newEnv04SettingsLines = settingsFileContent
                .Replace("\r\n", "\n")
                .Split('\n', StringSplitOptions.RemoveEmptyEntries)
                .Where(line => newSettingsKeys.Any(key => line.StartsWith($"{key}=", StringComparison.OrdinalIgnoreCase)))
                .ToList();

            foreach (var line in newEnv04SettingsLines)
            {
                var kv = line.Split('=', StringSplitOptions.RemoveEmptyEntries);

                if (!existingSettingsFileContentLines.Any(line => line.Trim().StartsWith($"{kv[0]}=", StringComparison.OrdinalIgnoreCase)))
                {
                    // Add new settings if they don't exist
                    existingSettingsFileContent += $"{line}\n";
                }
            }

            return existingSettingsFileContent;
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
