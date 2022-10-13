// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace CromwellOnAzureDeployer
{
    // Portions copied from deployer
    public static class Utility
    {
        public static string GetFileContent(params string[] pathComponentsRelativeToAppBase)
        {
            using var embeddedResourceStream = GetBinaryFileContent(pathComponentsRelativeToAppBase);
            using var reader = new StreamReader(embeddedResourceStream);
            return reader.ReadToEnd().Replace("\r\n", "\n");
        }

        public static Stream GetBinaryFileContent(params string[] pathComponentsRelativeToAppBase)
            => typeof(Program).Assembly.GetManifestResourceStream($"HostConfigConsole.{string.Join(".", pathComponentsRelativeToAppBase)}") ?? throw new InvalidOperationException("Embedded resource not found.");
    }
}
