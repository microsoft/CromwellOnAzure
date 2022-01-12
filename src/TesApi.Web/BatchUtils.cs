// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.IO;

namespace TesApi.Web
{
    /// <summary>
    /// Util class for Azure Batch related helper functions.
    /// </summary>
    public class BatchUtils
    {
        /// <summary>
        /// Converts the install-docker.sh shell script to a string.
        /// </summary>
        /// <returns>The string version of the shell script.</returns>
        public static string GetBatchDockerInstallationScript()
        {
            return File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Scripts/install-docker.sh"));
        }
    }
}
