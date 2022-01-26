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
        /// Readonly variable for the command line string so we're only reading from the file once.
        /// </summary>
        public static readonly string StartTaskScript = GetStartTaskScript();

        /// <summary>
        /// Converts the install-docker.sh shell script to a string.
        /// </summary>
        /// <returns>The string version of the shell script.</returns>
        private static string GetStartTaskScript()
        {
            return File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Scripts/start-task.sh"));
        }
    }
}
