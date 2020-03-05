// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;
using System.Linq;
using TesApi.Models;

namespace TesApi.Web
{
    /// <summary>
    /// <see cref="TesTask"/> extensions
    /// </summary>
    public static class TesTaskExtensions
    {
        /// <summary>
        /// Writes to <see cref="TesTask"/> system log.
        /// </summary>
        /// <param name="tesTask"><see cref="TesTask"/></param>
        /// <param name="logEntries">List of strings to write to the log.</param>
        public static void WriteToSystemLog(this TesTask tesTask, params string[] logEntries)
        {
            if (logEntries != null && logEntries.Any(e => !string.IsNullOrEmpty(e)))
            {
                tesTask.Logs = tesTask.Logs ?? new List<TesTaskLog>();
                tesTask.Logs.Add(new TesTaskLog { SystemLogs = logEntries.ToList() });
            }
        }
    }
}
