// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

#nullable enable

namespace Common.HostConfigs
{
    public static class Constants
    {
        public const string Config = "config.json";
        public const string StartTask = "start-task.sh";
        public const string StartApp = "start.zip";
        /*
        matcher.AddInclude("config.json");
        matcher.AddInclude("start.zip");
        matcher.AddInclude("job.zip");
        matcher.AddInclude("start-task.sh");
        matcher.AddInclude("job-task.sh");
         */

        public static IEnumerable<string> HostConfigFiles()
            => new[] { Config, StartTask, StartApp };
    }
}
