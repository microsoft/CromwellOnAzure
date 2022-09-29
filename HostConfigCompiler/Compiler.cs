// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace HostConfigCompiler
{
    using System;
    using System.IO;
    using System.Linq;

    using Microsoft.Build.Framework;
    using Microsoft.Build.Utilities;
    using static System.Net.WebRequestMethods;

    public class Compiler : Task
    {
        [Required]
        public ITaskItem? WorkDirectory { get; set; }

        [Required]
        public ITaskItem? HostConfigsDirectory { get; set; }

        public string[]? HostConfigs { get; set; }

        [Output]
        public ITaskItem[]? Resources { get; set; }

        public override bool Execute()
        {
            //System.Diagnostics.Debugger.Launch();
            //System.Diagnostics.Debugger.Break();

            try
            {
                var wd = new DirectoryInfo(WorkDirectory?.ItemSpec);
                var dir = new DirectoryInfo(HostConfigsDirectory?.ItemSpec);
                if (!dir.Exists) { return false; } // TODO: report/throw error

                wd.Create();
                var dirs = dir.EnumerateDirectories();

                if (HostConfigs is not null)
                {
                    dirs = dirs.Where(d => HostConfigs.Contains(d.Name));
                }

                new HostConfigParser(this, wd, this.BuildEngine9).Parse(dirs);

                Resources = wd.EnumerateFiles().Select(f => new TaskItem(f.FullName)).ToArray();
            }
            catch (Exception ex)
            {
                Log.LogErrorFromException(ex);
                return false;
            }

            return true;
        }
    }
}
