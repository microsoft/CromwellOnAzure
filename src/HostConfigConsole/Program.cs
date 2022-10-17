// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.CommandLine;

return await new RootCommand("Host Configuration utility.") { new HostConfigConsole.BuildCmd(), new HostConfigConsole.DeployCmd() }.InvokeAsync(args);

namespace HostConfigConsole
{
    internal interface BaseCommand
    {
        protected static FileInfo DefaultHostConfig()
            => new(Path.Combine(Environment.CurrentDirectory, "HostConfigs"));
    }
}
