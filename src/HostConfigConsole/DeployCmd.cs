// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.CommandLine;
using System.CommandLine.Invocation;

namespace HostConfigConsole
{
    internal sealed class DeployCmd : Command
    {
        public DeployCmd()
            : base("deploy", "Updates CoA deployment with built deployment assets")
        {
            TreatUnmatchedTokensAsErrors = true;
            AddOption(removeAppsOption);
            AddOption(removeFilesOption);
            AddOption(inFileOption);
            AddArgument(cmdArgs);

            removeAppsOption.AddValidator(results =>
            {
                if (results.GetValueForOption(removeAppsOption) && !results.GetValueForOption(removeFilesOption))
                {
                    Console.WriteLine("WARNING: --remove-obsolete-apps is set while --remove-obsolete-files is not set. This is not a generally expected use cage.");
                    Console.WriteLine();
                }
            });

            inFileOption.AddValidator(result =>
            {
                if (!(result.GetValueForOption(inFileOption)?.Exists ?? false))
                {
                    result.ErrorMessage = "--input must exist and be accessible.";
                }
            });

            Handler = new CommandHandler(Children.ToArray());
        }

        private readonly Argument<string[]> cmdArgs
            = new("deployer-update-args", "Arguments passed to the deployer (deploy-cromwell-on-azure) to update the intended deployment. Some (or all) can also be passed via 'config.json'. It is highly recommended to place after all options and prefix with '--'.");

        private readonly Option<FileInfo> inFileOption
            = new(new[] { "--input", "-i" }, "Path to output file (used to stage metadata for the update command).");

        private readonly Option<bool> removeFilesOption
            = new("--remove-obsolete-files", "Removes obsolete task script files.");

        private readonly Option<bool> removeAppsOption
            = new("--remove-obsolete-apps", "Removes obsolete application packages. (This can be particularly dangerous with shared batch accounts).");

        private sealed class CommandHandler : CommandHandler<bool?, bool?, FileInfo?, string[]>, BaseCommand
        {
            public CommandHandler(params Symbol[] symbols)
                : base(symbols) { }

            protected override async Task InvokeAsync(InvocationContext context, bool? removeApps, bool? removeFiles, FileInfo? inFile, string[]? args)
            {
                ArgumentNullException.ThrowIfNull(args);

                inFile ??= BaseCommand.DefaultHostConfig();
                if (!inFile.Exists)
                {
                    throw new InvalidOperationException("--input must exist and be accessible.");
                }

                CancellationTokenSource cts = new();

                try
                {
                    using var package = ConfigurationPackageReadable.Open(inFile);
                    var coa = await CromwellOnAzure.Create(cts, args);

                    var config = package.GetHostConfig() ?? throw new InvalidOperationException();
                    var updater = new Deployer(config, package.GetResources(), package.GetApplications(), await coa.GetHostConfig());

                    await coa.AddApplications(updater.GetApplicationsToAdd());
                    await coa.AddHostConfigBlobs(updater.GetStartTasksToAdd());
                    await coa.WriteHostConfig(config);

                    if (removeApps == true)
                    {
                        await coa.RemoveApplications(updater.GetApplicationsToRemove());
                    }

                    if (removeFiles == true)
                    {
                        await coa.RemoveHostConfigBlobs(updater.GetStartTasksToRemove());
                    }

                    await coa.RestartServices();
                }
                catch
                {
                    cts.Cancel();
                }
            }
        }
    }
}
