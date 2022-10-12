// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Linq;
using System.IO.Compression;
using System.CommandLine;
using System.CommandLine.Parsing;
using System.CommandLine.Binding;

using Common.HostConfigs;
using HostConfigConsole;
using System;
using Newtonsoft.Json.Linq;

//var tst = GitHub.GetDirectory(new("https://github.com/microsoft/CromwellOnAzure/tree/bmurri/host-config-auto-scale/HostConfigs"));

(var buildCmd, var buildCmdArgs, var buildCmdOutFileOpt, var buildCmdHostConfigsDirOpt) = new Command("build", "Builds HostConfig metadata into deployment assets.") { TreatUnmatchedTokensAsErrors = true }
    .ConfigureCommand(
        new Argument<string[]>("host-config", "Zero or more directory names of host configuration descriptor directories.") { Arity = ArgumentArity.ZeroOrMore },
        new Option<FileInfo>(new[] { "--output", "-o"}, "Path to output file (used to stage metadata for the update command)."),
        new Option<string>(new[] { "--host-configs-directory", "-d" }, "Path to directory containing all the host configuration descriptor directories to process.") { IsRequired = true });

buildCmd.SetHandler(BuildCmdAsync, buildCmdArgs, buildCmdOutFileOpt, buildCmdHostConfigsDirOpt);

buildCmdHostConfigsDirOpt.AddValidator(result =>
{
    var value = result.GetValueForOption(buildCmdHostConfigsDirOpt);
    if (value is null)
    {
        result.ErrorMessage = "--host-configs-directory must exist and be accessible.";
    }
    else if (Uri.TryCreate(value, UriKind.Absolute, out var uri))
    {
        if (uri.Scheme != "https" && uri.Host != "github.com" || uri.Segments.Length < 3)
        {
            result.ErrorMessage = "--host-configs-directory must in a GitHub repository.";
        }
    }
    else
    {
        var dir = new DirectoryInfo(value);
        if (!(dir.Exists))
        {
            result.ErrorMessage = "--host-configs-directory must exist and be accessible.";
        }
    }
});

buildCmdArgs.AddValidator(result =>
{
    var value = result.GetValueForOption(buildCmdHostConfigsDirOpt);
    if (value is null || Uri.TryCreate(value, UriKind.Absolute, out _))
    {
        return;
    }

    var dir = new DirectoryInfo(value);
    string? errMsg = default;
    result
        .GetValueForArgument(buildCmdArgs)
        .Where(arg => !Directory.Exists(Path.Combine(dir.FullName, arg)))
        .ForEach(arg =>
        {
            errMsg ??= string.Empty;
            errMsg += $"Host configuration directory '{arg}' must exist in '{dir.FullName}' and be accessible." + Environment.NewLine;
        });

    if (errMsg is not null)
    {
        result.ErrorMessage = errMsg.TrimEnd(Environment.NewLine.ToCharArray());
    }
});

(var updateCmd, var updateArgs, var updateCmdInFile, var updateRemoveFilesOpt, var updateRemoveAppsOpt) = new Command("update", "Updates CoA deployment with built deployment assets") { TreatUnmatchedTokensAsErrors = true }
    .ConfigureCommand<Argument<string[]>, string[], Option<FileInfo>, FileInfo, Option<bool>, bool, Option<bool>, bool>(UpdateCmdAsync,
        new Argument<string[]>("deployer-update-args", "Arguments passed to the deployer (deploy-cromwell-on-azure) to update the intended deployment."),
        new Option<FileInfo>(new[] { "--input", "-i" }, "Path to output file (used to stage metadata for the update command)."),
        new Option<bool>("--remove-obsolete-files", "Removes obsolete files. (May be dangerous to use while workflows are running)."),
        new Option<bool>("--remove-obsolete-apps", "Removes obsolete application packages. (This can be particularly dangerous with shared batch accounts)."));

updateRemoveAppsOpt.AddValidator(results =>
{
    if (results.GetValueForOption(updateRemoveAppsOpt) && !results.GetValueForOption(updateRemoveFilesOpt))
    {
        Console.WriteLine("WARNING: --remove-obsolete-apps is set while --remove-obsolete-files is not set. This is not a generally expected use cage.");
        Console.WriteLine();
    }
});

updateCmdInFile.AddValidator(result =>
{
    if (!(result.GetValueForOption(updateCmdInFile)?.Exists ?? false))
    {
        result.ErrorMessage = "--input must exist and be accessible.";
    }
});

return await new RootCommand("Host Configuration utility.") { buildCmd, updateCmd }.InvokeAsync(args);

static FileInfo DefaultHostConfig()
    => new(Path.Combine(Environment.CurrentDirectory, "HostConfigs"));

async Task<int> UpdateCmdAsync(string[] updateArgs, FileInfo? inFile, bool removeFiles, bool removeApps)
{
    inFile ??= DefaultHostConfig();
    if (!inFile.Exists)
    {
        throw new InvalidOperationException("--input must exist and be accessible.");
    }

    using var package = ConfigurationPackageReadable.Open(inFile);
    var coa = await CromwellOnAzure.Create(updateArgs);

    var config = package.GetHostConfig() ?? throw new InvalidOperationException();
    var updater = new Updater(config, package.GetResources(), package.GetApplications(), await coa.GetHostConfig());
    await coa.AddApplications(updater.GetApplicationsToAdd());
    await coa.AddHostConfigBlobs(updater.GetStartTasksToAdd());
    await coa.WriteHostConfig(config);

    if (removeApps)
    {
        await coa.RemoveApplications(updater.GetApplicationsToRemove());
    }

    if (removeFiles)
    {
        await coa.RemoveHostConfigBlobs(updater.GetStartTasksToRemove());
    }

    return 0;
}

async Task<int> BuildCmdAsync(string[] hostconfigs, FileInfo? outFile, string hostConfigs)
{
    outFile ??= DefaultHostConfig();
    outFile.Directory?.Create();

    var hostConfigsDir = Uri.TryCreate(hostConfigs, UriKind.Absolute, out var uri)
        ? await GitHub.GetDirectory(uri)
        : FileSystem.GetDirectory(new DirectoryInfo(hostConfigs));

    var (config, resources, applications) = new Builder(hostConfigsDir)
        .Build(Console.WriteLine, hostconfigs ?? Array.Empty<string>());

    bool areSame;
    {
        if (outFile.Exists)
        {
            try
            {
                using var package = ConfigurationPackageReadable.Open(outFile);
                areSame = Builder.AreSame(config, package.GetHostConfig() ?? new HostConfig());
            }
            catch
            {
                areSame = false;
            }
        }
        else
        {
            areSame = false;
        }
    }

    if (!areSame)
    {
        Console.WriteLine("Saving configuration");
        using var package = ConfigurationPackageWritable.Create(outFile);
        resources.ForEach(resource => package.SetResource(resource.Name, resource.Stream.Value));
        applications.ForEach(app =>
        {
            var name = config.PackageHashes.FirstOrDefault(t => t.Value.Equals(app.Version, StringComparison.OrdinalIgnoreCase)).Key;
            package.SetApplication(name, app.Version, app.Stream.Value);
        });

        package.SetHostConfig(config);
    }

    Console.WriteLine("Finished");

    return 0;
}

static class CommandLineExtensions
{
    public static Command Add(this Command cmd, Symbol[] symbols)
    {
        symbols.ForEach(symbol =>
        {
            switch (symbol)
            {
                case Option option:
                    cmd.Add(option);
                    break;
                case Argument argument:
                    cmd.Add(argument);
                    break;
                case Command command:
                    cmd.Add(command);
                    break;
            }
        });
        return cmd;
    }

    private static void ConfigureCommandImpl(Command command, params Symbol[] symbols)
    {
        ArgumentNullException.ThrowIfNull(command);
        ArgumentNullException.ThrowIfNull(symbols);
        if (symbols.Length != 0) command.Add(symbols);
    }

    public static Command
        ConfigureCommand(this Command command)
    {
        ConfigureCommandImpl(command);
        return command;
    }

    public static Command
        ConfigureCommand(this Command command, Func<Task> handler)
    {
        ConfigureCommandImpl(command);
        command.SetHandler(handler);
        return command;
    }

    public static (Command Command, T T)
        ConfigureCommand<T>(this Command command, T t)
        where T : Symbol
    {
        ConfigureCommandImpl(command, t);
        return (command, t);
    }

    public static (Command Command, T T)
        ConfigureCommand<T, P>(this Command command, Func<P, Task> handler, T t)
        where T : Symbol, IValueDescriptor<P>
    {
        ConfigureCommandImpl(command, t);
        command.SetHandler(handler, t);
        return (command, t);
    }

    public static (Command Command, T1 T1, T2 T2)
        ConfigureCommand<T1, T2>(this Command command, T1 t1, T2 t2)
        where T1 : Symbol
        where T2 : Symbol
    {
        ConfigureCommandImpl(command, t1, t2);
        return (command, t1, t2);
    }

    public static (Command Command, T1 T1, T2 T2)
        ConfigureCommand<T1, P1, T2, P2>(this Command command, Func<P1, P2, Task> handler, T1 t1, T2 t2)
        where T1 : Symbol, IValueDescriptor<P1>
        where T2 : Symbol, IValueDescriptor<P2>
    {
        ConfigureCommandImpl(command, t1, t2);
        command.SetHandler(handler, t1, t2);
        return (command, t1, t2);
    }

    public static (Command Command, T1 T1, T2 T2, T3 T3)
        ConfigureCommand<T1, T2, T3>(this Command command, T1 t1, T2 t2, T3 t3)
        where T1 : Symbol
        where T2 : Symbol
        where T3 : Symbol
    {
        ConfigureCommandImpl(command, t1, t2, t3);
        return (command, t1, t2, t3);
    }

    public static (Command Command, T1 T1, T2 T2, T3 T3)
        ConfigureCommand<T1, P1, T2, P2, T3, P3>(this Command command, Func<P1, P2, P3, Task> handler, T1 t1, T2 t2, T3 t3)
        where T1 : Symbol, IValueDescriptor<P1>
        where T2 : Symbol, IValueDescriptor<P2>
        where T3 : Symbol, IValueDescriptor<P3>
    {
        ConfigureCommandImpl(command, t1, t2, t3);
        command.SetHandler(handler, t1, t2, t3);
        return (command, t1, t2, t3);
    }

    public static (Command Command, T1 T1, T2 T2, T3 T3, T4 T4)
        ConfigureCommand<T1, T2, T3, T4>(this Command command, T1 t1, T2 t2, T3 t3, T4 t4)
        where T1 : Symbol
        where T2 : Symbol
        where T3 : Symbol
        where T4 : Symbol
    {
        ConfigureCommandImpl(command, t1, t2, t3, t4);
        return (command, t1, t2, t3, t4);
    }

    public static (Command Command, T1 T1, T2 T2, T3 T3, T4 T4)
        ConfigureCommand<T1, P1, T2, P2, T3, P3, T4, P4>(this Command command, Func<P1, P2, P3, P4, Task> handler, T1 t1, T2 t2, T3 t3, T4 t4)
        where T1 : Symbol, IValueDescriptor<P1>
        where T2 : Symbol, IValueDescriptor<P2>
        where T3 : Symbol, IValueDescriptor<P3>
        where T4 : Symbol, IValueDescriptor<P4>
    {
        ConfigureCommandImpl(command, t1, t2, t3, t4);
        command.SetHandler(handler, t1, t2, t3, t4);
        return (command, t1, t2, t3, t4);
    }

    public static (Command Command, T1 T1, T2 T2, T3 T3, T4 T4, T5 T5)
        ConfigureCommand<T1, T2, T3, T4, T5>(this Command command, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5)
        where T1 : Symbol
        where T2 : Symbol
        where T3 : Symbol
        where T4 : Symbol
        where T5 : Symbol
    {
        ConfigureCommandImpl(command, t1, t2, t3, t4, t5);
        return (command, t1, t2, t3, t4, t5);
    }

    public static (Command Command, T1 T1, T2 T2, T3 T3, T4 T4, T5 T5)
        ConfigureCommand<T1, P1, T2, P2, T3, P3, T4, P4, T5, P5>(this Command command, Func<P1, P2, P3, P4, P5, Task> handler, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5)
        where T1 : Symbol, IValueDescriptor<P1>
        where T2 : Symbol, IValueDescriptor<P2>
        where T3 : Symbol, IValueDescriptor<P3>
        where T4 : Symbol, IValueDescriptor<P4>
        where T5 : Symbol, IValueDescriptor<P5>
    {
        ConfigureCommandImpl(command, t1, t2, t3, t4, t5);
        command.SetHandler(handler, t1, t2, t3, t4, t5);
        return (command, t1, t2, t3, t4, t5);
    }

    public static (Command Command, T1 T1, T2 T2, T3 T3, T4 T4, T5 T5, T6 T6)
        ConfigureCommand<T1, T2, T3, T4, T5, T6>(this Command command, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6)
        where T1 : Symbol
        where T2 : Symbol
        where T3 : Symbol
        where T4 : Symbol
        where T5 : Symbol
        where T6 : Symbol
    {
        ConfigureCommandImpl(command, t1, t2, t3, t4, t5, t6);
        return (command, t1, t2, t3, t4, t5, t6);
    }

    public static (Command Command, T1 T1, T2 T2, T3 T3, T4 T4, T5 T5, T6 T6)
        ConfigureCommand<T1, P1, T2, P2, T3, P3, T4, P4, T5, P5, T6, P6>(this Command command, Func<P1, P2, P3, P4, P5, P6, Task> handler, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6)
        where T1 : Symbol, IValueDescriptor<P1>
        where T2 : Symbol, IValueDescriptor<P2>
        where T3 : Symbol, IValueDescriptor<P3>
        where T4 : Symbol, IValueDescriptor<P4>
        where T5 : Symbol, IValueDescriptor<P5>
        where T6 : Symbol, IValueDescriptor<P6>
    {
        ConfigureCommandImpl(command, t1, t2, t3, t4, t5, t6);
        command.SetHandler(handler, t1, t2, t3, t4, t5, t6);
        return (command, t1, t2, t3, t4, t5, t6);
    }

    public static (Command Command, T1 T1, T2 T2, T3 T3, T4 T4, T5 T5, T6 T6, T7)
        ConfigureCommand<T1, T2, T3, T4, T5, T6, T7>(this Command command, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6, T7 t7)
        where T1 : Symbol
        where T2 : Symbol
        where T3 : Symbol
        where T4 : Symbol
        where T5 : Symbol
        where T6 : Symbol
        where T7 : Symbol
    {
        ConfigureCommandImpl(command, t1, t2, t3, t4, t5, t6, t7);
        return (command, t1, t2, t3, t4, t5, t6, t7);
    }

    public static (Command Command, T1 T1, T2 T2, T3 T3, T4 T4, T5 T5, T6 T6, T7)
        ConfigureCommand<T1, P1, T2, P2, T3, P3, T4, P4, T5, P5, T6, P6, T7, P7>(this Command command, Func<P1, P2, P3, P4, P5, P6, P7, Task> handler, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6, T7 t7)
        where T1 : Symbol, IValueDescriptor<P1>
        where T2 : Symbol, IValueDescriptor<P2>
        where T3 : Symbol, IValueDescriptor<P3>
        where T4 : Symbol, IValueDescriptor<P4>
        where T5 : Symbol, IValueDescriptor<P5>
        where T6 : Symbol, IValueDescriptor<P6>
        where T7 : Symbol, IValueDescriptor<P7>
    {
        ConfigureCommandImpl(command, t1, t2, t3, t4, t5, t6, t7);
        command.SetHandler(handler, t1, t2, t3, t4, t5, t6, t7);
        return (command, t1, t2, t3, t4, t5, t6, t7);
    }

    public static (Command Command, T1 T1, T2 T2, T3 T3, T4 T4, T5 T5, T6 T6, T7, T8 T8)
        ConfigureCommand<T1, T2, T3, T4, T5, T6, T7, T8>(this Command command, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6, T7 t7, T8 t8)
        where T1 : Symbol
        where T2 : Symbol
        where T3 : Symbol
        where T4 : Symbol
        where T5 : Symbol
        where T6 : Symbol
        where T7 : Symbol
        where T8 : Symbol
    {
        ConfigureCommandImpl(command, t1, t2, t3, t4, t5, t6, t7, t8);
        return (command, t1, t2, t3, t4, t5, t6, t7, t8);
    }

    public static (Command Command, T1 T1, T2 T2, T3 T3, T4 T4, T5 T5, T6 T6, T7, T8 T8)
        ConfigureCommand<T1, P1, T2, P2, T3, P3, T4, P4, T5, P5, T6, P6, T7, P7, T8, P8>(this Command command, Func<P1, P2, P3, P4, P5, P6, P7, P8, Task> handler, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6, T7 t7, T8 t8)
        where T1 : Symbol, IValueDescriptor<P1>
        where T2 : Symbol, IValueDescriptor<P2>
        where T3 : Symbol, IValueDescriptor<P3>
        where T4 : Symbol, IValueDescriptor<P4>
        where T5 : Symbol, IValueDescriptor<P5>
        where T6 : Symbol, IValueDescriptor<P6>
        where T7 : Symbol, IValueDescriptor<P7>
        where T8 : Symbol, IValueDescriptor<P8>
    {
        ConfigureCommandImpl(command, t1, t2, t3, t4, t5, t6, t7, t8);
        command.SetHandler(handler, t1, t2, t3, t4, t5, t6, t7, t8);
        return (command, t1, t2, t3, t4, t5, t6, t7, t8);
    }
}
