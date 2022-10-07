// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Linq;
using System.IO.Compression;
using System.CommandLine;
using System.CommandLine.Parsing;
using System.CommandLine.Binding;

using Common.HostConfigs;
using HostConfigConsole;

(var buildCmd, var buildCmdArgs, var buildCmdHostConfigsOpt, var buildCmdResourcesZipOpt, var buildCmdHostConfigsDirOpt) = new Command("build", "Builds HostConfig metadata into deployment assets.") { TreatUnmatchedTokensAsErrors = true }
    .ConfigureCommand(
        new Argument<string[]>("host-config", "Zero or more directory names of host configuration descriptor directories.") { Arity = ArgumentArity.ZeroOrMore },
        new Option<FileInfo>(new[] { "--host-configurations", "-h" }, "Path to deployed host configuration file (must be named \"host-configurations.json\") (will be created/overwritten).") { IsRequired = true },
        new Option<FileInfo>(new[] { "--resources-zip", "-r" }, "Path to zip file containing deployed files (will be created/overwritten).") { IsRequired = true },
        new Option<DirectoryInfo>(new[] { "--host-configs-directory", "-d" }, "Path to directory containing all the host configuration descriptor directories to process.") { IsRequired = true });
buildCmd.SetHandler(BuildCmdAsync,
    new BuildHostConfigsBinder(buildCmdHostConfigsDirOpt, buildCmdArgs), buildCmdHostConfigsOpt, buildCmdResourcesZipOpt, buildCmdHostConfigsDirOpt);

var updateCmd = new Command("update", "Updates CoA deployment with built deployment assets") { TreatUnmatchedTokensAsErrors = true }
    .ConfigureCommand(UpdateCmdAsync);

return await new RootCommand("Host Configuration utility.") { buildCmd, updateCmd }.InvokeAsync(args);

Task UpdateCmdAsync() { return Task.CompletedTask; }

Task<int> BuildCmdAsync(DirectoryInfo[] hostConfigs, FileInfo hostConfigurations, FileInfo resourcesZip, DirectoryInfo hostConfigsDirectory)
    => Task.FromResult(BuildCmd(hostConfigs, hostConfigurations, resourcesZip, hostConfigsDirectory));

int BuildCmd(DirectoryInfo[] hostconfigs, FileInfo hostConfigs, FileInfo resourcesZip, DirectoryInfo hostConfigsDir)
{
#pragma warning disable CA2208 // Instantiate argument exceptions correctly
    if (!(hostConfigsDir?.Exists ?? false))
    {
        throw new ArgumentException(null, "host-configs-directory", new DirectoryNotFoundException());
    }

    ArgumentNullException.ThrowIfNull(hostConfigs, "host-configurations");
    ArgumentNullException.ThrowIfNull(resourcesZip, "resources-zip");
    ArgumentNullException.ThrowIfNull(hostConfigsDir, "host-configs-directory");
    if (hostConfigs is null || resourcesZip is null || hostConfigsDir is null) throw new InvalidOperationException(); // prevent build warnings (ThrowIfNull isn't recognized by the analyzers)
    var unknown = hostconfigs.Where(c => !c.Exists).ToList();

    if (unknown.Any())
    {
        throw new ArgumentException($"The following requested host configurations were not found: {string.Join(", ", unknown.Select(s => $"'{s.Name}'"))}", "host-config");
    }
#pragma warning restore CA2208 // Instantiate argument exceptions correctly

    resourcesZip.Directory?.Create();
    hostConfigs.Directory?.Create();
    var tmpZip = new FileInfo(Path.GetTempFileName());

    try
    {
        HostConfig config;
        IEnumerable<(string Version, Lazy<Stream> Stream)> applications;

        using (var zip = new ZipArchive(tmpZip.OpenWrite(), ZipArchiveMode.Create))
        {
            (config, applications) = new Builder(FileSystem.GetDirectory(hostConfigsDir)).Build(Console.WriteLine, zip, hostconfigs?.Select(d => d.Name).ToArray() ?? Array.Empty<string>());
        }
        tmpZip.Refresh();

        bool areSame;
        using (var reader = Builder.OpenConfiguration(hostConfigs))
        {
            areSame = Builder.AreSame(config, Builder.ReadJson(reader, () => new HostConfig()));
        }

        if (!areSame)
        {
            Console.WriteLine("Saving configuration");
            tmpZip.MoveTo(resourcesZip.FullName, true);
            tmpZip = default;
            File.WriteAllText(hostConfigs.FullName, Builder.WriteJson(config));
        }

        Console.WriteLine("Finished");
    }
    finally
    {
        tmpZip?.Refresh();
        tmpZip?.Delete();
    }

    return 0;
}

class BuildHostConfigsBinder : BinderBase<DirectoryInfo[]>
{
    private readonly Argument<string[]> argument;
    private readonly Option<DirectoryInfo> directory;

    public BuildHostConfigsBinder(Option<DirectoryInfo> dir, Argument<string[]> arg)
    {
        ArgumentNullException.ThrowIfNull(dir);
        ArgumentNullException.ThrowIfNull(arg);
        directory = dir;
        argument = arg;
    }

    protected override DirectoryInfo[] GetBoundValue(BindingContext bindingContext)
    {
        var dir = bindingContext.ParseResult.GetValueForOption(directory);
        if (dir is null) throw new InvalidOperationException("--host-configs-directory is required.");
        if (!dir.Exists) throw new InvalidOperationException("--host-configs-directory does not exist or is not accessible.");
        return bindingContext.ParseResult.GetValueForArgument(argument).Select(a => new DirectoryInfo(Path.Combine(dir.FullName, a))).ToArray();
    }
}

static class Extensions
{
    public static Command Add(this Command cmd, Symbol[] symbols)
    {
        foreach (var symbol in symbols)
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
        }

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
