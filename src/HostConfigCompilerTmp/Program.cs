// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.CommandLine;
using System.Linq;
using System.IO.Compression;

using Common.HostConfigs;

var buildCmd = new Command("build", "Builds HostConfig metadata into deployment assets");
buildCmd.AddArgument(new Argument<string[]>("host-config", "TODO") { Arity = ArgumentArity.ZeroOrMore });
buildCmd.AddOption(new Option<FileInfo>("--host-configurations", "TODO") { IsRequired = true });
buildCmd.AddOption(new Option<FileInfo>("--resources-zip", "TODO") { IsRequired = true });
buildCmd.AddOption(new Option<DirectoryInfo>("--host-configs-directory", "TODO") { IsRequired = true });
buildCmd.SetHandler(BuildCmd);
new RootCommand("Host Configuration utility") { buildCmd }.Invoke(args);

static void BuildCmd(System.CommandLine.Invocation.InvocationContext context)
{
    var parseResult = context.ParseResult;
    FileInfo? hostConfigs = default;
    FileInfo? resourcesZip = default;
    DirectoryInfo? hostConfigsDir = default;

    foreach (var option in parseResult.CommandResult.Command.Options)
    {
        switch (option.Name)
        {
            case "host-configurations":
                hostConfigs = parseResult.GetValueForOption((Option<FileInfo>)option);
                break;
            case "resources-zip":
                resourcesZip = parseResult.GetValueForOption((Option<FileInfo>)option);
                break;
            case "host-configs-directory":
                hostConfigsDir = parseResult.GetValueForOption((Option<DirectoryInfo>)option);
                break;
        }
    }

    var hostconfigs = Enumerable.Empty<string>();
    var hostconfig = parseResult.CommandResult.Command.Arguments.OfType<Argument<string[]>>().FirstOrDefault();
    if (hostconfig is not null)
    {
        hostconfigs = parseResult.GetValueForArgument(hostconfig);
    }

    if (!(hostConfigsDir?.Exists ?? false))
    {
        throw new ArgumentException(null, "host-configs-directory", new DirectoryNotFoundException());
    }

    ArgumentNullException.ThrowIfNull(hostConfigs, "host-configurations");
    ArgumentNullException.ThrowIfNull(resourcesZip, "resources-zip");
    ArgumentNullException.ThrowIfNull(hostConfigsDir, "host-configs-directory");
    if (hostConfigs is null || resourcesZip is null || hostConfigsDir is null) throw new InvalidOperationException(); // prevent build warnings (ThrowIfNull isn't recognized by the analyzers)
    var unknown = (hostconfigs ?? Enumerable.Empty<string>()).Where(c => !hostConfigsDir.EnumerateDirectories().Where(d => d.Name.Equals(c, StringComparison.OrdinalIgnoreCase)).Any()).ToList();

    if (unknown.Any())
    {
        throw new ArgumentException($"The following requested host configurations were not found: {string.Join(", ", unknown.Select(s => $"'{s}'"))}", "host-config");
    }

    resourcesZip.Directory?.Create();
    hostConfigs.Directory?.Create();
    var tmpZip = new FileInfo(Path.GetTempFileName());

    try
    {
        HostConfig config;
        IEnumerable<(string Version, Lazy<Stream> Stream)> applications;

        using (var zip = new ZipArchive(tmpZip.OpenWrite(), ZipArchiveMode.Create))
        {
            (config, applications) = new Parser(hostConfigsDir).Parse(Console.WriteLine, zip, hostconfigs?.ToArray() ?? Array.Empty<string>());
        }
        tmpZip.Refresh();

        bool areSame;
        using (var reader = Parser.OpenConfiguration(hostConfigs))
        {
            areSame = Parser.AreSame(config, Parser.ReadJson(reader, () => new HostConfig()));
        }

        if (!areSame)
        {
            Console.WriteLine("Saving configuration");
            tmpZip.MoveTo(resourcesZip.FullName, true);
            tmpZip = default;
            File.WriteAllText(hostConfigs.FullName, Parser.WriteJson(config));
        }

        Console.WriteLine("Finished");
    }
    finally
    {
        tmpZip?.Refresh();
        tmpZip?.Delete();
    }
}
