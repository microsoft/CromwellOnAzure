// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.CommandLine;
using System.Linq;
using System.Globalization;
using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

using Common.HostConfigs;
using Newtonsoft.Json;

var buildCmd = new Command("build", "Builds HostConfig metadata into deployment assets");
//buildCmd.AddArgument(new System.CommandLine.Argument<string>("host-config", "TODO") { Arity = System.CommandLine.ArgumentArity.ZeroOrMore });
buildCmd.AddOption(new Option<FileInfo>("--host-configurations", "TODO") { IsRequired = true });
buildCmd.AddOption(new Option<DirectoryInfo>("--zip-directory", "TODO") { IsRequired = true });
buildCmd.AddOption(new Option<DirectoryInfo>("--host-configs-directory", "TODO") { IsRequired = true });
buildCmd.SetHandler(BuildCmd);
new RootCommand("Host Configuration utility") { buildCmd }.Invoke(args);

void BuildCmd(System.CommandLine.Invocation.InvocationContext context)
{
    FileInfo? hostConfigs = default;
    DirectoryInfo? zipDir = default;
    DirectoryInfo? hostConfigsDir = default;

    foreach (var option in context.ParseResult.CommandResult.Command.Options)
    {
        switch (option.Name)
        {
            case "host-configurations":
                hostConfigs = context.ParseResult.GetValueForOption((System.CommandLine.Option<FileInfo>)option);
                break;
            case "zip-directory":
                zipDir = context.ParseResult.GetValueForOption((System.CommandLine.Option<DirectoryInfo>)option);
                break;
            case "host-configs-directory":
                hostConfigsDir = context.ParseResult.GetValueForOption((System.CommandLine.Option<DirectoryInfo>)option);
                break;
        }
    }

    if (!(hostConfigsDir?.Exists ?? false))
    {
        throw new ArgumentException(null, "host-configs-directory", new DirectoryNotFoundException());
    }

    if (hostConfigs is null || zipDir is null || hostConfigsDir is null)
    {
        throw new ArgumentNullException();
    }

    zipDir.Create();
    hostConfigs.Directory?.Create();

    Console.WriteLine("Collecting CoA host configurations");
    Console.WriteLine();

    using var reader = OpenConfiguration(hostConfigs);
    var config = ReadJson(reader, () => new HostConfig());
    var configHash = WriteJson(config).GetHashCode();

    var storedVersions = config.ApplicationVersions ?? new();
    var configs = config.HostConfigurations ?? new();
    var hashes = config.PackageHashes ?? new();
    var startTasks = configs.Select(GetStartTaskHash).Where(h => h is not null).ToDictionary<string?, string, FileInfo?>(h => h ?? throw new InvalidOperationException(), h => default);

    //var knownExtant = Enumerable.Empty<(string, string)>();
    var extraneousApplications = Enumerable.Empty<string>();
    foreach (var (dir, directory) in hostConfigsDir.EnumerateDirectories().DistinctBy(d => d.FullName).Select(ParseHostConfigDir).Select(d => (d.FirstOrDefault()?.Directory, d.ToList())))
    {
        if (dir is null)
        {
            continue;
        }

        Console.WriteLine($"Processing {dir.Name}");
        var configFile = directory.FirstOrDefault(f => "config.json".Equals(f.Name, StringComparison.OrdinalIgnoreCase));
        var startTask = directory.FirstOrDefault(f => "start-task.sh".Equals(f.Name, StringComparison.OrdinalIgnoreCase));
        var nonZipFiles = Enumerable.Empty<FileInfo>();
        nonZipFiles = configFile is null ? nonZipFiles : nonZipFiles.Append(configFile);
        nonZipFiles = startTask is null ? nonZipFiles : nonZipFiles.Append(startTask);
        var zipFiles = directory.Except(nonZipFiles).Cast<FileInfo>().ToList();

        if (configFile is not null)
        {
            var configuration = ParseConfig(configFile);
            MungeVirtualMachineSizes(configuration.VmSizes);
            configs[dir.Name] = configuration;

            if (startTask is not null)
            {
                startTasks[configuration.StartTask?.StartTaskHash ?? throw new InvalidOperationException()] = startTask;
            }

            static void MungeVirtualMachineSizes(VirtualMachineSizes vmSizes)
            {
                if (vmSizes is not null)
                {
                    foreach (var size in vmSizes)
                    {
                        if (size.Container is not null)
                        {
                            size.Container = Common.Utilities.NormalizeContainerImageName(size.Container).AbsoluteUri;
                        }
                    }
                }
            }
        }
        else
        {
            _ = configs.Remove(dir.Name);
        }

        var extraneousLocalApplications = storedVersions.Keys.Where(n => n.StartsWith($"{dir.Name}_")).ToList();

        foreach (var (name, hash, package) in
            zipFiles.Select(GetApplicationName).Zip(
            zipFiles.Select(GetFileHash),
            zipFiles.Select(ExtractPackage)))
        {
            _ = extraneousLocalApplications.Remove(name);
            hashes[name] = hash;

            if (storedVersions.TryGetValue(name, out var keyMetadata))
            {
                keyMetadata.Packages ??= new();
                keyMetadata.Packages[hash] = package;
            }
            else
            {
                var packages = Packages.Empty;
                packages.Add(hash, package ?? new());
                storedVersions.Add(name, new() { ApplicationId = BatchAppFromHostConfigName(name), Packages = packages });
            }
        }

        foreach (var name in extraneousLocalApplications)
        {
            storedVersions.Remove(name);
            hashes.Remove(name);
        }

        foreach (var packages in storedVersions.Where(t => t.Key.StartsWith($"{dir.Name}_")).Select(t => t.Value).Select(t => t.Packages))
        {
            var toRemove = new List<string>();
            foreach (var key in packages.Keys)
            {
                if (!hashes.ContainsValue(key))
                {
                    toRemove.Add(key);
                }
            }

            foreach (var key in toRemove)
            {
                packages.Remove(key);
            }
        }

        extraneousApplications = extraneousApplications.Concat(extraneousLocalApplications);
    }

    foreach (var key in extraneousApplications)
    {
        storedVersions.Remove(key);
    }

    // TODO: based on a switch, remove configurations from the existing config not found in this directory walk

    //var serverAppList = Enumerable.Empty<(string Name, string Version)>();
    //var startTaskList = Enumerable.Empty<(string Name, FileInfo Content)>();

    foreach (var item in zipDir.EnumerateFileSystemInfos())
    {
        item.Delete();
    }

    foreach (var startTask in startTasks.Where(p => p.Value is not null))
    {
        _ = startTask.Value?.CopyTo(Path.Combine(zipDir.FullName, startTask.Key), true) ?? throw new InvalidOperationException();
    }

    var result = WriteJson(config);
    if (result.GetHashCode() != configHash)
    {
        Console.WriteLine("Saving configuration");
        File.WriteAllText(hostConfigs.FullName, result);
    }

    Console.WriteLine("Finished");
}

static TextReader? OpenConfiguration(FileInfo config)
{
    try
    {
        return config.OpenText();
    }
    catch (DirectoryNotFoundException)
    {
        return default;
    }
    catch (FileNotFoundException)
    {
        return default;
    }
}

static string GetApplicationName(FileInfo file)
    => $"{file.Directory?.Name}_{Path.GetFileNameWithoutExtension(file.Name)}";

static Package ExtractPackage(FileInfo file)
{
    using var zip = new ZipArchive(file.OpenRead());
    return new()
    {
        ContainsTaskScript = zip.Entries
            .Any(e => "start-task.sh".Equals(e.Name, StringComparison.Ordinal)),
        DockerLoadables = zip.Entries
            .Where(e => e.Name.EndsWith(".tar", StringComparison.OrdinalIgnoreCase))
            .Select(e => Path.GetFileNameWithoutExtension(e.Name))
            .ToArray()
    };
}

static IEnumerable<FileInfo> ParseHostConfigDir(DirectoryInfo hostConfigDir)
{
    var matcher = new Microsoft.Extensions.FileSystemGlobbing.Matcher();
    matcher.AddInclude("config.json");
    matcher.AddInclude("start.zip");
    matcher.AddInclude("task.zip");
    matcher.AddInclude("start-task.sh");
    return matcher.Execute(new Microsoft.Extensions.FileSystemGlobbing.Abstractions.DirectoryInfoWrapper(hostConfigDir))
        .Files.Select(f => hostConfigDir.EnumerateFiles(f.Path).First());
}

static string BatchAppFromHostConfigName(string host)
{
    const char dash = '-';
    return new string(host.Normalize().Select(Mangle).ToArray());

    static char Mangle(char c)
        => char.GetUnicodeCategory(c) switch
        {
            UnicodeCategory.UppercaseLetter => MangleLetter(c),
            UnicodeCategory.LowercaseLetter => MangleLetter(c),
            UnicodeCategory.TitlecaseLetter => MangleLetter(c),
            UnicodeCategory.DecimalDigitNumber => c,
            UnicodeCategory.LetterNumber => MangleNumber(c),
            UnicodeCategory.OtherNumber => MangleNumber(c),
            UnicodeCategory.PrivateUse => dash,
            UnicodeCategory.ConnectorPunctuation => dash,
            UnicodeCategory.DashPunctuation => dash,
            UnicodeCategory.MathSymbol => dash,
            UnicodeCategory.CurrencySymbol => dash,
            _ => '_'
        };

    static char MangleLetter(char c)
        => char.IsLetterOrDigit(c) && /**/(c >= 0x0000 && c <= 0x007f) ? c : dash;

    static char MangleNumber(char c)
        => char.GetNumericValue(c) switch
        {
            (>= 0) and (<= 9) => (char)('0' + char.GetNumericValue(c)),
            _ => dash
        };
}


static string? GetStartTaskHash(KeyValuePair<string, HostConfiguration> keyValuePair)
    => keyValuePair.Value.StartTask?.StartTaskHash;

static HostConfiguration ParseConfig(FileInfo configFile)
    => ConvertUserConfig(ParseUserConfig(configFile.OpenText()), configFile.Directory.EnumerateFiles("start-task.sh").Select(GetFileHash).FirstOrDefault());

static UserHostConfig ParseUserConfig(TextReader textReader)
    => ReadJson<UserHostConfig>(textReader, () => throw new ArgumentException("File is not a HostConfig configuration.", nameof(textReader)));

static HostConfiguration ConvertUserConfig(UserHostConfig? userConfig, string? startTaskHash)
    => new() { BatchImage = userConfig?.BatchImage ?? new(), VmSizes = userConfig?.VirtualMachineSizes ?? new(), DockerRun = userConfig?.DockerRun ?? new(), StartTask = ConvertStartTask(userConfig?.StartTask, startTaskHash) };

static Common.HostConfigs.StartTask ConvertStartTask(UserHostConfig.UserStartTask? userStartTask, string? startTaskHash)
    => new() { ResourceFiles = userStartTask?.ResourceFiles ?? Array.Empty<Common.HostConfigs.ResourceFile>(), StartTaskHash = startTaskHash };

static string GetFileHash(FileInfo file)
    => Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(file.FullName)));

static T ReadJson<T>(TextReader? textReader, Func<T> defaultValue)
{
    return textReader is null
        ? defaultValue()
        : ReadFile();

    T ReadFile()
    {
        using var reader = new JsonTextReader(textReader);
        return JsonSerializer.CreateDefault().Deserialize<T>(reader) ?? defaultValue();
    }
}

static string WriteJson<T>(T value)
{
    using var result = new StringWriter();
    using var writer = new JsonTextWriter(result);
    JsonSerializer.CreateDefault().Serialize(writer, value);
    return result.ToString();
}
