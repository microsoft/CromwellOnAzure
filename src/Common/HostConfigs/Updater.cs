// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Resources;
using System.Text;
using System.Threading.Tasks;

#nullable enable

namespace Common.HostConfigs
{
    /// <summary>
    /// Compares two <see cref="HostConfigurations"/> and updates deployments
    /// </summary>
    public sealed class Updater : Base, IDisposable
    {
        private readonly ReadOnlyHostConfig _existing;
        private readonly ReadOnlyHostConfig _target;

        public IEnumerable<(string Path, Func<Stream> Open)> GetResourcesToAdd()
            => Enumerable.Empty<(string, Func<Stream>)>();

        public IEnumerable<string> GetResourcesToRemove()
            => Enumerable.Empty<string>();

        public IEnumerable<(string Name, string Version, Func<Stream> Open)> GetApplicationsToAdd()
            => Enumerable.Empty<(string, string, Func<Stream>)>();

        public IEnumerable<(string Name, string Version)> GetApplicationsToRemove()
            => Enumerable.Empty<(string, string)>();


        public Updater((HostConfig hostConfig, ZipArchive resources, IReadOnlyDictionary<string, Lazy<Stream>> packageVersions) existing, (HostConfig hostConfig, ZipArchive resources, IReadOnlyDictionary<string, Lazy<Stream>> packageVersions) target)
        {
            _existing = new(existing.hostConfig, existing.resources, existing.packageVersions);
            _target = new(target.hostConfig, target.resources, target.packageVersions);
        }

        private static string? GetStartTaskHash(KeyValuePair<string, HostConfiguration> keyValuePair)
            => keyValuePair.Value.StartTask?.StartTaskHash;

        public void Dispose()
        {
            _existing?.Dispose();
            _target?.Dispose();
        }

        private sealed class ReadOnlyHostConfig : IDisposable
        {
            public ReadOnlyDictionary<string, ApplicationVersion> ApplicationVersions { get; }
            public ReadOnlyDictionary<string, string> PackageHashes { get; }
            public ReadOnlyDictionary<string, HostConfiguration> HostConfigurations { get; }
            public ReadOnlyCollection<string> StartTaskHashes { get; }
            public ZipArchive Resources { get; }
            public IReadOnlyDictionary<string, Lazy<Stream>> PackageVersions { get; }

            public ReadOnlyHostConfig(HostConfig hostConfig, ZipArchive resources, IReadOnlyDictionary<string, Lazy<Stream>> packageVersions)
            {
                ArgumentNullException.ThrowIfNull(hostConfig, nameof(hostConfig));
                Resources = resources ?? throw new ArgumentNullException(nameof(resources));
                PackageVersions = packageVersions ?? throw new ArgumentNullException(nameof(packageVersions));
                ApplicationVersions = new(hostConfig.ApplicationVersions ?? HostConfigs.ApplicationVersions.Empty);
                PackageHashes = new(hostConfig.PackageHashes ?? HostConfigs.PackageHashes.Empty);
                HostConfigurations = new(hostConfig.HostConfigurations ?? HostConfigs.HostConfigurations.Empty);
                StartTaskHashes = new(HostConfigurations.Select(GetStartTaskHash).Where(h => h is not null).Cast<string>().ToList());
            }

            public void Dispose()
                => Resources?.Dispose();
        }
    }
}

//using var reader = OpenConfiguration(hostConfigs);
//var config = ReadJson(reader, () => new HostConfig());
//var configHash = WriteJson(config).GetHashCode();

//var storedVersions = config.ApplicationVersions ?? new();
//var hashes = config.PackageHashes ?? new();
//var configs = config.HostConfigurations ?? new();
//var startTasks = new Dictionary<string, FileInfo>();
////This code snippet can find all hashes declared in the config which can be used to potentially remove obsoleted files from the storage account: configs.Select(GetStartTaskHash).Where(h => h is not null);

////var knownExtant = Enumerable.Empty<(string, string)>();
//var extraneousApplications = Enumerable.Empty<string>();
//foreach (var (dir, directory) in hostConfigsDir.EnumerateDirectories()/*.DistinctBy(d => d.FullName)*/.Select(ParseHostConfigDir).Select(d => (d.FirstOrDefault()?.Directory, d.ToList())))
//{
//    if (dir is null) // TODO: filter by dir.Name (via build arguments)
//    {
//        continue;
//    }

//    Console.WriteLine($"Processing {dir.Name}");
//    var configFile = directory.FirstOrDefault(f => "config.json".Equals(f.Name, StringComparison.OrdinalIgnoreCase));
//    var startTask = directory.FirstOrDefault(f => "start-task.sh".Equals(f.Name, StringComparison.OrdinalIgnoreCase));
//    var nonZipFiles = Enumerable.Empty<FileInfo>();
//    nonZipFiles = configFile is null ? nonZipFiles : nonZipFiles.Append(configFile);
//    nonZipFiles = startTask is null ? nonZipFiles : nonZipFiles.Append(startTask);
//    var zipFiles = directory.Except(nonZipFiles).ToList();
//    //var otherFiles = dir.EnumerateFiles().Except(nonZipFiles).Except(zipFiles).ToList(); // To show the user for host-config selection determinations

//    if (configFile is not null)
//    {
//        var configuration = ParseConfig(configFile);
//        MungeVirtualMachineSizes(configuration.VmSizes);
//        configs[dir.Name] = configuration;

//        if (startTask is not null)
//        {
//            startTasks[configuration.StartTask?.StartTaskHash ?? throw new InvalidOperationException()] = startTask;
//        }

//        static void MungeVirtualMachineSizes(VirtualMachineSizes vmSizes)
//        {
//            if (vmSizes is not null)
//            {
//                foreach (var size in vmSizes)
//                {
//                    if (size.Container is not null)
//                    {
//                        size.Container = Common.Utilities.NormalizeContainerImageName(size.Container).AbsoluteUri;
//                    }
//                }
//            }
//        }
//    }
//    else
//    {
//        _ = configs.Remove(dir.Name);
//    }

//    var extraneousLocalApplications = storedVersions.Keys.Where(n => n.StartsWith($"{dir.Name}_")).ToList();

//    foreach (var (name, hash, package) in
//        zipFiles.Select(GetApplicationName).Zip(
//        zipFiles.Select(GetFileHash),
//        zipFiles.Select(ExtractPackage)))
//    {
//        _ = extraneousLocalApplications.Remove(name);
//        hashes[name] = hash;

//        if (storedVersions.TryGetValue(name, out var keyMetadata))
//        {
//            keyMetadata.Packages ??= new();
//            keyMetadata.Packages[hash] = package;
//        }
//        else
//        {
//            var packages = Packages.Empty;
//            packages.Add(hash, package ?? new());
//            storedVersions.Add(name, new() { ApplicationId = BatchAppFromHostConfigName(name), Packages = packages });
//        }
//    }

//    foreach (var name in extraneousLocalApplications)
//    {
//        storedVersions.Remove(name);
//        hashes.Remove(name);
//    }

//    foreach (var packages in storedVersions.Where(t => t.Key.StartsWith($"{dir.Name}_")).Select(t => t.Value).Select(t => t.Packages))
//    {
//        var toRemove = new List<string>();
//        foreach (var key in packages.Keys)
//        {
//            if (!hashes.ContainsValue(key))
//            {
//                toRemove.Add(key);
//            }
//        }

//        foreach (var key in toRemove)
//        {
//            packages.Remove(key);
//        }
//    }

//    extraneousApplications = extraneousApplications.Concat(extraneousLocalApplications);
//}

//foreach (var key in extraneousApplications)
//{
//    storedVersions.Remove(key);
//}

//// TODO: based on a switch, remove configurations from the existing config not found in this directory walk

////var serverAppList = Enumerable.Empty<(string Name, string Version)>();
////var startTaskList = Enumerable.Empty<(string Name, FileInfo Content)>();

//foreach (var item in zipDir.EnumerateFileSystemInfos())
//{
//    item.Delete();
//}

//foreach (var startTask in startTasks)
//{
//    _ = startTask.Value.CopyTo(Path.Combine(zipDir.FullName, startTask.Key), true) ?? throw new InvalidOperationException();
//}

//static TextReader? OpenConfiguration(FileInfo config)
//{
//    try
//    {
//        return config.OpenText();
//    }
//    catch (DirectoryNotFoundException)
//    {
//        return default;
//    }
//    catch (FileNotFoundException)
//    {
//        return default;
//    }
//}

//static string GetApplicationName(FileInfo file)
//    => $"{file.Directory?.Name}_{Path.GetFileNameWithoutExtension(file.Name)}";

//static Package ExtractPackage(FileInfo file)
//{
//    using var zip = new ZipArchive(file.OpenRead());
//    return new()
//    {
//        ContainsTaskScript = zip.Entries
//            .Any(e => "start-task.sh".Equals(e.Name, StringComparison.Ordinal)),
//        DockerLoadables = zip.Entries
//            .Where(e => e.Name.EndsWith(".tar", StringComparison.OrdinalIgnoreCase))
//            .Select(e => Path.GetFileNameWithoutExtension(e.Name))
//            .ToArray()
//    };
//}

//static IEnumerable<FileInfo> ParseHostConfigDir(DirectoryInfo hostConfigDir)
//{
//    var matcher = new Microsoft.Extensions.FileSystemGlobbing.Matcher();
//    matcher.AddInclude("config.json");
//    matcher.AddInclude("start.zip");
//    matcher.AddInclude("task.zip");
//    matcher.AddInclude("start-task.sh");
//    return matcher.Execute(new Microsoft.Extensions.FileSystemGlobbing.Abstractions.DirectoryInfoWrapper(hostConfigDir))
//        .Files.Select(f => hostConfigDir.EnumerateFiles(f.Path).First());
//}

//static string BatchAppFromHostConfigName(string host)
//{
//    const char dash = '-';
//    return new string(host.Normalize().Select(Mangle).ToArray());

//    static char Mangle(char c)
//        => char.GetUnicodeCategory(c) switch
//        {
//            UnicodeCategory.UppercaseLetter => MangleLetter(c),
//            UnicodeCategory.LowercaseLetter => MangleLetter(c),
//            UnicodeCategory.TitlecaseLetter => MangleLetter(c),
//            UnicodeCategory.DecimalDigitNumber => c,
//            UnicodeCategory.LetterNumber => MangleNumber(c),
//            UnicodeCategory.OtherNumber => MangleNumber(c),
//            UnicodeCategory.PrivateUse => dash,
//            UnicodeCategory.ConnectorPunctuation => dash,
//            UnicodeCategory.DashPunctuation => dash,
//            UnicodeCategory.MathSymbol => dash,
//            UnicodeCategory.CurrencySymbol => dash,
//            _ => '_'
//        };

//    static char MangleLetter(char c)
//        => char.IsLetterOrDigit(c) && /**/(c >= 0x0000 && c <= 0x007f) ? c : dash;

//    static char MangleNumber(char c)
//        => char.GetNumericValue(c) switch
//        {
//            (>= 0) and (<= 9) => (char)('0' + char.GetNumericValue(c)),
//            _ => dash
//        };
//}


////static string? GetStartTaskHash(KeyValuePair<string, HostConfiguration> keyValuePair)
////    => keyValuePair.Value.StartTask?.StartTaskHash;

//static HostConfiguration ParseConfig(FileInfo configFile)
//    => ConvertUserConfig(ParseUserConfig(configFile.OpenText()), configFile.Directory.EnumerateFiles("start-task.sh").Select(GetFileHash).FirstOrDefault());

//static UserHostConfig ParseUserConfig(TextReader textReader)
//    => ReadJson<UserHostConfig>(textReader, () => throw new ArgumentException("File is not a HostConfig configuration.", nameof(textReader)));

//static HostConfiguration ConvertUserConfig(UserHostConfig? userConfig, string? startTaskHash)
//    => new() { BatchImage = userConfig?.BatchImage ?? new(), VmSizes = userConfig?.VirtualMachineSizes ?? new(), DockerRun = userConfig?.DockerRun ?? new(), StartTask = ConvertStartTask(userConfig?.StartTask, startTaskHash) };

//static Common.HostConfigs.StartTask ConvertStartTask(UserHostConfig.UserStartTask? userStartTask, string? startTaskHash)
//    => new() { ResourceFiles = userStartTask?.ResourceFiles ?? Array.Empty<Common.HostConfigs.ResourceFile>(), StartTaskHash = startTaskHash };

//static string GetFileHash(FileInfo file)
//    => Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(file.FullName)));

//static T ReadJson<T>(TextReader? textReader, Func<T> defaultValue)
//{
//    return textReader is null
//        ? defaultValue()
//        : ReadFile();

//    T ReadFile()
//    {
//        using var reader = new JsonTextReader(textReader);
//        return JsonSerializer.CreateDefault().Deserialize<T>(reader) ?? defaultValue();
//    }
//}

//static string WriteJson<T>(T value)
//{
//    using var result = new StringWriter();
//    using var writer = new JsonTextWriter(result);
//    JsonSerializer.CreateDefault().Serialize(writer, value);
//    return result.ToString();
//}
