// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace HostConfigCompiler
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Globalization;
    using System.IO;
    using System.IO.Compression;
    using System.Reflection;
    using System.Security.Cryptography;
    using System.Text;

    using Common.HostConfigs;
    using Newtonsoft.Json;
    using Microsoft.Build.Framework;
    using Microsoft.Build.Utilities;

    internal class HostConfigParser
    {
        private static readonly IEqualityComparer<DirectoryInfo> FullNameDirectoryInfoEqualityComparer
            = new DirectoryInfoEqualityComparer<DirectoryInfo>(typeof(DirectoryInfo).GetProperty(nameof(DirectoryInfo.FullName)));

        private readonly Microsoft.Build.Utilities.Task _task;
        private readonly DirectoryInfo _workDirectory;
        private readonly IBuildEngine9 _buildEngine;

        public HostConfigParser(Microsoft.Build.Utilities.Task task, DirectoryInfo workDirectory, IBuildEngine9 buildEngine9)
        {
            _task = task;
            _workDirectory = workDirectory;
            _buildEngine = buildEngine9;
        }

        private class DirectoryInfoEqualityComparer<T> : IEqualityComparer<T>
        {
            private readonly PropertyInfo _propertyInfo;
            private readonly IEqualityComparer _equalityComparer;

            public DirectoryInfoEqualityComparer(PropertyInfo propertyInfo)
            {
                _propertyInfo = propertyInfo;
                var ecType = typeof(EqualityComparer<>).MakeGenericType(new Type[] { _propertyInfo.PropertyType });
                _equalityComparer = (IEqualityComparer)ecType.GetProperty(@"Default", BindingFlags.Public | BindingFlags.Static).GetValue(null);
            }

            bool IEqualityComparer<T>.Equals(T x, T y)
                => _equalityComparer.Equals(_propertyInfo.GetValue(x), _propertyInfo.GetValue(y));

            int IEqualityComparer<T>.GetHashCode(T obj)
                => _equalityComparer.GetHashCode(_propertyInfo.GetValue(obj));
        }

        internal void Parse(IEnumerable<DirectoryInfo> directories)
        {
            _task.Log.LogMessage(@"Collecting CoA host configurations");

            using var reader = OpenConfiguration();
            var config = ReadJson(reader, () => new HostConfig());
            var configHash = WriteJson(config).GetHashCode();

            var storedVersions = config.ApplicationVersions ?? new();
            var configs = config.HostConfigurations ?? new();
            var hashes = config.PackageHashes ?? new();
            var startTasks = configs.Select(GetStartTaskHash).Where(h => h is not null).ToDictionary<string?, string, FileInfo?>(h => h ?? throw new InvalidOperationException(), h => default);

            //var knownExtant = Enumerable.Empty<(string, string)>();
            var extraneousApplications = Enumerable.Empty<string>();
            foreach (var (dir, directory) in directories.Distinct(FullNameDirectoryInfoEqualityComparer).Select(ParseHostConfigDir).Select(d => (d.FirstOrDefault()?.Directory, d.ToList())))
            {
                if (dir is null)
                {
                    continue;
                }

                _task.Log.LogMessage(@"Processing {0}", dir.Name);
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

            var resourcesDir = _workDirectory.CreateSubdirectory("Resources");
            foreach (var item in resourcesDir.EnumerateFileSystemInfos())
            {
                item.Delete();
            }

            foreach (var startTask in startTasks.Where(p => p.Value is not null))
            {
                _ = startTask.Value?.CopyTo(Path.Combine(resourcesDir.FullName, startTask.Key), true) ?? throw new InvalidOperationException();
            }

            if (!new Microsoft.Build.Tasks.ZipDirectory
                {
                    BuildEngine = _buildEngine,
                    DestinationFile = new TaskItem(Path.Combine(_workDirectory.FullName, "resources.zip")),
                    SourceDirectory = new TaskItem(resourcesDir.FullName),
                    Overwrite = true
                }.Execute())
            {
                throw new InvalidOperationException();
            }

            foreach (var item in resourcesDir.EnumerateFileSystemInfos())
            {
                item.Delete();
            }

            var result = WriteJson(config);
            if (result.GetHashCode() != configHash)
            {
                _task.Log.LogMessage(@"Saving configuration");
                File.WriteAllText(Path.Combine(_workDirectory.FullName, "host-configurations.json"), result);
            }

            _task.Log.LogMessage(@"Finished");

            TextReader? OpenConfiguration()
            {
                try
                {
                    
                    return File.OpenText(Path.Combine(_workDirectory.FullName, "host-configurations.json"));
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
                => ConvertToHexString(SHA256HashData(File.ReadAllBytes(file.FullName)));

            static string ConvertToHexString(byte[] bytes)
            {
                var sb = new StringBuilder();

                foreach (var b in bytes)
                {
                    sb.AppendFormat(@"{0:X2}", b);
                }

                return sb.ToString();
            }

            static byte[] SHA256HashData(byte[] bytes)
            {
                using var hash = SHA256.Create();
                return hash.ComputeHash(bytes);
            }

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
        }
    }
}
