// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography;

#nullable enable

namespace Common.HostConfigs
{
    /// <summary>
    /// Reads a file system and generates <see cref="HostConfigurations"/>
    /// </summary>
    public sealed class Parser : Base
    {
        private readonly DirectoryInfo _hostConfigs;

        /// <summary>
        /// Creates a <see cref="Parser"/>
        /// </summary>
        /// <param name="hostConfigs">Directory containing the host configuration data.</param>
        /// <exception cref="ArgumentException">Directory was not found or is not accessible.</exception>
        public Parser(DirectoryInfo hostConfigs)
        {
            _hostConfigs = hostConfigs ?? throw new ArgumentNullException(nameof(hostConfigs));

            if (!_hostConfigs.Exists)
            {
                throw new ArgumentException("Directory not found.", nameof(hostConfigs));
            }
        }

        public IEnumerable<(string Name, IEnumerable<FileInfo> AdditionalFiles)> GetHostConfigs()
        {
            foreach(var dir in _hostConfigs.EnumerateDirectories())
            {
                if (IsDirViable(dir, out var files))
                {
                    yield return (dir.Name, files);
                }
            }
        }

        public (HostConfig HostConfig, IEnumerable<(string Version, Lazy<Stream> Stream)> ApplicationVersions) Parse(Action<string> writeLine, ZipArchive hostConfigBlobs, params string[] selected)
        {
            if ((hostConfigBlobs ?? throw new ArgumentNullException(nameof(hostConfigBlobs))).Mode != ZipArchiveMode.Create)
            {
                throw new ArgumentException("Zip archive must be in create mode.", nameof(hostConfigBlobs));
            }

            var filter = !(selected is null || 0 == selected.Length);
            var lazySelected = new Lazy<List<string>>(() => selected?.Select(s => s.ToUpperInvariant()).ToList() ?? new List<string>());
            var Filter = new Func<DirectoryInfo, bool>(dir => !filter || lazySelected.Value.Contains(dir.Name.ToUpperInvariant()));

            if (filter)
            {
                var dirList = _hostConfigs.EnumerateDirectories().Select(d => d.Name.ToUpperInvariant()).ToList(); //selected?.ToList() ?? new List<string>();
                foreach (var dir in selected ?? Array.Empty<string>())
                {
                    if (!dirList.Contains(dir.ToUpperInvariant()))
                    {
                        throw new ArgumentException($"HostConfig '{dir}' was not found in '{_hostConfigs.FullName}'.", nameof(selected));
                    }
                }
            }

            writeLine("Collecting CoA host configurations");
            var storedVersions = ApplicationVersions.Empty;
            var hashes = PackageHashes.Empty;
            var configs = HostConfigurations.Empty;
            var startTasks = new Dictionary<string, FileInfo>();
            var versions = Enumerable.Empty<(string, Lazy<Stream>)>();

            foreach (var (hostConfig, directory) in _hostConfigs.EnumerateDirectories().Where(Filter).Select(ParseHostConfigDir).Select(d => (d.FirstOrDefault()?.Directory?.Name, d.ToList())))
            {
                if (hostConfig is null) continue;
                writeLine($"Processing {hostConfig}");

                var configFile = directory.FirstOrDefault(f => "config.json".Equals(f.Name, StringComparison.OrdinalIgnoreCase));
                var startTask = directory.FirstOrDefault(f => Constants.StartTask.Equals(f.Name, StringComparison.OrdinalIgnoreCase));
                var nonZipFiles = Enumerable.Empty<FileInfo>();
                nonZipFiles = configFile is null ? nonZipFiles : nonZipFiles.Append(configFile);
                nonZipFiles = startTask is null ? nonZipFiles : nonZipFiles.Append(startTask);
                var zipFiles = directory.Except(nonZipFiles).ToList();

                if (configFile is not null)
                {
                    var configuration = ParseConfig(configFile);
                    MungeVirtualMachineSizes(configuration.VmSizes);
                    configs.Add(hostConfig, configuration);

                    if (startTask is not null)
                    {
                        startTasks.Add(configuration.StartTask?.StartTaskHash ?? throw new InvalidOperationException(), startTask);
                    }
                }

                foreach (var (name, hash, package) in
                    zipFiles.Select(GetApplicationName).Zip(
                    zipFiles.Select(GetFileHash),
                    zipFiles.Select(ExtractPackage)))
                {
                    versions = versions.Append((hash, new(package.FileInfo.OpenRead)));
                    hashes.Add(name, hash);

                    if (storedVersions.TryGetValue(name, out var keyMetadata))
                    {
                        keyMetadata.Packages ??= Packages.Empty;
                        keyMetadata.Packages.Add(hash, package.Package);
                    }
                    else
                    {
                        var packages = Packages.Empty;
                        packages.Add(hash, package.Package ?? new());
                        storedVersions.Add(name, new() { ApplicationId = BatchAppFromHostConfigName(name), Packages = packages });
                    }
                }
            }

            var config = new HostConfig
            {
                ApplicationVersions = storedVersions,
                PackageHashes = hashes,
                HostConfigurations = configs
            };

            foreach (var startTask in startTasks)
            {
                using var entry = hostConfigBlobs.CreateEntry(startTask.Key).Open();
                using var file = startTask.Value.OpenRead();
                file.CopyTo(entry);
            }

            return (config, versions);
        }

        private static void MungeVirtualMachineSizes(VirtualMachineSizes vmSizes)
        {
            if (vmSizes is not null)
            {
                foreach (var size in vmSizes)
                {
                    if (size.Container is not null)
                    {
                        size.Container = Utilities.NormalizeContainerImageName(size.Container).AbsoluteUri;
                    }
                }
            }
        }

        private static bool IsDirViable(DirectoryInfo dir, out IEnumerable<FileInfo> additionalFiles)
        {
            var extraFiles = Enumerable.Empty<FileInfo>();
            var viable = false;
            var fileList = Constants.HostConfigFiles().Select(s => s.ToUpperInvariant()).ToList();

            foreach (var file in dir.EnumerateFiles())
            {
                if (fileList.Contains(file.Name.ToUpperInvariant()))
                {
                    viable = true;
                }
                else
                {
                    extraFiles = extraFiles.Append(file);
                }
            }

            additionalFiles = viable ? extraFiles : Enumerable.Empty<FileInfo>();
            return viable;
        }

        private static string GetApplicationName(FileInfo file)
            => $"{file.Directory?.Name}_{Path.GetFileNameWithoutExtension(file.Name)}";

        private static (FileInfo FileInfo, Package Package) ExtractPackage(FileInfo file)
        {
            using var zip = new ZipArchive(file.OpenRead());
            return (file, new()
            {
                ContainsTaskScript = zip.Entries
                    .Any(e => Constants.StartTask.Equals(e.Name, StringComparison.Ordinal)),
                DockerLoadables = zip.Entries
                    .Where(e => e.Name.EndsWith(".tar", StringComparison.OrdinalIgnoreCase))
                    .Select(e => Path.GetFileNameWithoutExtension(e.Name))
                    .ToArray()
            });
        }

        private static IEnumerable<FileInfo> ParseHostConfigDir(DirectoryInfo dir)
        {
            var fileList = Constants.HostConfigFiles().Select(s => s.ToUpperInvariant()).ToList();
            foreach (var file in dir.EnumerateFiles())
            {
                if (fileList.Contains(file.Name.ToUpperInvariant()))
                {
                    yield return file;
                }
            }
        }

        private static string BatchAppFromHostConfigName(string host)
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

        private static HostConfiguration ParseConfig(FileInfo configFile)
            => ConvertUserConfig(ParseUserConfig(configFile.OpenText()), configFile.Directory?.EnumerateFiles(Constants.StartTask).Select(GetFileHash).FirstOrDefault());

        private static UserHostConfig ParseUserConfig(TextReader textReader)
            => ReadJson<UserHostConfig>(textReader, () => throw new ArgumentException("File is not a HostConfig configuration.", nameof(textReader)));

        private static HostConfiguration ConvertUserConfig(UserHostConfig? userConfig, string? startTaskHash)
            => new() { BatchImage = userConfig?.BatchImage ?? new(), VmSizes = userConfig?.VirtualMachineSizes ?? new(), DockerRun = userConfig?.DockerRun ?? new(), StartTask = ConvertStartTask(userConfig?.StartTask, startTaskHash) };

        private static StartTask ConvertStartTask(UserHostConfig.UserStartTask? userStartTask, string? startTaskHash)
            => new() { ResourceFiles = userStartTask?.ResourceFiles ?? Array.Empty<ResourceFile>(), StartTaskHash = startTaskHash };

        private static string GetFileHash(FileInfo file)
            => Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(file.FullName)));
    }
}
