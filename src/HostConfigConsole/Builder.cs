// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Drawing;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using Common.HostConfigs;
using HostConfigConsole.HostConfigs;
using LazyCache;
using Valleysoft.DockerRegistryClient;

namespace HostConfigConsole
{
    /// <summary>
    /// Reads a file system and generates <see cref="HostConfigurations"/>
    /// </summary>
    public sealed class Builder : Base
    {
        private readonly IDirectory _hostConfigs;

        /// <summary>
        /// Creates a <see cref="Builder"/>
        /// </summary>
        /// <param name="hostConfigs">Directory containing the host configuration data.</param>
        /// <exception cref="ArgumentException">Directory was not found or is not accessible.</exception>
        public Builder(IDirectory hostConfigs)
        {
            _hostConfigs = hostConfigs ?? throw new ArgumentNullException(nameof(hostConfigs));

            if (!_hostConfigs.Exists)
            {
                throw new ArgumentException("Directory not found.", nameof(hostConfigs));
            }
        }

        public async IAsyncEnumerable<(string Name, IEnumerable<IFile> AdditionalFiles)> GetHostConfigs()
        {
            await foreach (var dir in _hostConfigs.EnumerateDirectories())
            {
                var (viable, files) = await GetDirMetadata(dir);
                if (viable)
                {
                    yield return (dir.Name, files);
                }
            }
        }

        public async ValueTask<(HostConfig HostConfig, IEnumerable<(string Name, AsyncLazy<Stream> Stream)>, IEnumerable<(string Version, AsyncLazy<Stream> Stream)> ApplicationVersions)> Build(Action<string> writeLine, params string[] selected)
        {
            ArgumentNullException.ThrowIfNull(writeLine);
            ArgumentNullException.ThrowIfNull(selected);

            var filter = !(selected is null || 0 == selected.Length);
            var lazySelected = new Lazy<List<string>>(() => selected?.Select(s => s.ToUpperInvariant()).ToList() ?? new List<string>());
            var Filter = new Func<IDirectory, bool>(dir => !filter || lazySelected.Value.Contains(dir.Name.ToUpperInvariant()));

            if (filter)
            {
                var dirList = await _hostConfigs.EnumerateDirectories().Select(d => d.Name.ToUpperInvariant()).ToListAsync();
                (selected ?? Array.Empty<string>())
                    .Where(dir => !dirList.Contains(dir.ToUpperInvariant()))
                    .ForEach(dir => throw new ArgumentException($"HostConfig '{dir}' was not found in '{_hostConfigs.Name}'.", nameof(selected)));
            }

            writeLine("Collecting CoA host configurations");
            var storedVersions = ApplicationVersions.Empty;
            var hashes = PackageHashes.Empty;
            var configs = HostConfigurations.Empty;
            var startTasks = new Dictionary<string, IFile>();
            var versions = Enumerable.Empty<(string, AsyncLazy<Stream>)>();

            await foreach (var (hostConfig, hcFiles) in _hostConfigs.EnumerateDirectories().Where(Filter).Select(ParseHostConfigDir).SelectAwait(hcFiles => hcFiles.ToListAsync()).Select(hcFiles => (hcFiles.FirstOrDefault()?.Directory?.Name, hcFiles)))
            {
                if (hostConfig is null) continue;
                writeLine($"Processing {hostConfig}");

                var configFile = hcFiles.FirstOrDefault(f => "config.json".Equals(f.Name, StringComparison.OrdinalIgnoreCase));
                var startTask = hcFiles.FirstOrDefault(f => Constants.StartTask.Equals(f.Name, StringComparison.OrdinalIgnoreCase));
                var nonZipFiles = Enumerable.Empty<IFile>();
                nonZipFiles = configFile is null ? nonZipFiles : nonZipFiles.Append(configFile);
                nonZipFiles = startTask is null ? nonZipFiles : nonZipFiles.Append(startTask);
                var zipFiles = hcFiles.Except(nonZipFiles);

                if (configFile is not null)
                {
                    var configuration = await ParseConfig(configFile);
                    await MungeVirtualMachineSizes(configuration.VmSizes);
                    configs.Add(hostConfig, configuration);

                    if (startTask is not null)
                    {
                        startTasks.Add(configuration.StartTask?.StartTaskHash ?? throw new InvalidOperationException(), startTask);
                    }

                    await foreach (var (name, hash, package) in zipFiles.ToAsyncEnumerable()
                        .SelectAwait(async f => (GetApplicationName(f), await GetFileHash(f), await ExtractPackage(f))))
                    {
                        if (hash is null) continue;
                        versions = versions.Append((hash, new(async () => await package.FileInfo.OpenRead())));
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
                else if (startTask is not null || zipFiles.Any())
                {
                    var list = zipFiles;

                    if (startTask is not null)
                    {
                        list = list.Prepend(startTask);
                    }

                    writeLine($"Warning: files {string.Join(", ", list.Select(s => $"'{s.Name}'"))} are ignored because '{Constants.Config}' is missing.");
                }
            }

            var config = new HostConfig
            {
                ApplicationVersions = storedVersions,
                PackageHashes = hashes,
                HostConfigurations = configs
            };

            return (config, startTasks.Select<KeyValuePair<string, IFile>, (string Name, AsyncLazy<Stream> Stream)>(p => (p.Key, new(async () => await p.Value.OpenRead()))), versions);
        }

        private static async ValueTask MungeVirtualMachineSizes(VirtualMachineSizes vmSizes)
        {
            var results = await Task.WhenAll(vmSizes?.Where(size => size.Container is not null).Select(async size =>
                Enumerable.Empty<VirtualMachineSize>().Append(CopyWithNormalizedContainer(size, size.Container)).Append(CopyWithNormalizedContainer(size, await GetDigest(size))))
                .ToArray() ?? Array.Empty<Task<IEnumerable<VirtualMachineSize>>>());

            vmSizes?.Clear();
            vmSizes?.AddRange(results.SelectMany(l => l));

            static VirtualMachineSize CopyWithNormalizedContainer(VirtualMachineSize size, string? container)
                => new() { Container = Common.Utilities.NormalizeContainerImageName(container).AbsoluteUri, FamilyName = size.FamilyName, MinVmSize = size.MinVmSize, VmSize = size.VmSize };

            static async ValueTask<string> GetDigest(VirtualMachineSize size)
            {
                var uri = Common.Utilities.NormalizeContainerImageName(size.Container);
                var repository = uri.AbsolutePath.Split(':');
                var name = string.Join(':', repository.Take(repository.Length - 1)).TrimStart('/');
                var tag = repository.Skip(1).LastOrDefault() ?? "latest";
                var manifest = await new DockerRegistryClient(uri.Host).Manifests.GetAsync(name, tag);

                return uri.Scheme.Equals("docker", StringComparison.Ordinal)
                    ? $"{uri.Host}{(uri.IsDefaultPort ? string.Empty : ":" + uri.Port.ToString(CultureInfo.InvariantCulture))}/{name}@{manifest.DockerContentDigest}"
                    : $"{uri.GetLeftPart(UriPartial.Path)}@{manifest.DockerContentDigest}";
            }
        }

        private static async ValueTask<(bool Viable, IEnumerable<IFile> AdditionalFiles)> GetDirMetadata(IDirectory dir)
        {
            var extraFiles = Enumerable.Empty<IFile>();
            var viable = false;
            var fileList = Constants.HostConfigFiles().Select(s => s.ToUpperInvariant()).ToList();

            await foreach (var file in dir.EnumerateFiles())
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

            return (viable, viable ? extraFiles : Enumerable.Empty<IFile>());
        }

        private static string GetApplicationName(IFile file)
            => $"{file.Directory?.Name}_{Path.GetFileNameWithoutExtension(file.Name)}";

        private static async ValueTask<(IFile FileInfo, Package Package)> ExtractPackage(IFile file)
        {
            using var zip = new ZipArchive(await file.OpenRead());
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

        private static async IAsyncEnumerable<IFile> ParseHostConfigDir(IDirectory dir)
        {
            var fileList = Constants.HostConfigFiles().Select(s => s.ToUpperInvariant()).ToList();
            await foreach (var file in dir.EnumerateFiles())
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

        private static async ValueTask<HostConfiguration> ParseConfig(IFile configFile)
            => ConvertUserConfig(ParseUserConfig(OpenConfiguration(await configFile.OpenRead())), await GetFileHash(await (configFile.Directory?.GetFile(Constants.StartTask) ?? ValueTask.FromResult<IFile?>(null))));

        private static UserHostConfig ParseUserConfig(TextReader? textReader)
            => ReadJson<UserHostConfig>(textReader, () => throw new ArgumentException("File is not a HostConfig configuration.", nameof(textReader)));

        private static HostConfiguration ConvertUserConfig(UserHostConfig? userConfig, string? startTaskHash)
            => new() { BatchImage = userConfig?.BatchImage ?? new(), VmSizes = userConfig?.VirtualMachineSizes ?? new(), DockerRun = userConfig?.DockerRun ?? new(), StartTask = ConvertStartTask(userConfig?.StartTask, startTaskHash) };

        private static StartTask ConvertStartTask(UserStartTask? userStartTask, string? startTaskHash)
            => new() { ResourceFiles = userStartTask?.ResourceFiles ?? Array.Empty<ResourceFile>(), StartTaskHash = startTaskHash };

        private static async ValueTask<string?> GetFileHash(IFile? file)
            => file is null ? default : Convert.ToHexString(SHA256.HashData(await file.ReadAllBytes()));

        public interface IDirectory
        {
            string Name { get; }
            bool Exists { get; }

            IAsyncEnumerable<IDirectory> EnumerateDirectories();
            IAsyncEnumerable<IFile> EnumerateFiles();
            ValueTask<IFile?> GetFile(string name);
        }

        public interface IFile
        {
            string Name { get; }
            IDirectory? Directory { get; }

            ValueTask<Stream> OpenRead();
            ValueTask<byte[]> ReadAllBytes();
        }
    }
}
