// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.ObjectModel;
using System.IO.Compression;
using System.IO.Packaging;
using System.Xml.Linq;

namespace HostConfigConsole
{
    internal sealed class ConfigurationPackageReadable : ConfigurationPackage
    {
        private ConfigurationPackageReadable(Stream stream, Package package)
            : base(stream, package) { }

        public static ConfigurationPackageReadable Open(FileInfo file)
        {
            ArgumentNullException.ThrowIfNull(file);

            Stream? stream = default;
            Package? package = default;

            try
            {
                stream = file.OpenRead();
                package = Package.Open(stream, FileMode.Open, FileAccess.Read);
                var result = new ConfigurationPackageReadable(stream, package);
                stream = default;
                package = default;
                return result;
            }
            finally
            {
                package?.Close();
                stream?.Dispose();
            }
        }

        public Common.HostConfigs.HostConfig? GetHostConfig()
        {
            try
            {
                var part = _package.GetPart(_hostConfigsUri);

                using (var stream = part.GetStream())
                {
                    using (var reader = Builder.OpenConfiguration(stream))
                    {
#pragma warning disable CS8603 // Possible null reference return.
                        return Builder.ReadJson<Common.HostConfigs.HostConfig>(reader, () => default);
#pragma warning restore CS8603 // Possible null reference return.
                    }
                }
            }
            catch (IOException)
            {
                return default;
            }
            catch (FileFormatException)
            {
                return default;
            }
            catch (InvalidOperationException) // part not found
            {
                return default;
            }
        }

        public IReadOnlyDictionary<string, Lazy<Stream>> GetResources()
        {
            var result = Enumerable.Empty<(string, Lazy<Stream>)>();

            foreach (var uri in _package.GetRelationshipsByType(ResourceRel).Select(r => r.TargetUri))
            {
                try
                {
                    var part = _package.GetPart(uri);
                    result = result.Append((Path.GetFileName(uri.OriginalString) ?? string.Empty, new(() => part.GetStream())));
                }
                catch (IOException)
                {
                    continue;
                }
                catch (FileFormatException)
                {
                    continue;
                }
                catch (InvalidOperationException)
                {
                    continue;
                }
                catch (ArgumentException)
                {
                    continue;
                }
            }

            return new ReadOnlyDictionary<string, Lazy<Stream>>(result.ToDictionary(t => t.Item1, t => t.Item2));
        }

        public IReadOnlyDictionary<string, Lazy<Stream>> GetApplications()
        {
            var result = Enumerable.Empty<(string, Lazy<Stream>)>();

            foreach (var uri in _package.GetRelationshipsByType(ApplicationRel).Select(r => r.TargetUri))
            {
                try
                {
                    var part = _package.GetPart(uri);
                    result = result.Append((Path.GetFileName(uri.OriginalString) ?? string.Empty, new(() => part.GetStream())));
                }
                catch (IOException)
                {
                    continue;
                }
                catch (FileFormatException)
                {
                    continue;
                }
                catch (InvalidOperationException)
                {
                    continue;
                }
                catch (ArgumentException)
                {
                    continue;
                }
            }

            return new ReadOnlyDictionary<string, Lazy<Stream>>(result.ToDictionary(t => t.Item1, t => t.Item2));
        }
    }

    internal sealed class ConfigurationPackageWritable : ConfigurationPackage
    {
        private ConfigurationPackageWritable(Stream stream, Package package)
            : base(stream, package) { }

        public static ConfigurationPackageWritable Create(FileInfo file)
        {
            ArgumentNullException.ThrowIfNull(file);
            Stream? stream = default;
            Package? package = default;

            try
            {
                stream = file.OpenWrite();
                package = Package.Open(stream, FileMode.Create, FileAccess.Write);
                var result = new ConfigurationPackageWritable(stream, package);
                stream = default;
                package = default;
                return result;
            }
            finally
            {
                package?.Close();
                stream?.Dispose();
            }
        }

        public void SetHostConfig(Common.HostConfigs.HostConfig hostConfig)
        {
            ArgumentNullException.ThrowIfNull(hostConfig);
            var part = _package.CreatePart(_hostConfigsUri, System.Net.Mime.MediaTypeNames.Application.Json, CompressionOption.Maximum);

            using (var stream = part.GetStream())
            {
                using (var writer = new StreamWriter(stream))
                {
                    writer.NewLine = "\n";
                    writer.Write(Builder.WriteJson(hostConfig));
                    writer.Flush();
                    writer.Close();
                }
            }

            _ = _package.CreateRelationship(part.Uri, TargetMode.Internal, HostConfigsRel);
        }

        public void SetResource(string name, Stream fileStream)
        {
            ArgumentNullException.ThrowIfNull(name);
            ArgumentNullException.ThrowIfNull(fileStream);
            PackagePart part;

            using (var stream = fileStream)
            {
                part = _package.CreatePart(ResourceUri(name), System.Net.Mime.MediaTypeNames.Application.Octet/*, CompressionOption.SuperFast*/);
                CopyStream(stream, part.GetStream());
            }

            _ = _package.CreateRelationship(part.Uri, TargetMode.Internal, ResourceRel);
        }

        public void SetApplication(string name, string version, Stream zipStream)
        {
            ArgumentNullException.ThrowIfNull(name);
            ArgumentNullException.ThrowIfNull(version);
            ArgumentNullException.ThrowIfNull(zipStream);
            PackagePart part;

            using (var stream = zipStream)
            {
                part = _package.CreatePart(ApplicationUri(name, version), System.Net.Mime.MediaTypeNames.Application.Zip/*, CompressionOption.SuperFast*/);
                CopyStream(stream, part.GetStream());
            }

            _ = _package.CreateRelationship(part.Uri, TargetMode.Internal, ApplicationRel);
        }
    }

    internal abstract class ConfigurationPackage : IDisposable
    {
        protected const string HostConfigsRel = "host-configurations";
        protected const string ResourceRel = "resource-blob";
        protected const string ApplicationRel = "application-version";

        protected static readonly Uri _hostConfigsUri = new("/hostConfigs", UriKind.Relative);

        protected static Uri ResourceUri(string name)
            => new($"/resources/{name}", UriKind.Relative);

        protected static Uri ApplicationUri(string name, string version)
            => new($"/applications/{name}/{version}", UriKind.Relative);

        protected readonly Stream _stream;
        protected readonly Package _package;

        protected ConfigurationPackage(Stream stream, Package package)
        {
            ArgumentNullException.ThrowIfNull(stream);
            ArgumentNullException.ThrowIfNull(package);
            _stream = stream;
            _package = package;
        }

        protected static void CopyStream(Stream source, Stream target)
        {
            using var src = source;
            using var tgt = target;
            src.CopyTo(tgt);
        }

        public void Dispose()
        {
            _package?.Close();
            _stream?.Dispose();
        }
    }
}
