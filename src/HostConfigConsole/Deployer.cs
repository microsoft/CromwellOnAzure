// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.IO.Compression;
using System.Linq;
using Common.HostConfigs;

namespace HostConfigConsole
{
    /// <summary>
    /// Compares two <see cref="HostConfigurations"/> and updates deployments
    /// </summary>
    public sealed class Deployer : Base
    {
        private readonly ReadOnlyHostConfig _existing;
        private readonly ReadOnlyHostConfig _target;

        public IEnumerable<(string Name, string Version, Func<Stream> Open)> GetApplicationsToAdd()
            => _target.ApplicationVersions.Except(_existing.ApplicationVersions, ApplicationVersionsComparer.Instance)
                .Select<KeyValuePair<string, ApplicationVersion>, (string Name, string Version, Func<Stream> Open)>(p =>
                    (p.Value.ApplicationId ?? throw new InvalidOperationException("ApplicationName is missing"),
                    _target.PackageHashes[p.Key],
                    () => _target.PackageVersions.Value[p.Key].Value));

        public IEnumerable<(string Name, string Version)> GetApplicationsToRemove()
            => _existing.ApplicationVersions.Except(_target.ApplicationVersions, ApplicationVersionsComparer.Instance)
                .Select(p =>
                    (p.Value.ApplicationId ?? throw new InvalidOperationException("ApplicationName is missing"),
                    _existing.PackageHashes[p.Key]));

        public IEnumerable<(string Hash, Func<Stream> Open)> GetStartTasksToAdd()
            => _target.StartTaskHashes.Except(_existing.StartTaskHashes)
                .Select<string, (string Hash, Func<Stream> Open)>(h =>
                    (h,
                    () => _target.HostConfigBlobs.Value[h].Value));

        public IEnumerable<string> GetStartTasksToRemove()
            => _existing.StartTaskHashes.Except(_target.StartTaskHashes);

        public Deployer(HostConfig target, IReadOnlyDictionary<string, Lazy<Stream>> resources, IReadOnlyDictionary<string, Lazy<Stream>> packageVersions, HostConfig? existing = default)
        {
            ArgumentNullException.ThrowIfNull(target);
            ArgumentNullException.ThrowIfNull(resources);
            ArgumentNullException.ThrowIfNull(packageVersions);

            _existing = new(existing ?? new());
            _target = new(target, resources, packageVersions);
        }

        private sealed class ReadOnlyHostConfig
        {
            public ReadOnlyDictionary<string, ApplicationVersion> ApplicationVersions { get; }
            public ReadOnlyDictionary<string, string> PackageHashes { get; }
            public ReadOnlyDictionary<string, HostConfiguration> HostConfigurations { get; }
            public ReadOnlyCollection<string> StartTaskHashes { get; }

            public Lazy<IReadOnlyDictionary<string, Lazy<Stream>>> HostConfigBlobs { get; }
            public Lazy<IReadOnlyDictionary<string, Lazy<Stream>>> PackageVersions { get; }

            public ReadOnlyHostConfig(HostConfig hostConfig, IReadOnlyDictionary<string, Lazy<Stream>>? hostConfigBlobs = default, IReadOnlyDictionary<string, Lazy<Stream>>? packageVersions = default)
            {
                ArgumentNullException.ThrowIfNull(hostConfig, nameof(hostConfig));
                HostConfigBlobs = hostConfigBlobs is null
                    ? new(() => throw new InvalidOperationException("File information is required for target hostconfigs", new ArgumentNullException(nameof(hostConfigBlobs))))
                    : new(() => hostConfigBlobs);
                PackageVersions = packageVersions is null
                    ? new(() => throw new InvalidOperationException("File information is required for target hostconfigs", new ArgumentNullException(nameof(packageVersions))))
                    : new(() => packageVersions);
                ApplicationVersions = new(hostConfig.ApplicationVersions ?? Common.HostConfigs.ApplicationVersions.Empty);
                PackageHashes = new(hostConfig.PackageHashes ?? Common.HostConfigs.PackageHashes.Empty);
                HostConfigurations = new(hostConfig.HostConfigurations ?? Common.HostConfigs.HostConfigurations.Empty);
                StartTaskHashes = new(HostConfigurations.Select(p => p.Value.StartTask?.StartTaskHash).Where(h => h is not null).Cast<string>().ToList());
            }
        }

        private sealed class ApplicationVersionsComparer : IEqualityComparer<KeyValuePair<string, ApplicationVersion>>
        {
            internal static readonly ApplicationVersionsComparer Instance = new();

            public bool Equals(KeyValuePair<string, ApplicationVersion> x, KeyValuePair<string, ApplicationVersion> y)
            {
                throw new NotImplementedException();
            }

            public int GetHashCode([DisallowNull] KeyValuePair<string, ApplicationVersion> obj)
            {
                ArgumentNullException.ThrowIfNull(obj);
                throw new NotImplementedException();
            }
        }
    }
}
