// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common.HostConfigs;

namespace HostConfigConsole
{
    internal class CromwellOnAzure
    {
        private readonly string dirName;

        public CromwellOnAzure(string dirName)
        {
            ArgumentNullException.ThrowIfNull(dirName);
            if (string.IsNullOrEmpty(dirName)) throw new ArgumentException(null, nameof(dirName));
            this.dirName = dirName;
        }

        public HostConfig? GetHostConfig()
        {
            var oldConfig = new FileInfo(Path.Combine(dirName, "StorageAccount", "host-configurations.json"));
#pragma warning disable CS8604 // Possible null reference argument.
            return  Updater.ReadJson(Updater.OpenConfiguration(oldConfig.Exists ? oldConfig.OpenRead() : default), () => new HostConfig());
#pragma warning restore CS8604 // Possible null reference argument.
        }

        public void WriteHostConfig(HostConfig config)
        {
            File.WriteAllText(Path.Combine(dirName, "StorageAccount", "host-configurations.json"), Updater.WriteJson(config));
        }

        public void AddApplications(IEnumerable<(string Name, string Version, Func<Stream> Open)> packages)
            => packages.ForEach(app =>
            {
                Console.WriteLine($"Adding {app.Name}:{app.Version}");
                var file = new FileInfo(Path.Combine(dirName, "StorageAccount", "Apps", app.Name, app.Version));
                file.Directory?.Create();
                using var read = app.Open();
                using var write = file.OpenWrite();
                read.CopyTo(write);
            });

        public void RemoveApplications(IEnumerable<(string Name, string Version)> packages)
            => packages.ForEach(app =>
            {
                Console.WriteLine($"Removing {app.Name}:{app.Version}");
                new FileInfo(Path.Combine(dirName, "StorageAccount", "Apps", app.Name, app.Version)).Delete();
            });

        public void AddHostConfigBlobs(IEnumerable<(string Hash, Func<Stream> Open)> blobs)
            => blobs.ForEach(start =>
            {
                Console.WriteLine($"Adding {start.Hash}");
                var file = new FileInfo(Path.Combine(dirName, "StorageAccount", "Blobs", start.Hash));
                file.Directory?.Create();
                using var read = start.Open();
                using var write = file.OpenWrite();
                read.CopyTo(write);
            });

        public void RemoveHostConfigBlobs(IEnumerable<string> blobs)
            => blobs.ForEach(hash =>
            {
                Console.WriteLine($"Removing {hash}");
                new FileInfo(Path.Combine(dirName, "StorageAccount", "Blobs", hash)).Delete();
            });
    }
}
