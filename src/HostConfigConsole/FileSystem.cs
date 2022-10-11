// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using static HostConfigConsole.Builder;

namespace HostConfigConsole
{
    internal static class FileSystem
    {
        internal static IDirectory GetDirectory(DirectoryInfo info)
            => new Directory(info);

        private class Directory : FileSystemInfo, IDirectory
        {
            private readonly DirectoryInfo _info;

            internal Directory(DirectoryInfo info)
            {
                ArgumentNullException.ThrowIfNull(info);
                _info = info;
            }

            public override bool Exists
                => _info.Exists;

            public override string Name
                => _info.Name;

            public DirectoryInfo? DirectoryInfo
                => _info;

            public override void Delete()
                => _info.Delete();

            public IEnumerable<IDirectory> EnumerateDirectories()
                => _info.EnumerateDirectories().Select(i => new Directory(i));

            public IEnumerable<IFile> EnumerateFiles()
                => _info.EnumerateFiles().Select(i => new File(i));

            public IEnumerable<IFile> EnumerateFiles(string searchPattern)
                => _info.EnumerateFiles(searchPattern).Select(i => new File(i));
        }

        private class File : FileSystemInfo, IFile
        {
            private readonly FileInfo _info;

            public File(FileInfo info)
            {
                ArgumentNullException.ThrowIfNull(info);
                _info = info;
            }

            public override bool Exists
                => _info.Exists;

            public override string Name
                => _info.Name;

            public IDirectory? Directory
                => _info.Directory is null ? default : new Directory(_info.Directory);

            public FileInfo? FileInfo
                => _info;

            public override void Delete()
                => _info.Delete();

            public Stream OpenRead()
                => _info.OpenRead();

            public byte[] ReadAllBytes()
                => System.IO.File.ReadAllBytes(_info.FullName);
        }
    }
}
