// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using static HostConfigConsole.Builder;

namespace HostConfigConsole
{
    internal static class FileSystem
    {
        internal static IDirectory GetDirectory(DirectoryInfo info)
            => new Directory(info);

        private class Directory : IDirectory
        {
            private readonly DirectoryInfo _info;

            internal Directory(DirectoryInfo info)
            {
                ArgumentNullException.ThrowIfNull(info);
                _info = info;
            }

            public bool Exists
                => _info.Exists;

            public string Name
                => _info.Name;

            public void Delete()
                => _info.Delete();

            public IEnumerable<IDirectory> EnumerateDirectories()
                => _info.EnumerateDirectories().Select(i => new Directory(i));

            public IEnumerable<IFile> EnumerateFiles()
                => _info.EnumerateFiles().Select(i => new File(i));

            public IFile? GetFile(string name)
                => _info.EnumerateFiles(name).Select(i => new File(i)).FirstOrDefault();
        }

        private class File : IFile
        {
            private readonly FileInfo _info;

            public File(FileInfo info)
            {
                ArgumentNullException.ThrowIfNull(info);
                _info = info;
            }

            public bool Exists
                => _info.Exists;

            public string Name
                => _info.Name;

            public IDirectory? Directory
                => _info.Directory is null ? default : new Directory(_info.Directory);

            public void Delete()
                => _info.Delete();

            public Stream OpenRead()
                => _info.OpenRead();

            public byte[] ReadAllBytes()
                => System.IO.File.ReadAllBytes(_info.FullName);
        }
    }
}
