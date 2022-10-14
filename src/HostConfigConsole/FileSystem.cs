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

            public IAsyncEnumerable<IDirectory> EnumerateDirectories()
                => _info.EnumerateDirectories().Select(i => new Directory(i)).ToAsyncEnumerable();

            public IAsyncEnumerable<IFile> EnumerateFiles()
                => _info.EnumerateFiles().Select(i => new File(i)).ToAsyncEnumerable();

            public ValueTask<IFile?> GetFile(string name)
                => ValueTask.FromResult<IFile?>(_info.EnumerateFiles(name).Select(i => new File(i)).FirstOrDefault());
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

            public ValueTask<Stream> OpenRead()
                => ValueTask.FromResult<Stream>(_info.OpenRead());

            public async ValueTask<byte[]> ReadAllBytes()
                => await System.IO.File.ReadAllBytesAsync(_info.FullName);
        }
    }
}
