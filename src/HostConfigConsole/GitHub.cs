// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.ObjectModel;
//using System.Threading.Tasks;
using Octokit;
using static HostConfigConsole.Builder;

namespace HostConfigConsole
{
    internal static class GitHub
    {
        internal static async ValueTask<IDirectory> GetDirectory(Uri uri)
        {
            ArgumentNullException.ThrowIfNull(uri);

            if (!uri.IsAbsoluteUri || uri.Scheme != "https" || uri.Host != "github.com" || uri.Segments.Length < 3)
            {
                throw new ArgumentException("Url is not a github repository", nameof(uri));
            }

            // TODO: authentication
            GitHubClient _client = new(new ProductHeaderValue("CromwellOnAzure-HostConfigConsole"));
            var _owner = uri.Segments[1].Trim('/');
            var _name = uri.Segments[2].Trim('/');
            string? _ref = default;
            var _path = "/";

            if (uri.Segments.Length > 4 && uri.Segments[3] == "tree/")
            {
                foreach (var branch in await _client.Repository.Branch.GetAll(_owner, _name))
                {
                    var parts = branch.Name.Split('/');
                    if (parts.Length + 4 <= uri.Segments.Length && parts.SequenceEqual(uri.Segments.Skip(4).Take(parts.Length).Select(s => s.TrimEnd('/'))))
                    {
                        _ref = branch.Name;
                        _path = string.Join(string.Empty, uri.Segments.Skip(4 + parts.Length));
                        if (string.IsNullOrWhiteSpace(_path)) _path = "/";
                        break;
                    }
                }
            }

            return new Directory(default, _client, _owner, _name, _path, _ref);
        }

        private class Directory : Base, IDirectory
        {
            private bool _enumerated = false;
            private IReadOnlyList<Lazy<IDirectory>> _directories = new ReadOnlyCollection<Lazy<IDirectory>>(Enumerable.Empty< Lazy<IDirectory>>().ToList());
            private IReadOnlyList<Lazy<IFile>> _files = new ReadOnlyCollection<Lazy<IFile>>(Enumerable.Empty< Lazy<IFile>>().ToList());

            internal Directory(IDirectory? parent, GitHubClient client, string owner, string name, string path, string? @ref)
                : base(parent, client, owner, name, path, @ref) { }

            public string FullName
                => _path;

            public bool Exists
                => true;

            public async IAsyncEnumerable<IDirectory> EnumerateDirectories()
            {
                await EnsureContents();
                foreach (var item in _directories.Select(i => i.Value))
                {
                    yield return item;
                }
            }

            public async IAsyncEnumerable<IFile> EnumerateFiles()
            {
                await EnsureContents();
                foreach (var item in _files.Select(i => i.Value))
                {
                    yield return item;
                }
            }

            public async ValueTask<IFile?> GetFile(string name)
            {
                await EnsureContents();
                return _files.Select(i => i.Value).FirstOrDefault(i => i.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
            }

            private async ValueTask EnsureContents()
            {
                if (!_enumerated)
                {
                    await EnumerateContents();
                }
            }

            private async ValueTask EnumerateContents()
            {
                var dirs = Enumerable.Empty<Lazy<IDirectory>>();
                var files = Enumerable.Empty<Lazy<IFile>>();

                foreach (var item in await (_ref is null ? _client.Repository.Content.GetAllContents(_owner, _name, _path) : _client.Repository.Content.GetAllContentsByRef(_owner, _name, _path, _ref)))
                {
                    if (item.Type.TryParse(out var type))
                    {
                        switch (type)
                        {
                            case ContentType.File:
                                files = files.Append(new(() => new File(this, _client, _owner, _name, $"{_path.TrimEnd('/')}/{item.Name}", _ref)));
                                break;
                            case ContentType.Dir:
                                dirs = dirs.Append(new(() => new Directory(this, _client, _owner, _name, $"{_path.TrimEnd('/')}/{item.Name}", _ref)));
                                break;
                            case ContentType.Symlink:
                                //
                                break;
                            case ContentType.Submodule:
                                //
                                break;

                        }
                    }
                }

                _directories = new ReadOnlyCollection<Lazy<IDirectory>>(dirs.ToList());
                _files = new ReadOnlyCollection<Lazy<IFile>>(files.ToList());
                _enumerated = true;
            }
        }

        private class File : Base, IFile
        {
            private byte[]? _data = default;

            public File(IDirectory? parent, GitHubClient client, string owner, string name, string path, string? @ref)
                : base(parent, client, owner, name, path, @ref) { }

            public IDirectory? Directory
                => _parent;

            public async ValueTask<Stream> OpenRead()
                => new MemoryStream(await ReadAllBytes());

            public async ValueTask<byte[]> ReadAllBytes()
            {
                _data ??= await (_ref is null ? _client.Repository.Content.GetRawContent(_owner, _name, _path) : _client.Repository.Content.GetRawContentByRef(_owner, _name, _path, _ref));
                return _data;
            }
        }

        private abstract class Base
        {
            protected readonly GitHubClient _client;
            protected readonly string _owner;
            protected readonly string _name;
            protected readonly string _path;
            protected readonly string? _ref;
            protected readonly IDirectory? _parent;

            protected Base(IDirectory? parent, GitHubClient client, string owner, string name, string path, string? @ref)
            {
                ArgumentNullException.ThrowIfNull(client);
                ArgumentNullException.ThrowIfNull(owner);
                ArgumentNullException.ThrowIfNull(name);
                ArgumentNullException.ThrowIfNull(path);
                ArgumentNullException.ThrowIfNull(@ref);

                _parent = parent;
                _client = client;
                _owner = owner;
                _name = name;
                _path = path;
                _ref = @ref;
            }

            public string Name
                => Path.GetFileName(_path);
        }
    }
}
