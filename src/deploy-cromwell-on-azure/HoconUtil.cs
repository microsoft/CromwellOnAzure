// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Hocon;

namespace CromwellOnAzureDeployer
{
    /// <summary>
    /// Extends <see cref="Config"/> to facilitate roundtrip editing of HOCON configurations
    /// </summary>
    internal class HoconUtil : IDisposable
    {
        private readonly List<RootText> _rootTexts = [];
        private readonly string _originalText;
        private readonly bool withBraces;
        private readonly Dictionary<IncludeCallbackArgs, string> _includeCallbackArgs = [];
        //private readonly Dictionary<IncludeCallbackArgs, Guid> _includeResourceGuids = [];
        private readonly Dictionary<IncludeCallbackArgs, Guid> _includeFileGuids = [];
        private readonly Dictionary<IncludeCallbackArgs, Guid> _includeUrlGuids = [];

        private readonly DirectoryInfo _fileDirectory;
        private readonly Lazy<HttpClient> _httpClient = new(() => new());

        private readonly HoconIncludeCallbackAsync _hoconIncludeCallback;

        /// <param name="file">File containing HOCON configuration.</param>
        /// <param name="callback">HOCON include processing delegate.</param>
        public HoconUtil(FileInfo file, HoconIncludeCallbackAsync callback = default)
            : this(File.ReadAllText(file.FullName), callback)
        {
            _fileDirectory = file.Directory;
        }

        /// <param name="hocon">HOCON configuration.</param>
        /// <param name="callback">HOCON include processing delegate.</param>
        public HoconUtil(string hocon, HoconIncludeCallbackAsync callback = default)
        {
            var text = hocon.Trim();
            withBraces = text.StartsWith('{') && text.EndsWith('}');

            if (withBraces)
            {
                text = text.Trim('{', '}').Trim();
            }

            _originalText = text + Environment.NewLine + Environment.NewLine;
            _hoconIncludeCallback = callback;
        }

        /// <summary>
        /// Parses a HOCON configuration
        /// </summary>
        /// <returns>A <see cref="Config"/> containing the parsed configuration.</returns>
        public HoconRoot Parse()
        {
            _includeCallbackArgs.Clear();
            var result = HoconParser.Parse(_originalText, GetHoconIncludeCallback(incremental: false));
            var remainder = _originalText;

            // Incrementally parse the configuration text to store the original text for each element visible from the root.
            while (!string.IsNullOrEmpty(remainder))
            {
                var element = IncrementalParse(remainder!, out remainder);

                if (element is not null)
                {
                    _rootTexts.Add(element);
                }
            }

            // Connect each element to the fully parsed tree to discover edits.
            foreach (var section in _rootTexts)
            {
                switch (section.Root.Value.Type)
                {
                    case HoconType.Empty: // Skip setting the field for empty items (currently some root-level includes). By not setting the field, the original text will be used (no modification is possible).
                        break;

                    default:
                        foreach (var root in section.Root.AsEnumerable())
                        {
                            var entry = root;
                            // Walk the object tree in order to inline the path as far as possible.
                            while (entry.Value.Type == HoconType.Object && entry.Value.GetObject().Values.Count == 1)
                            {
                                entry = entry.Value.GetObject().First();
                            }

                            // Retrieve the value from the fully parsed tree
                            if (result.TryGetValue(entry.Value.Path, out var value) && value.Parent is HoconField field)
                            {
                                // Clone the value for the modification check.
                                section.Field = new((HoconField)field.Clone(entry.Value.Parent), section);
                            }
                            else
                            {
                                //section.Field = new(new HoconField(string.Empty, new HoconObject(new HoconValue(null))), section);
                                //SetFieldChildren(section.Field.Value.Field, [(HoconValue)section.Root.Value.Clone(section.Field.Value.Field)]);
                                throw new InvalidProgramException("TODO: not sure.");
                            }
                        }
                        break;
                }
            }

            return result;
        }


        [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "Maintain API")]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("CodeQuality", "IDE0079:Remove unnecessary suppression", Justification = "CA1822 suppression is appropriate in this use case.")]
        public bool Remove(HoconRoot config, string path)
        {
            var value = config.Value.GetObject();
            var pathParts = path.Split('.');

            foreach (var part in pathParts.Take(pathParts.Length - 1))
            {
                if (!value.TryGetValue(part, out var nextValue))
                {
                    return false;
                }

                value = nextValue.GetObject();
            }

            return value.Remove(pathParts.Last());
        }

        /// <summary>
        /// Converts <paramref name="config"/> into a more stable configuration
        /// </summary>
        /// <param name="config">An edited <see cref="Config"/>.</param>
        /// <returns>An edited HOCON configuration.</returns>
        public string ToString(HoconRoot config)
        {
            StringBuilder sb = new();

            foreach (var section in AsEnumerable(config))
            {
                if (!section.Field.HasValue)
                {
                    // Root-level includes. TODO: comments
                    sb.Append(section.Text.ReplaceLineEndings());
                }
                else
                {
                    // If the element was not removed
                    if (config.TryGetValue(section.Field.Value.Field.Path, out var result))
                    {
                        // Determine if element was changed. Note that formatting is normalized if the element was modified.
                        sb.Append(section.Field.Value.Field.Value.Equals(result)
                            ? section.Text.ReplaceLineEndings()
                            : $"{section.Field.Value.Field.Path} : {result.ToString(withBraces ? 3 : 1, 2)}{Environment.NewLine}{Environment.NewLine}");
                    }
                }
            }

            var text = sb.ToString().Trim(); // Remove excess whitespace introduce to separate elements

            if (withBraces) // Restore root braces
            {
                text = $"{{{Environment.NewLine}{text}{Environment.NewLine}}}";
            }

            return text;
        }

        // Enumerates configuration with associated metadata
        private IEnumerable<RootText> AsEnumerable(HoconRoot config)
        {
            var configItems = config.AsEnumerable().ToList();

            // Try to return elements in the original order. This is not required for HOCON, but it is a huge aid for humans.
            foreach (var item in _rootTexts)
            {
                if (!item.Field.HasValue)
                {
                    yield return item;
                    continue;
                }

                var sections = configItems.Where(pair => item.Field.HasValue && IsChildPathOf(item.Field.Value.Field.Path, pair.Value.Path)).ToList();

                sections.ForEach(section => _ = configItems.Remove(section));

                yield return item;
            }

            // Place new root elements at the end
            foreach (var item in configItems)
            {
                RootText entry = new(config, string.Empty);
                entry.Field = new(new(item.Value.Path.Last(), (HoconObject)item.Value.Parent), entry)
                {
                    // This field will deliberately have an empty value so it will not match the config (since it doesn't have prior text, it needs to be treated as "modified").
                    Field = new HoconEmptyValue(null).AtKey(item.Value.Path.Value).AsEnumerable().Single().Value
                };

                yield return entry;
            }

            static bool IsChildPathOf(HoconPath path, HoconPath parentPath)
            {
                if (path.Count < parentPath.Count)
                {
                    return false;
                }

                for (var i = 0; i < parentPath.Count; ++i)
                {
                    if (path[i] != parentPath[i])
                    {
                        return false;
                    }
                }

                return true;
            }
        }

        // Parse one full element from content by incrementally attempting to parse substrings of that content
        private RootText IncrementalParse(string content, out string remainder)
        {
            StringBuilder sb = new(content);
            if (sb.Length == 0)
            {
                remainder = null;
                return new(HoconParser.Parse(@"{}"), content);
            }

            if (!content.EndsWith('\n'))
            {
                sb.Append('\n');
            }

            foreach (var text in Enumerable.Range(1, sb.Length).Select(i => sb.ToString(0, i)))
            {
                try
                {
                    var value = HoconParser.Parse(text, GetHoconIncludeCallback(incremental: true));
                    remainder = sb.ToString(text.Length, sb.Length - text.Length);
                    if (remainder.TrimStart().Length == remainder.Length && text.EndsWith('\n'))
                    {
                        return new(value.Value.Children.All(IsEmpty) ? new(new HoconEmptyValue()) : value, text);
                    }

                    static bool IsEmpty(IHoconElement element) => element.Type switch
                    {
                        HoconType.Empty => true,
                        HoconType.Array => ((HoconArray)element).Count != 0,
                        HoconType.Object => ((HoconObject)element).IsEmpty,
                        _ => false,
                    };
                }
                catch (HoconParserException)
                { }
                catch (ArgumentOutOfRangeException)
                { }
            }

            remainder = null;
            return null;
        }

        private HoconIncludeCallbackAsync GetHoconIncludeCallback(bool incremental)
            => (callbackType, value) => IncludeCallbackAsync(incremental, callbackType, value);

        private async Task<string> IncludeCallbackAsync(bool incremental, HoconCallbackType callbackType, string value)
        {
            IncludeCallbackArgs args = new(callbackType, value);

            var result = await (callbackType switch
            {
                HoconCallbackType.Resource => IncludeCallbackResourceAsync(/*incremental, args, _hoconIncludeCallback is not null*/),
                HoconCallbackType.File => IncludeCallbackFileAsync(incremental, args, _hoconIncludeCallback is not null),
                HoconCallbackType.Url => IncludeCallbackUrlAsync(incremental, args, _hoconIncludeCallback is not null),
                _ => throw new ArgumentException("Unknown include type", nameof(callbackType)),
            });

            return _hoconIncludeCallback is null
                ? result
                : await CallUserCallback(this, args);

            static async ValueTask<string> CallUserCallback(HoconUtil hoconUtils, IncludeCallbackArgs args)
            {
                if (!hoconUtils._includeCallbackArgs.TryGetValue(args, out var result))
                {
                    result = await hoconUtils._hoconIncludeCallback!(args.CallbackType, args.Value);
                    hoconUtils._includeCallbackArgs.Add(args, result);
                }

                return result;
            }
        }

        private static Task<string> IncludeCallbackResourceAsync(/*bool incremental, IncludeCallbackArgs args, bool userCallbackProvided*/)
            => Task.FromResult(@"{}");

        private Task<string> IncludeCallbackFileAsync(bool incremental, IncludeCallbackArgs args, bool userCallbackProvided)
        {
            if (incremental)
            {
                if (!_includeFileGuids.TryGetValue(args, out var guid))
                {
                    guid = Guid.NewGuid();
                    _includeFileGuids.Add(args, guid);
                }

                return Task.FromResult($"{{\"id\": \"f{guid:N}\", \"type\": \"file\", \"value\": \"{args.Value}\"}}");
            }
            else if (!userCallbackProvided)
            {
                return Task.FromResult(@"{}");
            }

            string path;
            try
            {
                path = Path.Combine(_fileDirectory?.FullName ?? Environment.CurrentDirectory, args.Value);
            }
            catch (ArgumentException e)
            {
                throw new ArgumentException(e.Message, nameof(args), e);
            }

            return File.ReadAllTextAsync(path);
        }

        private Task<string> IncludeCallbackUrlAsync(bool incremental, IncludeCallbackArgs args, bool userCallbackProvided)
        {
            if (incremental)
            {
                if (!_includeUrlGuids.TryGetValue(args, out var guid))
                {
                    guid = Guid.NewGuid();
                    _includeUrlGuids.Add(args, guid);
                }

                return Task.FromResult($"{{\"id\": \"u{guid:N}\", \"type\": \"url\", \"value\": \"{args.Value}\"}}");
            }
            else if (!userCallbackProvided)
            {
                return Task.FromResult(@"{}");
            }

            Uri uri;
            try
            {
                uri = new(args.Value);
            }
            catch (ArgumentException e)
            {
                throw new ArgumentException(e.Message, nameof(args), e);
            }
            catch (UriFormatException e)
            {
                throw new ArgumentException(e.Message, nameof(args), e);
            }

            return _httpClient.Value.GetStringAsync(uri);
        }

        void IDisposable.Dispose()
        {
            if (_httpClient.IsValueCreated)
            {
                _httpClient.Value.Dispose();
            }
        }

        private record struct IncludeCallbackArgs(HoconCallbackType CallbackType, string Value);
    }

    public record class RootText(HoconRoot Root, string Text)
    {
        public FieldWithRoot? Field { get; set; }
    }

    public record struct FieldWithRoot(HoconField Field, RootText Root);
}
