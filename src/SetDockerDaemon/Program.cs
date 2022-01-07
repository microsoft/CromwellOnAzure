// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace SetDockerDaemon
{
    class Program
    {
        private readonly string privateContainerRegistry;
        private const string propName = @"registry-mirrors";
        private string propValue => $"https://{privateContainerRegistry.Trim()}.azurecr.io";

        public Program(string[] args)
        {
            if (args is null)
            {
                throw new ArgumentNullException(nameof(args));
            }

            if (args.Length != 1)
            {
                throw new ArgumentException("Only valid argument is the name of private container registry's name.", nameof(args));
            }

            privateContainerRegistry = args[0];
        }

        static async Task<int> Main(string[] args)
        {
            try
            {
                return await new Program(args).Run();
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.Message);
                return 0 == ex.HResult ? 1 : ex.HResult;
            }
        }

        private Task<int> Run()
        {
            var file = new FileInfo(@"/etc/docker/daemon.json");
            if (file.Exists)
            {
                var tmp = new FileInfo(Path.ChangeExtension(file.FullName, "new"));
                using (var docStrm = file.OpenRead())
                {
                    using (var doc = JsonDocument.Parse(docStrm))
                    {
                        if (doc?.RootElement.ValueKind != JsonValueKind.Object)
                        {
                            throw new InvalidOperationException("Bad Json format for docker daemon.json file.");
                        }

                        var replace = doc.RootElement.TryGetProperty(propName, out var testProperty);
                        if (replace)
                        {
                            if (testProperty.ValueKind != JsonValueKind.Object)
                            {
                                throw new InvalidOperationException("Bad Json format for docker daemon.json file.");
                            }

                            if (testProperty.EnumerateArray().Select(e => e.GetString()).Contains(propValue))
                            {
                                return Task.FromResult(0);
                            }
                        }

                        using (var wrtrStrm = tmp.Create())
                        {
                            using (var writer = new Utf8JsonWriter(wrtrStrm, new JsonWriterOptions { Indented = true }))
                            {
                                writer.WriteStartObject();
                                foreach (var property in doc.RootElement.EnumerateObject())
                                {
                                    if (replace && property.NameEquals(propName))
                                    {
                                        var entries = property.Value.EnumerateArray().Select(e => e.GetString()).ToList();
                                        if (entries.Contains(propValue))
                                        {
                                            property.WriteTo(writer);
                                        }
                                        else
                                        {
                                            writer.WriteStartArray(propName);
                                            entries.ForEach(writer.WriteStringValue);
                                            writer.WriteStringValue(propValue);
                                            writer.WriteEndArray();
                                        }
                                    }
                                    else
                                    {
                                        property.WriteTo(writer);
                                    }
                                }

                                if (!replace)
                                {
                                    writer.WriteStartArray(propName);
                                    writer.WriteStringValue(propValue);
                                    writer.WriteEndArray();
                                }
                                writer.WriteEndObject();
                                writer.Flush();
                            }
                        }
                    }
                }

                using (var strm = tmp.AppendText())
                {
                    strm.WriteLine();
                }

                _ = file.CopyTo(Path.ChangeExtension(file.FullName, "bak"), true);
                file = tmp.CopyTo(file.FullName, true);
                tmp.Delete();
            }
            else
            {
                file.Directory.Create();
                using (var wrtrStrm = file.Create())
                {
                    using (var writer = new Utf8JsonWriter(wrtrStrm, new JsonWriterOptions { Indented = true }))
                    {
                        writer.WriteStartObject();
                        writer.WriteStartArray(propName);
                        writer.WriteStringValue(propValue);
                        writer.WriteEndArray();
                        writer.WriteEndObject();
                        writer.Flush();
                    }
                }
                using (var strm = file.AppendText())
                {
                    strm.WriteLine();
                }
            }

            return Task.FromResult(0);
        }
    }
}
