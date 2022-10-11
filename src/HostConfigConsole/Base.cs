// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Common.HostConfigs;

namespace HostConfigConsole
{
    public abstract class Base
    {

        public static bool AreSame(HostConfig a, HostConfig b)
            => WriteJson(a)?.GetHashCode() == WriteJson(b)?.GetHashCode();

        public static TextReader? OpenConfiguration(Stream config)
        {
            try
            {
                return new StreamReader(config);
            }
            catch (ArgumentException)
            {
                config?.Close();
                return default;
            }
        }

        public static T ReadJson<T>(TextReader? textReader, Func<T> defaultValue)
        {
            return textReader is null
                ? defaultValue()
                : ReadFile();

            T ReadFile()
            {
                using var reader = new JsonTextReader(textReader) { CloseInput = true };
                return JsonSerializer.CreateDefault().Deserialize<T>(reader) ?? defaultValue();
            }
        }

        public static string WriteJson<T>(T value)
        {
            using var result = new StringWriter();
            using var writer = new JsonTextWriter(result) { CloseOutput = false };
            JsonSerializer.CreateDefault().Serialize(writer, value);
            return result.ToString();
        }
    }
}
