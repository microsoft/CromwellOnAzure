// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

#nullable enable

namespace Common.HostConfigs
{
    public abstract class Base
    {

        public static bool AreSame(HostConfig a, HostConfig b)
            => WriteJson(a)?.GetHashCode() == WriteJson(b)?.GetHashCode();

        public static TextReader? OpenConfiguration(FileInfo config)
        {
            try
            {
                return config.OpenText();
            }
            catch (DirectoryNotFoundException)
            {
                return default;
            }
            catch (FileNotFoundException)
            {
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
                using var reader = new JsonTextReader(textReader);
                return JsonSerializer.CreateDefault().Deserialize<T>(reader) ?? defaultValue();
            }
        }

        public static string WriteJson<T>(T value)
        {
            using var result = new StringWriter();
            using var writer = new JsonTextWriter(result);
            JsonSerializer.CreateDefault().Serialize(writer, value);
            return result.ToString();
        }
    }
}
