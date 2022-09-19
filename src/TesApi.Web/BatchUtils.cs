// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections;
using System.IO;
using System.Linq;
using Newtonsoft.Json;

namespace TesApi.Web
{
    /// <summary>
    /// Util class for Azure Batch related helper functions.
    /// </summary>
    public static class BatchUtils
    {
        /// <summary>
        /// Readonly variable for the command line string so we're only reading from the file once.
        /// </summary>
        public static readonly string StartTaskScript = GetStartTaskScript();

        /// <summary>
        /// Converts the install-docker.sh shell script to a string.
        /// </summary>
        /// <returns>The string version of the shell script.</returns>
        private static string GetStartTaskScript()
            => File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Scripts/start-task.sh"));

        /// <summary>
        /// TODO
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="textReader"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public static T ReadJson<T>(TextReader textReader, Func<T> defaultValue)
        {
            return textReader is null
                ? defaultValue()
                : ReadJsonFile();

            T ReadJsonFile()
            {
                using var reader = new JsonTextReader(textReader);
                return JsonSerializer.CreateDefault().Deserialize<T>(reader) ?? defaultValue();
            }
        }

        /// <summary>
        /// TODO
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string WriteJson<T>(T value)
        {
            using var result = new StringWriter();
            using var writer = new JsonTextWriter(result);
            JsonSerializer.CreateDefault(new() { Error = (o, e) => throw e.ErrorContext.Error }).Serialize(writer, value);
            return result.ToString();
        }

        private static readonly char[] Rfc4648Base32 = new[] { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '2', '3', '4', '5', '6', '7' };

        /// <summary>
        /// Converts binary to Base32
        /// </summary>
        /// <param name="bytes">Data to convert.</param>
        /// <returns>RFC 4648 Base32 representation</returns>
        /// <exception cref="InvalidOperationException"></exception>
        public static string ConvertToBase32(byte[] bytes) // https://datatracker.ietf.org/doc/html/rfc4648#section-6
        {
            const int groupBitlength = 5;
            return new string(new BitArray(bytes)
                    .Cast<bool>()
                    .Select((b, i) => (Index: i, Value: b ? 1 << (groupBitlength - 1 - (i % groupBitlength)) : 0))
                    .GroupBy(t => t.Index / groupBitlength)
                    .Select(g => Rfc4648Base32[g.Sum(t => t.Value)])
                    .ToArray())
                + (bytes.Length % groupBitlength) switch
                {
                    0 => string.Empty,
                    1 => @"======",
                    2 => @"====",
                    3 => @"===",
                    4 => @"=",
                    _ => throw new InvalidOperationException(), // Keeps the compiler happy.
                };
        }
    }
}
