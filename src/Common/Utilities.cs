// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace Common
{
    public static class Utilities
    {
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

            if (BitConverter.IsLittleEndian)
            {
                bytes = bytes.Select(FlipByte).ToArray();
            }

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

            static byte FlipByte(byte data)
                => (byte)(
                    (((data & 0x01) == 0) ? 0 : 0x80) |
                    (((data & 0x02) == 0) ? 0 : 0x40) |
                    (((data & 0x04) == 0) ? 0 : 0x20) |
                    (((data & 0x08) == 0) ? 0 : 0x10) |
                    (((data & 0x10) == 0) ? 0 : 0x08) |
                    (((data & 0x20) == 0) ? 0 : 0x04) |
                    (((data & 0x40) == 0) ? 0 : 0x02) |
                    (((data & 0x80) == 0) ? 0 : 0x01));
        }

        /// <summary>
        /// Create canonical docker image path
        /// </summary>
        /// <param name="image"></param>
        /// <returns></returns>
        /// <remarks>
        /// This value is expected to be used for comparison purposes only, although in most circumstances it should be usable.
        /// There's very little validation performed at this time.
        /// Some URIs may be mangled by this implementation.
        /// </remarks>
        public static Uri NormalizeContainerImageName(string image)
        {
            var tag = ":latest"; // default value if none specified

            // Find and store docker tag
            var tagIdx = image.LastIndexOf(':');

            if (-1 != tagIdx && image.LastIndexOf('/') <= tagIdx)
            {
                tag = image[tagIdx..];
                image = image[..tagIdx];
            }

            // Add scheme if none specified
            if (image.IndexOf('/') - 1 != image.IndexOf(':'))
            {
                image = "docker://" + image;
            }

            // Parse docker image path
            var builder = new UriBuilder(image);

            // If path didn't include host, move parsed "host" value to path
            if (!builder.Host.Contains('.') || builder.Path == "/")
            {
                builder.Path = ("/" + builder.Host + builder.Path).TrimEnd('/');
                builder.Host = string.Empty;
            }

            // If path doesn't contain repository, add the default
            if (builder.Path.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries).Length < 2)
            {
                builder.Path = "/library" + builder.Path;
            }

            // Add or normalize the default host
            if (string.IsNullOrWhiteSpace(builder.Host) || builder.Host.Equals("index.docker.io"))
            {
                builder.Host = "docker.io";
            }

            // Add/restore docker tag
            builder.Path += tag;

            return builder.Uri;
        }
    }
}
