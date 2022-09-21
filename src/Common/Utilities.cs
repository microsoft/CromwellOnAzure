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
    public class Utilities
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
    }
}
