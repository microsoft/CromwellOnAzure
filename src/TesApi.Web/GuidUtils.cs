// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web
{
    using System;
    using System.IO;
    using System.Net;
    using System.Security.Cryptography;
    using System.Text;

    /// <summary>
    /// RFC 4122 defined Name-Based namespaces.
    /// </summary>
    public enum UUIDNameSpace
    {
        /// <summary>
        /// Name string is a fully-qualified domain name.
        /// </summary>
        Dns = 0,
        /// <summary>
        /// Name string is a URL.
        /// </summary>
        Url = 1,
        /// <summary>
        /// Name string is an ISO OID.
        /// </summary>
        Oid = 2,
        /// <summary>
        /// Name string is an X.500 DN (in DER or a text output format).
        /// </summary>
        X500 = 3
    }

    /// <summary>
    /// Extension class implementing RFC 4122 version 3 &amp; 5 UUIDs.
    /// </summary>
    public static class GuidUtils
    {
        private static readonly Guid[] NameSpaceGuids = {
            Guid.Parse("6ba7b810-9dad-11d1-80b4-00c04fd430c8"), // DNS
            Guid.Parse("6ba7b811-9dad-11d1-80b4-00c04fd430c8"), // URL
            Guid.Parse("6ba7b812-9dad-11d1-80b4-00c04fd430c8"), // OID
            Guid.Parse("6ba7b814-9dad-11d1-80b4-00c04fd430c8") // X500
        };

        private enum UUIDVersion : byte
        {
            TimeBased = 0x10,
            NameBasedWithMd5 = 0x30,
            Random = 0x40,
            NamedBasedWithSha1 = 0x50
        }

        /// <summary>
        /// Writes entire array to stream.
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="buffer"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public static void Write(this Stream stream, byte[] buffer) =>
            (stream ?? throw new ArgumentNullException(nameof(stream)))
                .Write(buffer ?? throw new ArgumentNullException(nameof(buffer)),
                    0, buffer.Length);

        /// <summary>
        /// Generates a name-based GUID using SHA1.
        /// </summary>
        /// <param name="ns"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        public static Guid GenerateGuid(this UUIDNameSpace ns, string name) =>
            ns.GetNsGuid().GenerateGuid(name);

        /// <summary>
        /// Generates a name-based GUID using SHA1.
        /// </summary>
        /// <param name="ns"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        public static Guid GenerateGuid(this UUIDNameSpace ns, byte[] name) =>
            ns.GetNsGuid().GenerateGuid(name);

        /// <summary>
        /// Generates a name-based GUID using <paramref name="hash"/>.
        /// </summary>
        /// <param name="ns"></param>
        /// <param name="name"></param>
        /// <param name="hash"></param>
        /// <returns></returns>
        public static Guid GenerateGuid(this UUIDNameSpace ns, string name, HashAlgorithmName hash) =>
            ns.GetNsGuid().GenerateGuid(name, hash);

        /// <summary>
        /// Generates a name-based GUID using <paramref name="hash"/>.
        /// </summary>
        /// <param name="ns"></param>
        /// <param name="name"></param>
        /// <param name="hash"></param>
        /// <returns></returns>
        public static Guid GenerateGuid(this UUIDNameSpace ns, byte[] name, HashAlgorithmName hash) =>
            ns.GetNsGuid().GenerateGuid(name, hash);

        private static Guid GetNsGuid(this UUIDNameSpace ns) =>
            NameSpaceGuids[(int)ns];

        /// <summary>
        /// Generates a name-based GUID using SHA1 using the provided namespace.
        /// </summary>
        /// <param name="ns"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        public static Guid GenerateGuid(this Guid ns, string name) =>
            ns.GenerateGuid(name, HashAlgorithmName.SHA1);

        /// <summary>
        /// Generates a name-based GUID using SHA1 using the provided namespace.
        /// </summary>
        /// <param name="ns"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        public static Guid GenerateGuid(this Guid ns, byte[] name) =>
            ns.GenerateGuid(name, HashAlgorithmName.SHA1);

        /// <summary>
        /// Generates a name-based GUID using <paramref name="hash"/> using the provided namespace.
        /// </summary>
        /// <param name="ns"></param>
        /// <param name="name"></param>
        /// <param name="hash"></param>
        /// <returns></returns>
        public static Guid GenerateGuid(this Guid ns, string name, HashAlgorithmName hash)
            => ns.GenerateGuid(Encoding.UTF8.GetBytes(name), hash);

        //[System.Diagnostics.CodeAnalysis.SuppressMessage("Security", "CA5350:Do Not Use Weak Cryptographic Algorithms", Justification = "Algorithm is required to maintain compatibility with the relevate standards. No security guarantee is expected or required.")]
        //[System.Diagnostics.CodeAnalysis.SuppressMessage("Security", "CA5351:Do Not Use Broken Cryptographic Algorithms", Justification = "Algorithm is required to maintain compatibility with the relevate standards. No security guarantee is expected or required.")]
        /// <summary>
        /// Generates a name-based GUID using <paramref name="hash"/> using the provided namespace.
        /// </summary>
        /// <param name="ns"></param>
        /// <param name="name"></param>
        /// <param name="hash"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="ArgumentException"></exception>
        public static Guid GenerateGuid(this Guid ns, byte[] name, HashAlgorithmName hash)
        {
            _ = name ?? throw new ArgumentNullException(nameof(name));
            if (Guid.Empty.Equals(ns)) { throw new ArgumentException("A namespace is required.", nameof(ns)); }
            var (version, algorithm) = hash switch
            {
                HashAlgorithmName x when HashAlgorithmName.MD5.Equals(x) => (UUIDVersion.NameBasedWithMd5, (HashAlgorithm)MD5.Create()),
                HashAlgorithmName x when HashAlgorithmName.SHA1.Equals(x) => (UUIDVersion.NamedBasedWithSha1, SHA1.Create()),
                _ => throw new ArgumentException("Hash algorithm must be either MD5 or SHA1.", nameof(hash)),
            };
            using (algorithm)
            {
                using var stream = new MemoryStream();
                stream.Write(FixGuidForEndianness(ns.ToByteArray(), true));
                stream.Write(name);
                stream.Seek(0, SeekOrigin.Begin);
                return new Guid(
                    FixGuidForEndianness(algorithm.ComputeHash(stream), false)
                    .AddVariantMarker()
                    .AddVersionMarker(version));
            }
        }

        /*
         * BitConverterReader & MoveBits may be useful outside of this method. Consider extracting them if needed.
         */
        private delegate T BitConverterReader<T>(byte[] value, int startIndex);

        private static byte[] FixGuidForEndianness(byte[] buffer, bool bigEndian)
        {
            if ((buffer?.Length ?? 0) < 16)
            {
                throw new ArgumentException("Guids must always be 128 bits.", nameof(buffer));
            }

            var result = new byte[16];
            if (BitConverter.IsLittleEndian)
            {
                MoveBits(result, 0, BitConverter.ToUInt32);
                MoveBits(result, 4, BitConverter.ToUInt16);
                MoveBits(result, 6, BitConverter.ToUInt16);
                Array.Copy(buffer, 8, result, 8, 8);
            }
            else
            {
                Array.Copy(buffer, 0, result, 0, 16);
            }
            return result;

            void MoveBits<T>(byte[] r, int index, BitConverterReader<T> bitConverterTo) where T : unmanaged, IComparable, IEquatable<T>
            {
                var getBytesName = "GetBytes";
                var tName = typeof(T).Name;
                var (length, getBytes) = tName switch
                {
                    nameof(Int16) => (length: 2, getBytes: typeof(BitConverter).GetMethod(getBytesName, new Type[] { typeof(short) })),
                    nameof(UInt16) => (length: 2, getBytes: typeof(BitConverter).GetMethod(getBytesName, new Type[] { typeof(ushort) })),
                    nameof(Int32) => (length: 4, getBytes: typeof(BitConverter).GetMethod(getBytesName, new Type[] { typeof(int) })),
                    nameof(UInt32) => (length: 4, getBytes: typeof(BitConverter).GetMethod(getBytesName, new Type[] { typeof(uint) })),
                    nameof(Int64) => (length: 8, getBytes: typeof(BitConverter).GetMethod(getBytesName, new Type[] { typeof(long) })),
                    nameof(UInt64) => (length: 8, getBytes: typeof(BitConverter).GetMethod(getBytesName, new Type[] { typeof(ulong) })),
                    _ => throw new ArgumentException("Only 16/32/64-bit integers (signed and unsigned) are supported.", nameof(bitConverterTo))
                };

                Array.Copy((byte[])getBytes.Invoke(default, new object[] { Convert<T>(bitConverterTo(buffer, index)) }), 0, r, index, length);
            }

            T Convert<T>(T value) where T : unmanaged, IComparable, IEquatable<T> =>
                bigEndian ? HostToNetworkOrder(value) : NetworkToHostOrder(value);

            static T HostToNetworkOrder<T>(T host) where T : unmanaged, IComparable, IEquatable<T> =>
                host switch
                {
                    short s => (T)(object)IPAddress.HostToNetworkOrder(s),
                    ushort s => (T)(object)unchecked((ushort)IPAddress.HostToNetworkOrder(unchecked((short)s))),
                    int i => (T)(object)IPAddress.HostToNetworkOrder(i),
                    uint i => (T)(object)unchecked((uint)IPAddress.HostToNetworkOrder(unchecked((int)i))),
                    long l => (T)(object)IPAddress.HostToNetworkOrder(l),
                    ulong l => (T)(object)unchecked((ulong)IPAddress.HostToNetworkOrder(unchecked((long)l))),
                    _ => throw new ArgumentException("Argument must be a supported integer", nameof(host)),
                };

            static T NetworkToHostOrder<T>(T network) where T : unmanaged, IComparable, IEquatable<T> =>
                network switch
                {
                    short s => (T)(object)IPAddress.NetworkToHostOrder(s),
                    ushort s => (T)(object)unchecked((ushort)IPAddress.NetworkToHostOrder(unchecked((short)s))),
                    int i => (T)(object)IPAddress.NetworkToHostOrder(i),
                    uint i => (T)(object)unchecked((uint)IPAddress.NetworkToHostOrder(unchecked((int)i))),
                    long l => (T)(object)IPAddress.NetworkToHostOrder(l),
                    ulong l => (T)(object)unchecked((ulong)IPAddress.NetworkToHostOrder(unchecked((long)l))),
                    _ => throw new ArgumentException("Argument must be a supported integer", nameof(network)),
                };
        }

        // These methods modify the array in-place. They return the same array to facilitate a fluent coding style.
        private const int VariantIndexPosition = 8;
        private const byte VariantMask = 0x3f;
        private const byte VariantBits = 0x80;
        private const int VersionIndexPosition = 7;
        private const byte VersionMask = 0x0f;

        private static byte[] AddVariantMarker(this byte[] array)
        {
            array[VariantIndexPosition] &= VariantMask;
            array[VariantIndexPosition] |= VariantBits;
            return array;
        }

        private static byte[] AddVersionMarker(this byte[] array, UUIDVersion version)
        {
            array[VersionIndexPosition] &= VersionMask;
            array[VersionIndexPosition] |= (byte)version;
            return array;
        }
    }
}
