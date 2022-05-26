// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

#nullable enable

namespace Common.HostConfigs
{
    /// <summary>
    /// Versions for each defined batch application Zip file (<seealso cref="Package"/>).
    /// </summary>
    /// <remarks>
    /// The version is the SHA-256 hash of the Zip file.
    /// </remarks>
    public class PackageHashes : Dictionary<string, string>
    {
        public static PackageHashes Empty
            => new();
    }
}
