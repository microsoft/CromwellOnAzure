// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;

namespace Common
{
    public static class Utilities
    {
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
            if (builder.Path.Split('/', StringSplitOptions.RemoveEmptyEntries).Length < 2)
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
