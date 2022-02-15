// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Text.RegularExpressions;

namespace TesApi.Web
{
    /// <summary>
    /// Represents segments of Azure Blob Storage URL
    /// </summary>
    public class StorageAccountUrlSegments
    {
        private static readonly Regex localPathRegex = new(@"/?([^/]+)/([^/]+)/?(.+)?");
        private static readonly Regex accountNameRegex = new(@"^https*://([^\.]*).*");

        /// <summary>
        /// Create from provided segments
        /// </summary>
        /// <param name="blobEndpoint">Blob endpoint, for example http://myaccount.blob.core.windows.net</param>
        /// <param name="containerName">Container name</param>
        /// <param name="blobName">Blob name</param>
        /// <param name="sasToken">SAS token</param>
        public StorageAccountUrlSegments(string blobEndpoint, string containerName, string blobName = "", string sasToken = "")
        {
            this.AccountName = accountNameRegex.Replace(blobEndpoint, "$1");
            this.BlobEndpoint = blobEndpoint.TrimEnd('/');
            this.ContainerName = containerName;
            this.BlobName = blobName;
            this.SasToken = sasToken;
        }

        private StorageAccountUrlSegments()
        {
        }

        /// <summary>
        /// The storage account name
        /// </summary>
        public string AccountName { get; private set; }
        /// <summary>
        /// The blob endpoint, for example http://myaccount.blob.core.windows.net. Is empty for local paths.
        /// </summary>
        public string BlobEndpoint { get; private set; }
        /// <summary>
        /// The container name
        /// </summary>
        public string ContainerName { get; private set; }
        /// <summary>
        /// The blob name within the container
        /// </summary>
        public string BlobName { get; private set; }
        /// <summary>
        /// The SAS token
        /// </summary>
        public string SasToken { get; set; }

        /// <summary>
        /// Tries to parse the provided string. The following formats are supported:
        /// - /accountName/containerName/blobName
        /// - https://accountName.blob.core.windows.net/containerName/blobName?sasToken
        /// </summary>
        /// <param name="uriString">String representing an Azure Storage object location</param>
        /// <param name="result"><see cref="StorageAccountUrlSegments"/> representing the provided object location</param>
        /// <returns>True if parsing was successful</returns>
        public static bool TryCreate(string uriString, out StorageAccountUrlSegments result)
        {
            if (Uri.TryCreate(uriString, UriKind.Absolute, out var uri) && (uri.Scheme.Equals(Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase) || uri.Scheme.Equals(Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase)))
            {
                result = new StorageAccountUrlSegments
                {
                    AccountName = uri.Host.Split('.', 2)[0],
                    BlobEndpoint = $"{uri.Scheme}://{uri.Host}",
                    ContainerName = uri.Segments.Skip(1).FirstOrDefault()?.Trim('/') ?? string.Empty,
                    BlobName = string.Join(string.Empty, uri.Segments.Skip(2)).Trim('/'),
                    SasToken = uri.Query
                };

                return true;
            }

            var match = localPathRegex.Match(uriString);

            if (match.Success)
            {
                result = new StorageAccountUrlSegments
                {
                    AccountName = match.Groups[1].Value,
                    BlobEndpoint = string.Empty,
                    ContainerName = match.Groups[2].Value,
                    BlobName = match.Groups[3].Value,
                    SasToken = string.Empty
                };

                return true;
            }

            result = null;
            return false;
        }

        /// <summary>
        /// Parses the provided string. The following formats are supported:
        /// - /accountName/containerName/blobName
        /// - https://accountName.blob.core.windows.net/containerName/blobName?sasToken
        /// Throws if string cannot be parsed.
        /// </summary>
        /// <param name="uriString">String representing an Azure Storage object location</param>
        /// <returns><see cref="StorageAccountUrlSegments"/> representing the provided object location</returns>
        public static StorageAccountUrlSegments Create(string uriString)
            => TryCreate(uriString, out var result) ? result : throw new ArgumentException($"Invalid blob URI: {uriString}");

        /// <summary>
        /// Returns the Blob URL string
        /// </summary>
        /// <returns>Blob URL</returns>
        public string ToUriString()
            => $"{this.BlobEndpoint}/{this.ContainerName}/{this.BlobName}{this.SasToken}".TrimEnd('/');

        /// <summary>
        /// Returns the Blob URI
        /// </summary>
        /// <returns>Blob URI</returns>
        public Uri ToUri()
            => new(this.ToUriString());

        /// <summary>
        /// Returns true if the segments represent a container
        /// </summary>
        public bool IsContainer => !string.IsNullOrEmpty(this.ContainerName) && string.IsNullOrEmpty(this.BlobName);
    }
}
