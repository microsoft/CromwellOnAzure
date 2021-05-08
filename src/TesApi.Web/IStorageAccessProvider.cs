using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Web;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;

namespace TesApi.Web
{
    /// <summary>
    /// Provides methods for abstracting storage access by using local path references in form of /storageaccount/container/blobpath
    /// </summary>
    public interface IStorageAccessProvider
    {
        /// <summary>
        /// Retrieves file content
        /// </summary>
        /// <param name="blobRelativePath">Path to the file in form of /storageaccountname/container/path</param>
        /// <returns>The content of the file</returns>
        public Task<string> DownloadBlobAsync(string blobRelativePath);

        /// <summary>
        /// Tries to retrieves file content
        /// </summary>
        /// <param name="blobRelativePath">Path to the file in form of /storageaccountname/container/path</param>
        /// <param name="action">Action to invoke if content is downloaded</param>
        /// <returns>True if content was downloaded</returns>
        public Task<bool> TryDownloadBlobAsync(string blobRelativePath, Action<string> action);

        /// <summary>
        /// Updates the content of the file, creating the file if necessary
        /// </summary>
        /// <param name="blobRelativePath">Path to the file in form of /storageaccountname/container/path</param>
        /// <param name="content">The new content</param>
        /// <returns></returns>
        public Task UploadBlobAsync(string blobRelativePath, string content);

        /// <summary>
        /// Updates the content of the file, creating the file if necessary
        /// </summary>
        /// <param name="blobRelativePath">Path to the file in form of /storageaccountname/container/path</param>
        /// <param name="sourceLocalFilePath">Path to the local file to get the content from</param>
        /// <returns></returns>
        public Task UploadBlobFromFileAsync(string blobRelativePath, string sourceLocalFilePath);

        /// <summary>
        /// Checks if the specified string represents a HTTP URL that is publicly accessible
        /// </summary>
        /// <param name="uriString">URI string</param>
        /// <returns>True if the URL can be used as is, without adding SAS token to it</returns>
        public Task<bool> IsPublicHttpUrl(string uriString);

        /// <summary>
        /// Returns an Azure Storage Blob or Container URL with SAS token given a path that uses one of the following formats: 
        /// - /accountName/containerName
        /// - /accountName/containerName/blobName
        /// - /cromwell-executions/blobName
        /// - https://accountName.blob.core.windows.net/containerName
        /// - https://accountName.blob.core.windows.net/containerName/blobName
        /// </summary>
        /// <param name="path">The file path to convert. Two-part path is treated as container path. Paths with three or more parts are treated as blobs.</param>
        /// <param name="getContainerSas">Get the container SAS even if path is longer than two parts</param>
        /// <returns>An Azure Block Blob or Container URL with SAS token</returns>
        public Task<string> MapLocalPathToSasUrlAsync(string path, bool getContainerSas = false);
    }
}
