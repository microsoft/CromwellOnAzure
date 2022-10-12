// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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
    /// Provides methods for blob storage access by using local path references in form of /storageaccount/container/blobpath
    /// </summary>
    public class StorageAccessProvider : IStorageAccessProvider
    {
        private readonly ILogger logger;
        private readonly IAzureProxy azureProxy;
        private const string CromwellPathPrefix = "/cromwell-executions/";
        private readonly string defaultStorageAccountName;
        private static readonly TimeSpan sasTokenDuration = TimeSpan.FromDays(3);
        private readonly List<ExternalStorageContainerInfo> externalStorageContainers;

        /// <summary>
        /// Provides methods for blob storage access by using local path references in form of /storageaccount/container/blobpath
        /// </summary>
        /// <param name="logger">Logger <see cref="ILogger"/></param>
        /// <param name="configuration">Configuration <see cref="IConfiguration"/></param>
        /// <param name="azureProxy">Azure proxy <see cref="IAzureProxy"/></param>
        public StorageAccessProvider(ILogger logger, IConfiguration configuration, IAzureProxy azureProxy)
        {
            this.logger = logger;
            this.azureProxy = azureProxy;

            this.defaultStorageAccountName = configuration["DefaultStorageAccountName"];    // This account contains the cromwell-executions container
            logger.LogInformation($"DefaultStorageAccountName: {defaultStorageAccountName}");

            externalStorageContainers = configuration["ExternalStorageContainers"]?.Split(new[] { ',', ';', '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(uri => {
                    if (StorageAccountUrlSegments.TryCreate(uri, out var s))
                    {
                        return new ExternalStorageContainerInfo { BlobEndpoint = s.BlobEndpoint, AccountName = s.AccountName, ContainerName = s.ContainerName, SasToken = s.SasToken };
                    }
                    else
                    {
                        logger.LogError($"Invalid value '{uri}' found in 'ExternalStorageContainers' configuration. Value must be a valid azure storage account or container URL.");
                        return null;
                    }
                })
                .Where(storageAccountInfo => storageAccountInfo is not null)
                .ToList();
        }

        /// <inheritdoc />
        public async Task<string> DownloadBlobAsync(string blobRelativePath)
        {
            try
            {
                return await this.azureProxy.DownloadBlobAsync(new Uri(await MapLocalPathToSasUrlAsync(blobRelativePath)));
            }
            catch
            {
                return null;
            }
        }

        /// <inheritdoc />
        public async Task<bool> TryDownloadBlobAsync(string blobRelativePath, Action<string> action)
        {
            try
            {
                var content = await this.azureProxy.DownloadBlobAsync(new Uri(await MapLocalPathToSasUrlAsync(blobRelativePath)));
                action?.Invoke(content);
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <inheritdoc />
        public async Task UploadBlobAsync(string blobRelativePath, string content)
            => await this.azureProxy.UploadBlobAsync(new Uri(await MapLocalPathToSasUrlAsync(blobRelativePath, true)), content);

        /// <inheritdoc />
        public async Task UploadBlobFromFileAsync(string blobRelativePath, string sourceLocalFilePath)
            => await this.azureProxy.UploadBlobFromFileAsync(new Uri(await MapLocalPathToSasUrlAsync(blobRelativePath, true)), sourceLocalFilePath);

        /// <inheritdoc />
        public async Task<bool> IsPublicHttpUrl(string uriString)
        {
            var isHttpUrl = Uri.TryCreate(uriString, UriKind.Absolute, out var uri) && (uri.Scheme.Equals(Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase) || uri.Scheme.Equals(Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase));

            if (!isHttpUrl)
            {
                return false;
            }

            if (HttpUtility.ParseQueryString(uri.Query).Get("sig") is not null)
            {
                return true;
            }

            if (StorageAccountUrlSegments.TryCreate(uriString, out var parts))
            {
                if (await TryGetStorageAccountInfoAsync(parts.AccountName))
                {
                    return false;
                }

                if (TryGetExternalStorageAccountInfo(parts.AccountName, parts.ContainerName, out _))
                {
                    return false;
                }
            }

            return true;
        }

        /// <inheritdoc />
        public async Task<string> MapLocalPathToSasUrlAsync(string path, bool getContainerSas = false)
        {
            // TODO: Optional: If path is /container/... where container matches the name of the container in the default storage account, prepend the account name to the path.
            // This would allow the user to omit the account name for files stored in the default storage account

            // /cromwell-executions/... URLs become /defaultStorageAccountName/cromwell-executions/... to unify how URLs starting with /acct/container/... pattern are handled.
            if (path.StartsWith(CromwellPathPrefix, StringComparison.OrdinalIgnoreCase))
            {
                path = $"/{defaultStorageAccountName}{path}";
            }

            if (!StorageAccountUrlSegments.TryCreate(path, out var pathSegments))
            {
                logger.LogError($"Could not parse path '{path}'.");
                return null;
            }

            if (TryGetExternalStorageAccountInfo(pathSegments.AccountName, pathSegments.ContainerName, out var externalStorageAccountInfo))
            {
                return new StorageAccountUrlSegments(externalStorageAccountInfo.BlobEndpoint, pathSegments.ContainerName, pathSegments.BlobName, externalStorageAccountInfo.SasToken).ToUriString();
            }
            else
            {
                StorageAccountInfo storageAccountInfo = null;

                if (!await TryGetStorageAccountInfoAsync(pathSegments.AccountName, info => storageAccountInfo = info))
                {
                    logger.LogError($"Could not find storage account '{pathSegments.AccountName}' corresponding to path '{path}'. Either the account does not exist or the TES app service does not have permission to it.");
                    return null;
                }

                try
                {
                    var accountKey = await azureProxy.GetStorageAccountKeyAsync(storageAccountInfo);
                    var resultPathSegments = new StorageAccountUrlSegments(storageAccountInfo.BlobEndpoint, pathSegments.ContainerName, pathSegments.BlobName);

                    if (pathSegments.IsContainer || getContainerSas)
                    {
                        var policy = new SharedAccessBlobPolicy()
                        {
                            Permissions = SharedAccessBlobPermissions.Add | SharedAccessBlobPermissions.Create | SharedAccessBlobPermissions.List | SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Write,
                            SharedAccessExpiryTime = DateTime.Now.Add(sasTokenDuration)
                        };

                        var containerUri = new StorageAccountUrlSegments(storageAccountInfo.BlobEndpoint, pathSegments.ContainerName).ToUri();
                        resultPathSegments.SasToken = new CloudBlobContainer(containerUri, new StorageCredentials(storageAccountInfo.Name, accountKey)).GetSharedAccessSignature(policy, null, SharedAccessProtocol.HttpsOnly, null);
                    }
                    else
                    {
                        var policy = new SharedAccessBlobPolicy() { Permissions = SharedAccessBlobPermissions.Read, SharedAccessExpiryTime = DateTime.Now.Add(sasTokenDuration) };
                        resultPathSegments.SasToken = new CloudBlob(resultPathSegments.ToUri(), new StorageCredentials(storageAccountInfo.Name, accountKey)).GetSharedAccessSignature(policy, null, null, SharedAccessProtocol.HttpsOnly, null);
                    }

                    return resultPathSegments.ToUriString();
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, $"Could not get the key of storage account '{pathSegments.AccountName}'. Make sure that the TES app service has Contributor access to it.");
                    return null;
                }
            }
        }

        private async Task<bool> TryGetStorageAccountInfoAsync(string accountName, Action<StorageAccountInfo> onSuccess = null)
        {
            try
            {
                var storageAccountInfo = await azureProxy.GetStorageAccountInfoAsync(accountName);

                if (storageAccountInfo is not null)
                {
                    onSuccess?.Invoke(storageAccountInfo);
                    return true;
                }
                else
                {
                    logger.LogError($"Could not find storage account '{accountName}'. Either the account does not exist or the TES app service does not have permission to it.");
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, $"Exception while getting storage account '{accountName}'");
            }

            return false;
        }

        private bool TryGetExternalStorageAccountInfo(string accountName, string containerName, out ExternalStorageContainerInfo result)
        {
            result = externalStorageContainers?.FirstOrDefault(c =>
                c.AccountName.Equals(accountName, StringComparison.OrdinalIgnoreCase)
                && (string.IsNullOrEmpty(c.ContainerName) || c.ContainerName.Equals(containerName, StringComparison.OrdinalIgnoreCase)));

            return result is not null;
        }

        private class ExternalStorageContainerInfo
        {
            public string AccountName { get; set; }
            public string ContainerName { get; set; }
            public string BlobEndpoint { get; set; }
            public string SasToken { get; set; }
        }
    }
}
