// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web
{
    /// <summary>
    /// A representation associating an Azure Storage account with an Azure Subscription
    /// </summary>
    public class StorageAccountInfo
    {
        /// <summary>
        /// The storage account id
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// The storage account name
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The storage account blob endpoint
        /// </summary>
        public string BlobEndpoint { get; set; }

        /// <summary>
        /// The subscription id associated with the storage account
        /// </summary>
        public string SubscriptionId { get; set; }
    }
}
