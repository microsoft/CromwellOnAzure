// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;

namespace TriggerService
{
    public class StorageAccountInfo
    {
        public Azure.ResourceManager.Storage.StorageAccountResource StorageAccount { get; set; }
        public string Name { get; set; }
        public Uri BlobEndpoint { get; set; }
        public Azure.ResourceManager.Resources.SubscriptionResource Subscription { get; set; }
    }
}
