// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TriggerService
{
    public class StorageAccountInfo
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string BlobEndpoint { get; set; }
        public string SubscriptionId { get; set; }
    }
}
