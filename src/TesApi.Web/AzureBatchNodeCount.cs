// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web
{
    public struct AzureBatchNodeCount
    {
        public string VirtualMachineSize { get; set; }
        public int DedicatedNodeCount { get; set; }
        public int LowPriorityNodeCount { get; set; }
    }
}
