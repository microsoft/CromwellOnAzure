// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web
{
    /// <summary>
    /// AzureBatchNodeCount
    /// </summary>
    public struct AzureBatchNodeCount
    {
        /// <summary>
        /// VirtualMachineSize
        /// </summary>
        public string VirtualMachineSize { get; set; }

        /// <summary>
        /// DedicatedNodeCount
        /// </summary>
        public int DedicatedNodeCount { get; set; }

        /// <summary>
        /// LowPriorityNodeCount
        /// </summary>
        public int LowPriorityNodeCount { get; set; }
    }
}
