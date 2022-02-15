// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web
{
    /// <summary>
    /// Gets the counts of active batch nodes for each VmSize
    /// </summary>
    public struct AzureBatchNodeCount
    {
        /// <summary>
        /// VmSize for which counts are accumulated.
        /// </summary>
        public string VirtualMachineSize { get; set; }
        /// <summary>
        /// Gets the number of dedicated compute nodes targeted or currently in the pool.
        /// </summary>
        public int DedicatedNodeCount { get; set; }
        /// <summary>
        /// Gets the number of low-priority compute nodes targeted or currently in the pool.
        /// </summary>
        public int LowPriorityNodeCount { get; set; }
    }
}
