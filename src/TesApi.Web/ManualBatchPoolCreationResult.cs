// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web
{
    /// <summary>
    /// The result of creating a manual batch pool (non-auto pool)
    /// </summary>
    public class ManualBatchPoolCreationResult
    {
        /// <summary>
        /// Indicates whether the pool is using a ContainerConfiguration
        /// </summary>
        public bool PoolHasContainerConfig { get; set; }
    }
}
