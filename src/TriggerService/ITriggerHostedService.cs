// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Tasks;
using CromwellApiClient;

namespace TriggerService
{
    public interface ITriggerHostedService
    {
        /// <summary>
        /// Queries and processes status of all trigger blobs with Abort prefix
        /// </summary>
        Task AbortWorkflowsAsync();

        /// <summary>
        /// Queries and processes status of all trigger blobs with New prefix
        /// </summary>
        Task ExecuteNewWorkflowsAsync();

        /// <summary>
        /// Downloads and returns simple filename and content at <paramref name="url"/>"/>
        /// </summary>
        Task<ProcessedWorkflowItem> GetBlobFileNameAndData(string url);

        /// <summary>
        /// Checks availability of storage account blob service
        /// </summary>
        Task<bool> IsAzureStorageAvailableAsync();

        /// <summary>
        /// Checks availability of cromwell server's api
        /// </summary>
        Task<bool> IsCromwellAvailableAsync();

        /// <summary>
        /// Processes abort and new workflow requests
        /// </summary>
        Task ProcessAndAbortWorkflowsAsync();

        /// <summary>
        /// Processes running workflows
        /// </summary>
        Task UpdateExistingWorkflowsAsync();

        /// <summary>
        /// Queries and processes status of all trigger blobs with InProgress prefix
        /// </summary>
        Task UpdateWorkflowStatusesAsync();
    }
}
