// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Tasks;
using CromwellApiClient;

namespace TriggerService
{
    public interface ICromwellOnAzureEnvironment
    {
        Task AbortWorkflowsAsync();
        Task ExecuteNewWorkflowsAsync();
        Task<ProcessedWorkflowItem> GetBlobFileNameAndData(string url);
        Task<bool> IsAzureStorageAvailableAsync();
        Task<bool> IsCromwellAvailableAsync();
        Task ProcessAndAbortWorkflowsAsync();
        Task UpdateExistingWorkflowsAsync();
        Task UpdateWorkflowStatusesAsync();
    }
}
