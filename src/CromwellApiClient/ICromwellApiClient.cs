// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading.Tasks;

namespace CromwellApiClient
{
    public interface ICromwellApiClient
    {
        string GetUrl();
        Task<PostWorkflowResponse> PostWorkflowAsync(ProcessedTriggerInfo processedTriggerInfo);
        Task<GetStatusResponse> GetStatusAsync(Guid id);
        Task<GetOutputsResponse> GetOutputsAsync(Guid id);
        Task<GetMetadataResponse> GetMetadataAsync(Guid id);
        Task<GetLogsResponse> GetLogsAsync(Guid id);
        Task<PostAbortResponse> PostAbortAsync(Guid id);
        Task<GetTimingResponse> GetTimingAsync(Guid id);
        Task<PostQueryResponse> PostQueryAsync(string queryJson);
    }
}
