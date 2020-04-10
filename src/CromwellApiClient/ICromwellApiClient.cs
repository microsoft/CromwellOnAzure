// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CromwellApiClient
{
    public interface ICromwellApiClient
    {
        string GetUrl();
        Task<PostWorkflowResponse> PostWorkflowAsync(
            string workflowSourceFilename, byte[] workflowSourceData,
            List<string> workflowInputsFilename, List<byte[]> workflowInputsData,
            string workflowOptionsFilename = null, byte[] workflowOptionsData = null,
            string workflowDependenciesFilename = null, byte[] workflowDependenciesData = null);
        Task<GetStatusResponse> GetStatusAsync(Guid id);
        Task<GetOutputsResponse> GetOutputsAsync(Guid id);
        Task<GetMetadataResponse> GetMetadataAsync(Guid id);
        Task<GetLogsResponse> GetLogsAsync(Guid id);
        Task<PostAbortResponse> PostAbortAsync(Guid id);
        Task<GetTimingResponse> GetTimingAsync(Guid id);
        Task<PostQueryResponse> PostQueryAsync(string queryJson);
    }
}
