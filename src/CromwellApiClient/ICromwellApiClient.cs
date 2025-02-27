// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace CromwellApiClient
{
    public interface ICromwellApiClient
    {
        /// <summary>
        /// Cromwell service URL
        /// </summary>
        /// <returns>URL of the cromwell service.</returns>
        string GetUrl();

        /// <summary>
        /// Submit a workflow
        /// </summary>
        /// <param name="workflowSourceFilename">Workflow file name.</param>
        /// <param name="workflowSourceData">Workflow file content.</param>
        /// <param name="workflowInputsFilename">Inputs data file name.</param>
        /// <param name="workflowInputsData">Inputs data file content.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="workflowOptionsFilename">Options data file name.</param>
        /// <param name="workflowOptionsData">Options data file content.</param>
        /// <param name="workflowDependenciesFilename">Workflow imports zip file name.</param>
        /// <param name="workflowDependenciesData">Workflow imports zip file content.</param>
        /// <param name="workflowLabelsFilename">Labels file name.</param>
        /// <param name="workflowLabelsData">Labels file content.</param>
        /// <returns>A <see cref="PostWorkflowResponse"/.></returns>
        Task<PostWorkflowResponse> PostWorkflowAsync(
            string workflowSourceFilename, byte[] workflowSourceData,
            List<string> workflowInputsFilename, List<byte[]> workflowInputsData,
            CancellationToken cancellationToken, string workflowOptionsFilename = null,
            byte[] workflowOptionsData = null, string workflowDependenciesFilename = null,
            byte[] workflowDependenciesData = null, string workflowLabelsFilename = null, byte[] workflowLabelsData = null);

        /// <summary>
        /// Get the <see cref="WorkflowStatus"/> of the workflow
        /// </summary>
        /// <param name="id">Workflow Id.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>The <see cref="WorkflowStatus"/> of the workflow.</returns>
        Task<GetStatusResponse> GetStatusAsync(Guid id, CancellationToken cancellationToken);

        /// <summary>
        /// Get the workflow outputs
        /// </summary>
        /// <param name="id">Workflow Id.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>JSON outputs of the workflow.</returns>
        Task<GetOutputsResponse> GetOutputsAsync(Guid id, CancellationToken cancellationToken);

        /// <summary>
        /// Get the workflow metadata
        /// </summary>
        /// <param name="id">Workflow Id.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>JSON metadata of the workflow.</returns>
        Task<GetMetadataResponse> GetMetadataAsync(Guid id, CancellationToken cancellationToken);

        /// <summary>
        /// Get the logs of the workflow's calls
        /// </summary>
        /// <param name="id">Workflow Id.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>Logs of the various workflow calls.</returns>
        Task<GetLogsResponse> GetLogsAsync(Guid id, CancellationToken cancellationToken);

        /// <summary>
        /// Request workflow cancellation
        /// </summary>
        /// <param name="id">Workflow Id.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>Workflow Id.</returns>
        Task<PostAbortResponse> PostAbortAsync(Guid id, CancellationToken cancellationToken);

        /// <summary>
        /// Get workflow timing
        /// </summary>
        /// <param name="id">Workflow Id.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>HTML of the timing of the workflow's operation.</returns>
        Task<GetTimingResponse> GetTimingAsync(Guid id, CancellationToken cancellationToken);

        /// <summary>
        /// List workflows via a query.
        /// </summary>
        /// <param name="queryJson">JSON query parameters.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>Quantity of matching workflows.</returns>
        Task<PostQueryResponse> PostQueryAsync(string queryJson, CancellationToken cancellationToken);
    }
}
