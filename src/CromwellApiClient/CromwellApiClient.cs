// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;

[assembly: InternalsVisibleTo("TriggerService.Tests")]
namespace CromwellApiClient
{
    public class CromwellApiClient : ICromwellApiClient
    {
        private const string Version = "v1";
        private static readonly string basePath = $"/api/workflows/{Version}";
        private static readonly HttpClient httpClient = new();
        private readonly string url;

        public CromwellApiClient(IOptions<CromwellApiClientOptions> cromwellApiClientOptions)
        {
            ArgumentException.ThrowIfNullOrEmpty(cromwellApiClientOptions.Value.BaseUrl, nameof(cromwellApiClientOptions.Value.BaseUrl));

            Common.NewtonsoftJsonSafeInit.SetDefaultSettings();

            url = $"{cromwellApiClientOptions.Value.BaseUrl.TrimEnd('/')}{basePath}";
        }

        /// <inheritdoc/>
        public string GetUrl()
            => url;

        /// <inheritdoc/>
        public async Task<GetLogsResponse> GetLogsAsync(Guid id, CancellationToken cancellationToken)
            => await GetAsync<GetLogsResponse>($"/{id}/logs", cancellationToken);

        /// <inheritdoc/>
        public async Task<GetOutputsResponse> GetOutputsAsync(Guid id, CancellationToken cancellationToken)
            => new() { Id = id, Json = await GetAsyncWithMediaType($"/{id}/outputs", "application/json", cancellationToken) };

        /// <inheritdoc/>
        public async Task<GetMetadataResponse> GetMetadataAsync(Guid id, CancellationToken cancellationToken)
            => new() { Id = id, Json = await GetAsyncWithMediaType($"/{id}/metadata?expandSubWorkflows=true", "application/json", cancellationToken) };

        /// <inheritdoc/>
        /// <inheritdoc/>
        public async Task<GetStatusResponse> GetStatusAsync(Guid id, CancellationToken cancellationToken)
            => await GetAsync<GetStatusResponse>($"/{id}/status", cancellationToken);

        /// <inheritdoc/>
        public async Task<GetTimingResponse> GetTimingAsync(Guid id, CancellationToken cancellationToken)
            => new() { Id = id, Html = await GetAsyncWithMediaType($"/{id}/timing", "text/html", cancellationToken) };

        /// <inheritdoc/>
        public async Task<PostAbortResponse> PostAbortAsync(Guid id, CancellationToken cancellationToken)
            => await PostAsync<PostAbortResponse>($"/{id}/abort", id, cancellationToken);

        /// <inheritdoc/>
        public async Task<PostWorkflowResponse> PostWorkflowAsync(
            string workflowSourceFilename,
            byte[] workflowSourceData,
            List<string> workflowInputsFilename,
            List<byte[]> workflowInputsData,
            CancellationToken cancellationToken,
            string workflowOptionsFilename = null,
            byte[] workflowOptionsData = null,
            string workflowDependenciesFilename = null,
            byte[] workflowDependenciesData = null,
            string workflowLabelsFilename = null, byte[] workflowLabelsData = null)
        {
            var files = AccumulatePostFiles(
                workflowSourceFilename,
                workflowSourceData,
                workflowInputsFilename,
                workflowInputsData,
                workflowOptionsFilename,
                workflowOptionsData,
                workflowDependenciesFilename,
                workflowDependenciesData,
                workflowLabelsFilename,
                workflowLabelsData);
            return await PostAsync<PostWorkflowResponse>(string.Empty, files, cancellationToken);
        }

        internal static List<FileToPost> AccumulatePostFiles(
            string workflowSourceFilename,
            byte[] workflowSourceData,
            List<string> workflowInputsFilename,
            List<byte[]> workflowInputsData,
            string workflowOptionsFilename = null,
            byte[] workflowOptionsData = null,
            string workflowDependenciesFilename = null,
            byte[] workflowDependenciesData = null,
            string workflowLabelsFilename = null,
            byte[] workflowLabelsData = null)
        {
            var files = new List<FileToPost> {
                new(workflowSourceFilename, workflowSourceData, "workflowSource", removeTabs: true)
            };

            for (var i = 0; i < workflowInputsFilename.Count; i++)
            {
                var parameterName = i == 0 ? "workflowInputs" : "workflowInputs_" + (i + 1);
                files.Add(new(workflowInputsFilename[i], workflowInputsData[i], parameterName, removeTabs: true));
            }

            if (workflowOptionsFilename is not null && workflowOptionsData is not null)
            {
                files.Add(new(workflowOptionsFilename, workflowOptionsData, "workflowOptions", removeTabs: true));
            }

            if (workflowDependenciesFilename is not null && workflowDependenciesData is not null)
            {
                files.Add(new(workflowDependenciesFilename, workflowDependenciesData, "workflowDependencies"));
            }

            if (workflowLabelsFilename is not null && workflowLabelsData is not null)
            {
                files.Add(new FileToPost(workflowLabelsFilename, workflowLabelsData, "workflowLabels"));
            }

            return files;
        }

        /// <inheritdoc/>
        public async Task<PostQueryResponse> PostQueryAsync(string queryJson, CancellationToken cancellationToken)
            => await PostAsync<PostQueryResponse>("/query", queryJson, cancellationToken);

        private string GetApiUrl(string path)
            => $"{url}{path}";

        private async Task<T> GetAsync<T>(string path, CancellationToken cancellationToken)
        {
            HttpResponseMessage response = null;
            var url = string.Empty;

            try
            {
                url = GetApiUrl(path);
                response = await httpClient.GetAsync(url, cancellationToken);
                response.EnsureSuccessStatusCode();
                return await response.Content.ReadAsAsync<T>(cancellationToken);
            }
            catch (HttpRequestException httpRequestException)
            {
                throw await WrapHttpRequestExceptionAsync(httpRequestException, url, response, cancellationToken);
            }
            catch (Exception exc)
            {
                throw new CromwellApiException(exc.Message, exc, response?.StatusCode);
            }
        }

        private async Task<string> GetAsyncWithMediaType(string path, string mediaType, CancellationToken cancellationToken)
        {
            HttpResponseMessage response = null;
            var url = string.Empty;

            try
            {
                url = GetApiUrl(path);

                var request = new HttpRequestMessage()
                {
                    RequestUri = new(url),
                    Method = HttpMethod.Get,
                };

                request.Headers.Accept.Add(new(mediaType));
                response = await httpClient.SendAsync(request, cancellationToken);
                response.EnsureSuccessStatusCode();
                return await response.Content.ReadAsStringAsync(cancellationToken);
            }
            catch (HttpRequestException httpRequestException)
            {
                throw await WrapHttpRequestExceptionAsync(httpRequestException, url, response, cancellationToken);
            }
            catch (Exception exc)
            {
                throw new CromwellApiException(exc.Message, exc, response?.StatusCode);
            }
        }

        private async Task<T> PostAsync<T>(string path, Guid id, CancellationToken cancellationToken)
        {
            HttpResponseMessage response = null;
            var url = string.Empty;

            try
            {
                url = GetApiUrl(path);
                var content = new FormUrlEncodedContent([new("id", id.ToString())]);
                response = await httpClient.PostAsync(url, content, cancellationToken);
                response.EnsureSuccessStatusCode();
                return await response.Content.ReadAsAsync<T>(cancellationToken);
            }
            catch (HttpRequestException httpRequestException)
            {
                throw await WrapHttpRequestExceptionAsync(httpRequestException, url, response, cancellationToken);
            }
            catch (Exception exc)
            {
                throw new CromwellApiException(exc.Message, exc, response?.StatusCode);
            }
        }

        private async Task<T> PostAsync<T>(string path, string body, CancellationToken cancellationToken)
        {
            HttpResponseMessage response = null;
            var url = string.Empty;

            try
            {
                url = GetApiUrl(path);
                response = await httpClient.PostAsync(url, new StringContent(body, Encoding.UTF8, "application/json"), cancellationToken);
                response.EnsureSuccessStatusCode();
                return await response.Content.ReadAsAsync<T>(cancellationToken);
            }
            catch (HttpRequestException httpRequestException)
            {
                throw await WrapHttpRequestExceptionAsync(httpRequestException, url, response, cancellationToken);
            }
            catch (Exception exc)
            {
                throw new CromwellApiException(exc.Message, exc, response?.StatusCode);
            }
        }

        private async Task<T> PostAsync<T>(string path, IEnumerable<FileToPost> files, CancellationToken cancellationToken)
        {
            HttpResponseMessage response = null;
            var url = string.Empty;

            try
            {
                url = GetApiUrl(path);
                var content = new MultipartFormDataContent();

                foreach (var file in files)
                {
                    var contentPart = new ByteArrayContent(file.Data);
                    content.Add(contentPart, file.ParameterName, file.Filename);
                }

                response = await httpClient.PostAsync(url, content, cancellationToken);
                response.EnsureSuccessStatusCode();
                return await response.Content.ReadAsAsync<T>(cancellationToken);
            }
            catch (HttpRequestException httpRequestException)
            {
                throw await WrapHttpRequestExceptionAsync(httpRequestException, url, response, cancellationToken);
            }
            catch (Exception exc)
            {
                throw new CromwellApiException(exc.Message, exc, response?.StatusCode);
            }
        }

        private static async ValueTask<CromwellApiException> WrapHttpRequestExceptionAsync(HttpRequestException httpRequestException, string url, HttpResponseMessage response, CancellationToken cancellationToken)
        {
            var messageBuilder = new StringBuilder();
            messageBuilder.AppendLine($"URL: {url}");
            messageBuilder.AppendLine($"StatusCode: {response?.StatusCode}");
            messageBuilder.AppendLine($"Exception message: {httpRequestException.Message}");

            await AppendResponseBodyAsync(response, messageBuilder, cancellationToken);

            return new (messageBuilder.ToString(), httpRequestException, response?.StatusCode);
        }

        private static async Task AppendResponseBodyAsync(HttpResponseMessage response, StringBuilder messageBuilder, CancellationToken cancellationToken)
        {
            try
            {
                // Attempt to append the response body for additional error info
                var contents = await response.Content.ReadAsStringAsync(cancellationToken);
                messageBuilder.AppendLine(contents);
            }
            catch
            {
                // Ignore exceptions. Retrieve extra error info only if possible
            }
        }

        internal class FileToPost
        {
            public string ParameterName { get; set; }
            public string Filename { get; set; }
            public byte[] Data { get; set; }

            internal FileToPost(string filename, byte[] data, string parameterName, bool removeTabs = false)
            {
                this.Filename = filename;
                this.ParameterName = parameterName;

                this.Data = removeTabs ? EncodeToUtf8AndRemoveTabsAndDecode(data) : data;
            }

            /// <summary>
            /// Encodes a byte array to Utf8, removes tabs, and decodes back to a byte array.
            /// As of 1/10/2020, Cromwell has a bug that requires tabs to be removed from JSON data
            /// https://github.com/broadinstitute/cromwell/issues/3487
            /// </summary>
            /// <param name="data">The byte array of the file</param>
            /// <returns>A new byte array of the file</returns>
            private static byte[] EncodeToUtf8AndRemoveTabsAndDecode(byte[] data)
            {
                if ((data?.Length ?? 0) == 0)
                {
                    return data;
                }

                return Encoding.UTF8.GetBytes(Encoding.UTF8.GetString(data).Replace("\t", " ")); // Simply removing an embedded tab may change the meaning and break JSON. The safest option is to replace one kind of whitespace with another.
            }
        }
    }
}
