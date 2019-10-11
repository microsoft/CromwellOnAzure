// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace CromwellApiClient
{
    public class CromwellApiClient : ICromwellApiClient
    {
        private const string Version = "v1";
        private static readonly string basePath = $"/api/workflows/{Version}";
        private static readonly HttpClient httpClient = new HttpClient();
        private readonly string url;

        public CromwellApiClient(string baseUrl)
        {
            if (string.IsNullOrWhiteSpace(baseUrl))
            {
                throw new ArgumentException(nameof(baseUrl));
            }

            url = $"{baseUrl.TrimEnd('/')}{basePath}";
        }

        public string GetUrl()
        {
            return url;
        }

        public async Task<GetLogsResponse> GetLogsAsync(Guid id)
        {
            return await GetAsync<GetLogsResponse>($"/{id}/logs");
        }

        public async Task<GetOutputsResponse> GetOutputsAsync(Guid id)
        {
            return new GetOutputsResponse { Id = id, Json = await GetAsyncWithMediaType($"/{id}/outputs", "application/json") };
        }

        public async Task<GetMetadataResponse> GetMetadataAsync(Guid id)
        {
            return new GetMetadataResponse { Id = id, Json = await GetAsyncWithMediaType($"/{id}/metadata", "application/json") };
        }

        public async Task<GetStatusResponse> GetStatusAsync(Guid id)
        {
            return await GetAsync<GetStatusResponse>($"/{id}/status");
        }

        public async Task<GetTimingResponse> GetTimingAsync(Guid id)
        {
            return new GetTimingResponse { Id = id, Html = await GetAsyncWithMediaType($"/{id}/timing", "text/html") };
        }

        public async Task<PostAbortResponse> PostAbortAsync(Guid id)
        {
            return await PostAsync<PostAbortResponse>($"/{id}/abort", id);
        }

        public async Task<PostWorkflowResponse> PostWorkflowAsync(
            string workflowSourceFilename,
            byte[] workflowSourceData,
            string workflowInputsFilename,
            byte[] workflowInputsData,
            string workflowOptionsFilename = null,
            byte[] workflowOptionsData = null,
            string workflowDependenciesFilename = null,
            byte[] workflowDependenciesData = null)
        {
            var files = new List<FileToPost> {
                new FileToPost { ParameterName = "workflowSource", Filename = workflowSourceFilename, Data = workflowSourceData },
                new FileToPost { ParameterName = "workflowInputs", Filename = workflowInputsFilename, Data = workflowInputsData }
            };

            if (workflowOptionsFilename != null && workflowOptionsData != null)
            {
                files.Add(new FileToPost { ParameterName = "workflowOptions", Filename = workflowOptionsFilename, Data = workflowOptionsData });
            }

            if (workflowDependenciesFilename != null && workflowDependenciesData != null)
            {
                files.Add(new FileToPost { ParameterName = "workflowDependencies", Filename = workflowDependenciesFilename, Data = workflowDependenciesData });
            }

            return await PostAsync<PostWorkflowResponse>(string.Empty, files);
        }

        public async Task<PostQueryResponse> PostQueryAsync(string queryJson)
        {
            return await PostAsync<PostQueryResponse>("/query", queryJson);
        }

        private string GetApiUrl(string path)
        {
            return $"{url}{path}";
        }

        private async Task<T> GetAsync<T>(string path)
        {
            HttpResponseMessage response = null;
            var url = string.Empty;

            try
            {
                url = GetApiUrl(path);
                response = await httpClient.GetAsync(url);
                response.EnsureSuccessStatusCode();
                return await response.Content.ReadAsAsync<T>();
            }
            catch (HttpRequestException httpRequestException)
            {
                var messageBuilder = new StringBuilder();
                messageBuilder.AppendLine($"URL: {url}");
                messageBuilder.AppendLine($"StatusCode: {response?.StatusCode}");
                messageBuilder.AppendLine($"Exception message: {httpRequestException.Message}");
                throw new CromwellApiException(messageBuilder.ToString(), httpRequestException, response?.StatusCode);
            }
            catch (Exception exc)
            {
                throw new CromwellApiException(exc.Message, exc, response?.StatusCode);
            }
        }

        private async Task<string> GetAsyncWithMediaType(string path, string mediaType)
        {
            HttpResponseMessage response = null;
            var url = string.Empty;

            try
            {
                url = GetApiUrl(path);

                var request = new HttpRequestMessage()
                {
                    RequestUri = new Uri(url),
                    Method = HttpMethod.Get,
                };

                request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue(mediaType));
                response = await httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();
                return await response.Content.ReadAsStringAsync();
            }
            catch (HttpRequestException httpRequestException)
            {
                var messageBuilder = new StringBuilder();
                messageBuilder.AppendLine($"URL: {url}");
                messageBuilder.AppendLine($"StatusCode: {response?.StatusCode}");
                messageBuilder.AppendLine($"Exception message: {httpRequestException.Message}");
                throw new CromwellApiException(messageBuilder.ToString(), httpRequestException, response?.StatusCode);
            }
            catch (Exception exc)
            {
                throw new CromwellApiException(exc.Message, exc, response?.StatusCode);
            }
        }
        private async Task<string> GetAsyncAsString(string path)
        {
            HttpResponseMessage response = null;
            var url = string.Empty;

            try
            {
                url = GetApiUrl(path);
                response = await httpClient.GetAsync(url);
                response.EnsureSuccessStatusCode();
                return await response.Content.ReadAsStringAsync();
            }
            catch (HttpRequestException httpRequestException)
            {
                var messageBuilder = new StringBuilder();
                messageBuilder.AppendLine($"URL: {url}");
                messageBuilder.AppendLine($"StatusCode: {response?.StatusCode}");
                messageBuilder.AppendLine($"Exception message: {httpRequestException.Message}");
                throw new CromwellApiException(messageBuilder.ToString(), httpRequestException, response?.StatusCode);
            }
            catch (Exception exc)
            {
                throw new CromwellApiException(exc.Message, exc, response?.StatusCode);
            }
        }

        private async Task<T> PostAsync<T>(string path, Guid id)
        {
            HttpResponseMessage response = null;
            var url = string.Empty;

            try
            {
                url = GetApiUrl(path);
                var content = new FormUrlEncodedContent(new[] { new KeyValuePair<string, string>("id", id.ToString()) });
                response = await httpClient.PostAsync(url, content);
                response.EnsureSuccessStatusCode();
                return await response.Content.ReadAsAsync<T>();
            }
            catch (HttpRequestException httpRequestException)
            {
                var messageBuilder = new StringBuilder();
                messageBuilder.AppendLine($"URL: {url}");
                messageBuilder.AppendLine($"StatusCode: {response?.StatusCode}");
                messageBuilder.AppendLine($"Exception message: {httpRequestException.Message}");
                throw new CromwellApiException(messageBuilder.ToString(), httpRequestException, response?.StatusCode);
            }
            catch (Exception exc)
            {
                throw new CromwellApiException(exc.Message, exc, response?.StatusCode);
            }
        }

        private async Task<T> PostAsync<T>(string path, string body)
        {
            HttpResponseMessage response = null;
            var url = string.Empty;

            try
            {
                url = GetApiUrl(path);
                response = await httpClient.PostAsync(url, new StringContent(body, Encoding.UTF8, "application/json"));
                response.EnsureSuccessStatusCode();
                return await response.Content.ReadAsAsync<T>();
            }
            catch (HttpRequestException httpRequestException)
            {
                var messageBuilder = new StringBuilder();
                messageBuilder.AppendLine($"URL: {url}");
                messageBuilder.AppendLine($"StatusCode: {response?.StatusCode}");
                messageBuilder.AppendLine($"Exception message: {httpRequestException.Message}");
                throw new CromwellApiException(messageBuilder.ToString(), httpRequestException, response?.StatusCode);
            }
            catch (Exception exc)
            {
                throw new CromwellApiException(exc.Message, exc, response?.StatusCode);
            }
        }

        private async Task<T> PostAsync<T>(string path, IEnumerable<FileToPost> files)
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

                response = await httpClient.PostAsync(url, content);
                response.EnsureSuccessStatusCode();
                return await response.Content.ReadAsAsync<T>();
            }
            catch (HttpRequestException httpRequestException)
            {
                var messageBuilder = new StringBuilder();
                messageBuilder.AppendLine($"URL: {url}");
                messageBuilder.AppendLine($"StatusCode: {response?.StatusCode}");
                messageBuilder.AppendLine($"Exception message: {httpRequestException.Message}");
                throw new CromwellApiException(messageBuilder.ToString(), httpRequestException, response?.StatusCode);
            }
            catch (Exception exc)
            {
                throw new CromwellApiException(exc.Message, exc, response?.StatusCode);
            }
        }

        private class FileToPost
        {
            public string ParameterName { get; set; }
            public string Filename { get; set; }
            public byte[] Data { get; set; }
        }
    }
}
