// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;

namespace CromwellOnAzureDeployer
{
    public class CosmosRestClient
    {
        private readonly string endpoint;
        private readonly string key;

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2208:Instantiate argument exceptions correctly", Justification = "Matching standard.")]
        public CosmosRestClient(string endpoint, string key)
        {
            this.endpoint = endpoint ?? throw new ArgumentNullException("accountEndpoint");
            this.key = key ?? throw new ArgumentNullException("authKeyOrResourceToken");
        }

        public async Task<ThroughputProperties> GetContainerRequestThroughputAsync(string databaseName, string containerName)
        {
            var containerResponse = await GetAsync(this.endpoint, $"dbs/{databaseName}/colls/{containerName}", this.key, "colls", $"dbs/{databaseName}/colls/{containerName}");
            var containerResourceId = JsonConvert.DeserializeObject<JObject>(containerResponse)["_rid"];

            var query = $"{{\"query\":\"select * from root r where r.offerResourceId='{containerResourceId}'\"}}";
            var offerQueryResponse = await QueryAsync(this.endpoint, "offers", this.key, "offers", string.Empty, query);

            return JsonConvert.DeserializeObject<OfferQueryResponse>(offerQueryResponse).Offers.FirstOrDefault();
        }

        public async Task SwitchContainerRequestThroughputToAutoAsync(string databaseName, string containerName)
        {
            var offer = await GetContainerRequestThroughputAsync(databaseName, containerName);
            await PutAsync(this.endpoint, offer.SelfLink, this.key, "offers", offer.OfferRID.ToLower(), JsonConvert.SerializeObject(offer), ("x-ms-cosmos-migrate-offer-to-autopilot", "true"));
        }

        private static async Task<string> GetAsync(string baseUrl, string relativeUrl, string key, string resourceType, string resourceId)
        {
            var dateString = DateTime.UtcNow.ToString("R");
            var authHeader = GenerateAuthToken("GET", resourceType, resourceId, dateString, key, "master", "1.0");

            var client = new HttpClient();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(authHeader);
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            client.DefaultRequestHeaders.Add("x-ms-version", "2018-12-31");
            client.DefaultRequestHeaders.Add("x-ms-date", dateString);

            return await client.GetStringAsync($"{baseUrl}/{relativeUrl}");
        }

        private static async Task<string> QueryAsync(string baseUrl, string relativeUrl, string key, string resourceType, string resourceId, string body)
        {
            var dateString = DateTime.UtcNow.ToString("R");
            var authHeader = GenerateAuthToken("POST", resourceType, resourceId, dateString, key, "master", "1.0");

            var client = new HttpClient();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(authHeader);
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            client.DefaultRequestHeaders.Add("x-ms-version", "2018-12-31");
            client.DefaultRequestHeaders.Add("x-ms-date", dateString);
            client.DefaultRequestHeaders.Add("x-ms-documentdb-isquery", "True");

            var content = new StringContent(body);
            content.Headers.ContentType = new MediaTypeWithQualityHeaderValue("application/query+json");
            var result = await client.PostAsync($"{baseUrl}/{relativeUrl}", content);

            return await result.Content.ReadAsStringAsync();
        }

        private static async Task<string> PutAsync(string baseUrl, string relativeUrl, string key, string resourceType, string resourceId, string body, params (string name, string value)[] additionalHeaders)
        {
            var dateString = DateTime.UtcNow.ToString("R");
            var authHeader = GenerateAuthToken("PUT", resourceType, resourceId, dateString, key, "master", "1.0");

            var client = new HttpClient();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(authHeader);
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            client.DefaultRequestHeaders.Add("x-ms-version", "2018-12-31");
            client.DefaultRequestHeaders.Add("x-ms-date", dateString);

            foreach (var (name, value) in additionalHeaders)
            {
                client.DefaultRequestHeaders.Add(name, value);
            }

            var content = new StringContent(body, Encoding.UTF8, "application/json");
            var result = await client.PutAsync($"{baseUrl}/{relativeUrl}", content);

            return await result.Content.ReadAsStringAsync();
        }

        private static string GenerateAuthToken(string verb, string resourceType, string resourceId, string date, string key, string keyType, string tokenVersion)
        {
            var hmacSha256 = new System.Security.Cryptography.HMACSHA256 { Key = Convert.FromBase64String(key) };

            verb ??= string.Empty;
            resourceType ??= string.Empty;
            resourceId ??= string.Empty;

            var payload = string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}\n{1}\n{2}\n{3}\n{4}\n",
                    verb.ToLowerInvariant(),
                    resourceType.ToLowerInvariant(),
                    resourceId,
                    date.ToLowerInvariant(),
                    string.Empty
            );

            var hashPayload = hmacSha256.ComputeHash(System.Text.Encoding.UTF8.GetBytes(payload));
            var signature = Convert.ToBase64String(hashPayload);

            return System.Web.HttpUtility.UrlEncode(String.Format(System.Globalization.CultureInfo.InvariantCulture, "type={0}&ver={1}&sig={2}",
                keyType,
                tokenVersion,
                signature));
        }
    }

    public class OfferQueryResponse
    {
        [JsonProperty(PropertyName = "Offers", NullValueHandling = NullValueHandling.Ignore)]
        public ThroughputProperties[] Offers { get; set; }
    }

    public class ThroughputProperties
    {
        [JsonProperty(PropertyName = "_etag", NullValueHandling = NullValueHandling.Ignore)]
        public string ETag { get; set; }

        [JsonConverter(typeof(UnixDateTimeConverter))]
        [JsonProperty(PropertyName = "_ts", NullValueHandling = NullValueHandling.Ignore)]
        public DateTime? LastModified { get; set; }

        [JsonProperty(PropertyName = "_self", NullValueHandling = NullValueHandling.Ignore)]
        public string SelfLink { get; set; }

        [JsonProperty(PropertyName = "_rid", NullValueHandling = NullValueHandling.Ignore)]
        public string OfferRID { get; set; }

        [JsonProperty(PropertyName = "offerResourceId", NullValueHandling = NullValueHandling.Ignore)]
        public string ResourceRID { get; set; }

        [JsonProperty(PropertyName = "offerVersion", DefaultValueHandling = DefaultValueHandling.Ignore)]
        public string OfferVersion { get; set; }

        [JsonProperty(PropertyName = "content", DefaultValueHandling = DefaultValueHandling.Ignore)]
        public OfferContentProperties Content { get; set; }

        [JsonIgnore]
        public int? Throughput => Content?.OfferThroughput;

        [JsonIgnore]
        public int? AutoscaleMaxThroughput => Content?.OfferAutoscaleSettings?.MaxThroughput;
    }

    public class OfferContentProperties
    {
        [JsonProperty(PropertyName = "offerThroughput", DefaultValueHandling = DefaultValueHandling.Ignore)]
        public int? OfferThroughput { get; set; }

        [JsonProperty(PropertyName = "offerAutopilotSettings", DefaultValueHandling = DefaultValueHandling.Ignore)]
        public OfferAutoscaleProperties OfferAutoscaleSettings { get; set; }
    }

    public class OfferAutoscaleProperties
    {
        [JsonProperty(PropertyName = "maxThroughput", DefaultValueHandling = DefaultValueHandling.Ignore)]
        public int? MaxThroughput { get; set; }
    }
}
