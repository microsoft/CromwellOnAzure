using System;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace CromwellOnAzureDeployer
{
    public class CosmosRestClient
    {
        private readonly string endpoint;
        private readonly string key;

        public CosmosRestClient(string endpoint, string key)
        {
            if (endpoint == null)
            {
                throw new ArgumentNullException("accountEndpoint");
            }

            if (key == null)
            {
                throw new ArgumentNullException("authKeyOrResourceToken");
            }

            this.endpoint = endpoint;
            this.key = key;
        }

        public async Task<ThroughputProperties> GetContainerRequestThroughputAsync(string databaseName, string containerName)
        {
            var containerResponse = await GetAsync(this.endpoint, $"dbs/{databaseName}/colls/{containerName}", this.key, "colls", $"dbs/{databaseName}/colls/{containerName}");
            var containerResourceId = JsonDocument.Parse(containerResponse).RootElement.GetProperty("_rid").GetString();

            var query = $"{{\"query\":\"select * from root r where r.offerResourceId='{containerResourceId}'\"}}";
            var offerQueryResponse = await QueryAsync(this.endpoint, "offers", this.key, "offers", "", query);

            return JsonSerializer.Deserialize<OfferQueryResponse>(offerQueryResponse).Offers.FirstOrDefault();
        }

        public async Task SwitchContainerRequestThroughputToAutoAsync(string databaseName, string containerName)
        {
            var offer = await GetContainerRequestThroughputAsync(databaseName, containerName);
            await PutAsync(this.endpoint, offer.SelfLink, this.key, "offers", offer.OfferRID.ToLower(), JsonSerializer.Serialize(offer), ("x-ms-cosmos-migrate-offer-to-autopilot", "true"));
        }

        async Task<string> GetAsync(string baseUrl, string relativeUrl, string key, string resourceType, string resourceId)
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

        private async Task<string> QueryAsync(string baseUrl, string relativeUrl, string key, string resourceType, string resourceId, string body)
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

        async Task<string> PutAsync(string baseUrl, string relativeUrl, string key, string resourceType, string resourceId, string body, params (string name, string value)[] additionalHeaders)
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

        private string GenerateAuthToken(string verb, string resourceType, string resourceId, string date, string key, string keyType, string tokenVersion)
        {
            var hmacSha256 = new System.Security.Cryptography.HMACSHA256 { Key = Convert.FromBase64String(key) };

            verb = verb ?? "";
            resourceType = resourceType ?? "";
            resourceId = resourceId ?? "";

            string payload = string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}\n{1}\n{2}\n{3}\n{4}\n",
                    verb.ToLowerInvariant(),
                    resourceType.ToLowerInvariant(),
                    resourceId,
                    date.ToLowerInvariant(),
                    ""
            );

            byte[] hashPayload = hmacSha256.ComputeHash(System.Text.Encoding.UTF8.GetBytes(payload));
            string signature = Convert.ToBase64String(hashPayload);

            return System.Web.HttpUtility.UrlEncode(String.Format(System.Globalization.CultureInfo.InvariantCulture, "type={0}&ver={1}&sig={2}",
                keyType,
                tokenVersion,
                signature));
        }
    }

    public class OfferQueryResponse
    {
        [JsonPropertyName("Offers")]
        public ThroughputProperties[] Offers { get; set; }
    }

    public class ThroughputProperties
    {
        [JsonPropertyName("_etag")]
        public string ETag { get; set; }

        [JsonPropertyName("_ts")]
        public DateTime? LastModified { get; set; }

        [JsonPropertyName("_self")]
        public string SelfLink { get; set; }

        [JsonPropertyName("_rid")]
        public string OfferRID { get; set; }

        [JsonPropertyName("offerResourceId")]
        public string ResourceRID { get; set; }

        [JsonPropertyName("offerVersion")]
        public string OfferVersion { get; set; }

        [JsonPropertyName("content")]
        public OfferContentProperties Content { get; set; }

        [JsonIgnore]
        public int? Throughput => Content?.OfferThroughput;

        [JsonIgnore]
        public int? AutoscaleMaxThroughput => Content?.OfferAutoscaleSettings?.MaxThroughput;
    }

    public class OfferContentProperties
    {
        [JsonPropertyName("offerThroughput")]
        public int? OfferThroughput { get; set; }

        [JsonPropertyName("offerAutopilotSettings")]
        public OfferAutoscaleProperties OfferAutoscaleSettings { get; set; }
    }

    public class OfferAutoscaleProperties
    {
        [JsonPropertyName("maxThroughput")]
        public int? MaxThroughput { get; set; }
    }
}
