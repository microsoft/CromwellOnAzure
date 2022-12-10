using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using TesApi.Web.Management.Models.Pricing;

namespace TesApi.Web.Management
{
    /// <summary>
    /// Azure Retail Price API client. 
    /// </summary>
    public class PriceApiClient
    {
        private static readonly HttpClient httpClient = new HttpClient();
        private static readonly Uri apiEndpoint = new Uri($"https://prices.azure.com/api/retail/prices");


        public async IAsyncEnumerable<PricingItem> GetAllPricingInformationAsync(string region)
        {
            var skip = 0;
            while (true)
            {
                var page = await GetPricingInformationPageAsync(skip, region);

                if (page is null || page.Items is null || page.Items.Length == 0)
                {
                    yield break;
                }


                foreach (var pricingItem in page.Items)
                {
                    yield return pricingItem;
                }

                skip = skip + 100;
            }
        }

        public IAsyncEnumerable<PricingItem> GetAllPricingInformationForNonWindowsAndNonSpotVmsAsync(string region)
        {
            return GetAllPricingInformationAsync(region)
                .WhereAwait(p => ValueTask.FromResult(!p.productName.Contains(" Windows") && !p.meterName.Contains(" Spot")));
        }

        public async Task<RetailPricingData> GetPricingInformationPageAsync(int skip, string region)
        {
            var builder = new UriBuilder(apiEndpoint);
            builder.Query = BuildRequestQueryString(skip, region);

            var response = await httpClient.GetStringAsync(builder.Uri);

            if (string.IsNullOrWhiteSpace(response))
            {
                return null;
            }

            return JsonSerializer.Deserialize<RetailPricingData>(response);
        }


        private string BuildRequestQueryString(int skip, string region)
        {
            var filter = ParseFilterCondition("and",
                ParseEq("serviceName", "Virtual Machines"),
                ParseEq("currencyCode", "USD"),
                ParseEq("priceType", "Consumption"),
                ParseEq("armRegionName", region),
                ParseEq("isPrimaryMeterRegion", true));

            var skipKeyValue = ParseQueryStringKeyIntValue("$skip", skip);

            return $"{filter}&{skipKeyValue}";

        }

        private string ParseFilterCondition(string conditionOperator, params string[] condition)
        {
            return $"$filter={String.Join($" {conditionOperator} ", condition)}";
        }

        private string ParseQueryStringKeyIntValue(string key, int value)
        {
            return $"{key}={value}";
        }

        private string ParseEq(string name, string value)
        {
            return $"{name} eq '{value}'";
        }
        private string ParseEq(string name, bool value)
        {
            return $"{name} eq {value.ToString().ToLowerInvariant()}";
        }

    }
}
