namespace TesApi.Web.Management.Models.Pricing
{
    public class RetailPricingData
    {
        public string BillingCurrency { get; set; }
        public string CustomerEntityId { get; set; }
        public string CustomerEntityType { get; set; }
        public PricingItem[] Items { get; set; }
        public string NextPageLink { get; set; }
        public int Count { get; set; }
    }
}
