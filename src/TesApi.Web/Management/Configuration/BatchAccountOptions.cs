namespace TesApi.Web.Management.Configuration
{
    public class BatchAccountOptions
    {
        public const string BatchAccount = "BatchAccount";
        public const string DefaultAzureOfferDurableId = "MS-AZR-0003p";

        public string AccountName { get; set; }
        public string BaseUrl { get; set; }
        public string AppKey { get; set; }
        public string Region { get; set; }
        public string SubscriptionId { get; set; }
        public string ResourceGroup { get; set; }
        public string AzureOfferDurableId { get; set; } = DefaultAzureOfferDurableId;
    }
}
