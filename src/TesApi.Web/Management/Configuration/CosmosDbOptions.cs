namespace TesApi.Web.Management.Configuration
{
    public class CosmosDbOptions
    {
        public const string CosmosDbAccount = "CosmosDb";
        public string CosmosDbKey { get; set; }
        public string CosmosDbEndpoint { get; set; }
        public string AccountName { get; set; }
    }
}
