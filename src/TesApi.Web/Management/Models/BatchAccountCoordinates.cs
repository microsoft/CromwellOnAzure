namespace TesApi.Web.Management.Models;

public record BatchAccountCoordinates(string BaseUrl, string AccountName, string KeyValue, string Location,
    string SubscriptionId, string ResourceGroup)
{
}
