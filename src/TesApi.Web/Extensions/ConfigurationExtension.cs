using Microsoft.Extensions.Configuration;

namespace TesApi.Web.Extensions
{
    public static class ConfigurationExtension
    {
        public static bool IsPresent(this IConfiguration config, string key)
        {
            return !string.IsNullOrWhiteSpace(config["key"]);
        }

    }
}
