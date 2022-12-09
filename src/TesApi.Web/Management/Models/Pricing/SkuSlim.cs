namespace TesApi.Web.Management.Models.Pricing
{
    public class SkuSlim
    {
        public string ArmRegionName { get; set; }
        public string SkuName { get; set; }
        public bool IsLowPriority { get; set; }
        public double PricePerHour { get; set; }
    }
}
