using Microsoft.VisualStudio.TestTools.UnitTesting;
using TesApi.Web;

namespace TesApi.Tests
{
    [TestClass]
    public class AzureRegionUtilsTests
    {
        [TestMethod]
        public void Billing_Region_Name_For_westus_Is_Valid()
        {
            Assert.IsTrue(AzureRegionUtils.GetBillingRegionName("WESTUS") == "US West");
            Assert.IsTrue(AzureRegionUtils.GetBillingRegionName("westus") == "US West");
        }
    }
}
