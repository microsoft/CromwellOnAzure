using Microsoft.VisualStudio.TestTools.UnitTesting;
using TesApi.Web;

namespace TesApi.Tests
{
    [TestClass]
    public class AzureRegionUtilsTests
    {
        [TestMethod]
        public void Alternate_Region_Name_For_westus_Is_Valid()
        {
            Assert.IsTrue(AzureRegionUtils.GetAltName("WESTUS") == "USWest");
            Assert.IsTrue(AzureRegionUtils.GetAltName("westus") == "USWest");
        }
    }
}
