using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TesApi.Web.Management;

namespace TesApi.Tests
{
    [TestClass, TestCategory("Integration")]
    public class PriceApiClientTests
    {
        private PriceApiClient pricingApiClient;

        [TestInitialize]
        public void Initialize()
        {
            pricingApiClient = new PriceApiClient();
        }

        [TestMethod]
        public async Task GetPricingInformationPageAsync_ReturnsSinglePageWithItemsWithMaxPageSize()
        {
            var page = await pricingApiClient.GetPricingInformationPageAsync(0, "westus2");

            Assert.IsNotNull(page);
            Assert.IsTrue(page.Items.Length == 100);
        }

        [TestMethod]
        public async Task GetPricingInformationAsync_ReturnsMoreThan100Items()
        {
            var pages = await pricingApiClient.GetAllPricingInformationAsync("westus2").ToListAsync();


            Assert.IsNotNull(pages);
            Assert.IsTrue(pages.Count > 100);

        }

        [TestMethod]
        public async Task GetAllPricingInformationForNonWindowsAndNonSpotVmsAsync_ReturnsOnlyNonWindowsAndNonSpotInstances()
        {
            var pages = await pricingApiClient.GetAllPricingInformationForNonWindowsAndNonSpotVmsAsync("westus2").ToListAsync();

            Assert.IsTrue(pages.Count > 0);
            Assert.IsFalse(pages.Any(r => r.productName.Contains(" Windows")));
            Assert.IsFalse(pages.Any(r => r.productName.Contains(" Spot")));

        }
    }
}
