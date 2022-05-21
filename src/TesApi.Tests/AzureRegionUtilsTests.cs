// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.VisualStudio.TestTools.UnitTesting;
using TesApi.Web;

namespace TesApi.Tests
{
    [TestClass]
    public class AzureRegionUtilsTests
    {
        [TestMethod]
        public void BillingRegionNameIsValid()
        {
            Assert.IsTrue(AzureRegionUtils.TryGetBillingRegionName("westus", out var billingRegionName));
            Assert.AreEqual(billingRegionName, "US West");
        }

        [TestMethod]
        public void BillingRegionLookupIsCaseInsensitive()
        {
            Assert.IsTrue(AzureRegionUtils.TryGetBillingRegionName("WeStUs", out var billingRegionName));
            Assert.AreEqual(billingRegionName, "US West");
        }

        [TestMethod]
        public void BillingRegionLookupFailsIfRegionDoesNotExist()
        {
            Assert.IsFalse(AzureRegionUtils.TryGetBillingRegionName("unknown", out var billingRegionName));
            Assert.IsNull(billingRegionName);
        }
    }
}
