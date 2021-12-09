using System.Collections.Generic;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CromwellOnAzureDeployer
{
    [TestClass]
    public class UtilityTests
    {
        [TestMethod]
        public void CromwelUpdateExistingSettingsFileContentV300()
        {
            var sb = new StringBuilder();
            sb.AppendLine("UsePreemptibleVmsOnly=false");
            sb.AppendLine("DisableBatchScheduling=false");
            sb.AppendLine("AzureOfferDurableId=MS-AZR-0003p");
            var existingFileContent = sb.ToString();

            var newContent = Utility.UpdateExistingSettingsFileContentV300(existingFileContent);

            var newSettingsKeys = new List<string>
                {
                    "BatchImageOffer",
                    "BatchImagePublisher",
                    "BatchImageSku",
                    "BatchImageVersion",
                    "BatchNodeAgentSkuId",
                    "XilinxFpgaVmSizePrefixes",
                    "XilinxFpgaBatchImageOffer",
                    "XilinxFpgaBatchImagePublisher",
                    "XilinxFpgaBatchImageSku",
                    "XilinxFpgaBatchImageVersion",
                    "XilinxFpgaBatchNodeAgentSkuId",
                    "MarthaUrl",
                    "MarthaKeyVaultName",
                    "MarthaSecretName"
                };

            foreach (var key in newSettingsKeys)
            {
                Assert.IsTrue(newContent.Contains($"{key}="));
            }
        }
    }
}
