using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace TriggerService.Tests
{
    public class Mutect2IntegrationTest
    {
        [TestCategory("Integration")]
        [TestMethod]
        public async Task RunScaleTestWithMutect2WaitTilDoneAsync()
        {
            const string triggerFile = "https://raw.githubusercontent.com/microsoft/CromwellOnAzure/main/src/TriggerService.Tests/test-wdls/mutect2/mutect2.trigger.json";
            const string workflowFriendlyName = $"mutect2";

            await IntegrationTests.RunIntegrationTestAsync(new List<(string triggerFileBlobUrl, string workflowFriendlyName)> { (triggerFile, workflowFriendlyName) });
        }
    }
}
