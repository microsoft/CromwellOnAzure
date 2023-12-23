// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using TriggerService;

namespace CromwellApiClient.Tests
{
    [TestClass]
    public class CromwellApiClientTests
    {
        [Ignore]
        [TestMethod]
        public async Task PostWorkflowAsyncTest()
        {
            var options = new Mock<IOptions<CromwellApiClientOptions>>();

            options.Setup(o => o.Value).Returns(new CromwellApiClientOptions()
            {
                BaseUrl = "http://cromwell"
            }); ;

            var cromwellApiClient = new CromwellApiClient(options.Object);
            var workflowInputsFilenames = new List<string> { "inputs.json" };
            var workflowInputsDatas = new List<byte[]> { Encoding.UTF8.GetBytes("{}") };

            await cromwellApiClient.PostWorkflowAsync(
                "https://raw.githubusercontent.com/microsoft/gatk4-somatic-snvs-indels-azure/main-azure/mutect2.wdl",
                workflowInputsFilenames,
                workflowInputsDatas);
        }
    }
}
