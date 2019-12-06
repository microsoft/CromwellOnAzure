using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Linq;
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
            Assert.IsTrue(AzureRegionUtils.TryGetBillingRegionName("westus", out string billingRegionName));
            Assert.AreEqual(billingRegionName, "US West");
        }

        [TestMethod]
        public void ArmLocation_Is_Case_Insensitive()
        {
            Assert.IsTrue(AzureRegionUtils.TryGetBillingRegionName("WeStUs", out string billingRegionName));
            Assert.AreEqual(billingRegionName, "US West");
        }

        [TestMethod]
        public void Unknown_ArmLocation_results_in_null()
        {
            Assert.IsFalse(AzureRegionUtils.TryGetBillingRegionName("unknown", out string billingRegionName));
            Assert.IsNull(billingRegionName);
        }

        /// <summary>
        /// This is used to generate the list of billing regions, using an internal Microsoft region XML file
        /// </summary>
        [Ignore]
        [TestMethod]
        private void GenerateBillingRegionDictionary()
        {
            const bool generateCsv = false;
            const bool generateCsharp = true;

            var xml = XElement.Load(new FileStream(@"c:\temp\Regions.xml", FileMode.Open));

            var regions = xml.Descendants("Region").Select(x => new
            {
                ArmLocation = (string)x.Attribute("ArmLocation"),
                BillingRegionName = (string)x.Attribute("BillingRegionName"),
                FriendlyName = (string)x.Attribute("FriendlyName")
            }).ToList();

            if (generateCsv)
            {
                var csv = new StringBuilder();
                csv.AppendLine(string.Join(",", regions.First().GetType().GetProperties().Select(p => p.Name)));
                regions.Where(r => !r.GetType().GetProperties().Any(p => string.IsNullOrWhiteSpace(p.GetValue(r) as string))).ToList()
                    .ForEach(r => csv.AppendLine(string.Join(",", r.GetType().GetProperties().Select(p => p.GetValue(r)))));
                string path = Path.GetTempFileName() + ".csv";
                File.WriteAllText(path, csv.ToString());
                Console.WriteLine(path);
            }

            if (generateCsharp)
            {
                var cSharp = new StringBuilder();
                cSharp.AppendLine("Dictionary<string, string> billingRegionLookup = new Dictionary<string, string> (");
                cSharp.AppendLine("\tnew List<KeyValuePair<string, string>> {");
                var finalRegions = regions.Where(r => !r.GetType().GetProperties().Any(p => string.IsNullOrWhiteSpace(p.GetValue(r) as string)));

                foreach (var region in finalRegions.OrderBy(r => r.ArmLocation))
                {
                    cSharp.AppendLine($"\t\tnew KeyValuePair<string, string>(\"{region.ArmLocation}\", \"{region.BillingRegionName}\"),");
                }

                cSharp.AppendLine("\t});");
                string csharpPath = Path.GetTempFileName() + ".txt";
                File.WriteAllText(csharpPath, cSharp.ToString());
                Console.WriteLine(csharpPath);
            }
        }
    }
}
