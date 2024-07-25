// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.IO;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;

namespace CromwellOnAzureDeployer.Tests
{
    [TestClass]
    public class CromwellOnAzureDeployerTests
    {
        [TestMethod]
        public void SerializeAndDeserializeConfiguration()
        {
            var config = new Configuration();
            var path = Path.Combine(Path.GetTempPath(), "config.json");
            var json = JsonConvert.SerializeObject(config);
            File.WriteAllText(path, json);
            Console.WriteLine($"Config file path: {path}");
            JsonConvert.DeserializeObject<Configuration>(File.ReadAllText(path));
            File.Delete(path);
        }
    }
}
