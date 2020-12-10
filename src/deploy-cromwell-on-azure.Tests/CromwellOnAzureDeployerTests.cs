// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.IO;
using System.Text.Json;
using Microsoft.Rest.Azure;
using Microsoft.VisualStudio.TestTools.UnitTesting;

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
            var json = JsonSerializer.Serialize(config);
            File.WriteAllText(path, json);
            Console.WriteLine($"Config file path: {path}");
            JsonSerializer.Deserialize<Configuration>(File.ReadAllText(path));
            File.Delete(path);
        }

        [TestMethod]
        public void ToCloudErrorTypeHandlesNull()
        {
            CloudException exception = null;
            var error = exception.ToCloudErrorType();
            Assert.IsTrue(error == CloudErrorType.NotSet);
        }
    }
}
