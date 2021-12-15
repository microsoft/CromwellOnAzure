// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.IO;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Tes.Models;
using TesApi.Web;

namespace TesApi.Tests
{
    [TestClass]
    public class ContractResolverTests
    {
        private TesTask tesTask;

        [TestInitialize]
        public void Init()
            => tesTask = JsonConvert.DeserializeObject<TesTask>(File.ReadAllText("tesTaskExample.json"));

        [TestMethod]
        public void MinimalContractResolver_ReturnsMinimalJsonResult()
        {
            // Arrange
            var expectedMinimalJsonResult = JObject.Parse(File.ReadAllText("expectedMinimalJsonResult.json"));

            // Act
            var jsonResult = JsonConvert.SerializeObject(tesTask, new JsonSerializerSettings { ContractResolver = MinimalTesTaskContractResolver.Instance });
            var jsonObjectResult = JObject.Parse(jsonResult);

            // Assert
            Assert.IsTrue(JToken.DeepEquals(expectedMinimalJsonResult, jsonObjectResult));
        }

        [TestMethod]
        public void BasicContractResolver_ReturnsBasicJsonResult()
        {
            // Arrange
            var expectedBasicJsonResult = JObject.Parse(File.ReadAllText("expectedBasicJsonResult.json"));

            // Act
            var jsonResult = JsonConvert.SerializeObject(tesTask, new JsonSerializerSettings { ContractResolver = BasicTesTaskContractResolver.Instance });
            var jsonObjectResult = JObject.Parse(jsonResult);

            // Assert
            Assert.IsTrue(JToken.DeepEquals(expectedBasicJsonResult, jsonObjectResult));
        }

        [TestMethod]
        public void BasicContractResolver_ReturnsBasicJsonResult_WithoutChangedContent()
        {
            // Arrange
            var expectedBasicJsonResult = JObject.Parse(File.ReadAllText("expectedBasicJsonResult.json"));
            tesTask.Inputs[0].Content = "test content";

            // Act
            var jsonResult = JsonConvert.SerializeObject(tesTask, new JsonSerializerSettings { ContractResolver = BasicTesTaskContractResolver.Instance });
            var jsonObjectResult = JObject.Parse(jsonResult);

            // Assert
            Assert.IsTrue(JToken.DeepEquals(expectedBasicJsonResult, jsonObjectResult));
        }

        [TestMethod]
        public void FullContractResolver_ReturnsFullJsonResult()
        {
            // Arrange
            var expectedFullJsonResult = JObject.Parse(File.ReadAllText("expectedFullJsonResult.json"));

            // Act
            var jsonResult = JsonConvert.SerializeObject(tesTask, new JsonSerializerSettings { ContractResolver = FullTesTaskContractResolver.Instance });
            var jsonObjectResult = JObject.Parse(jsonResult);

            // Assert
            Assert.IsTrue(JToken.DeepEquals(expectedFullJsonResult, jsonObjectResult));
        }

        [TestMethod]
        public void FullContractResolver_ReturnsFullJsonResult_WithChangedContent()
        {
            // Arrange
            var expectedFullJsonResult = JObject.Parse(File.ReadAllText("expectedFullJsonResult.json"));
            tesTask.Inputs[0].Content = "test content";

            // Act
            var jsonResult = JsonConvert.SerializeObject(tesTask, new JsonSerializerSettings { ContractResolver = FullTesTaskContractResolver.Instance });
            var jsonObjectResult = JObject.Parse(jsonResult);
            var content = jsonObjectResult.GetValue("inputs").First.Value<string>("content");

            // Assert
            Assert.IsFalse(JToken.DeepEquals(expectedFullJsonResult, jsonObjectResult));
            Assert.AreEqual("test content", content);
        }

        [TestMethod]
        public void FullContractResolver_ReturnsFullJsonResult_WithCorrectState()
        {
            // Arrange
            var expectedFullJsonResult = JObject.Parse(File.ReadAllText("expectedFullJsonResult.json"));
            tesTask.State = TesState.CANCELEDEnum;

            // Act
            var jsonResult = JsonConvert.SerializeObject(tesTask, new JsonSerializerSettings { ContractResolver = FullTesTaskContractResolver.Instance });
            var jsonObjectResult = JObject.Parse(jsonResult);

            // Assert
            Assert.IsFalse(JToken.DeepEquals(expectedFullJsonResult, jsonObjectResult));
            Assert.AreEqual("CANCELED", jsonObjectResult.GetValue("state"));
        }
    }
}
