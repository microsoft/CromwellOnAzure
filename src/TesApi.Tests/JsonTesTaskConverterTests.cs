// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Json;
using System.Text.Json;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TesApi.Models;
using TesApi.Web;

namespace TesApi.Tests
{
    [TestClass]
    public class JsonTesTaskConverterTests
    {
        private TesTask tesTask;

        private bool DeepEquals(JsonElement first, JsonElement second)
        {
            if (first.ValueKind != second.ValueKind)
                return false;

            switch (first.ValueKind)
            {
                case JsonValueKind.Null:
                case JsonValueKind.True:
                case JsonValueKind.False:
                case JsonValueKind.Undefined:
                    return true;

                case JsonValueKind.Number:
                    return first.GetDecimal().Equals(second.GetDecimal());

                case JsonValueKind.String:
                    return first.GetString().Equals(second.GetString());

                case JsonValueKind.Array:
                    {
                        var firstArray = first.EnumerateArray();
                        var secondArray = second.EnumerateArray();

                        foreach (var (firstElement, secondElement) in firstArray.Zip(secondArray))
                        {
                            if (!DeepEquals(firstElement, secondElement))
                                return false;
                        }

                        return true;
                    }

                case JsonValueKind.Object:
                    {
                        var firstArray = first.EnumerateObject();
                        var secondArray = second.EnumerateObject();

                        if (firstArray.Count() != secondArray.Count())
                            return false;

                        var firstSorted = firstArray.OrderBy(p => p.Name, StringComparer.Ordinal);
                        var secondSorted = secondArray.OrderBy(p => p.Name, StringComparer.Ordinal);

                        foreach (var (firstElement, secondElement) in firstSorted.Zip(secondSorted))
                        {
                            if (!firstElement.NameEquals(secondElement.Name) || !DeepEquals(firstElement.Value, secondElement.Value))
                                return false;
                        }

                        return true;
                    }

                default:
                    return false;
            }
        }

        [TestInitialize]
        public void Init()
        {
            tesTask = new TesTask();
            var serializer = new DataContractJsonSerializer(tesTask.GetType());
//            tesTask = serializer.ReadObject(File.OpenRead("tesTaskExample.json")) as TesTask;
            tesTask = Newtonsoft.Json.JsonConvert.DeserializeObject<TesTask>(File.ReadAllText("tesTaskExample.json"));
        }

        [TestMethod]
        public void MinimalConverter_ReturnsMinimalJsonResult()
        {
            // Arrange
            var expectedMinimalJsonResult = JsonDocument.Parse(File.ReadAllText("expectedMinimalJsonResult.json"));

            // Act
            var jsonResult = JsonSerializer.Serialize(tesTask, new JsonSerializerOptions { Converters = { new JsonTesTaskConverter(TesView.MINIMAL) } });
            var jsonObjectResult = JsonDocument.Parse(jsonResult);

            // Assert
            Assert.IsTrue(DeepEquals(expectedMinimalJsonResult.RootElement, jsonObjectResult.RootElement));
        }

        [TestMethod]
        public void BasicConverter_ReturnsBasicJsonResult()
        {
            // Arrange
            var expectedBasicJsonResult = JsonDocument.Parse(File.ReadAllText("expectedBasicJsonResult.json"));

            // Act
            var jsonResult = JsonSerializer.Serialize(tesTask, new JsonSerializerOptions { Converters = { new JsonTesTaskConverter(TesView.BASIC) } });
            var jsonObjectResult = JsonDocument.Parse(jsonResult);

            // Assert
            Assert.IsTrue(DeepEquals(expectedBasicJsonResult.RootElement, jsonObjectResult.RootElement));
        }

        [TestMethod]
        public void BasicConverter_ReturnsBasicJsonResult_WithoutChangedContent()
        {
            // Arrange
            var expectedBasicJsonResult = JsonDocument.Parse(File.ReadAllText("expectedBasicJsonResult.json"));
            tesTask.Inputs[0].Content = "test content";

            // Act
            var jsonResult = JsonSerializer.Serialize(tesTask, new JsonSerializerOptions { Converters = { new JsonTesTaskConverter(TesView.BASIC) } });
            var jsonObjectResult = JsonDocument.Parse(jsonResult);

            // Assert
            Assert.IsTrue(DeepEquals(expectedBasicJsonResult.RootElement, jsonObjectResult.RootElement));
        }

        [TestMethod]
        public void FullConverter_ReturnsFullJsonResult()
        {
            // Arrange
            var expectedFullJsonResult = JsonDocument.Parse(File.ReadAllText("expectedFullJsonResult.json"));

            // Act
            var jsonResult = JsonSerializer.Serialize(tesTask, new JsonSerializerOptions { Converters = { new JsonTesTaskConverter(TesView.FULL) } });
            var jsonObjectResult = JsonDocument.Parse(jsonResult);

            // Assert
            Assert.IsTrue(DeepEquals(expectedFullJsonResult.RootElement, jsonObjectResult.RootElement));
        }

        [TestMethod]
        public void FullConverter_ReturnsFullJsonResult_WithChangedContent()
        {
            // Arrange
            var expectedFullJsonResult = JsonDocument.Parse(File.ReadAllText("expectedFullJsonResult.json"));
            tesTask.Inputs[0].Content = "test content";

            // Act
            var jsonResult = JsonSerializer.Serialize(tesTask, new JsonSerializerOptions { Converters = { new JsonTesTaskConverter(TesView.FULL) } });
            var jsonObjectResult = JsonDocument.Parse(jsonResult);
            var content = jsonObjectResult.RootElement.GetProperty("inputs").EnumerateArray().First().GetProperty("content").GetString();

            // Assert
            Assert.IsFalse(DeepEquals(expectedFullJsonResult.RootElement, jsonObjectResult.RootElement));
            Assert.AreEqual("test content", content);
        }

        [TestMethod]
        public void FullConverter_ReturnsFullJsonResult_WithCorrectState()
        {
            // Arrange
            var expectedFullJsonResult = JsonDocument.Parse(File.ReadAllText("expectedFullJsonResult.json"));
            tesTask.State = TesState.CANCELEDEnum;

            // Act
            var jsonResult = JsonSerializer.Serialize(tesTask, new JsonSerializerOptions { Converters = { new JsonTesTaskConverter(TesView.FULL) } });
            var jsonObjectResult = JsonDocument.Parse(jsonResult);

            // Assert
            Assert.IsFalse(DeepEquals(expectedFullJsonResult.RootElement, jsonObjectResult.RootElement));
            Assert.AreEqual("CANCELED", jsonObjectResult.RootElement.GetProperty("state").GetString());
        }
    }
}
