// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Tests
{
    using System;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using TesApi.Web;

    [TestClass]
    public class GuidUtilsTests
    {
        [TestMethod]
        public void GenerateGuidTest_Sha1() =>
            Assert.AreEqual(
                new Guid("2ed6657d-e927-568b-95e1-2665a8aea6a2"),
                UUIDNameSpace.Dns.GenerateGuid("www.example.com"));

        [TestMethod]
        public void GenerateGuidTest_MD5() =>
            Assert.AreEqual(
                new Guid("5df41881-3aed-3515-88a7-2f4a814cf09e"),
                UUIDNameSpace.Dns.GenerateGuid("www.example.com",
                    System.Security.Cryptography.HashAlgorithmName.MD5));
    }
}
