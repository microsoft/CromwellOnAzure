// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Common.Tests
{
    [TestClass]
    public class UtilitiesTests
    {
        [DataTestMethod] // https://datatracker.ietf.org/doc/html/rfc4648#section-10
        [DataRow("", "")]
        [DataRow("f", "MY======")]
        [DataRow("fo", "MZXQ====")]
        [DataRow("foo", "MZXW6===")]
        [DataRow("foob", "MZXW6YQ=")]
        [DataRow("fooba", "MZXW6YTB")]
        [DataRow("foobar", "MZXW6YTBOI======")]
        public void ValidateConvertToBase32(string data, string expected)
        {
            Assert.AreEqual(expected, Utilities.ConvertToBase32(Encoding.UTF8.GetBytes(data)));
        }

        [TestMethod]
        public void NormalizeContainerImageNameRecognizesUri()
        {
            var test = "docker://gcr.io/my-org/my-image:latest";

            var result = Utilities.NormalizeContainerImageName(test);

            Assert.AreEqual(test, result.AbsoluteUri);
        }

        [TestMethod]
        public void NormalizeContainerImageNameRespectsRepository()
        {
            var expected = "docker://gcr.io/my-org/my-image:latest";

            var result = Utilities.NormalizeContainerImageName("gcr.io/my-org/my-image");

            Assert.AreEqual(expected, result.AbsoluteUri);
        }

        [TestMethod]
        public void NormalizeContainerImageNameAddsDefaultRepository()
        {
            var expected = "docker://docker.io/library/ubuntu:latest";

            var result = Utilities.NormalizeContainerImageName("docker.io/ubuntu");

            Assert.AreEqual(expected, result.AbsoluteUri);
        }

        [TestMethod]
        public void NormalizeContainerImageNameAddsDefaultHost()
        {
            var expected = "docker://docker.io/aptible/ubuntu:latest";

            var result = Utilities.NormalizeContainerImageName("aptible/ubuntu");

            Assert.AreEqual(expected, result.AbsoluteUri);
        }

        [TestMethod]
        public void NormalizeContainerImageNameAddsDefaultRepositoryAndHost()
        {
            var expected = "docker://docker.io/library/ubuntu:latest";

            var result = Utilities.NormalizeContainerImageName("ubuntu");

            Assert.AreEqual(expected, result.AbsoluteUri);
        }

        [TestMethod]
        public void NormalizeContainerImageNamePreservesTag()
        {
            var expected = "docker://gcr.io/my-org/my-image:1.2.3.4";

            var result = Utilities.NormalizeContainerImageName("gcr.io/my-org/my-image:1.2.3.4");

            Assert.AreEqual(expected, result.AbsoluteUri);
        }
    }
}
