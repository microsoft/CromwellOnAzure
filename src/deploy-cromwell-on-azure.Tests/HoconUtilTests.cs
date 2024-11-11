// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Hocon;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CromwellOnAzureDeployer.Tests
{
    [TestClass]
    public class HoconUtilTests
    {
        // TODO: check ability to handle substitutions, ideally by not making them

        [TestMethod]
        public void TestIncludeResourceValue()
        {
            using HoconUtil hocon = new(@"include required(classpath(""application""))");

            Assert.AreEqual(@"include required(classpath(""application""))".ReplaceLineEndings(), hocon.ToString(hocon.Parse()).ReplaceLineEndings());
        }

        [TestMethod]
        public void TestIncludeFileValue()
        {
            using HoconUtil hocon = new(@"{
  text : include ""application.txt""
}");

            Assert.AreEqual(@"{
text : include ""application.txt""
}".ReplaceLineEndings(), hocon.ToString(hocon.Parse()).ReplaceLineEndings());
        }

        [TestMethod]
        public void TestIncludeUrlValue()
        {
            using HoconUtil hocon = new(@"{
  text : include ""http://www.bing.com/""
}");

            Assert.AreEqual(@"{
text : include ""http://www.bing.com/""
}".ReplaceLineEndings(), hocon.ToString(hocon.Parse()).ReplaceLineEndings());
        }

        [TestMethod]
        public void TestMultipleIncludes()
        {
            using HoconUtil hocon = new(@"{
text1 : include ""application.txt""
text2 : include ""http://www.bing.com/""
}");

            Assert.AreEqual(@"{
text1 : include ""application.txt""
text2 : include ""http://www.bing.com/""
}".ReplaceLineEndings(), hocon.ToString(hocon.Parse()).ReplaceLineEndings());
        }

        [TestMethod]
        public void TestUpdate()
        {
            using HoconUtil hocon = new(@"include required(classpath(""application""))

akka.http.host-connection-pool.max-open-requests = 16384
akka.http.host-connection-pool.max-connections = 2000

call-caching {
  enabled = false
}

system {
  input-read-limits {
    lines = 1000000
  }
}

engine {
  filesystems {
    local {
      enabled: true
    }
    http {
      enabled: true
    }
  }
}

workflow-options {
  workflow-log-dir: ""/cromwell-workflow-logs""
  workflow-log-temporary: false
}

backend {
  default = ""TES""
  providers {
    TES {
      actor-factory = ""cromwell.backend.impl.tes.TesBackendLifecycleActorFactory""
      config {
        filesystems {
          http { }
        }
        root = ""/cromwell-executions""
        dockerRoot = ""/cromwell-executions""
        endpoint = ""http://tes/v1/tasks""
        use_tes_11_preview_backend_parameters = true
        default-runtime-attributes {
          cpu: 1
          failOnStderr: false
          continueOnReturnCode: 0
          memory: ""2 GB""
          disk: ""10 GB""
          preemptible: true
        }
      }
    }
  }
}

database {
  db.url = ""jdbc:postgresql://db.postgres.database.azure.com/cromwell_db?sslmode=require""
  db.user = ""cromwell""
  db.password = ""password""
  db.driver = ""org.postgresql.Driver""
  profile = ""slick.jdbc.PostgresProfile$""
  db.connectionTimeout = 15000
}");
            var conf = hocon.Parse();

            var changes = HoconParser.Parse($@"
filesystems.blob {{
  class = ""cromwell.filesystems.blob.BlobPathBuilderFactory""
  global {{
    class = ""cromwell.filesystems.blob.BlobFileSystemManager""
    config.subscription = ""{"subscription"}""
  }}
}}

engine.filesystems.blob.enabled: true

backend.providers.TES.config {{
  filesystems {{
    http.enabled: true
    local.enabled: true
    blob.enabled: true
  }}
  root = ""https://{"storageAccount"}.blob.{"storageSuffix"}/cromwell-executions/""
}}").Value.GetObject();

            conf.Value.GetObject().Merge(changes);

            Assert.AreEqual(@"include required(classpath(""application""))

akka.http.host-connection-pool.max-open-requests = 16384
akka.http.host-connection-pool.max-connections = 2000

call-caching {
  enabled = false
}

system {
  input-read-limits {
    lines = 1000000
  }
}

engine.filesystems : {
  local : {
    enabled : true
  },
  http : {
    enabled : true
  },
  blob : {
    enabled : true
  }
} 

workflow-options {
  workflow-log-dir: ""/cromwell-workflow-logs""
  workflow-log-temporary: false
}

backend : {
  default : ""TES"",
  providers : {
    TES : {
      actor-factory : ""cromwell.backend.impl.tes.TesBackendLifecycleActorFactory"",
      config : {
        filesystems : {
          http : {
            enabled : true
          },
          local : {
            enabled : true
          },
          blob : {
            enabled : true
          }
        },
        root : ""https://storageAccount.blob.storageSuffix/cromwell-executions/"",
        dockerRoot : ""/cromwell-executions"",
        endpoint : ""http://tes/v1/tasks"",
        use_tes_11_preview_backend_parameters : true,
        default-runtime-attributes : {
          cpu : 1,
          failOnStderr : false,
          continueOnReturnCode : 0,
          memory : ""2 GB"",
          disk : ""10 GB"",
          preemptible : true
        }
      }
    }
  }
} 

database {
  db.url = ""jdbc:postgresql://db.postgres.database.azure.com/cromwell_db?sslmode=require""
  db.user = ""cromwell""
  db.password = ""password""
  db.driver = ""org.postgresql.Driver""
  profile = ""slick.jdbc.PostgresProfile$""
  db.connectionTimeout = 15000
}

filesystems : {
  blob : {
    class : ""cromwell.filesystems.blob.BlobPathBuilderFactory"",
    global : {
      class : ""cromwell.filesystems.blob.BlobFileSystemManager"",
      config : {
        subscription : ""subscription""
      }
    }
  }
}".ReplaceLineEndings(), hocon.ToString(conf).ReplaceLineEndings());
        }
    }
}
