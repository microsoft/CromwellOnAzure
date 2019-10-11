// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace CromwellApiClient
{
    public enum WorkflowStatus
    {
        // https://github.com/broadinstitute/cromwell/blob/1898d8103a06d160dc721d464862313e78ee7a2c/cromwellApiClient/src/main/scala/cromwell/api/model/WorkflowStatus.scala
        NotSet = 0,
        Submitted,
        Running,
        Aborting,
        Aborted,
        Failed,
        Succeeded
    }
}
