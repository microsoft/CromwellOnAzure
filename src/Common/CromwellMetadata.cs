// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;
using System.Linq;

namespace Common
{
    public class CromwellMetadata
    {
        // "failures"
        public List<CromwellMetadataCausedBy> Failures { get; set; }

        public bool ShouldSerializeFailures() => Failures?.Count > 0;
    }

    public class CromwellMetadataCausedBy
    {
        // "message"
        public string Message { get; set; }
        // "causedBy"
        public List<CromwellMetadataCausedBy> CausedBy { get; set; }
    }
}
