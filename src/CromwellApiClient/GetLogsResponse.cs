// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;

namespace CromwellApiClient
{
    public class GetLogsResponse : CromwellResponse
    {
        public Dictionary<string, Log[]> Calls { get; set; }

        public class Log
        {
            public string StdErr { get; set; }
            public string StdOut { get; set; }
            public int Attempt { get; set; }
            public int ShardIndex { get; set; }
        }
    }
}
