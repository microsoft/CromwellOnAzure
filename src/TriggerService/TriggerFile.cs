// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;

namespace TriggerService
{
    /// <summary>
    /// Represents a trigger file in the workflows blob container
    /// </summary>
    public struct TriggerFile
    {
        public string Uri { get; set; }
        public string ContainerName { get; set; }
        public string Name { get; set; }
        public DateTimeOffset LastModified { get; set; }
    }
}
