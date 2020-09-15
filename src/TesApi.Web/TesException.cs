// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;

namespace TesApi.Web
{
    [Serializable]
    internal class TesException : Exception
    {
        public string FailureReason { get; private set; }

        public TesException(string failureReason, string message) : this(failureReason, message, null)
        {
        }

        public TesException(string failureReason, string message, Exception innerException) : base(message, innerException)
        {
            this.FailureReason = failureReason;
        }
    }
}
