// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Runtime.Serialization;

namespace TesApi.Web
{
    [Serializable]
    internal class AzureBatchLowQuotaException : Exception
    {
        public AzureBatchLowQuotaException()
        {
        }

        public AzureBatchLowQuotaException(string message) : base(message)
        {
        }

        public AzureBatchLowQuotaException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected AzureBatchLowQuotaException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
