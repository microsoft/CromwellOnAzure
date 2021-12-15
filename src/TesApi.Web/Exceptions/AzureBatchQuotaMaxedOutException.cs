// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Runtime.Serialization;

namespace TesApi.Web
{
    [Serializable]
    internal class AzureBatchQuotaMaxedOutException : Exception
    {
        private readonly Exception exception;

        public AzureBatchQuotaMaxedOutException()
        {
        }

        public AzureBatchQuotaMaxedOutException(Exception exception)
            => this.exception = exception;

        public AzureBatchQuotaMaxedOutException(string message) : base(message)
        {
        }

        public AzureBatchQuotaMaxedOutException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected AzureBatchQuotaMaxedOutException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
