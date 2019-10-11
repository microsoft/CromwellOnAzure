// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Runtime.Serialization;

namespace TesApi.Web
{
    [Serializable]
    internal class AzureBatchManagementAccessException : Exception
    {
        public AzureBatchManagementAccessException()
        {
        }

        public AzureBatchManagementAccessException(string message) : base(message)
        {
        }

        public AzureBatchManagementAccessException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected AzureBatchManagementAccessException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}