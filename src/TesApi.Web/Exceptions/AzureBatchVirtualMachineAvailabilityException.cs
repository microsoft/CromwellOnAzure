// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Runtime.Serialization;

namespace TesApi.Web
{
    [Serializable]
    internal class AzureBatchVirtualMachineAvailabilityException : Exception
    {
        public AzureBatchVirtualMachineAvailabilityException()
        {
        }

        public AzureBatchVirtualMachineAvailabilityException(string message) : base(message)
        {
        }

        public AzureBatchVirtualMachineAvailabilityException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected AzureBatchVirtualMachineAvailabilityException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}