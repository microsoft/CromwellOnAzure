// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Net;

namespace CromwellApiClient
{
    public class CromwellApiException(string message, Exception exc, HttpStatusCode? httpStatusCode) : Exception(message, exc)
    {
        public HttpStatusCode? HttpStatusCode { get; set; } = httpStatusCode;
    }
}
