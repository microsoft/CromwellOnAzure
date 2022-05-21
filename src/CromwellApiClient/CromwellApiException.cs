// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Net;

namespace CromwellApiClient
{
    public class CromwellApiException : Exception
    {
        public HttpStatusCode? HttpStatusCode { get; set; }
        public CromwellApiException(string message, Exception exc, HttpStatusCode? httpStatusCode) : base(message, exc)
	    => HttpStatusCode = httpStatusCode;
    }
}
