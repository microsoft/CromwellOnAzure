// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;

namespace Tes.Models
{
    /// <summary>
    /// Exception that extends <see cref="Exception"/> with FailureReason property.
    /// </summary>
    [Serializable]
    public class TesException : Exception
    {
        /// <summary>
        /// The primary reason code for the task failure
        /// </summary>
        public string FailureReason { get; private set; }

        /// <summary>
        /// Creates an exception with failure reason, message and inner exception
        /// </summary>
        /// <param name="failureReason">The primary reason code for the task failure</param>
        /// <param name="message">Message</param>
        /// <param name="innerException">Inner exception</param>
        public TesException(string failureReason, string message = null, Exception innerException = null) : base(message, innerException)
	    => this.FailureReason = failureReason;
    }
}
