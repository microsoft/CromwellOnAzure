using System;

namespace TesApi.Web
{
    /// <summary>
    /// A general exception when an Azure Batch job and/or pool throws an exception during deletion
    /// </summary>
    public class BatchDeletionException : Exception
    {
        /// <summary>
        /// The exception from deleting the job
        /// </summary>
        public Exception JobDeletionException { get; set; }

        /// <summary>
        /// The exception from deleting the pool
        /// </summary>
        public Exception PoolDeletionException { get; set; }
    }
}
