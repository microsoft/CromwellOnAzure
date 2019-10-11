// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web
{
    /// <summary>
    /// <see cref="CosmosDbRepository{T}"/> item.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class RepositoryItem<T> where T : class
    {
        /// <summary>
        /// The value of the repository item
        /// </summary>
        public T Value { get; set; }

        /// <summary>
        /// The etag associated with the repository item
        /// </summary>
        public string ETag { get; set; }
    }
}
