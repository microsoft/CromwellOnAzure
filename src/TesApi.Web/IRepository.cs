// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace TesApi.Web
{
    /// <summary>
    /// A general repository interface for persistence of T instances
    /// </summary>
    /// <typeparam name="T">The type of the instance</typeparam>
    public interface IRepository<T> where T : class
    {
        /// <summary>
        /// Create a new item
        /// </summary>
        /// <param name="item">The item to create</param>
        /// <returns>The created item</returns>
        Task<RepositoryItem<T>> CreateItemAsync(T item);

        /// <summary>
        /// Delete an existing item
        /// </summary>
        /// <param name="id">The ID of the item to delete</param>
        Task DeleteItemAsync(string id);

        /// <summary>
        /// Get an item by ID
        /// </summary>
        /// <param name="id">The ID of the item to retrieve</param>
        /// <returns>The item instance</returns>
        Task<RepositoryItem<T>> GetItemAsync(string id);

        /// <summary>
        /// Reads a collection of items from the repository
        /// </summary>
        /// <param name="predicate">The 'where' clause</param>
        /// <param name="pageSize">The max number of items to retrieve</param>
        /// <param name="continuationToken">A token to continue retrieving items if the max is returned</param>
        /// <returns>A continuation token string, and the collection of retrieved items</returns>
        Task<(string, IEnumerable<RepositoryItem<T>>)> GetItemsAsync(Expression<Func<T, bool>> predicate, int pageSize, string continuationToken);

        /// <summary>
        /// Update the item in the repository
        /// </summary>
        /// <param name="id">The ID of the item to update</param>
        /// <param name="item">The item to persist</param>
        /// <returns>The created document</returns>
        Task<RepositoryItem<T>> UpdateItemAsync(string id, RepositoryItem<T> item);
    }
}