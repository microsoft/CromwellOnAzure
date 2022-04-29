// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Tes.Repository;

namespace TesApi.Web
{
    /// <summary>
    /// Indicates and provides access to the <see cref="RepositoryItem{T}"/> metadata contained in an object.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IHasRepositoryItem<T> where T : RepositoryItem<T>
    {
        /// <summary>
        /// Returns the associated <see cref="RepositoryItem{T}"/>.
        /// </summary>
        T RepositoryItem { get; }

        /// <summary>
        /// Replaces the <see cref="RepositoryItem{T}"/> metadata (helps prevent CosmosDB consistency errors).
        /// </summary>
        /// <param name="replacementItem"></param>
        void ReplaceRepositoryItem(T replacementItem);

        /// <summary>
        /// Provides access to the <see cref="IHasChangesRepositoryItem{T}"/> change manager.
        /// </summary>
        IHasChangesRepositoryItem<T> ChangesRepositoryItem { get; }
    }
}
