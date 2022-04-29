// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Tes.Repository;

namespace TesApi.Web
{
    /// <summary>
    /// <see cref="RepositoryItem{T}"/> change management.
    /// </summary>
    public interface IHasChangesRepositoryItem<T> where T : RepositoryItem<T>
    {
        /// <summary>
        /// Indicates that changes have occured.
        /// </summary>
        bool HasChanges { get; }

        /// <summary>
        /// Clears <see cref="IHasChangesRepositoryItem{T}.HasChanges"/>.
        /// </summary>
        void ResetChanges();
    }
}
