// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using Tes.Repository;

namespace TesApi.Web
{
    /// <inheritdoc/>
    public class HasChangesRepositoryItem<T> : IHasChangesRepositoryItem<T> where T : RepositoryItem<T>
    {
        private readonly IHasRepositoryItem<T> _repositoryItemOwner;
        private int _checkpoint;

        /// <summary>
        /// Creates an <see cref="HasChangesRepositoryItem{T}"/>.
        /// </summary>
        /// <param name="repositoryItemOwner"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public HasChangesRepositoryItem(IHasRepositoryItem<T> repositoryItemOwner)
        {
            _repositoryItemOwner = repositoryItemOwner ?? throw new ArgumentNullException(nameof(repositoryItemOwner));
            ResetChanges();
        }

        /// <inheritdoc/>
        public bool HasChanges
            => _checkpoint != _repositoryItemOwner.RepositoryItem.GetHashCode();

        /// <inheritdoc/>
        public void ResetChanges()
            => _checkpoint = _repositoryItemOwner.RepositoryItem.GetHashCode();
    }
}
