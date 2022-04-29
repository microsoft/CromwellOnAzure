// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using Tes.Repository;

namespace TesApi.Web
{
    /// <summary>
    /// Represents a set of <see cref="RepositoryItem{T}"/>.
    /// </summary>
    /// <typeparam name="TElement">The type of elements in the hash set.</typeparam>
    /// <typeparam name="TRepositoryItem"></typeparam>
    public class GroupableSetWithRepositoryElements<TElement, TRepositoryItem> : GroupableSet<TElement>, ISet<TElement> where TElement : IHasRepositoryItem<TRepositoryItem> where TRepositoryItem : RepositoryItem<TRepositoryItem>
    {
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="GroupableSetWithRepositoryElements{TElement, TRepositoryItem}"/> class that is empty and uses the default equality comparer for the set type.
        /// </summary>
        public GroupableSetWithRepositoryElements() : base() { }

        /// <summary>
        /// Initializes a new instance of the <see cref="GroupableSetWithRepositoryElements{TElement, TRepositoryItem}"/> class that uses the default equality comparer for the set type, contains elements copied from the specified collection, and has sufficient capacity to accommodate the number of elements copied.
        /// </summary>
        /// <param name="collection">The collection whose elements are copied to the new set.</param>
        /// <exception cref="ArgumentNullException">collection is null.</exception>
        public GroupableSetWithRepositoryElements(IEnumerable<TElement> collection) : base(collection) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="GroupableSetWithRepositoryElements{TElement, TRepositoryItem}"/> class that is empty and uses the specified equality comparer for the set type.
        /// </summary>
        /// <param name="comparer">The <see cref="IEqualityComparer{T}"/> implementation to use when comparing values in the set, or null to use the default <see cref="IEqualityComparer{T}"/> implementation for the set type.</param>
        public GroupableSetWithRepositoryElements(IEqualityComparer<TElement> comparer) : base(comparer) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="GroupableSetWithRepositoryElements{TElement, TRepositoryItem}"/> class that uses the specified equality comparer for the set type, contains elements copied from the specified collection, and has sufficient capacity to accommodate the number of elements copied.
        /// </summary>
        /// <param name="collection">The collection whose elements are copied to the new set.</param>
        /// <param name="comparer">The <see cref="IEqualityComparer{T}"/> implementation to use when comparing values in the set, or null to use the default <see cref="IEqualityComparer{T}"/> implementation for the set type.</param>
        public GroupableSetWithRepositoryElements(IEnumerable<TElement> collection, IEqualityComparer<TElement> comparer) : base(collection, comparer) { }
        #endregion

        #region Public methods
        /// <summary>
        /// Enumerates the <see cref="RepositoryItem{T}"/> of each element in this object.
        /// </summary>
        /// <returns>An <see cref="IEnumerable{T}"/> of the <see cref="RepositoryItem{T}"/> of each element.</returns>
        public IEnumerable<TRepositoryItem> GetRepositoryItems()
            => this.Select(e => e.RepositoryItem);


        /// <summary>
        /// Indicates that any element has changes in any <see cref="RepositoryItem{T}"/>.
        /// </summary>
        public bool HasChanges
            => this.Any(e => e.ChangesRepositoryItem.HasChanges);

        /// <summary>
        /// Clears <see cref="GroupableSetWithRepositoryElements{TElement, TRepositoryItem}.HasChanges"/>.
        /// </summary>
        public void ResetChanges()
        {
            foreach (var element in this)
            {
                element.ChangesRepositoryItem.ResetChanges();
            }
        }
        #endregion
    }
}
