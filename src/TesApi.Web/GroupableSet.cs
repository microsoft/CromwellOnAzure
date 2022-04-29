// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections;
using System.Collections.Generic;

namespace TesApi.Web
{
    /// <summary>
    /// Represents a set of values.
    /// </summary>
    /// <typeparam name="T">The type of elements in the hash set.</typeparam>
    public class GroupableSet<T> : ISet<T>
    {
        private readonly HashSet<T> elements;

        #region Constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="GroupableSet{TElement}"/> class that is empty and uses the default equality comparer for the set type.
        /// </summary>
        public GroupableSet()
            => elements = new();

        /// <summary>
        /// Initializes a new instance of the <see cref="GroupableSet{TElement}"/> class that uses the default equality comparer for the set type, contains elements copied from the specified collection, and has sufficient capacity to accommodate the number of elements copied.
        /// </summary>
        /// <param name="collection">The collection whose elements are copied to the new set.</param>
        /// <exception cref="ArgumentNullException">collection is null.</exception>
        public GroupableSet(IEnumerable<T> collection)
            => elements = new(collection);

        /// <summary>
        /// Initializes a new instance of the <see cref="GroupableSet{TElement}"/> class that is empty and uses the specified equality comparer for the set type.
        /// </summary>
        /// <param name="comparer">The <see cref="IEqualityComparer{T}"/> implementation to use when comparing values in the set, or null to use the default <see cref="IEqualityComparer{T}"/> implementation for the set type.</param>
        public GroupableSet(IEqualityComparer<T> comparer)
            => elements = new(comparer);

        /// <summary>
        /// Initializes a new instance of the <see cref="GroupableSet{TElement}"/> class that uses the specified equality comparer for the set type, contains elements copied from the specified collection, and has sufficient capacity to accommodate the number of elements copied.
        /// </summary>
        /// <param name="collection">The collection whose elements are copied to the new set.</param>
        /// <param name="comparer">The <see cref="IEqualityComparer{T}"/> implementation to use when comparing values in the set, or null to use the default <see cref="IEqualityComparer{T}"/> implementation for the set type.</param>
        public GroupableSet(IEnumerable<T> collection, IEqualityComparer<T> comparer)
            => elements = new(collection, comparer);
        #endregion

        #region Public interface or implementation object methods
        /// <summary>
        /// Gets the <see cref="IEqualityComparer{T}"/> object that is used to determine equality for the values in the set.
        /// </summary>
        public IEqualityComparer<T> Comparer
            => elements.Comparer;

        /// <inheritdoc/>
        public int Count
            => elements.Count;

        /// <inheritdoc/>
        public bool Add(T item)
            => elements.Add(item);

        /// <inheritdoc/>
        public void Clear()
            => elements.Clear();

        /// <inheritdoc/>
        public bool Contains(T item)
            => elements.Contains(item);

        /// <summary>
        /// Copies the elements of this object to an array.
        /// </summary>
        /// <param name="array">The one-dimensional array that is the destination of the elements copied from this object. The array must have zero-based indexing.</param>
        /// <exception cref="ArgumentNullException"><paramref name="array"/> is null.</exception>
        public void CopyTo(T[] array)
            => elements.CopyTo(array);

        /// <inheritdoc/>
        public void CopyTo(T[] array, int arrayIndex)
            => elements.CopyTo(array, arrayIndex);

        /// <summary>
        /// Copies the specified number of elements of this object to an array, starting at the specified array index.
        /// </summary>
        /// <param name="array">The one-dimensional array that is the destination of the elements copied from this object. The array must have zero-based indexing.</param>
        /// <param name="arrayIndex">The zero-based index in array at which copying begins.</param>
        /// <param name="count">The number of elements to copy to array.</param>
        /// <exception cref="ArgumentNullException"><paramref name="array"/> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="arrayIndex"/> is less than 0. -or- <paramref name="count"/> is less than 0.</exception>
        /// <exception cref="ArgumentException"><paramref name="arrayIndex"/> is greater than the length of the destination array. -or- <paramref name="count"/> is greater than the available space from the index to the end of the destination array.</exception>
        public void CopyTo(T[] array, int arrayIndex, int count)
            => elements.CopyTo(array, arrayIndex, count);

        /// <summary>
        /// Returns an enumerator that iterates through this object.
        /// </summary>
        /// <returns>A <see cref="HashSet{T}.Enumerator"/> object for this object.</returns>
        public HashSet<T>.Enumerator GetEnumerator()
            => elements.GetEnumerator();

        /// <inheritdoc/>
        public void ExceptWith(IEnumerable<T> other)
            => elements.ExceptWith(other);

        /// <inheritdoc/>
        public void IntersectWith(IEnumerable<T> other)
            => elements.IntersectWith(other);

        /// <inheritdoc/>
        public bool IsProperSubsetOf(IEnumerable<T> other)
            => elements.IsProperSubsetOf(other);

        /// <inheritdoc/>
        public bool IsProperSupersetOf(IEnumerable<T> other)
            => elements.IsProperSupersetOf(other);

        /// <inheritdoc/>
        public bool IsSubsetOf(IEnumerable<T> other)
            => elements.IsSubsetOf(other);

        /// <inheritdoc/>
        public bool IsSupersetOf(IEnumerable<T> other)
            => elements.IsSupersetOf(other);

        /// <inheritdoc/>
        public bool Overlaps(IEnumerable<T> other)
            => elements.Overlaps(other);

        /// <inheritdoc/>
        public bool Remove(T item)
            => elements.Remove(item);

        /// <inheritdoc/>
        public bool SetEquals(IEnumerable<T> other)
            => elements.SetEquals(other);

        /// <inheritdoc/>
        public void SymmetricExceptWith(IEnumerable<T> other)
            => elements.SymmetricExceptWith(other);

        /// <inheritdoc/>
        public void UnionWith(IEnumerable<T> other)
            => elements.UnionWith(other);

        /// <summary>
        /// Removes all elements that match the conditions defined by the specified predicate from the current collection.
        /// </summary>
        /// <param name="match">The <see cref="Predicate{T}"/> delegate that defines the conditions of the elements to remove.</param>
        /// <returns></returns>
        public int RemoveWhere(Predicate<T> match)
            => elements.RemoveWhere(match);

        /// <summary>
        /// Sets the capacity of a System.Collections.Generic.HashSet`1 object to the actual number of elements it contains, rounded up to a nearby, implementation-specific value.
        /// </summary>
        public void TrimExcess()
            => elements.TrimExcess();

        /// <summary>
        /// Searches the set for a given value and returns the equal value it finds, if any.
        /// </summary>
        /// <param name="equalValue">The value to search for.</param>
        /// <param name="actualValue">The value from the set that the search found, or the default value of T when the search yielded no match.</param>
        /// <returns></returns>
        public bool TryGetValue(T equalValue, [System.Diagnostics.CodeAnalysis.MaybeNullWhen(false)] out T actualValue)
            => elements.TryGetValue(equalValue, out actualValue);
        #endregion

        #region Explicit interface methods
        /// <inheritdoc/>
        bool ICollection<T>.IsReadOnly
            => ((ICollection<T>)elements).IsReadOnly;

        /// <inheritdoc/>
        void ICollection<T>.Add(T item)
            => ((ICollection<T>)elements).Add(item);

        /// <inheritdoc/>
        IEnumerator<T> IEnumerable<T>.GetEnumerator()
            => GetEnumerator();

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator()
            => GetEnumerator();
        #endregion
    }
}
