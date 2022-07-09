// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace TesApi.Web
{
    /// <summary>
    /// Provides the abstract base class for a collection whose keys are derivable algorithmically from the values.
    /// </summary>
    /// <typeparam name="TElement"></typeparam>
    /// <typeparam name="TSet"></typeparam>
    public abstract class KeyedGroup<TElement, TSet> : IEnumerable<TElement> where TSet : GroupableSet<TElement>
    {
        #region Private fields
        private readonly KeyedCollectionWrapper _sets;
        #endregion

        #region Constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="KeyedGroup{TElement, TSet}"/> class that uses the specified key extraction method and the specified equality comparer.
        /// </summary>
        /// <param name="getKeyForItemFunc">A method that extracts the key from the specified element.</param>
        /// <param name="comparer">The implementation of the <see cref="IEqualityComparer{T}"/> generic interface to use when comparing keys, or null to use the default equality comparer for the type of the key, obtained from <see cref="EqualityComparer{T}.Default"/>.</param>
        protected KeyedGroup(Func<TElement, string> getKeyForItemFunc, IEqualityComparer<string> comparer = default)
            => _sets = new(getKeyForItemFunc, comparer);
        #endregion

        #region Protected properties
        /// <summary>
        /// A method that creates new TSet objects.
        /// </summary>
        protected abstract Func<IEnumerable<TElement>, TSet> CreateSetFunc { get; }
        #endregion

        #region Public properties
        /// <summary>
        /// Gets an <see cref="IEnumerable{T}"/> containing the keys of this object.
        /// </summary>
        public virtual IEnumerable<string> Keys
            => _sets.Keys;
        #endregion

        #region Private methods
        private bool CreateSet(TElement element)
        {
            _sets.Add(CreateSetFunc(Enumerable.Empty<TElement>().Append(element)));
            return true;
        }
        #endregion

        #region Public methods
        /// <summary>
        /// Enumerates all elements grouped by their computed keys
        /// </summary>
        /// <returns>An <see cref="IEnumerable{T}"/> of <see cref="KeyValuePair{TKey, TValue}"/> where <see cref="KeyValuePair{TKey, TValue}.Value"/> is a TSet object.</returns>
        public virtual IEnumerable<KeyValuePair<string, TSet>> GetGroups()
            => _sets.GetGroups();

        /// <summary>
        /// Adds the specified element to a set.
        /// </summary>
        /// <param name="element">The element to add to the set.</param>
        /// <returns>true if the element is added to this object; false if the element is already present.</returns>
        public virtual bool Add(TElement element)
            => _sets.TryGetValue(_sets.GetKeyForItemFunc(element), out var set) ? set.Add(element) : CreateSet(element);

        /// <summary>
        /// Removes the first occurrence of a specific object from this object.
        /// </summary>
        /// <param name="element">The object to remove from this object.</param>
        /// <returns>true if item was successfully removed from this object; otherwise, false. This method also returns false if item is not found in this object.</returns>
        /// <exception cref="InvalidOperationException">This object is corrupt.</exception>
        public virtual bool Remove(TElement element)
        {
            if (_sets.TryGetValue(_sets.GetKeyForItemFunc(element), out var set))
            {
                if (set.Remove(element))
                {
                    if (0 == set.Count)
                    {
                        if (!_sets.Remove(set))
                        {
                            throw new InvalidOperationException("Unexpected internal error: unable to remove empty set from KeyedGroupWithRepositoryItem.");
                        }
                    }
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Tries to get an item from the collection using the specified key.
        /// </summary>
        /// <param name="key">The key of the item to search in the collection.</param>
        /// <param name="set">When this method returns true, the item from the collection that matches the provided key; when this method returns false, the default value for the type of the collection.</param>
        /// <exception cref="ArgumentNullException"><paramref name="key"/> is null.</exception>
        /// <returns>true if an item for the specified key was found in the collection; otherwise, false.</returns>
        public virtual bool TryGetValue(string key, out TSet set)
            => _sets.TryGetValue(key, out set);
        #endregion

        #region IEnumerable
        IEnumerator<TElement> IEnumerable<TElement>.GetEnumerator()
            => _sets.SelectMany(s => s).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator()
            => _sets.SelectMany(s => s).GetEnumerator();
        #endregion

        #region Embedded classes
        private class KeyedCollectionWrapper : KeyedCollection<string, TSet>
        {
            private readonly Dictionary<TSet, string> keyMap = new();

            public KeyedCollectionWrapper(Func<TElement, string> getKeyForItemFunc, IEqualityComparer<string> comparer = default)
                : base(comparer ?? StringComparer.Ordinal)
                => GetKeyForItemFunc = getKeyForItemFunc ?? throw new ArgumentNullException(nameof(getKeyForItemFunc));

            public Func<TElement, string> GetKeyForItemFunc { get; }

            public virtual IEnumerable<KeyValuePair<string, TSet>> GetGroups()
                => Dictionary ?? Enumerable.Empty<KeyValuePair<string, TSet>>();

            public virtual IEnumerable<string> Keys
                => Dictionary?.Keys ?? Enumerable.Empty<string>();

            #region Overrides
            protected override string GetKeyForItem(TSet item)
            {
                if (keyMap.TryGetValue(item, out var key))
                {
                    return key;
                }

                key = GetKeyForItemFunc(item.First());
                keyMap.Add(item, key);
                return key;
            }

            protected override void ClearItems()
            {
                base.ClearItems();
                keyMap.Clear();
            }

            protected override void RemoveItem(int index)
            {
                var item = Items[index];
                base.RemoveItem(index);
                keyMap.Remove(item);
            }
            #endregion
        }
        #endregion
    }
}
