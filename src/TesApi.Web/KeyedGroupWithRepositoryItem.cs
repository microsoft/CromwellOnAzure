// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Runtime.Serialization;
using Tes.Repository;

namespace TesApi.Web
{
    /// <summary>
    /// TODO
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class KeyedGroup<T> : IEnumerable<T>
    {
        #region Private implementation
        private readonly KeyedCollectionWithRepositoryItem _sets;

        private bool CreateSet(T element)
        {
            _sets.Add(CreateSetFunc(Enumerable.Empty<T>().Append(element)));
            return true;
        }
        #endregion

        #region Constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="KeyedGroupWithRepositoryElements{TElement, TRepositoryItem}"/> class that uses the specified key extraction method and the specified equality comparer.
        /// </summary>
        /// <param name="getKeyForItemFunc">A method that extracts the key from the specified element.</param>
        /// <param name="comparer">The implementation of the <see cref="IEqualityComparer{T}"/> generic interface to use when comparing keys, or null to use the default equality comparer for the type of the key, obtained from <see cref="EqualityComparer{T}.Default"/>.</param>
        public KeyedGroup(Func<T, string> getKeyForItemFunc, IEqualityComparer<string> comparer = default)
            => _sets = new(getKeyForItemFunc, comparer);
        #endregion

        #region Protected implementation
        /// <summary>
        /// TODO:
        /// </summary>
        protected IReadOnlyDictionary<string, GroupableSet<T>> Dictionary
            => new ReadOnlyDictionary<string, GroupableSet<T>>(_sets.Dictionary ?? Enumerable.Empty<KeyValuePair<string, GroupableSet<T>>>().ToDictionary(p => p.Key, p => p.Value));

        /// <summary>
        /// A method that creates new <see cref="GroupablSetWithRepositoryElements{TElement, TRepositoryItem}"/> objects.
        /// </summary>
        protected virtual Func<IEnumerable<T>, GroupableSet<T>> CreateSetFunc
            => c => new GroupableSet<T>(c);
        #endregion

        #region Public methods
        /// <summary>
        /// Enumerates all elements grouped by their computed keys
        /// </summary>
        /// <returns>An <see cref="IEnumerable{T}"/> of <see cref="KeyValuePair{TKey, TValue}"/> where <see cref="KeyValuePair{TKey, TValue}.Value"/> is an <see cref="GroupablSetWithRepositoryElements{TElement, TRepositoryItem}"/> object.</returns>
        public virtual IEnumerable<KeyValuePair<string, GroupableSet<T>>> GetGroups()
            => Dictionary;


        /// <summary>
        /// Adds the specified element to a set.
        /// </summary>
        /// <param name="element">The element to add to the set.</param>
        /// <returns>true if the element is added to this object; false if the element is already present.</returns>
        public virtual bool Add(T element)
            => _sets.TryGetValue(_sets.GetKeyForItemFunc(element), out var set) ? set.Add(element) : CreateSet(element);

        /// <summary>
        /// Removes the first occurrence of a specific object from this object.
        /// </summary>
        /// <param name="element">The object to remove from this object.</param>
        /// <returns>true if item was successfully removed from this object; otherwise, false. This method also returns false if item is not found in this object.</returns>
        /// <exception cref="InvalidOperationException">This object is corrupt.</exception>
        public virtual bool Remove(T element)
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
        /// Gets an <see cref="IEnumerable{T}"/> containing the keys of this object.
        /// </summary>
        public virtual IEnumerable<string> Keys
            => Dictionary.Keys;

        /// <summary>
        /// Tries to get an item from the collection using the specified key.
        /// </summary>
        /// <param name="key">The key of the item to search in the collection.</param>
        /// <param name="set">When this method returns true, the item from the collection that matches the provided key; when this method returns false, the default value for the type of the collection.</param>
        /// <exception cref="ArgumentNullException"><paramref name="key"/> is null.</exception>
        /// <returns>true if an item for the specified key was found in the collection; otherwise, false.</returns>
        public virtual bool TryGetValue(string key, out GroupableSet<T> set)
            => _sets.TryGetValue(key, out set);

        //public ISet<IBatchPool> this[string key] { get => pools.Dictionary[key]; set => DictionarySet(key, value); }

        //private void DictionarySet(string key, ISet<IBatchPool> value)
        //{
        //    if (pools.Dictionary? .ContainsKey(key) ?? false)
        //    {
        //        pools.Dictionary[key] = new HashSet<IBatchPool>(value, new BatchPoolEqualityComparer());
        //        return;
        //    }

        //    pools.Add(new HashSet<IBatchPool>(value, new BatchPoolEqualityComparer()));
        //}
        //public Values
        //    => pools.Dictionary.Values ?? Enumerable.Empty<ISet<IBatchPool>>().ToList();

        //public int Count
        //    => pools.Count;

        //public void Add(string key, ISet<IBatchPool> value)
        //    => ((ICollection<KeyValuePair<string, ISet<IBatchPool>>>)this).Add(new KeyValuePair<string, ISet<IBatchPool>>(key, value));

        //public void Add(KeyValuePair<string, ISet<IBatchPool>> item)
        //{
        //    //TODO: validate that key is correct for each element in value

        //    if (!item.Key.Equals(pools.KeyForItem(item.Value)))
        //    {
        //        throw new InvalidOperationException("Mismatched key");
        //    }

        //    pools.Add(new HashSet<IBatchPool>(item.Value, new BatchPoolEqualityComparer()));
        //}

        //public bool Contains(KeyValuePair<string, ISet<IBatchPool>> item)
        //    => pools.Dictionary?.Contains(item) ?? false;

        //public bool ContainsKey(string key)
        //    => pools.Dictionary?.ContainsKey(key) ?? false;

        //public void CopyTo(KeyValuePair<string, ISet<IBatchPool>>[] array, int arrayIndex)
        //    => pools.Dictionary?.CopyTo(array, arrayIndex);

        //public bool Remove(KeyValuePair<string, ISet<IBatchPool>> item)
        //    => pools.Dictionary?.Remove(item) ?? false;

        //public bool Remove(string key)
        //    => pools.TryGetValue(key, out var item) && pools.Remove(item);

        //public void Clear()
        //    => pools.Clear();
        #endregion

        #region IEnumerable
        IEnumerator<T> IEnumerable<T>.GetEnumerator()
            => _sets.SelectMany(s => s).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator()
            => _sets.SelectMany(s => s).GetEnumerator();
        #endregion

        #region Embedded classes
        private class KeyedCollectionWithRepositoryItem : KeyedCollection<string, GroupableSet<T>>
        {
            public Func<T, string> GetKeyForItemFunc { get; }
            public new IDictionary<string, GroupableSet<T>> Dictionary
                => base.Dictionary;

            public KeyedCollectionWithRepositoryItem(Func<T, string> getKeyForItemFunc, IEqualityComparer<string> comparer = default)
                : base(comparer ?? StringComparer.Ordinal)
                => GetKeyForItemFunc = getKeyForItemFunc ?? throw new ArgumentNullException(nameof(getKeyForItemFunc));
            protected override string GetKeyForItem(GroupableSet<T> item)
                => GetKeyForItemFunc(item.FirstOrDefault());
        }
        #endregion
    }

    /// <summary>
    /// TODO
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class KeyedGroupRepositoryItem<T> : RepositoryItem<KeyedGroupRepositoryItem<T>>, IEnumerable<T>
    {
        #region Private implementation
        private readonly WrappedKeyedGroup _group;

        private class WrappedKeyedGroup : KeyedGroup<T>
        {
            public WrappedKeyedGroup(Func<T, string> getKeyForItemFunc, IEqualityComparer<string> comparer = null) : base(getKeyForItemFunc, comparer) { }

            internal IReadOnlyDictionary<string, GroupableSet<T>> WrappedDictionary
                => base.Dictionary;

            internal Func<IEnumerable<T>, GroupableSet<T>> BaseCreateSetFunc => base.CreateSetFunc;
            internal Func<IEnumerable<T>, GroupableSet<T>> WrappedCreateSetFunc { get; set; }
            protected override Func<IEnumerable<T>, GroupableSet<T>> CreateSetFunc => WrappedCreateSetFunc;

        }
        #endregion

        #region Constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="KeyedGroupWithRepositoryElements{TElement, TRepositoryItem}"/> class that uses the specified key extraction method and the specified equality comparer.
        /// </summary>
        /// <param name="getKeyForItemFunc">A method that extracts the key from the specified element.</param>
        /// <param name="comparer">The implementation of the <see cref="IEqualityComparer{T}"/> generic interface to use when comparing keys, or null to use the default equality comparer for the type of the key, obtained from <see cref="EqualityComparer{T}.Default"/>.</param>
        public KeyedGroupRepositoryItem(Func<T, string> getKeyForItemFunc, IEqualityComparer<string> comparer = default)
        {
            _group = new(getKeyForItemFunc, comparer);
            _group.WrappedCreateSetFunc = CreateSetFunc;
        }
        #endregion

        #region Protected implementation
        /// <summary>
        /// TODO:
        /// </summary>
        protected IReadOnlyDictionary<string, GroupableSet<T>> Dictionary
            => _group.WrappedDictionary;

        /// <summary>
        /// A method that creates new <see cref="GroupablSetWithRepositoryElements{TElement, TRepositoryItem}"/> objects.
        /// </summary>
        protected virtual Func<IEnumerable<T>, GroupableSet<T>> CreateSetFunc
            => _group.BaseCreateSetFunc;
        #endregion

        #region Public implementation
        /// <summary>
        /// Database identifier
        /// </summary>
        [DataMember(Name = "id")]
        public string Id { get; set; }

        /// <summary>
        /// Enumerates all elements grouped by their computed keys
        /// </summary>
        /// <returns>An <see cref="IEnumerable{T}"/> of <see cref="KeyValuePair{TKey, TValue}"/> where <see cref="KeyValuePair{TKey, TValue}.Value"/> is an <see cref="GroupablSetWithRepositoryElements{TElement, TRepositoryItem}"/> object.</returns>
        public virtual IEnumerable<KeyValuePair<string, GroupableSet<T>>> GetGroups()
            => _group.GetGroups();


        /// <summary>
        /// Adds the specified element to a set.
        /// </summary>
        /// <param name="element">The element to add to the set.</param>
        /// <returns>true if the element is added to this object; false if the element is already present.</returns>
        public virtual bool Add(T element)
            => _group.Add(element);

        /// <summary>
        /// Removes the first occurrence of a specific object from this object.
        /// </summary>
        /// <param name="element">The object to remove from this object.</param>
        /// <returns>true if item was successfully removed from this object; otherwise, false. This method also returns false if item is not found in this object.</returns>
        /// <exception cref="InvalidOperationException">This object is corrupt.</exception>
        public virtual bool Remove(T element)
            => _group.Remove(element);

        /// <summary>
        /// Gets an <see cref="IEnumerable{T}"/> containing the keys of this object.
        /// </summary>
        public virtual IEnumerable<string> Keys
            => _group.Keys;

        /// <summary>
        /// Tries to get an item from the collection using the specified key.
        /// </summary>
        /// <param name="key">The key of the item to search in the collection.</param>
        /// <param name="set">When this method returns true, the item from the collection that matches the provided key; when this method returns false, the default value for the type of the collection.</param>
        /// <exception cref="ArgumentNullException"><paramref name="key"/> is null.</exception>
        /// <returns>true if an item for the specified key was found in the collection; otherwise, false.</returns>
        public virtual bool TryGetValue(string key, out GroupableSet<T> set)
            => _group.TryGetValue(key, out set);
        #endregion

        #region IEnumerable
        IEnumerator<T> IEnumerable<T>.GetEnumerator()
            => ((IEnumerable<T>)_group).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator()
            => ((IEnumerable)_group).GetEnumerator();
        #endregion
    }

    /// <summary>
    /// TODO
    /// </summary>
    /// <typeparam name="TElement"></typeparam>
    /// <typeparam name="TRepositoryItem"></typeparam>
    public class KeyedGroupWithRepositoryElements<TElement, TRepositoryItem> : KeyedGroup<TElement>, IEnumerable<TElement> where TElement : IHasRepositoryItem<TRepositoryItem> where TRepositoryItem : RepositoryItem<TRepositoryItem>
    {
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="KeyedGroupWithRepositoryElements{TElement, TRepositoryItem}"/> class that uses the specified key extraction method and the specified equality comparer.
        /// </summary>
        /// <param name="getKeyForItemFunc">A method that extracts the key from the specified element.</param>
        /// <param name="comparer">The implementation of the <see cref="IEqualityComparer{T}"/> generic interface to use when comparing keys, or null to use the default equality comparer for the type of the key, obtained from <see cref="EqualityComparer{T}.Default"/>.</param>
        public KeyedGroupWithRepositoryElements(Func<TElement, string> getKeyForItemFunc, IEqualityComparer<string> comparer = default) : base(getKeyForItemFunc, comparer) { }
        #endregion

        #region Protected implementation
        /// <summary>
        /// TODO:
        /// </summary>
        protected new IReadOnlyDictionary<string, GroupablSetWithRepositoryElements<TElement, TRepositoryItem>> Dictionary
            => (IReadOnlyDictionary<string, GroupablSetWithRepositoryElements<TElement, TRepositoryItem>>)base.Dictionary;

        /// <summary>
        /// A method that creates new <see cref="GroupablSetWithRepositoryElements{TElement, TRepositoryItem}"/> objects.
        /// </summary>
        protected override Func<IEnumerable<TElement>, GroupableSet<TElement>> CreateSetFunc
            => c => new GroupablSetWithRepositoryElements<TElement, TRepositoryItem>(c);
        #endregion

        #region Public methods
        /// <summary>
        /// TODO
        /// </summary>
        public bool HasChanges
            => this.Any(e => e.ChangesRepositoryItem.HasChanges);

        /// <summary>
        /// TODO
        /// </summary>
        public void ResetChanges()
        {
            foreach (var element in this)
            {
                element.ChangesRepositoryItem.ResetChanges();
            }
        }

        /// <summary>
        /// TODO
        /// </summary>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public IEnumerable<TRepositoryItem> GetRepositoryItems(Predicate<TElement> predicate = default)
            => this.Where(e => predicate?.Invoke(e) ?? true).Select(e => e.RepositoryItem);

        #endregion
    }
}
