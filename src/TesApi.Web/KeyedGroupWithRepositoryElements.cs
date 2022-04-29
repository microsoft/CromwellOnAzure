// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using Tes.Repository;

namespace TesApi.Web
{
    /// <summary>
    /// Provides the abstract base class for a collection of <see cref="RepositoryItem{T}"/> elements whose keys are derivable algorithmically from the values.
    /// </summary>
    /// <typeparam name="TElement"></typeparam>
    /// <typeparam name="TSet"></typeparam>
    /// <typeparam name="TRepositoryItem"></typeparam>
    public abstract class KeyedGroupWithRepositoryElements<TElement, TSet, TRepositoryItem> : KeyedGroup<TElement, TSet>, IEnumerable<TElement> where TElement : IHasRepositoryItem<TRepositoryItem> where TRepositoryItem : RepositoryItem<TRepositoryItem> where TSet : GroupableSetWithRepositoryElements<TElement, TRepositoryItem>
    {
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="KeyedGroupWithRepositoryElements{TElement, TSet, TRepositoryItem}"/> class that uses the specified key extraction method and the specified equality comparer.
        /// </summary>
        /// <param name="getKeyForItemFunc">A method that extracts the key from the specified element.</param>
        /// <param name="comparer">The implementation of the <see cref="IEqualityComparer{T}"/> generic interface to use when comparing keys, or null to use the default equality comparer for the type of the key, obtained from <see cref="EqualityComparer{T}.Default"/>.</param>
        protected KeyedGroupWithRepositoryElements(Func<TElement, string> getKeyForItemFunc, IEqualityComparer<string> comparer = default) : base(getKeyForItemFunc, comparer) { }
        #endregion

        #region Public methods
        /// <summary>
        /// Indicates that any element has changes in any <see cref="RepositoryItem{T}"/>.
        /// </summary>
        public bool HasChanges
            => this.Any(e => e.ChangesRepositoryItem.HasChanges);

        /// <summary>
        /// Clears <see cref="KeyedGroupWithRepositoryElements{TElement, TSet, TRepositoryItem}.HasChanges"/>.
        /// </summary>
        public void ResetChanges()
        {
            foreach (var element in this)
            {
                element.ChangesRepositoryItem.ResetChanges();
            }
        }

        /// <summary>
        /// Enumerates the <see cref="RepositoryItem{T}"/> metadata of each element.
        /// </summary>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public IEnumerable<TRepositoryItem> GetRepositoryItems(Predicate<TElement> predicate = default)
            => this.Where(e => predicate?.Invoke(e) ?? true).Select(e => e.RepositoryItem);

        #endregion
    }

    ///// <summary>
    ///// Provides the abstract base class which itself is a <see cref="RepositoryItem{T}"/> for a collection whose keys are derivable algorithmically from the values.
    ///// </summary>
    ///// <typeparam name="TElement"></typeparam>
    ///// <typeparam name="TSet"></typeparam>
    //public abstract class KeyedGroupRepositoryItem<TElement, TSet> : RepositoryItem<KeyedGroupRepositoryItem<TElement, TSet>>, IEnumerable<TElement> where TSet : GroupableSet<TElement>
    //{
    //    #region Private implementation
    //    private readonly WrappedKeyedGroup _group;

    //    private class WrappedKeyedGroup : KeyedGroup<TElement, TSet>
    //    {
    //        public WrappedKeyedGroup(Func<TElement, string> getKeyForItemFunc, IEqualityComparer<string> comparer = null) : base(getKeyForItemFunc, comparer) { }

    //        internal Func<IEnumerable<TElement>, TSet> WrappedCreateSetFunc { get; set; }
    //        protected override Func<IEnumerable<TElement>, TSet> CreateSetFunc
    //            => WrappedCreateSetFunc;
    //    }
    //    #endregion

    //    #region Constructors
    //    /// <summary>
    //    /// Initializes a new instance of the <see cref="KeyedGroupRepositoryItem{TElement, TSet}"/> class that uses the specified key extraction method and the specified equality comparer.
    //    /// </summary>
    //    /// <param name="getKeyForItemFunc">A method that extracts the key from the specified element.</param>
    //    /// <param name="comparer">The implementation of the <see cref="IEqualityComparer{T}"/> generic interface to use when comparing keys, or null to use the default equality comparer for the type of the key, obtained from <see cref="EqualityComparer{T}.Default"/>.</param>
    //    protected KeyedGroupRepositoryItem(Func<TElement, string> getKeyForItemFunc, IEqualityComparer<string> comparer = default)
    //    {
    //        _group = new(getKeyForItemFunc, comparer);
    //        _group.WrappedCreateSetFunc = CreateSetFunc;
    //    }
    //    #endregion

    //    #region Protected implementation
    //    /// <summary>
    //    /// A method that creates new <see cref="GroupablSetWithRepositoryElements{TElement, TRepositoryItem}"/> objects.
    //    /// </summary>
    //    protected abstract Func<IEnumerable<TElement>, TSet> CreateSetFunc { get; }
    //    #endregion

    //    #region Public implementation
    //    /// <summary>
    //    /// Database identifier
    //    /// </summary>
    //    [DataMember(Name = "id")]
    //    public string Id { get; set; }

    //    /// <summary>
    //    /// Enumerates all elements grouped by their computed keys
    //    /// </summary>
    //    /// <returns>An <see cref="IEnumerable{T}"/> of <see cref="KeyValuePair{TKey, TValue}"/> where <see cref="KeyValuePair{TKey, TValue}.Value"/> is an <see cref="GroupablSetWithRepositoryElements{TElement, TRepositoryItem}"/> object.</returns>
    //    public virtual IEnumerable<KeyValuePair<string, TSet>> GetGroups()
    //        => _group.GetGroups();


    //    /// <summary>
    //    /// Adds the specified element to a set.
    //    /// </summary>
    //    /// <param name="element">The element to add to the set.</param>
    //    /// <returns>true if the element is added to this object; false if the element is already present.</returns>
    //    public virtual bool Add(TElement element)
    //        => _group.Add(element);

    //    /// <summary>
    //    /// Removes the first occurrence of a specific object from this object.
    //    /// </summary>
    //    /// <param name="element">The object to remove from this object.</param>
    //    /// <returns>true if item was successfully removed from this object; otherwise, false. This method also returns false if item is not found in this object.</returns>
    //    /// <exception cref="InvalidOperationException">This object is corrupt.</exception>
    //    public virtual bool Remove(TElement element)
    //        => _group.Remove(element);

    //    /// <summary>
    //    /// Gets an <see cref="IEnumerable{T}"/> containing the keys of this object.
    //    /// </summary>
    //    public virtual IEnumerable<string> Keys
    //        => _group.Keys;

    //    /// <summary>
    //    /// Tries to get an item from the collection using the specified key.
    //    /// </summary>
    //    /// <param name="key">The key of the item to search in the collection.</param>
    //    /// <param name="set">When this method returns true, the item from the collection that matches the provided key; when this method returns false, the default value for the type of the collection.</param>
    //    /// <exception cref="ArgumentNullException"><paramref name="key"/> is null.</exception>
    //    /// <returns>true if an item for the specified key was found in the collection; otherwise, false.</returns>
    //    public virtual bool TryGetValue(string key, out TSet set)
    //        => _group.TryGetValue(key, out set);
    //    #endregion

    //    #region IEnumerable
    //    IEnumerator<TElement> IEnumerable<TElement>.GetEnumerator()
    //        => ((IEnumerable<TElement>)_group).GetEnumerator();

    //    IEnumerator IEnumerable.GetEnumerator()
    //        => ((IEnumerable)_group).GetEnumerator();
    //    #endregion
    //}
}
