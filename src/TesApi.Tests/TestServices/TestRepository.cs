// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Tes.Repository;

namespace TesApi.Tests.TestServices
{
    /// <summary>
    /// This is an imperfect model of a universe of CosmosDb SQL databases
    /// </summary>
    /// <remarks>This must be a singleton, within each individual test, but not across tests. Thus, it needs to be a singleton service in the test DI.</remarks>
    internal sealed class TestRepositoryStorage
    {
        private readonly System.Collections.Concurrent.ConcurrentDictionary<string, // endpoint
            System.Collections.Concurrent.ConcurrentDictionary<string,              // databaseId
                System.Collections.Concurrent.ConcurrentDictionary<string,          // containerId
                    System.Collections.Concurrent.ConcurrentDictionary<string,      // partitionKeyValue
                            System.Collections.IDictionary>>>> storage = new();

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        internal IDictionary<string, T> Items<T>(string endpoint, string databaseId, string containerId, string partitionKeyValue, Func<IDictionary<string, T>> CreateDictionary) where T : RepositoryItem<T>
        {
            endpoint ??= string.Empty;
            databaseId ??= string.Empty;
            containerId ??= string.Empty;
            partitionKeyValue ??= string.Empty;

            // Create as needed endpoint, db, container, & partitionKey
            AssertResult(storage.TryAdd(endpoint, new(StringComparer.Ordinal)));
            AssertResult(storage.GetValueOrDefault(endpoint)?.TryAdd(databaseId, new(StringComparer.Ordinal)));
            AssertResult(storage.GetValueOrDefault(endpoint)?.GetValueOrDefault(databaseId)?.TryAdd(containerId, new(StringComparer.Ordinal)));
            AssertResult(storage.GetValueOrDefault(endpoint)?.GetValueOrDefault(databaseId)?.GetValueOrDefault(containerId)?.TryAdd(partitionKeyValue, CreateDictionary() as System.Collections.IDictionary));

            // Create as needed and return document storage
            return storage.GetValueOrDefault(endpoint)?.GetValueOrDefault(databaseId)?.GetValueOrDefault(containerId)?.GetValueOrDefault(partitionKeyValue) as IDictionary<string, T> ?? throw new InvalidOperationException();
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private static void AssertResult(bool? result)
        {
            if (!result.HasValue) throw new InvalidOperationException();
        }
    }

    internal abstract class DefaultRepositoryBase
    {
        private readonly string _endpoint;
        private readonly string _databaseId;
        private readonly string _containerId;
        private readonly string _partitionKeyValue;
        private readonly TestRepositoryStorage _storage;

        protected DefaultRepositoryBase(string endpoint, string /*key*/_1, string databaseId, string containerId, string partitionKeyValue, TestRepositoryStorage storage)
        {
            _endpoint = endpoint;
            //TODO: validate key against endpoint
            _databaseId = databaseId;
            _containerId = containerId;
            _partitionKeyValue = partitionKeyValue;
            _storage = storage;
        }

        /// <summary>
        /// Retrieves the directory holding this <see cref="IRepository{T}"/> instance's data.
        /// </summary>
        /// <typeparam name="T"><see cref="RepositoryItem{T}"/></typeparam>
        /// <returns>The configured <see cref="IDictionary{string, T}"/> for the constructor parameters.</returns>
        /// <exception cref="InvalidOperationException"></exception>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        protected IDictionary<string, T> Items<T>() where T : RepositoryItem<T>
            => _storage.Items(_endpoint, _databaseId, _containerId, _partitionKeyValue, CreateDictionary<T>);

        /// <summary>
        /// Creates the directory of the type used to store data
        /// </summary>
        /// <typeparam name="T"><see cref="RepositoryItem{T}"/></typeparam>
        /// <returns>The instance of <see cref="IDictionary{string, T}"/> to use. Must implement <see cref="System.Collections.IDictionary"/>.</returns>
        /// <remarks>Defaults to <see cref="Dictionary{string, T}">. The instance returned by this method may not be used, and any data it contains may be lost.</remarks>
        protected virtual IDictionary<string, T> CreateDictionary<T>() where T : RepositoryItem<T>
            => new Dictionary<string, T>();
    }

    internal abstract class BaseRepository<T> : DefaultRepositoryBase, IRepository<T> where T : RepositoryItem<T>
    {
        private bool isDisposed = false;

        protected BaseRepository(string endpoint, string key, string databaseId, string containerId, string partitionKeyValue, TestRepositoryStorage storage)
            : base(endpoint, key, databaseId, containerId, partitionKeyValue, storage) { }

        private static T Clone(T obj)
        {
            if (ReferenceEquals(obj, null)) return default;

            return JsonConvert.DeserializeObject<T>(JsonConvert.SerializeObject(obj), new JsonSerializerSettings { ObjectCreationHandling = ObjectCreationHandling.Replace });
        }

        Task<T> IRepository<T>.CreateItemAsync(T item)
        {
            ThrowIfDisposed();
            Items<T>().Add(item.GetId(), Clone(item));
            return Task.FromResult(Clone(item));
        }

        Task IRepository<T>.DeleteItemAsync(string id)
        {
            ThrowIfDisposed();
            _ = Items<T>().Remove(id);
            return Task.CompletedTask;
        }

        public void Dispose()
            => isDisposed = true;

        private TFunc ThrowIfDisposed<TFunc>()
            => isDisposed ? throw new ObjectDisposedException(GetType().FullName) : default;

        private void ThrowIfDisposed()
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(GetType().FullName);
            }
        }

        Task<IEnumerable<T>> IRepository<T>.GetItemsAsync(Expression<Func<T, bool>> predicate)
            => ThrowIfDisposed<Task<IEnumerable<T>>>() ?? Task.FromResult(Items<T>().Values.Where(predicate.Compile().Invoke).Select(Clone));

        Task<(string, IEnumerable<T>)> IRepository<T>.GetItemsAsync(Expression<Func<T, bool>> predicate, int size, string token)
        {
            ThrowIfDisposed();
            var count = Items<T>().Count;
            var start = string.IsNullOrWhiteSpace(token) ? 0 : int.Parse(token, System.Globalization.CultureInfo.InvariantCulture);
            var continuation = (size > start + count) ? (start + count).ToString("G", System.Globalization.CultureInfo.InvariantCulture) : null;
            return Task.FromResult((continuation, Items<T>().Values.Skip(start).Take(size).Where(predicate.Compile().Invoke).Select(Clone)));
        }

        Task<bool> IRepository<T>.TryGetItemAsync(string id, Action<T> onSuccess)
        {
            ThrowIfDisposed();
            if (Items<T>().TryGetValue(id, out var item))
            {
                onSuccess?.Invoke(Clone(item));
                return Task.FromResult(true);
            }
            else
            {
                return Task.FromResult(false);
            }
        }

        Task<T> IRepository<T>.UpdateItemAsync(T item)
        {
            ThrowIfDisposed();
            if (Items<T>().ContainsKey(item.GetId()))
            {
                Items<T>()[item.GetId()] = Clone(item);
                return Task.FromResult(Clone(item));
            }

            throw new InvalidOperationException();
        }
    }
}
