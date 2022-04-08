// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Management.Storage.Fluent.Models;
using Moq;
using Newtonsoft.Json;
using Tes.Repository;

namespace TesApi.Tests
{
    /// <summary>
    /// This is an incorrect model of a universe of CosmosDb SQL databases
    /// </summary>
    /// <remarks>This must be a singleton, within each individual test, but not across tests. Thus, it needs to be a service in the test DI.</remarks>
    internal sealed class TestRepositoryStorage
    {
        private readonly System.Collections.Concurrent.ConcurrentDictionary<string, // endpoint
            System.Collections.Concurrent.ConcurrentDictionary<string,              // databaseId
                System.Collections.Concurrent.ConcurrentDictionary<string,          // containerId
                    System.Collections.Concurrent.ConcurrentDictionary<string,      // partitionKeyValue
                        System.Collections.Concurrent.ConcurrentDictionary<Type,    // document type
                            System.Collections.IDictionary>>>>> storage = new();

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
            AssertResult(storage.GetValueOrDefault(endpoint)?.GetValueOrDefault(databaseId)?.GetValueOrDefault(containerId)?.TryAdd(partitionKeyValue, new()));

            // Create as needed and return document storage
            AssertResult(storage.GetValueOrDefault(endpoint)?.GetValueOrDefault(databaseId)?.GetValueOrDefault(containerId)?.GetValueOrDefault(partitionKeyValue)?.TryAdd(typeof(T), CreateDictionary() as System.Collections.IDictionary));
            return storage.GetValueOrDefault(endpoint)?.GetValueOrDefault(databaseId)?.GetValueOrDefault(containerId)?.GetValueOrDefault(partitionKeyValue)?.GetValueOrDefault(typeof(T)) as IDictionary<string, T> ?? throw new InvalidOperationException();
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

    internal abstract class DefaultRepository<T> : DefaultRepositoryBase where T : RepositoryItem<T>
    {
        protected DefaultRepository(string endpoint, string key, string databaseId, string containerId, string partitionKeyValue, TestRepositoryStorage storage)
            : base(endpoint, key, databaseId, containerId, partitionKeyValue, storage) { }

        protected void DefaultActions(Mock<IRepository<T>> mock)
        {
            mock.Setup(c => c.CreateItemAsync(It.IsAny<T>())).Returns<T>(i => { Items<T>().Add(i.GetId(), Clone(i)); return Task.FromResult(Clone(i)); });
            mock.Setup(c => c.DeleteItemAsync(It.IsAny<string>())).Callback<string>(i => _ = Items<T>().Remove(i)); // TODO: throw error?
            mock.Setup(c => c.GetItemOrDefaultAsync(It.IsAny<string>())).Returns<string>(id => Task.FromResult(Clone(Items<T>().FirstOrDefault(i => i.Key.Equals(id)).Value)));
            mock.Setup(c => c.GetItemsAsync(It.IsAny<Expression<Func<T, bool>>>())).Returns<Expression<Func<T, bool>>>(predicate => Task.FromResult(Items<T>().Values.Where(predicate.Compile().Invoke).Select(Clone)));
            mock.Setup(c => c.GetItemsAsync(It.IsAny<Expression<Func<T, bool>>>(), It.IsAny<int>(), It.IsAny<CancellationToken>())).Returns<Expression<Func<T, bool>>, int, CancellationToken>((predicate, size, token) => Items<T>().Values.Where(predicate.Compile().Invoke).Select(Clone).ToAsyncEnumerable());
            mock.Setup(c => c.GetItemsAsync(It.IsAny<Expression<Func<T, bool>>>(), It.IsAny<int>(), It.IsAny<string>())).Returns<Expression<Func<T, bool>>, int, string>((predicate, size, token) => { (var start, var continuation) = ParseToken(token, size, Items<T>().Count); return Task.FromResult((continuation, Items<T>().Values.Skip(start).Take(size).Where(predicate.Compile().Invoke).Select(Clone))); });
            mock.Setup(c => c.TryGetItemAsync(It.IsAny<string>(), It.IsAny<Action<T>>())).Returns<string, Action<T>>((i, a) => { if (Items<T>().TryGetValue(i, out var item)) { a?.Invoke(Clone(item)); return Task.FromResult(true); } else { return Task.FromResult(false); } });
            mock.Setup(c => c.UpdateItemAsync(It.IsAny<T>())).Callback<T>(i => { if (Items<T>().ContainsKey(i.GetId())) { Items<T>()[i.GetId()] = Clone(i); } else { throw new InvalidOperationException(); } }).Returns<T>(i => Task.FromResult(Clone(i)));

            static (int start, string continuation) ParseToken(string token, int count, int size)
            {
                var start = string.IsNullOrWhiteSpace(token) ? 0 : int.Parse(token, System.Globalization.CultureInfo.InvariantCulture);
                return (start, (size > start + count) ? (start + count).ToString("G", System.Globalization.CultureInfo.InvariantCulture) : null);
            }
        }

        static T Clone(T obj)
        {
            if (ReferenceEquals(obj, null)) return default;

            return JsonConvert.DeserializeObject<T>(JsonConvert.SerializeObject(obj), new JsonSerializerSettings { ObjectCreationHandling = ObjectCreationHandling.Replace });
        }
    }

    internal abstract class BaseRepository<T> : DefaultRepository<T>, IRepository<T> where T : RepositoryItem<T>
    {
        public BaseRepository(string endpoint, string key, string databaseId, string containerId, string partitionKeyValue, TestRepositoryStorage storage, Action<string, string, string, string, string> validateConstructor, Action<Mock<IRepository<T>>> setupType)
            : base(endpoint, key, databaseId, containerId, partitionKeyValue, storage)
        {
            validateConstructor?.Invoke(endpoint, key, databaseId, containerId, partitionKeyValue);
            var mock = new Mock<IRepository<T>>();
            DefaultActions(mock);
            setupType?.Invoke(mock);
            repository = mock.Object;
        }

        private readonly IRepository<T> repository;

        public Task<T> CreateItemAsync(T item) => repository.CreateItemAsync(item);
        public Task DeleteItemAsync(string id) => repository.DeleteItemAsync(id);
        public void Dispose() => repository.Dispose();
        public Task<T> GetItemOrDefaultAsync(string id) => repository.GetItemOrDefaultAsync(id);
        public Task<IEnumerable<T>> GetItemsAsync(Expression<Func<T, bool>> predicate) => repository.GetItemsAsync(predicate);
        public IAsyncEnumerable<T> GetItemsAsync(Expression<Func<T, bool>> predicate, int pageSize, CancellationToken cancellationToken) => repository.GetItemsAsync(predicate, pageSize, cancellationToken);
        public Task<(string, IEnumerable<T>)> GetItemsAsync(Expression<Func<T, bool>> predicate, int pageSize, string continuationToken) => repository.GetItemsAsync(predicate, pageSize, continuationToken);
        public Task<bool> TryGetItemAsync(string id, Action<T> onSuccess = null) => repository.TryGetItemAsync(id, onSuccess);
        public Task<T> UpdateItemAsync(T item) => repository.UpdateItemAsync(item);
    }
}
