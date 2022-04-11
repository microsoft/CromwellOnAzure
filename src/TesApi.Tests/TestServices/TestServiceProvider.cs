// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using LazyCache;
using LazyCache.Providers;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Swashbuckle.AspNetCore.SwaggerGen;
using Tes.Models;
using Tes.Repository;
using TesApi.Web;

namespace TesApi.Tests.TestServices
{
    internal sealed class TestServiceProvider<T> : IDisposable, IAsyncDisposable
    {
        private readonly IServiceProvider provider;

        internal T GetT()
            => GetT(Array.Empty<Type>(), Array.Empty<object>());
            //=> ActivatorUtilities.CreateInstance<T>(provider);

        internal T GetT<T1>(T1 t1)
            => GetT(new Type[] { typeof(T1) }, new object[] { t1 });
            //=> (T)ActivatorUtilities.CreateFactory(typeof(T), new Type[] { typeof(T1) })(provider, new object[] { t1 });

        internal T GetT<T1, T2>(T1 t1, T2 t2)
            => GetT(new Type[] { typeof(T1), typeof(T2) }, new object[] { t1, t2 });
            //=> (T)ActivatorUtilities.CreateFactory(typeof(T), new Type[] { typeof(T1), typeof(T2) })(provider, new object[] { t1, t2 });

        internal T GetT<T1, T2, T3>(T1 t1, T2 t2, T3 t3)
            => GetT(new Type[] { typeof(T1), typeof(T2), typeof(T3) }, new object[] { t1, t2, t3 });
            //=> (T)ActivatorUtilities.CreateFactory(typeof(T), new Type[] { typeof(T1), typeof(T2), typeof(T3) })(provider, new object[] { t1, t2, t3 });

        internal T GetT<T1, T2, T3, T4>(T1 t1, T2 t2, T3 t3, T4 t4)
            => GetT(new Type[] { typeof(T1), typeof(T2), typeof(T3), typeof(T4) }, new object[] { t1, t2, t3, t4 });
            //=> (T)ActivatorUtilities.CreateFactory(typeof(T), new Type[] { typeof(T1), typeof(T2), typeof(T3), typeof(T4) })(provider, new object[] { t1, t2, t3, t4 });

        internal T GetT<T1, T2, T3, T4, T5>(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5)
            => GetT(new Type[] { typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5) }, new object[] { t1, t2, t3, t4, t5});
            //=> (T)ActivatorUtilities.CreateFactory(typeof(T), new Type[] { typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5) })(provider, new object[] { t1, t2, t3, t4, t5 });

        internal T GetT<T1, T2, T3, T4, T5, T6>(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6)
            => GetT(new Type[] { typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5), typeof(T6) }, new object[] { t1, t2, t3, t4, t5, t6 });
            //=> (T)ActivatorUtilities.CreateFactory(typeof(T), new Type[] { typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5), typeof(T6) })(provider, new object[] { t1, t2, t3, t4, t5, t6 });

        internal T GetT(Type[] types, object[] args)
        {
            types ??= Array.Empty<Type>();
            args ??= Array.Empty<object>();
            if (types.Length != args.Length) throw new ArgumentException("The quantity of argument types and arguments does not match.", nameof(types));
            foreach (var (type, arg) in types.Zip(args))
            {
                if (type is null) throw new ArgumentException("One or more of the types is null.", nameof(types));
                if (!type.IsInstanceOfType(arg) && type.GetDefaultValue() != arg)
                {
                    throw new ArgumentException("One or more arguments are not of the corresponding type.", nameof(args));
                }
            }

            return (T)ActivatorUtilities.CreateFactory(typeof(T), types)(provider, args);
        }

        internal TService GetService<TService>()
            => provider.GetService<TService>();

        internal IConfiguration Configuration { get; private set; }
        internal Mock<IAzureProxy> AzureProxy { get; private set; }
        //internal Mock<IBatchPools> BatchPools { get; private set; }
        internal Mock<IRepository<TesTask>> TesTaskRepository { get; private set; }
        internal Mock<IRepository<BatchPools.PoolList>> PoolListRepository { get; private set; }
        internal Mock<IRepository<BatchPool.PoolData>> PoolDataRepository { get; private set; }
        internal Mock<IRepository<BatchPool.PendingReservationItem>> PendingReservationRepository { get; private set; }
        internal Mock<IStorageAccessProvider> StorageAccessProvider { get; private set; }


        internal TestServiceProvider(
            bool mockStorageAccessProvider = false,
            bool wrapAzureProxy = false,
            IEnumerable<(string Key, string Value)> configuration = default,
            Action<Mock<IAzureProxy>> azureProxy = default,
            //Action<Mock<IBatchPools>> batchPools = default,
            Action<Mock<IRepository<TesTask>>> tesTaskRepository = default,
            (string endpoint, string key, string databaseId, string containerId, string partitionKeyValue) batchPoolRepositoryArgs = default,
            Action<Mock<IStorageAccessProvider>> storageAccessProvider = default)
            => provider = new ServiceCollection()
                .AddSingleton(_ => GetConfiguration(configuration))
                .AddSingleton(s => wrapAzureProxy ? ActivatorUtilities.CreateInstance<CachingWithRetriesAzureProxy>(s, GetAzureProxy(azureProxy).Object) : GetAzureProxy(azureProxy).Object)
                //.AddSingleton(_ => new Lazy<IBatchPools>(() => GetBatchPools(batchPools).Object))
                .AddSingleton(_ => GetTesTaskRepository(tesTaskRepository).Object)
                .AddSingleton<IRepository<BatchPools.PoolList>>(s => ActivatorUtilities.CreateInstance<Repository<BatchPools.PoolList>>(s, ParseRepositoryArgs(batchPoolRepositoryArgs)))
                .AddSingleton<IRepository<BatchPool.PoolData>>(s => ActivatorUtilities.CreateInstance<Repository<BatchPool.PoolData>>(s, ParseRepositoryArgs(batchPoolRepositoryArgs)))
                .AddSingleton<IRepository<BatchPool.PendingReservationItem>>(s => ActivatorUtilities.CreateInstance<Repository<BatchPool.PendingReservationItem>>(s, ParseRepositoryArgs(batchPoolRepositoryArgs)))
                .AddSingleton(s => mockStorageAccessProvider ? GetStorageAccessProvider(storageAccessProvider).Object : ActivatorUtilities.CreateInstance<StorageAccessProvider>(s))
                .AddSingleton<IAppCache>(_ => new CachingService(new MemoryCacheProvider(new MemoryCache(new MemoryCacheOptions()))))
                .AddTransient<ILogger<T>>(_ => NullLogger<T>.Instance)
                .IfThenElse(mockStorageAccessProvider, s => s, s => s.AddTransient<ILogger<StorageAccessProvider>>(_ => NullLogger<StorageAccessProvider>.Instance))
                .AddTransient<ILogger<BatchPools>>(_ => NullLogger<BatchPools>.Instance)
                .AddTransient<ILogger<BatchPool>>(_ => NullLogger<BatchPool>.Instance)
                .AddSingleton<TestRepositoryStorage>()
                .AddSingleton<BatchPoolFactory>()
                .AddSingleton<IBatchPools, BatchPools>()
                .AddSingleton(s => ActivatorUtilities.CreateInstance<PoolRepositoryFactory>(s, typeof(Repository<BatchPool.PoolData>), typeof(Repository<BatchPool.PendingReservationItem>)))
            .BuildServiceProvider();

        private IConfiguration GetConfiguration(IEnumerable<(string Key, string Value)> configuration)
            => Configuration = new ConfigurationBuilder().AddInMemoryCollection(configuration?.Select(t => new KeyValuePair<string, string>(t.Key, t.Value)) ?? Enumerable.Empty<KeyValuePair<string, string>>()).Build();

        private Mock<IAzureProxy> GetAzureProxy(Action<Mock<IAzureProxy>> action)
        {
            var proxy = new Mock<IAzureProxy>();
            action?.Invoke(proxy);
            return AzureProxy = proxy;
        }

        //private Mock<IBatchPools> GetBatchPools(Action<Mock<IBatchPools>> action)
        //{
        //    var proxy = new Mock<IBatchPools>();
        //    action?.Invoke(proxy);
        //    return BatchPools = proxy;
        //}

        private Mock<IRepository<TesTask>> GetTesTaskRepository(Action<Mock<IRepository<TesTask>>> action)
        {
            var proxy = new Mock<IRepository<TesTask>>();
            action?.Invoke(proxy);
            return TesTaskRepository = proxy;
        }

        private static object[] ParseRepositoryArgs((string endpoint, string key, string databaseId, string containerId, string partitionKeyValue) batchPoolRepositoryArgs)
            => Enumerable
                .Repeat(batchPoolRepositoryArgs.endpoint, 1)
                .Append(batchPoolRepositoryArgs.key)
                .Append(batchPoolRepositoryArgs.databaseId)
                .Append(batchPoolRepositoryArgs.containerId)
                .Append(batchPoolRepositoryArgs.partitionKeyValue)
                .ToArray();

        private Mock<IStorageAccessProvider> GetStorageAccessProvider(Action<Mock<IStorageAccessProvider>> action)
        {
            var proxy = new Mock<IStorageAccessProvider>();
            action?.Invoke(proxy);
            return StorageAccessProvider = proxy;
        }

        public void Dispose()
            => (provider as IDisposable)?.Dispose();

        public ValueTask DisposeAsync()
        {
            if (provider is IAsyncDisposable asyncDisposable)
                return asyncDisposable.DisposeAsync();
            else
                Dispose();
            return ValueTask.CompletedTask;
        }

        private sealed class Repository<M> : BaseRepository<M> where M : RepositoryItem<M>
        {
            public Repository(string endpoint, string key, string databaseId, string containerId, string partitionKeyValue, TestRepositoryStorage storage)
                : base(endpoint, key, databaseId, containerId, partitionKeyValue, storage, default, default) { }
        }
    }
}
