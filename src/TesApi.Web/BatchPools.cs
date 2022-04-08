// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Tes.Repository;
using BatchModels = Microsoft.Azure.Management.Batch.Models;

namespace TesApi.Web
{
    internal interface IBatchPoolsImpl
    {
        Task RemovePoolFromList(string poolId);

        IAsyncEnumerable<BatchPools.PoolList> GetPoolGroups();
        IAsyncEnumerable<IBatchPool> GetPools(string key);
    }

    /// <summary>
    /// Managed Azure Batch Pools service
    /// </summary>
    public sealed class BatchPools : IBatchPools, IBatchPoolsImpl
    {
        /// <summary>
        /// CosmosDB container id for storing pool metadata that needs to survive reboots/etc.
        /// </summary>
        public const string CosmosDbContainerId = "Pools";

        private readonly ILogger _logger;
        private readonly IAzureProxy _azureProxy;
        private readonly BatchPoolFactory _poolFactory;
        private readonly IRepository<PoolList> _poolListRepository;
        private readonly bool isDisabled;

        private readonly Random random = new();

        /// <summary>
        /// Constructor for the Managed Azure Batch Pools service
        /// </summary>
        /// <param name="azureProxy"></param>
        /// <param name="logger"></param>
        /// <param name="configuration"></param>
        /// <param name="poolListRepository"></param>
        /// <param name="poolFactory"></param>
        public BatchPools(IAzureProxy azureProxy, ILogger<BatchPools> logger, IConfiguration configuration, IRepository<PoolList> poolListRepository, BatchPoolFactory poolFactory)
        {
            this.isDisabled = configuration.GetValue("BatchAutopool", false);
            if (!this.isDisabled)
            {
                _poolListRepository = poolListRepository;
                _poolFactory = poolFactory;
                _azureProxy = azureProxy;
                _logger = logger;
            }
        }

        /// <inheritdoc/>
        public bool TryGet(string poolId, out IBatchPool batchPool)
        {
            batchPool = GetAllBatchPoolsAsync().Result.FirstOrDefault(p => p.Pool.PoolId.Equals(poolId, StringComparison.OrdinalIgnoreCase));
            return batchPool is not null;
        }

        /// <inheritdoc/>
        public bool IsPoolAvailable(string key)
            => GetBatchPoolsByKeyAsync(key).Result?.Any(p => p.IsAvailable) ?? false;

        /// <inheritdoc/>
        public async Task<IBatchPool> GetOrAddAsync(string key, Func<string, ValueTask<BatchModels.Pool>> valueFactory)
        {
            if (isDisabled)
            {
                return default;
            }

            _ = valueFactory ?? throw new ArgumentNullException(nameof(valueFactory));
            var keyLength = key?.Length ?? 0;
            if (keyLength > 50 || keyLength < 1)
            {
                throw new ArgumentException("Key must be between 1-50 chars in length", nameof(key));
            }

            // TODO: Make sure key doesn't contain any nonsupported chars

            var queue = await GetBatchPoolsByKeyAsync(key);
            var result = queue?.LastOrDefault(Available);
            if (result is null)
            {
                var poolQuota = _azureProxy.GetBatchAccountQuotasAsync().Result.PoolQuota;
                var activePoolsCount = _azureProxy.GetBatchActivePoolCount();
                if (activePoolsCount + 1 > poolQuota)
                {
                    throw new AzureBatchQuotaMaxedOutException($"No remaining pool quota available. There are {activePoolsCount} pools in use out of {poolQuota}.");
                }
                var uniquifier = new byte[8]; // This always becomes 13 chars, if you remove the three '=' at the end. We won't ever decode this, so we don't need any '='s
                random.NextBytes(uniquifier);
                var pool = await valueFactory($"{key}-{ConvertToBase32(uniquifier).TrimEnd('=')}"); // '-' is required by GetOrAddAsync(CloudPool)
                result = _poolFactory.CreateNew(_azureProxy.CreateBatchPoolAsync(pool).Result, this);
                await result.ServicePoolAsync(IBatchPool.ServiceKind.Create);
                await AddPool(key, result);
            }
            return result;

            static bool Available(IBatchPool pool)
                => pool.IsAvailable;

            static string ConvertToBase32(byte[] bytes) // https://datatracker.ietf.org/doc/html/rfc4648#section-6
            {
                var groupBitlength = 5;
                var Rfc4648Base32 = @"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567".ToArray();
                return new string(new BitArray(bytes)
                        .Cast<bool>()
                        .Select((b, i) => (Index: i, Value: b ? 1 << (groupBitlength - 1 - (i % groupBitlength)) : 0))
                        .GroupBy(t => t.Index / groupBitlength)
                        .Select(g => Rfc4648Base32[g.Sum(t => t.Value)])
                        .ToArray())
                    + (bytes.Length % groupBitlength) switch
                    {
                        0 => string.Empty,
                        1 => @"======",
                        2 => @"====",
                        3 => @"===",
                        4 => @"=",
                        _ => throw new InvalidOperationException(), // Keep the compiler happy. Also guard against anyone changing the permissible values of 0-4, inclusive.
                    };
            }
        }

        /// <inheritdoc/>
        public Task ScheduleReimage(ComputeNodeInformation nodeInformation, BatchTaskState taskState)
        {
            if (!isDisabled && nodeInformation is not null)
            {
                if (TryGet(nodeInformation.PoolId, out var pool))
                {
                    return pool.ScheduleReimage(nodeInformation, taskState);
                }
            }

            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<IBatchPool> GetPoolsAsync(System.Threading.CancellationToken cancellationToken = default)
            => _poolListRepository.GetItemsAsync(p => true, 256, cancellationToken).SelectManyAwait(GetPoolsAsyncAdapter);

        private async ValueTask AddPool(string key, IBatchPool pool)
        {
            if (!await _poolListRepository.TryGetItemAsync(key, async l => { l.Pools.Add(pool.Pool.PoolId); await _poolListRepository.UpdateItemAsync(l); }))
            {
                await _poolListRepository.CreateItemAsync(new PoolList { Key = key, Pools = new(Enumerable.Empty<string>().Append(pool.Pool.PoolId)) });
            }
        }

        private ValueTask<IAsyncEnumerable<IBatchPool>> GetPoolsAsyncAdapter(PoolList poolList)
            => ValueTask.FromResult(GetPoolsAsync(poolList.Pools));

        private IAsyncEnumerable<IBatchPool> GetPoolsAsync(IEnumerable<string> pools)
            => pools.Select(i => _poolFactory.Retrieve(i, this)).ToAsyncEnumerable();

        private async Task<IEnumerable<IBatchPool>> GetAllBatchPoolsAsync()
            => (await _poolListRepository.GetItemsAsync(i => true)).SelectMany(l => l.Pools).Select(i => _poolFactory.Retrieve(i, this));

        private async Task<IEnumerable<IBatchPool>> GetBatchPoolsByKeyAsync(string key)
            => (await _poolListRepository.GetItemOrDefaultAsync(key))?.Pools.Select(i => _poolFactory.Retrieve(i, this));

        #region IBatchPoolsImpl
        async Task IBatchPoolsImpl.RemovePoolFromList(string poolId)
        {
            var pool = (await _poolListRepository.GetItemsAsync(l => l.Pools.Contains(poolId))).FirstOrDefault();
            pool.Pools.Remove(poolId);
            if (pool.Pools.Count == 0)
            {
                await _poolListRepository.DeleteItemAsync(pool.Key);
            }
            else
            {
                await _poolListRepository.UpdateItemAsync(pool);
            }
        }

        IAsyncEnumerable<PoolList> IBatchPoolsImpl.GetPoolGroups()
            => _poolListRepository.GetItemsAsync(i => true).Result.ToAsyncEnumerable();

        IAsyncEnumerable<IBatchPool> IBatchPoolsImpl.GetPools(string key)
            => GetBatchPoolsByKeyAsync(key).Result?.ToAsyncEnumerable();
        #endregion

        #region Embedded classes
        /// <summary>
        /// List of Azure Batch account pools stored in CosmosDB
        /// </summary>
        public class PoolList : RepositoryItem<PoolList>
        {
            /// <summary>
            /// <see cref="IBatchPools"/> "key" value.
            /// </summary>
            [DataMember(Name = "id")]
            public string Key { get; set; }

            /// <summary>
            /// List of PoolId (<seealso cref="BatchPool.PoolData"/>)
            /// </summary>
            [DataMember(Name = "pools")]
            public List<string> Pools { get; set; }
        }
        #endregion
    }
}
