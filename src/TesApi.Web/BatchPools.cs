// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using BatchModels = Microsoft.Azure.Management.Batch.Models;

namespace TesApi.Web
{
    internal interface IBatchPoolsImpl
    {
        ConcurrentDictionary<string, ConcurrentQueue<IBatchPool>> ManagedBatchPools { get; }
        object syncObject { get; }
        ILogger Logger { get; }
        IAzureProxy azureProxy { get; }

        TimeSpan IdleNodeCheck { get; }
        TimeSpan IdlePoolCheck { get; }
        TimeSpan ForcePoolRotationAge { get; }
    }

    /// <summary>
    /// Managed Azure Batch Pools service
    /// </summary>
    public sealed class BatchPools : IBatchPools, IBatchPoolsImpl
    {
        private readonly TimeSpan _idleNodeCheck;
        private readonly TimeSpan _idlePoolCheck;
        private readonly TimeSpan _forcePoolRotationAge;

        private readonly ILogger _logger;
        private readonly IAzureProxy _azureProxy;
        private readonly bool isDisabled;

        private readonly Random random = new();

        /// <summary>
        /// Constructor for the Managed Azure Batch Pools service
        /// </summary>
        /// <param name="azureProxy"></param>
        /// <param name="logger"></param>
        /// <param name="configuration"></param>
        public BatchPools(IAzureProxy azureProxy, ILogger<BatchPools> logger, IConfiguration configuration)
        {
            this.isDisabled = configuration.GetValue("BatchAutopool", false);
            if (!this.isDisabled)
            {
                _idleNodeCheck = TimeSpan.FromMinutes(configuration.GetValue<double>("BatchPoolIdleNodeTime", 5)); // TODO: set this to an appropriate value
                _idlePoolCheck = TimeSpan.FromMinutes(configuration.GetValue<double>("BatchPoolIdlePoolTime", 30)); // TODO: set this to an appropriate value
                _forcePoolRotationAge = TimeSpan.FromDays(configuration.GetValue<double>("BatchPoolRotationForcedTime", 60)); // TODO: set this to an appropriate value
                _azureProxy = azureProxy;
                _logger = logger;
            }
        }

        /// <inheritdoc/>
        public bool IsEmpty
            => ManagedBatchPools.Values.All(q => q.IsEmpty);

        /// <inheritdoc/>
        public bool TryGet(string poolId, out IBatchPool batchPool)
        {
            batchPool = ManagedBatchPools.Values.SelectMany(q => q).FirstOrDefault(p => p.Pool.PoolId.Equals(poolId, StringComparison.OrdinalIgnoreCase));
            return batchPool is not null;
        }

        /// <inheritdoc/>
        public bool IsPoolAvailable(string key)
            => ManagedBatchPools.TryGetValue(key, out var queue) && queue.Any(p => p.IsAvailable);

        /// <inheritdoc/>
        public async Task<IBatchPool> GetOrAddAsync(string key, Func<string, BatchModels.Pool> valueFactory)
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

            var queue = ManagedBatchPools.GetOrAdd(key, k => new());
            var result = queue.LastOrDefault(Available);
            if (result is null)
            {
                await Task.Run(() =>
                {
                    lock (syncObject)
                    {
                        result = queue.LastOrDefault(Available);
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
                            var pool = valueFactory($"{key}-{ConvertToBase32(uniquifier).TrimEnd('=')}"); // '-' is required by GetOrAddAsync(CloudPool)
                            result = BatchPool.Create(_azureProxy.CreateBatchPoolAsync(pool).Result, pool.VmSize, this);
                            queue.Enqueue(result);
                        }
                    }
                });
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
        public async Task<IBatchPool> GetOrAddAsync(CloudPool pool)
        {
            if (isDisabled)
            {
                return BatchPool.Create(new PoolInformation { PoolId = pool.Id }, pool.VirtualMachineSize, this);
            }

            IBatchPool result;
            lock (syncObject)
            {
                result = ManagedBatchPools.Values.SelectMany(q => q).FirstOrDefault(p => p.Pool.PoolId.Equals(pool.Id));
                if (result is not null)
                {
                    return result;
                }

                var separator = pool.Id?.LastIndexOf('-') ?? -1;
                if (separator == -1 || pool.Id?.Length - separator != 14 || pool.AutoScaleEnabled != false) // this can't be our pool
                {
                    return default;
                }

                var key = pool.Id[0..separator];
                DateTime? creationTime = default;
                DateTime? allocationStateTransitionTime = default;
                try { creationTime = pool.CreationTime; } catch (InvalidOperationException) { }
                try { allocationStateTransitionTime = pool.AllocationStateTransitionTime; } catch (InvalidOperationException) { }
                result = BatchPool.Create(new PoolInformation { PoolId = pool.Id }, pool.VirtualMachineSize, this, creationTime, allocationStateTransitionTime);
                ManagedBatchPools.GetOrAdd(key, k => new()).Enqueue(result);
            }

            await result.ServicePoolAsync(IBatchPool.ServiceKind.SyncSize);
            return result;
        }

        private readonly object syncObject = new();
        private ConcurrentDictionary<string, ConcurrentQueue<IBatchPool>> ManagedBatchPools { get; } = new();

        #region IBatchPoolsImpl
        TimeSpan IBatchPoolsImpl.IdleNodeCheck => _idleNodeCheck;
        TimeSpan IBatchPoolsImpl.IdlePoolCheck => _idlePoolCheck;
        TimeSpan IBatchPoolsImpl.ForcePoolRotationAge => _forcePoolRotationAge;

        ConcurrentDictionary<string, ConcurrentQueue<IBatchPool>> IBatchPoolsImpl.ManagedBatchPools => ManagedBatchPools;
        object IBatchPoolsImpl.syncObject => syncObject;
        ILogger IBatchPoolsImpl.Logger => _logger;
        IAzureProxy IBatchPoolsImpl.azureProxy => _azureProxy;
        #endregion
    }
}
