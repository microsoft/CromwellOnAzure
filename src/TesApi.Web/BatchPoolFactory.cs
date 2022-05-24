// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using Microsoft.Azure.Batch;
using Microsoft.Extensions.DependencyInjection;

namespace TesApi.Web
{
    /// <summary>
    /// Factory to create BatchPool instances.
    /// </summary>
    public sealed class BatchPoolFactory
    {
        private readonly Func<PoolInformation, IBatchScheduler, IBatchPool> _batchPoolCreator;
        private readonly Func<string, BatchPool.PoolData, IBatchScheduler, IBatchPool> _batchPoolRequester;

        /// <summary>
        /// Constructor for <see cref="BatchPoolFactory"/>.
        /// </summary>
        /// <param name="serviceProvider"></param>
        public BatchPoolFactory(IServiceProvider serviceProvider)
        {
            _batchPoolCreator = (pool, batchScheduler) => (IBatchPool)ActivatorUtilities.CreateFactory(typeof(BatchPool), new Type[] { typeof(PoolInformation), typeof(IBatchScheduler) })(serviceProvider, new object[] { pool, batchScheduler });
            _batchPoolRequester = (id, pool, batchScheduler) => (IBatchPool)ActivatorUtilities.CreateFactory(typeof(BatchPool), new Type[] { typeof(string), typeof(BatchPool.PoolData), typeof(IBatchScheduler) })(serviceProvider, new object[] { id, pool, batchScheduler });
        }

        /// <summary>
        /// Creates <see cref="BatchPool"/> instances.
        /// </summary>
        /// <param name="poolInformation"></param>
        /// <param name="batchScheduler"></param>
        /// <returns></returns>
        public IBatchPool CreateNew(PoolInformation poolInformation, IBatchScheduler batchScheduler)
            => _batchPoolCreator(poolInformation, batchScheduler);

        /// <summary>
        /// Retrieves <see cref="BatchPool"/> instances.
        /// </summary>
        /// <param name="poolId"></param>
        /// <param name="poolData"></param>
        /// <param name="batchScheduler"></param>
        /// <returns></returns>
        public IBatchPool Retrieve(string poolId, BatchPool.PoolData poolData, IBatchScheduler batchScheduler)
            => _batchPoolRequester(poolId, poolData, batchScheduler);
    }
}
