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
        private readonly Func<string, IBatchScheduler, IBatchPool> _batchPoolAltCreator;
        private readonly Func<PoolInformation, bool, IBatchScheduler, IBatchPool> _batchPoolCreator;
        private readonly Func<CloudPool, DateTime, IBatchScheduler, IBatchPool> _batchPoolRequester;

        /// <summary>
        /// Constructor for <see cref="BatchPoolFactory"/>.
        /// </summary>
        /// <param name="serviceProvider"></param>
        public BatchPoolFactory(IServiceProvider serviceProvider)
        {
            _batchPoolAltCreator = (poolId, batchScheduler) => (IBatchPool)ActivatorUtilities.CreateFactory(typeof(BatchPool), new Type[] { typeof(string), typeof(IBatchScheduler) })(serviceProvider, new object[] { poolId, batchScheduler });
            _batchPoolCreator = (pool, isPreemptable, batchScheduler) => (IBatchPool)ActivatorUtilities.CreateFactory(typeof(BatchPool), new Type[] { typeof(PoolInformation), typeof(bool), typeof(IBatchScheduler) })(serviceProvider, new object[] { pool, isPreemptable, batchScheduler });
            _batchPoolRequester = (pool, changed, batchScheduler) => (IBatchPool)ActivatorUtilities.CreateFactory(typeof(BatchPool), new Type[] { typeof(CloudPool), typeof(DateTime), typeof(IBatchScheduler) })(serviceProvider, new object[] { pool, changed, batchScheduler });
        }

        /// <summary>
        /// Creates <see cref="BatchPool"/> instances.
        /// </summary>
        /// <param name="poolId"></param>
        /// <param name="batchScheduler"></param>
        /// <returns></returns>
        public IBatchPool CreateNew(string poolId, IBatchScheduler batchScheduler)
            => _batchPoolAltCreator(poolId, batchScheduler);

        /// <summary>
        /// Creates <see cref="BatchPool"/> instances.
        /// </summary>
        /// <param name="poolInformation"></param>
        /// <param name="isPreemptable"></param>
        /// <param name="batchScheduler"></param>
        /// <returns></returns>
        public IBatchPool CreateNew(PoolInformation poolInformation, bool isPreemptable, IBatchScheduler batchScheduler)
            => _batchPoolCreator(poolInformation, isPreemptable, batchScheduler);

        /// <summary>
        /// Retrieves <see cref="BatchPool"/> instances.
        /// </summary>
        /// <param name="pool"></param>
        /// <param name="changed"></param>
        /// <param name="batchScheduler"></param>
        /// <returns></returns>
        public IBatchPool Retrieve(CloudPool pool, DateTime changed, IBatchScheduler batchScheduler)
            => _batchPoolRequester(pool, changed, batchScheduler);
    }
}
