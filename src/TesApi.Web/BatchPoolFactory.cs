// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using Microsoft.Azure.Batch;
using Microsoft.Extensions.DependencyInjection;

namespace TesApi.Web
{
    /// <summary>
    /// Factory to create BatchPool instances
    /// </summary>
    public sealed class BatchPoolFactory
    {
        private readonly Func<PoolInformation, string, IBatchPools, DateTime?, DateTime?, IBatchPool> _batchPoolCreator;

        /// <summary>
        /// Constructor for <see cref="BatchPoolFactory"/>
        /// </summary>
        /// <param name="serviceProvider"></param>
        public BatchPoolFactory(IServiceProvider serviceProvider)
        {
            var factory = ActivatorUtilities.CreateFactory(typeof(BatchPool), new Type[] { typeof(PoolInformation), typeof(string), typeof(IBatchPools), typeof(DateTime?), typeof(DateTime?) });
            _batchPoolCreator = (pool, size, pools, create, change) => (IBatchPool)factory(serviceProvider, new object[] { pool, size, pools, create, change });
        }

        /// <summary>
        /// Creates <see cref="BatchPool"/> instances
        /// </summary>
        /// <param name="poolInformation"></param>
        /// <param name="vmSize"></param>
        /// <param name="batchPools"></param>
        /// <param name="creationTime"></param>
        /// <param name="changedTime"></param>
        /// <returns></returns>
        public IBatchPool Create(PoolInformation poolInformation, string vmSize, IBatchPools batchPools, DateTime? creationTime = null, DateTime? changedTime = null)
            => _batchPoolCreator(poolInformation, vmSize, batchPools, creationTime, changedTime);
    }
}
