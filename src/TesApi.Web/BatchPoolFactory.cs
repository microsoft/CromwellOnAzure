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
    /// <remarks>
    /// Each factory method should correspond to a separate public constructor of any class implementing the
    /// <see cref="IBatchPool"/> interface. It should only provide the parameters needed for the specific instance of
    /// that class. No parameters that can reasonably be provided by dependency injection should be included in any of
    /// these methods.
    /// </remarks>
    public sealed class BatchPoolFactory
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly ObjectFactory _batchPoolAltCreator;
        private readonly ObjectFactory _batchPoolCreator;
        private readonly ObjectFactory _batchPoolRequester;

        /// <summary>
        /// Constructor for <see cref="BatchPoolFactory"/>.
        /// </summary>
        /// <param name="serviceProvider">A service object.</param>
        public BatchPoolFactory(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
            _batchPoolAltCreator = BatchPoolAltCreatorFactory();
            _batchPoolCreator = BatchPoolCreatorFactory();
            _batchPoolRequester = BatchPoolRequesterFactory();
        }

        /// <summary>
        /// Creates <see cref="BatchPool"/> instances.
        /// </summary>
        /// <param name="poolId"></param>
        /// <param name="batchScheduler"></param>
        /// <remarks>Creates a BatchPool object to delete any pool with a matching Id that may have been created. Used when timeout errors happen during pool cration.</remarks>
        /// <returns></returns>
        public IBatchPool CreateNew(string poolId, IBatchScheduler batchScheduler)
            => (IBatchPool)_batchPoolAltCreator(_serviceProvider, new object[] { poolId, batchScheduler });

        private static ObjectFactory BatchPoolAltCreatorFactory()
            => ActivatorUtilities.CreateFactory(typeof(BatchPool), new Type[] { typeof(string), typeof(IBatchScheduler) });

        /// <summary>
        /// Creates <see cref="BatchPool"/> instances.
        /// </summary>
        /// <param name="poolInformation"></param>
        /// <param name="batchScheduler"></param>
        /// <remarks>Creates a BatchPool object to manage a newly created batch pool.</remarks>
        /// <returns></returns>
        public IBatchPool CreateNew(PoolInformation poolInformation, IBatchScheduler batchScheduler)
            => (IBatchPool)_batchPoolCreator(_serviceProvider, new object[] { poolInformation, batchScheduler });

        private static ObjectFactory BatchPoolCreatorFactory()
            => ActivatorUtilities.CreateFactory(typeof(BatchPool), new Type[] { typeof(PoolInformation), typeof(IBatchScheduler) });

        /// <summary>
        /// Retrieves <see cref="BatchPool"/> instances.
        /// </summary>
        /// <param name="pool"></param>
        /// <param name="batchScheduler"></param>
        /// <remarks>Creates a BatchPool object to manange an existing batch pool.</remarks>
        /// <returns></returns>
        public IBatchPool Retrieve(CloudPool pool, IBatchScheduler batchScheduler)
            => (IBatchPool)_batchPoolRequester(_serviceProvider, new object[] { pool, batchScheduler });

        private static ObjectFactory BatchPoolRequesterFactory()
            => ActivatorUtilities.CreateFactory(typeof(BatchPool), new Type[] { typeof(CloudPool), typeof(IBatchScheduler) });
    }
}
