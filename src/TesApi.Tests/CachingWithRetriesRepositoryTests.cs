// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Polly.Utilities;
using Tes.Models;
using Tes.Repository;
using TesApi.Web;

namespace TesApi.Tests
{
    [TestClass]
    public class CachingWithRetriesRepositoryTests
    {
        [TestMethod]
        public async Task CreateItemAsync_ClearsAllItemsPredicateCacheKeys()
        {
            var repository = GetMockRepository();
            var cachingRepository = new CachingWithRetriesRepository<TesTask>(repository.Object);
            var tesTask = new TesTask { Id = "createItem", State = TesState.QUEUEDEnum };

            Expression<Func<TesTask, bool>> predicate = t => t.State == TesState.QUEUEDEnum
                       || t.State == TesState.INITIALIZINGEnum
                       || t.State == TesState.RUNNINGEnum
                       || (t.State == TesState.CANCELEDEnum && t.IsCancelRequested);

            var items = await cachingRepository.GetItemsAsync(predicate);
            await cachingRepository.CreateItemAsync(tesTask);
            var items2 = await cachingRepository.GetItemsAsync(predicate);

            repository.Verify(mock => mock.GetItemsAsync(It.IsAny<Expression<Func<TesTask, bool>>>()), Times.Exactly(2));
            repository.Verify(mock => mock.CreateItemAsync(It.IsAny<TesTask>()), Times.Once());
            Assert.AreEqual(items, items2);
        }

        [TestMethod]
        public async Task DeleteItemAsync_ClearsAllItemsPredicateCacheKeys()
        {
            var repository = GetMockRepository();
            var cachingRepository = new CachingWithRetriesRepository<TesTask>(repository.Object);

            Expression<Func<TesTask, bool>> predicate = t => t.State == TesState.QUEUEDEnum
                       || t.State == TesState.INITIALIZINGEnum
                       || t.State == TesState.RUNNINGEnum
                       || (t.State == TesState.CANCELEDEnum && t.IsCancelRequested);

            var items = await cachingRepository.GetItemsAsync(predicate);
            await cachingRepository.DeleteItemAsync("deleteItem");
            var items2 = await cachingRepository.GetItemsAsync(predicate);

            repository.Verify(mock => mock.GetItemsAsync(It.IsAny<Expression<Func<TesTask, bool>>>()), Times.Exactly(2));
            repository.Verify(mock => mock.DeleteItemAsync(It.IsAny<string>()), Times.Once());
            Assert.AreEqual(items, items2);
        }

        [TestMethod]
        public async Task DeleteItemAsync_RemovesItemFromCache()
        {
            var repository = GetMockRepository();
            var cachingRepository = new CachingWithRetriesRepository<TesTask>(repository.Object);

            TesTask tesTask = null;
            var success = await cachingRepository.TryGetItemAsync("tesTaskId1", item => tesTask = item);
            await cachingRepository.DeleteItemAsync("tesTaskId1");
            var success2 = await cachingRepository.TryGetItemAsync("tesTaskId1", item => tesTask = item);

            repository.Verify(mock => mock.TryGetItemAsync("tesTaskId1", It.IsAny<Action<TesTask>>()), Times.Exactly(2));
            repository.Verify(mock => mock.DeleteItemAsync("tesTaskId1"), Times.Once());
            Assert.IsTrue(success);
            Assert.IsTrue(success2);
        }

        [TestMethod]
        public async Task UpdateItemAsync_ClearsAllItemsPredicateCacheKeys()
        {
            var repository = GetMockRepository();
            var cachingRepository = new CachingWithRetriesRepository<TesTask>(repository.Object);
            var tesTask = new TesTask { Id = "updateItem" };

            Expression <Func<TesTask, bool>> predicate = t => t.State == TesState.QUEUEDEnum
                       || t.State == TesState.INITIALIZINGEnum
                       || t.State == TesState.RUNNINGEnum
                       || (t.State == TesState.CANCELEDEnum && t.IsCancelRequested);

            var items = await cachingRepository.GetItemsAsync(predicate);
            await cachingRepository.UpdateItemAsync(tesTask);
            var items2 = await cachingRepository.GetItemsAsync(predicate);

            repository.Verify(mock => mock.GetItemsAsync(It.IsAny<Expression<Func<TesTask, bool>>>()), Times.Exactly(2));
            repository.Verify(mock => mock.UpdateItemAsync(It.IsAny<TesTask>()), Times.Once());
            Assert.AreEqual(items, items2);
        }

        [TestMethod]
        public async Task UpdateItemAsync_RemovesItemFromCache()
        {
            var repository = GetMockRepository();
            var cachingRepository = new CachingWithRetriesRepository<TesTask>(repository.Object);

            TesTask tesTask = null;
            var success = await cachingRepository.TryGetItemAsync("tesTaskId1", item => tesTask = item);
            await cachingRepository.UpdateItemAsync(tesTask);
            var success2 = await cachingRepository.TryGetItemAsync("tesTaskId1", item => tesTask = item);

            repository.Verify(mock => mock.TryGetItemAsync("tesTaskId1", It.IsAny<Action<TesTask>>()), Times.Exactly(2));
            repository.Verify(mock => mock.UpdateItemAsync(It.IsAny<TesTask>()), Times.Once());
            Assert.IsTrue(success);
            Assert.IsTrue(success2);
        }

        [TestMethod]
        public async Task GetItemsAsync_UsesCache()
        {
            var repository = GetMockRepository();
            var cachingRepository = new CachingWithRetriesRepository<TesTask>(repository.Object);

            Expression<Func<TesTask, bool>> predicate = t => t.State == TesState.QUEUEDEnum
                       || t.State == TesState.INITIALIZINGEnum
                       || t.State == TesState.RUNNINGEnum
                       || (t.State == TesState.CANCELEDEnum && t.IsCancelRequested);

            var items = await cachingRepository.GetItemsAsync(predicate);
            var items2 = await cachingRepository.GetItemsAsync(predicate);

            repository.Verify(mock => mock.GetItemsAsync(It.IsAny<Expression<Func<TesTask, bool>>>()), Times.Once());
            Assert.AreEqual(items, items2);
            Assert.AreEqual(3, items.Count());
        }

        [TestMethod]
        public async Task GetItemsAsync_ThrowsException_DoesNotSetCache()
        {
            SystemClock.SleepAsync = (_, __) => Task.FromResult(true);
            SystemClock.Sleep = (_, __) => { };
            var repository = GetMockRepository();
            var cachingRepository = new CachingWithRetriesRepository<TesTask>(repository.Object);
            Expression<Func<TesTask, bool>> predicate = t => t.WorkflowId.Equals("doesNotExist");

            await Assert.ThrowsExceptionAsync<Exception>(async () => await cachingRepository.GetItemsAsync(predicate));
            repository.Verify(mock => mock.GetItemsAsync(predicate), Times.Exactly(4));
        }

        [TestMethod]
        public async Task TryGetItemAsync_UsesCache()
        {
            var repository = GetMockRepository();
            var cachingRepository = new CachingWithRetriesRepository<TesTask>(repository.Object);

            TesTask tesTask = null;
            var success = await cachingRepository.TryGetItemAsync("tesTaskId1", item => tesTask = item);
            var success2 = await cachingRepository.TryGetItemAsync("tesTaskId1", item => tesTask = item);

            repository.Verify(mock => mock.TryGetItemAsync("tesTaskId1", It.IsAny<Action<TesTask>>()), Times.Once());
            Assert.IsTrue(success);
            Assert.IsTrue(success2);
        }

        [TestMethod]
        public async Task TryGetItemAsync_IfItemNotFound_DoesNotSetCache()
        {
            var repository = GetMockRepository();
            var cachingRepository = new CachingWithRetriesRepository<TesTask>(repository.Object);

            TesTask tesTask = null;
            var success = await cachingRepository.TryGetItemAsync("notFound", item => tesTask = item);
            var success2 = await cachingRepository.TryGetItemAsync("notFound", item => tesTask = item);

            repository.Verify(mock => mock.TryGetItemAsync("notFound", It.IsAny<Action<TesTask>>()), Times.Exactly(2));
            Assert.IsFalse(success);
            Assert.IsFalse(success2);
        }

        [TestMethod]
        public async Task TryGetItemAsync_ThrowsException_DoesNotSetCache()
        {
            SystemClock.SleepAsync = (_, __) => Task.FromResult(true);
            SystemClock.Sleep = (_, __) => { };
            var repository = GetMockRepository();
            var cachingRepository = new CachingWithRetriesRepository<TesTask>(repository.Object);

            TesTask tesTask = null;
            await Assert.ThrowsExceptionAsync<Exception>(async () => await cachingRepository.TryGetItemAsync("throws", item => tesTask = item));
            repository.Verify(mock => mock.TryGetItemAsync("throws", It.IsAny<Action<TesTask>>()), Times.Exactly(4));
        }

        private static Mock<IRepository<TesTask>> GetMockRepository()
        {
            var repository = new Mock<IRepository<TesTask>>();

            IEnumerable<TesTask> tasks = new List<TesTask>()
            {
                new TesTask { Id = "tesTaskId1", State = TesState.QUEUEDEnum, ETag = Guid.NewGuid().ToString() },
                new TesTask { Id = "tesTaskId2", State = TesState.INITIALIZINGEnum, ETag = Guid.NewGuid().ToString() },
                new TesTask { Id = "tesTaskId3", State = TesState.RUNNINGEnum, ETag = Guid.NewGuid().ToString() }
            };

            repository.Setup(a => a.GetItemsAsync(It.IsAny<Expression<Func<TesTask, bool>>>()))
                .Returns(Task.FromResult(tasks));

            repository.Setup(a => a.GetItemsAsync(t => t.WorkflowId.Equals("doesNotExist")))
                .Throws(new Exception());

            repository.Setup(a => a.TryGetItemAsync("tesTaskId1", It.IsAny<Action<TesTask>>()))
                .Returns(Task.FromResult(true));

            repository.Setup(repo => repo.TryGetItemAsync("tesTaskId1", It.IsAny<Action<TesTask>>()))
            .Callback<string, Action<TesTask>>((id, action) =>
            {
                action(tasks.First(t => t.Id.Equals(id)));
            })
            .ReturnsAsync(true);

            repository.Setup(a => a.TryGetItemAsync("notFound", It.IsAny<Action<TesTask>>()))
                .Returns(Task.FromResult(false));

            repository.Setup(a => a.TryGetItemAsync("throws", It.IsAny<Action<TesTask>>()))
                .Throws(new Exception());

            return repository;
        }

    }
}
