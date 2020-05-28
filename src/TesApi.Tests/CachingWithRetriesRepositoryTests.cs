using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Polly.Utilities;
using TesApi.Models;
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
            RepositoryItem<TesTask> repositoryItem = null;
            Action<RepositoryItem<TesTask>> action = item => repositoryItem = item;

            var success = await cachingRepository.TryGetItemAsync("tesTask1", action);
            await cachingRepository.DeleteItemAsync("tesTask1");
            var success2 = await cachingRepository.TryGetItemAsync("tesTask1", action);

            repository.Verify(mock => mock.TryGetItemAsync("tesTask1", It.IsAny<Action<RepositoryItem<TesTask>>>()), Times.Exactly(2));
            repository.Verify(mock => mock.DeleteItemAsync("tesTask1"), Times.Once());
            Assert.AreEqual(success, success2);
        }

        [TestMethod]
        public async Task UpdateItemAsync_ClearsAllItemsPredicateCacheKeys()
        {
            var repository = GetMockRepository();
            var cachingRepository = new CachingWithRetriesRepository<TesTask>(repository.Object);
            RepositoryItem<TesTask> repositoryItem = null;
            Expression<Func<TesTask, bool>> predicate = t => t.State == TesState.QUEUEDEnum
                       || t.State == TesState.INITIALIZINGEnum
                       || t.State == TesState.RUNNINGEnum
                       || (t.State == TesState.CANCELEDEnum && t.IsCancelRequested);

            var items = await cachingRepository.GetItemsAsync(predicate);
            await cachingRepository.UpdateItemAsync("updateItem", repositoryItem);
            var items2 = await cachingRepository.GetItemsAsync(predicate);

            repository.Verify(mock => mock.GetItemsAsync(It.IsAny<Expression<Func<TesTask, bool>>>()), Times.Exactly(2));
            repository.Verify(mock => mock.UpdateItemAsync(It.IsAny<string>(), It.IsAny<RepositoryItem<TesTask>>()), Times.Once());
            Assert.AreEqual(items, items2);
        }

        [TestMethod]
        public async Task UpdateItemAsync_RemovesItemFromCache()
        {
            var repository = GetMockRepository();
            var cachingRepository = new CachingWithRetriesRepository<TesTask>(repository.Object);
            RepositoryItem<TesTask> repositoryItem = null;
            Action<RepositoryItem<TesTask>> action = item => repositoryItem = item;

            var success = await cachingRepository.TryGetItemAsync("tesTask1", action);
            await cachingRepository.UpdateItemAsync("tesTask1", repositoryItem);
            var success2 = await cachingRepository.TryGetItemAsync("tesTask1", action);

            repository.Verify(mock => mock.TryGetItemAsync("tesTask1", It.IsAny<Action<RepositoryItem<TesTask>>>()), Times.Exactly(2));
            repository.Verify(mock => mock.UpdateItemAsync(It.IsAny<string>(), It.IsAny<RepositoryItem<TesTask>>()), Times.Once());
            Assert.AreEqual(success, success2);
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
            RepositoryItem<TesTask> repositoryItem = null;
            var cachingRepository = new CachingWithRetriesRepository<TesTask>(repository.Object);
            Action<RepositoryItem<TesTask>> action = item => repositoryItem = item;

            var success = await cachingRepository.TryGetItemAsync("tesTask1", action);
            var success2 = await cachingRepository.TryGetItemAsync("tesTask1", action);

            repository.Verify(mock => mock.TryGetItemAsync("tesTask1", It.IsAny<Action<RepositoryItem<TesTask>>>()), Times.Once());
            Assert.IsTrue(success);
            Assert.IsTrue(success2);
        }

        [TestMethod]
        public async Task TryGetItemAsync_IfItemNotFound_DoesNotSetCache()
        {
            var repository = GetMockRepository();
            RepositoryItem<TesTask> repositoryItem = null;
            var cachingRepository = new CachingWithRetriesRepository<TesTask>(repository.Object);
            Action<RepositoryItem<TesTask>> action = item => repositoryItem = item;

            var success = await cachingRepository.TryGetItemAsync("notFound", action);
            var success2 = await cachingRepository.TryGetItemAsync("notFound", action);

            repository.Verify(mock => mock.TryGetItemAsync("notFound", It.IsAny<Action<RepositoryItem<TesTask>>>()), Times.Exactly(2));
            Assert.IsFalse(success);
            Assert.IsFalse(success2);
        }

        [TestMethod]
        public async Task TryGetItemAsync_ThrowsException_DoesNotSetCache()
        {
            SystemClock.SleepAsync = (_, __) => Task.FromResult(true);
            SystemClock.Sleep = (_, __) => { };
            var repository = GetMockRepository();
            RepositoryItem<TesTask> repositoryItem = null;
            var cachingRepository = new CachingWithRetriesRepository<TesTask>(repository.Object);
            Action<RepositoryItem<TesTask>> action = item => repositoryItem = item;

            await Assert.ThrowsExceptionAsync<Exception>(async () => await cachingRepository.TryGetItemAsync("throws", action));
            repository.Verify(mock => mock.TryGetItemAsync("throws", It.IsAny<Action<RepositoryItem<TesTask>>>()), Times.Exactly(4));
        }

        private Mock<IRepository<TesTask>> GetMockRepository()
        {
            var repository = new Mock<IRepository<TesTask>>();
            var tasks = new List<TesTask>()
            {
                new TesTask { Id = "tesTaskId1", State = TesState.QUEUEDEnum },
                new TesTask { Id = "tesTaskId2", State = TesState.INITIALIZINGEnum },
                new TesTask { Id = "tesTaskId3", State = TesState.RUNNINGEnum },
            };

            var repositoryItems = tasks.Select(
                t => new RepositoryItem<TesTask> { ETag = Guid.NewGuid().ToString(), Value = t });

            repository.Setup(a => a.GetItemsAsync(It.IsAny<Expression<Func<TesTask, bool>>>()))
                .Returns(Task.FromResult(repositoryItems));
            repository.Setup(a => a.GetItemsAsync(t => t.WorkflowId.Equals("doesNotExist")))
                .Throws(new Exception());

            repository.Setup(a => a.TryGetItemAsync("tesTask1", It.IsAny<Action<RepositoryItem<TesTask>>>()))
                .Returns(Task.FromResult(true));
            repository.Setup(a => a.TryGetItemAsync("notFound", It.IsAny<Action<RepositoryItem<TesTask>>>()))
                .Returns(Task.FromResult(false));
            repository.Setup(a => a.TryGetItemAsync("throws", It.IsAny<Action<RepositoryItem<TesTask>>>()))
                .Throws(new Exception());

            return repository;
        }

    }
}
