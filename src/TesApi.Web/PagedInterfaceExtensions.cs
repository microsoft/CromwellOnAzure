// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;

// TODO: move this to Common.csproj?
namespace TesApi.Web
{
    /// <summary>
    /// Extension methods and implementations for enumerating paged enumeration/collection types from Azure
    /// </summary>
    public static partial class PagedInterfaceExtensions
    {
        ///// <summary>
        ///// Calls each element in the <see cref="IAsyncEnumerable{T}"/>, stopping when <paramref name="body"/> returns false.
        ///// </summary>
        ///// <typeparam name="T">The type of objects to enumerate.</typeparam>
        ///// <param name="source">The <see cref="IAsyncEnumerable{T}"/> to enumerate.</param>
        ///// <param name="body">The <see cref="Predicate{T}"/> that visits each element. Enumeration continues as long as <paramref name="body"/> returns true.</param>
        ///// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        ///// <returns><see cref="Task"/></returns>
        ///// <exception cref="ArgumentNullException"></exception>
        ///// <exception cref="OperationCanceledException"></exception>
        //public static async Task ForEachAsync<T>(this IAsyncEnumerable<T> source, Predicate<T> body, CancellationToken cancellationToken = default)
        //{
        //    await foreach (var item in (source ?? throw new ArgumentNullException(nameof(source))).WithCancellation(cancellationToken).ConfigureAwait(false))
        //    {
        //        if (!body(item)) return;
        //    }
        //}

        ///// <summary>
        ///// Calls each element in the <see cref="IAsyncEnumerable{T}"/> with <paramref name="body"/>.
        ///// </summary>
        ///// <typeparam name="T">The type of objects to enumerate.</typeparam>
        ///// <param name="source">The <see cref="IAsyncEnumerable{T}"/> to enumerate.</param>
        ///// <param name="body">The <see cref="Action{Task}"/> that visits each element.</param>
        ///// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        ///// <returns><see cref="Task"/></returns>
        ///// <exception cref="ArgumentNullException"></exception>
        ///// <exception cref="OperationCanceledException"></exception>
        //public static Task ForEachAsync<T>(this IAsyncEnumerable<T> source, Action<T> body, CancellationToken cancellationToken = default)
        //    => source.ForEachAsync(t => { body(t); return true; }, cancellationToken);


        /// <summary>
        /// Creates an <see cref="IAsyncEnumerable{T}"/> from an <see cref="IPagedEnumerable{T}"/>.
        /// </summary>
        /// <typeparam name="T">The type of objects to enumerate.</typeparam>
        /// <param name="source">The <see cref="IPagedEnumerable{T}"/> to enumerate.</param>
        /// <returns>An <see cref="IAsyncEnumerable{T}"/></returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static IAsyncEnumerable<T> ToAsyncEnumerable<T>(this IPagedEnumerable<T> source)
            => new AsyncEnumerable<T>(source ?? throw new ArgumentNullException(nameof(source)));

        /// <summary>
        /// Creates an <see cref="IAsyncEnumerable{T}"/> from an <see cref="IPagedCollection{T}"/>
        /// </summary>
        /// <typeparam name="T">The type of objects to enumerate.</typeparam>
        /// <param name="source">The <see cref="IPagedCollection{T}"/> to enumerate.</param>
        /// <returns>An <see cref="IAsyncEnumerable{T}"/></returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static IAsyncEnumerable<T> ToAsyncEnumerable<T>(this IPagedCollection<T> source)
            => new AsyncEnumerable<T>(source ?? throw new ArgumentNullException(nameof(source)));


        #region Implementation classes
        private struct AsyncEnumerable<T> : IAsyncEnumerable<T>
        {
            private readonly Func<CancellationToken, IAsyncEnumerator<T>> getEnumerator;

            public AsyncEnumerable(IPagedEnumerable<T> source)
            {
                _ = source ?? throw new ArgumentNullException(nameof(source));
                getEnumerator = c => new PagedEnumerableEnumerator<T>(source, c);
            }

            public AsyncEnumerable(IPagedCollection<T> source)
            {
                _ = source ?? throw new ArgumentNullException(nameof(source));
                getEnumerator = c => new PagedCollectionEnumerator<T>(source, c);
            }

            public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
                => getEnumerator(cancellationToken);
        }

        private sealed class PagedCollectionEnumerator<T> : IAsyncEnumerator<T>
        {
            private IPagedCollection<T> source;
            private readonly CancellationToken cancellationToken;
            private IEnumerator<T> enumerator;

            public PagedCollectionEnumerator(IPagedCollection<T> source, CancellationToken cancellationToken)
            {
                this.source = source ?? throw new ArgumentNullException(nameof(source));
                this.cancellationToken = cancellationToken;
                enumerator = source.GetEnumerator();
            }

            public T Current => enumerator.Current;

            public ValueTask DisposeAsync()
            {
                enumerator?.Dispose();
                return ValueTask.CompletedTask;
            }

            public ValueTask<bool> MoveNextAsync()
            {
                cancellationToken.ThrowIfCancellationRequested();
                return enumerator.MoveNext()
                    ? ValueTask.FromResult(true)
                    : new(MoveToNextSource());

                async Task<bool> MoveToNextSource()
                {
                    do
                    {
                        enumerator?.Dispose();
                        enumerator = null;
                        source = await source.GetNextPageAsync(cancellationToken).ConfigureAwait(false);
                        if (source is null)
                        {
                            return false;
                        }
                        enumerator = source.GetEnumerator();
                    } while (!(enumerator?.MoveNext() ?? false));
                    return true;
                }
            }
        }

        private sealed class PagedEnumerableEnumerator<T> : IAsyncEnumerator<T>
        {
            private readonly IPagedEnumerator<T> source;
            private readonly CancellationToken cancellationToken;

            public PagedEnumerableEnumerator(IPagedEnumerable<T> source, CancellationToken cancellationToken)
            {
                this.source = (source ?? throw new ArgumentNullException(nameof(source))).GetPagedEnumerator();
                this.cancellationToken = cancellationToken;
            }

            public T Current => source.Current;

            public ValueTask DisposeAsync()
            {
                source.Dispose();
                return ValueTask.CompletedTask;
            }

            public async ValueTask<bool> MoveNextAsync()
            {
                var result = await source.MoveNextAsync(cancellationToken).ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();
                return result;
            }
        }
        #endregion
    }
}
