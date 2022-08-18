// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Polly.Retry;

// TODO: move this to Common.csproj?
namespace TesApi.Web
{
    /// <summary>
    /// Extension methods and implementations for enumerating paged enumeration/collection types from Azure
    /// </summary>
    public static class PagedInterfaceExtensions
    {
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

        /// <summary>
        /// Adapts calls returning <see cref="IAsyncEnumerable{T}"/> to <see cref="AsyncRetryPolicy"/>.
        /// </summary>
        /// <typeparam name="T">Type of results returned in <see cref="IAsyncEnumerable{T}"/> by <paramref name="func"/>.</typeparam>
        /// <param name="asyncRetryPolicy">Policy retrying calls made while enumerating results returned by <paramref name="func"/>.</param>
        /// <param name="func">Method returning <see cref="IAsyncEnumerable{T}"/>.</param>
        /// <param name="retryPolicy">Policy retrying call to <paramref name="func"/>.</param>
        /// <returns></returns>
        public static IAsyncEnumerable<T> ExecuteAsync<T>(this AsyncRetryPolicy asyncRetryPolicy, Func<IAsyncEnumerable<T>> func, RetryPolicy retryPolicy)
        {
            _ = func ?? throw new ArgumentNullException(nameof(func));
            return new PollyAsyncEnumerable<T>((retryPolicy ?? throw new ArgumentNullException(nameof(retryPolicy))).Execute(() => func()), asyncRetryPolicy ?? throw new ArgumentNullException(nameof(asyncRetryPolicy)));
        }

        #region Implementation classes
        private struct AsyncEnumerable<T> : IAsyncEnumerable<T>
        {
            private readonly Func<CancellationToken, IAsyncEnumerator<T>> _getEnumerator;

            public AsyncEnumerable(IPagedEnumerable<T> source)
            {
                _ = source ?? throw new ArgumentNullException(nameof(source));
                _getEnumerator = c => new PagedEnumerableEnumerator<T>(source, c);
            }

            public AsyncEnumerable(IPagedCollection<T> source)
            {
                _ = source ?? throw new ArgumentNullException(nameof(source));
                _getEnumerator = c => new PagedCollectionEnumerator<T>(source, c);
            }

            public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
                => _getEnumerator(cancellationToken);
        }

        private sealed class PollyAsyncEnumerable<T> : IAsyncEnumerable<T>
        {
            private readonly IAsyncEnumerable<T> _source;
            private readonly AsyncRetryPolicy _retryPolicy;

            public PollyAsyncEnumerable(IAsyncEnumerable<T> source, AsyncRetryPolicy retryPolicy)
            {
                _source = source ?? throw new ArgumentNullException(nameof(source));
                _retryPolicy = retryPolicy ?? throw new ArgumentNullException(nameof(retryPolicy));
            }

            IAsyncEnumerator<T> IAsyncEnumerable<T>.GetAsyncEnumerator(CancellationToken cancellationToken)
                => new PollyAsyncEnumerator<T>(_source.GetAsyncEnumerator(cancellationToken), _retryPolicy, cancellationToken);
        }

        private sealed class PollyAsyncEnumerator<T> : IAsyncEnumerator<T>
        {
            private readonly IAsyncEnumerator<T> _source;
            private readonly AsyncRetryPolicy _retryPolicy;
            private readonly CancellationToken _cancellationToken;

            public PollyAsyncEnumerator(IAsyncEnumerator<T> source, AsyncRetryPolicy retryPolicy, CancellationToken cancellationToken)
            {
                _source = source ?? throw new ArgumentNullException(nameof(source));
                _retryPolicy = retryPolicy ?? throw new ArgumentNullException(nameof(retryPolicy));
                _cancellationToken = cancellationToken;
            }

            T IAsyncEnumerator<T>.Current
                => _source.Current;

            ValueTask IAsyncDisposable.DisposeAsync()
                => _source.DisposeAsync();

            ValueTask<bool> IAsyncEnumerator<T>.MoveNextAsync()
                => new(_retryPolicy.ExecuteAsync(ct => _source.MoveNextAsync(ct).AsTask(), _cancellationToken));
        }

        private sealed class PagedCollectionEnumerator<T> : IAsyncEnumerator<T>
        {
            private IPagedCollection<T> _source;
            private readonly CancellationToken _cancellationToken;
            private IEnumerator<T> _enumerator;

            public PagedCollectionEnumerator(IPagedCollection<T> source, CancellationToken cancellationToken)
            {
                _source = source ?? throw new ArgumentNullException(nameof(source));
                _cancellationToken = cancellationToken;
                _enumerator = source.GetEnumerator();
            }

            public T Current => _enumerator.Current;

            public ValueTask DisposeAsync()
            {
                _enumerator?.Dispose();
                return ValueTask.CompletedTask;
            }

            public ValueTask<bool> MoveNextAsync()
            {
                _cancellationToken.ThrowIfCancellationRequested();
                return _enumerator.MoveNext()
                    ? ValueTask.FromResult(true)
                    : new(MoveToNextSource());

                async Task<bool> MoveToNextSource()
                {
                    do
                    {
                        _enumerator?.Dispose();
                        _enumerator = null;
                        _source = await _source.GetNextPageAsync(_cancellationToken);
                        if (_source is null)
                        {
                            return false;
                        }
                        _enumerator = _source.GetEnumerator();
                    } while (!(_enumerator?.MoveNext() ?? false));
                    return true;
                }
            }
        }

        private sealed class PagedEnumerableEnumerator<T> : IAsyncEnumerator<T>
        {
            private readonly IPagedEnumerator<T> _source;
            private readonly CancellationToken _cancellationToken;

            public PagedEnumerableEnumerator(IPagedEnumerable<T> source, CancellationToken cancellationToken)
            {
                _source = (source ?? throw new ArgumentNullException(nameof(source))).GetPagedEnumerator();
                _cancellationToken = cancellationToken;
            }

            public T Current => _source.Current;

            public ValueTask DisposeAsync()
            {
                _source.Dispose();
                return ValueTask.CompletedTask;
            }

            public async ValueTask<bool> MoveNextAsync()
            {
                _cancellationToken.ThrowIfCancellationRequested();
                return await _source.MoveNextAsync(_cancellationToken);
            }
        }
        #endregion
    }
}
