using System;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace Common
{
    public static class Retry
    {
        private static readonly TimeSpan defaultRetryInterval = TimeSpan.FromSeconds(1);
        private const int DefaultRetryCount = 3;

        /// <summary>
        /// Executes an action with retry.
        /// </summary>
        /// <param name="action">The real action that does the job.</param>
        /// /// <param name="exceptionHandler">Handler to decide if retry is needed for a specific exception.</param>
        /// <param name="retryInterval">Retry interval in milliseconds.</param>
        /// <param name="retryCount">Maximum retry count.</param>
        public static Task DoAsync(
            Func<Task> action,
            Func<Exception, bool> exceptionHandler = null,
            TimeSpan? retryInterval = default,
            int retryCount = DefaultRetryCount)
        {
            return DoAsync<object>(
                async () =>
                {
                    await action();
                    return null;
                },
                exceptionHandler,
                retryInterval,
                retryCount);
        }

        /// <summary>
        /// Executes a function with retry.
        /// </summary>
        /// <typeparam name="T">Function result type.</typeparam>
        /// <param name="function">The real function that does the job.</param>
        /// <param name="exceptionHandler">Handler to decide if retry is needed for a specific exception.</param>
        /// <param name="retryInterval">Retry interval in milliseconds.</param>
        /// <param name="retryCount">Maximum retry count.</param>
        /// <returns>The result of first successful function invocation.</returns>
        public static async Task<T> DoAsync<T>(
            Func<Task<T>> function,
            Func<Exception, bool> exceptionHandler = null,
            TimeSpan? retryInterval = default,
            int retryCount = DefaultRetryCount)
        {
            Exception lastException = null;
            exceptionHandler = exceptionHandler ?? (ex => true);

            for (var retry = 0; retry < retryCount; retry++)
            {
                try
                {
                    if (retry > 0)
                    {
                        await Task.Delay(retryInterval ?? defaultRetryInterval);
                    }

                    return await function();
                }
                catch (Exception ex)
                {
                    lastException = ex;

                    if (!exceptionHandler(ex))
                    {
                        throw;
                    }
                }
            }

            ExceptionDispatchInfo.Capture(lastException).Throw();
            return default(T); // Unreachable.
        }

        /// <summary>
        /// Executes an action with retry.
        /// </summary>
        /// <param name="action">The real action that does the job.</param>
        /// /// <param name="exceptionHandler">Handler to decide if retry is needed for a specific exception.</param>
        /// <param name="retryInterval">Retry interval in milliseconds.</param>
        /// <param name="retryCount">Maximum retry count.</param>
        public static void Do(
            Action action,
            Func<Exception, bool> exceptionHandler = null,
            TimeSpan? retryInterval = default,
            int retryCount = DefaultRetryCount)
        {
            Do<object>(
                () =>
                    {
                        action();
                        return null;
                    },
                exceptionHandler,
                retryInterval,
                retryCount);
        }

        /// <summary>
        /// Executes a function with retry.
        /// </summary>
        /// <typeparam name="T">Function result type.</typeparam>
        /// <param name="function">The real function that does the job.</param>
        /// <param name="exceptionHandler">Handler to decide if retry is needed for a specific exception.</param>
        /// <param name="retryInterval">Retry interval in milliseconds.</param>
        /// <param name="retryCount">Maximum retry count.</param>
        /// <returns>The result of first successful function invocation.</returns>
        public static T Do<T>(
            Func<T> function,
            Func<Exception, bool> exceptionHandler = null,
            TimeSpan? retryInterval = default,
            int retryCount = 3)
        {
            Exception lastException = null;
            exceptionHandler = exceptionHandler ?? (ex => true);

            for (var retry = 0; retry < retryCount; retry++)
            {
                try
                {
                    if (retry > 0)
                    {
                        Thread.Sleep(retryInterval ?? defaultRetryInterval);
                    }

                    return function();
                }
                catch (Exception ex)
                {
                    lastException = ex;

                    if (!exceptionHandler(ex))
                    {
                        throw;
                    }
                }
            }

            ExceptionDispatchInfo.Capture(lastException).Throw();
            return default(T); // Unreachable.
        }

    }
}
