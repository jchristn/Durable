namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Runs asynchronous backend operations for the synchronous members of <see cref="RepositoryBase{T}"/> and
    /// <see cref="QueryBuilder{T}"/> without deadlocking under a synchronization context.
    /// The operation is started inline with no synchronization context installed, so a backend that completes
    /// synchronously (such as an in-memory store) returns its result directly, without blocking or hopping threads.
    /// When the operation does not complete synchronously its continuations cannot capture the caller's context and run
    /// on the thread pool, and the caller blocks until it finishes. When the caller runs under a non-default
    /// <see cref="TaskScheduler"/>, the whole operation is started on the thread pool instead.
    /// Thread safety: stateless; safe for concurrent use.
    /// </summary>
    internal static class SyncBridge
    {
        #region Public-Methods

        /// <summary>
        /// Runs an operation and returns its result.
        /// </summary>
        /// <typeparam name="TResult">Result type.</typeparam>
        /// <param name="operation">Operation. Must not be null.</param>
        /// <returns>The result.</returns>
        public static TResult Run<TResult>(Func<Task<TResult>> operation)
        {
            ArgumentNullException.ThrowIfNull(operation);
            Task<TResult> task = Start(operation);
            return task.GetAwaiter().GetResult();
        }

        /// <summary>
        /// Runs an operation to completion.
        /// </summary>
        /// <param name="operation">Operation. Must not be null.</param>
        public static void Run(Func<Task> operation)
        {
            ArgumentNullException.ThrowIfNull(operation);
            Task task = Start(operation);
            task.GetAwaiter().GetResult();
        }

        /// <summary>
        /// Enumerates an asynchronous stream synchronously and lazily.
        /// </summary>
        /// <typeparam name="T">Element type.</typeparam>
        /// <param name="source">Stream. Must not be null.</param>
        /// <returns>The elements.</returns>
        public static IEnumerable<T> Enumerate<T>(IAsyncEnumerable<T> source)
        {
            ArgumentNullException.ThrowIfNull(source);
            return EnumerateCore(source);
        }

        #endregion

        #region Private-Methods

        private static IEnumerable<T> EnumerateCore<T>(IAsyncEnumerable<T> source)
        {
            IAsyncEnumerator<T> enumerator = WithoutContext(() => source.GetAsyncEnumerator(CancellationToken.None));
            try
            {
                while (true)
                {
                    ValueTask<bool> next = WithoutContext(() => enumerator.MoveNextAsync());
                    bool hasNext = next.IsCompletedSuccessfully ? next.Result : next.AsTask().GetAwaiter().GetResult();
                    if (!hasNext) yield break;
                    yield return enumerator.Current;
                }
            }
            finally
            {
                ValueTask disposal = WithoutContext(() => enumerator.DisposeAsync());
                if (!disposal.IsCompletedSuccessfully) disposal.AsTask().GetAwaiter().GetResult();
            }
        }

        private static Task<TResult> Start<TResult>(Func<Task<TResult>> operation)
        {
            if (TaskScheduler.Current != TaskScheduler.Default) return Task.Run(operation);
            return WithoutContext(operation);
        }

        private static Task Start(Func<Task> operation)
        {
            if (TaskScheduler.Current != TaskScheduler.Default) return Task.Run(operation);
            return WithoutContext(operation);
        }

        private static TResult WithoutContext<TResult>(Func<TResult> operation)
        {
            SynchronizationContext? previous = SynchronizationContext.Current;
            if (previous == null) return operation();
            SynchronizationContext.SetSynchronizationContext(null);
            try
            {
                return operation();
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(previous);
            }
        }

        #endregion
    }
}
