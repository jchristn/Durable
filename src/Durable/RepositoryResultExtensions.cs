namespace Durable
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Unwraps the data from <see cref="IDurableResult{T}"/> and <see cref="IAsyncDurableResult{T}"/> (the results of the
    /// <c>*WithQuery</c> methods) when the query text is not needed. Stateless and thread-safe.
    /// </summary>
    public static class RepositoryResultExtensions
    {
        /// <summary>
        /// Returns the data of a result without the query text.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="result">The durable result. May be null, which yields an empty sequence.</param>
        /// <returns>The data. Never null.</returns>
        public static IEnumerable<T> AsEnumerable<T>(this IDurableResult<T>? result)
        {
            return result?.Result ?? Enumerable.Empty<T>();
        }

        /// <summary>
        /// Returns the streamed data of a result without the query text.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="result">The async durable result. May be null, which yields an empty stream.</param>
        /// <returns>The data stream. Never null.</returns>
        public static IAsyncEnumerable<T> AsAsyncEnumerable<T>(this IAsyncDurableResult<T>? result)
        {
            return result?.Result ?? AsyncEnumerableHelper.Empty<T>();
        }

        /// <summary>
        /// Awaits a pending streamed result and streams its data without the query text.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="resultTask">The task producing the async durable result. Must not be null.</param>
        /// <param name="token">Cancellation token, also passed to the underlying stream.</param>
        /// <returns>The data stream.</returns>
        /// <exception cref="System.ArgumentNullException">Thrown when <paramref name="resultTask"/> is null.</exception>
        /// <exception cref="System.OperationCanceledException">Thrown when <paramref name="token"/> is canceled.</exception>
        public static async IAsyncEnumerable<T> AsAsyncEnumerable<T>(this Task<IAsyncDurableResult<T>> resultTask, [EnumeratorCancellation] CancellationToken token = default)
        {
            System.ArgumentNullException.ThrowIfNull(resultTask);
            IAsyncDurableResult<T> result = await resultTask.ConfigureAwait(false);
            await foreach (T item in result.AsAsyncEnumerable().WithCancellation(token).ConfigureAwait(false))
            {
                yield return item;
            }
        }

        /// <summary>
        /// Returns the first item of a result (the entity of a single-entity operation such as <c>CreateWithQuery</c>).
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="result">The durable result. May be null.</param>
        /// <returns>The first item, or default when the result is null or empty.</returns>
        public static T AsEntity<T>(this IDurableResult<T>? result)
        {
            if (result?.Result == null) return default(T)!;
            return result.Result.FirstOrDefault() ?? default(T)!;
        }

        /// <summary>
        /// Awaits a pending result and returns its first item.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="resultTask">The task producing the durable result. Must not be null.</param>
        /// <param name="token">Cancellation token that stops waiting for <paramref name="resultTask"/> (the operation itself is not canceled).</param>
        /// <returns>The first item, or default when the result is empty.</returns>
        /// <exception cref="System.ArgumentNullException">Thrown when <paramref name="resultTask"/> is null.</exception>
        /// <exception cref="System.OperationCanceledException">Thrown when <paramref name="token"/> is canceled before the result is available.</exception>
        public static async Task<T> AsEntityAsync<T>(this Task<IDurableResult<T>> resultTask, CancellationToken token = default)
        {
            System.ArgumentNullException.ThrowIfNull(resultTask);
            IDurableResult<T> result = await resultTask.WaitAsync(token).ConfigureAwait(false);
            return result.AsEntity();
        }

        /// <summary>
        /// Returns the first value of a result (the value of a scalar operation such as <c>DeleteWithQuery</c>).
        /// </summary>
        /// <typeparam name="T">The value type.</typeparam>
        /// <param name="result">The durable result. May be null.</param>
        /// <returns>The first value, or default when the result is null or empty.</returns>
        public static T AsValue<T>(this IDurableResult<T>? result)
        {
            if (result?.Result == null) return default(T)!;
            return result.Result.FirstOrDefault() ?? default(T)!;
        }

        /// <summary>
        /// Awaits a pending result and returns its first value.
        /// </summary>
        /// <typeparam name="T">The value type.</typeparam>
        /// <param name="resultTask">The task producing the durable result. Must not be null.</param>
        /// <param name="token">Cancellation token that stops waiting for <paramref name="resultTask"/> (the operation itself is not canceled).</param>
        /// <returns>The first value, or default when the result is empty.</returns>
        /// <exception cref="System.ArgumentNullException">Thrown when <paramref name="resultTask"/> is null.</exception>
        /// <exception cref="System.OperationCanceledException">Thrown when <paramref name="token"/> is canceled before the result is available.</exception>
        public static async Task<T> AsValueAsync<T>(this Task<IDurableResult<T>> resultTask, CancellationToken token = default)
        {
            System.ArgumentNullException.ThrowIfNull(resultTask);
            IDurableResult<T> result = await resultTask.WaitAsync(token).ConfigureAwait(false);
            return result.AsValue();
        }

        /// <summary>
        /// Returns the count carried by a result (for example the rows affected by <c>DeleteManyWithQuery</c>).
        /// </summary>
        /// <param name="result">The durable result. May be null.</param>
        /// <returns>The count, or 0 when the result is null or empty.</returns>
        public static int AsCount(this IDurableResult<int>? result)
        {
            return result?.Result?.FirstOrDefault() ?? 0;
        }

        /// <summary>
        /// Awaits a pending result and returns the count it carries.
        /// </summary>
        /// <param name="resultTask">The task producing the durable result. Must not be null.</param>
        /// <param name="token">Cancellation token that stops waiting for <paramref name="resultTask"/> (the operation itself is not canceled).</param>
        /// <returns>The count, or 0 when the result is empty.</returns>
        /// <exception cref="System.ArgumentNullException">Thrown when <paramref name="resultTask"/> is null.</exception>
        /// <exception cref="System.OperationCanceledException">Thrown when <paramref name="token"/> is canceled before the result is available.</exception>
        public static async Task<int> AsCountAsync(this Task<IDurableResult<int>> resultTask, CancellationToken token = default)
        {
            System.ArgumentNullException.ThrowIfNull(resultTask);
            IDurableResult<int> result = await resultTask.WaitAsync(token).ConfigureAwait(false);
            return result.AsCount();
        }
    }
}
