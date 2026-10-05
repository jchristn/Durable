namespace Durable
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading;
    using System.Threading.Tasks;
    using System;

    /// <summary>
    /// Extension methods to provide backward compatibility for repository results.
    /// </summary>
    public static class RepositoryResultExtensions
    {
        /// <summary>
        /// Extracts just the data from a DurableResult for backward compatibility.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="result">The durable result.</param>
        /// <returns>The enumerable data without the query information.</returns>
        public static IEnumerable<T> AsEnumerable<T>(this IDurableResult<T> result)
        {
            return result?.Result ?? Enumerable.Empty<T>();
        }

        /// <summary>
        /// Extracts just the async enumerable data from an AsyncDurableResult for backward compatibility.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="result">The async durable result.</param>
        /// <returns>The async enumerable data without the query information.</returns>
        public static IAsyncEnumerable<T> AsAsyncEnumerable<T>(this IAsyncDurableResult<T> result)
        {
            return result?.Result ?? AsyncEnumerableHelper.Empty<T>();
        }

        /// <summary>
        /// Extension method to get async enumerable data from a Task of AsyncDurableResult.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="resultTask">The task containing the async durable result.</param>
        /// <returns>An async enumerable of the data.</returns>
        public static async IAsyncEnumerable<T> AsAsyncEnumerable<T>(this Task<IAsyncDurableResult<T>> resultTask)
        {
            IAsyncDurableResult<T> result = await resultTask;
            await foreach (T item in result.AsAsyncEnumerable())
            {
                yield return item;
            }
        }

        /// <summary>
        /// Extracts a single entity from a DurableResult for backward compatibility.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="result">The durable result.</param>
        /// <returns>The first entity or default.</returns>
        public static T AsEntity<T>(this IDurableResult<T> result)
        {
            if (result?.Result == null) return default(T)!;
            return result.Result.FirstOrDefault() ?? default(T)!;
        }

        /// <summary>
        /// Extracts a single entity from a Task&lt;DurableResult&lt;T&gt;&gt; for backward compatibility.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="resultTask">The task containing the durable result.</param>
        /// <returns>The first entity or default.</returns>
        public static async Task<T> AsEntity<T>(this Task<IDurableResult<T>> resultTask)
        {
            IDurableResult<T> result = await resultTask;
            return result.AsEntity();
        }

        /// <summary>
        /// Extracts a single value from a DurableResult for backward compatibility.
        /// </summary>
        /// <typeparam name="T">The value type.</typeparam>
        /// <param name="result">The durable result.</param>
        /// <returns>The first value or default.</returns>
        public static T AsValue<T>(this IDurableResult<T> result)
        {
            if (result?.Result == null) return default(T)!;
            return result.Result.FirstOrDefault() ?? default(T)!;
        }

        /// <summary>
        /// Extracts a single value from a Task&lt;DurableResult&lt;T&gt;&gt; for backward compatibility.
        /// </summary>
        /// <typeparam name="T">The value type.</typeparam>
        /// <param name="resultTask">The task containing the durable result.</param>
        /// <returns>The first value or default.</returns>
        public static async Task<T> AsValue<T>(this Task<IDurableResult<T>> resultTask)
        {
            IDurableResult<T> result = await resultTask;
            return result.AsValue();
        }

        /// <summary>
        /// Extracts a count value from a DurableResult&lt;int&gt; for backward compatibility.
        /// </summary>
        /// <param name="result">The durable result.</param>
        /// <returns>The count value.</returns>
        public static int AsCount(this IDurableResult<int> result)
        {
            return result?.Result?.FirstOrDefault() ?? 0;
        }

        /// <summary>
        /// Extracts a count value from a Task&lt;DurableResult&lt;int&gt;&gt; for backward compatibility.
        /// </summary>
        /// <param name="resultTask">The task containing the durable result.</param>
        /// <returns>The count value.</returns>
        public static async Task<int> AsCount(this Task<IDurableResult<int>> resultTask)
        {
            IDurableResult<int> result = await resultTask;
            return result.AsCount();
        }
    }
}
