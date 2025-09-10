namespace Durable
{
    using System.Collections.Generic;

    /// <summary>
    /// Represents an asynchronous durable result containing a query and its corresponding async enumerable result.
    /// </summary>
    /// <typeparam name="T">The type of items in the result enumerable.</typeparam>
    public class AsyncDurableResult<T> : IAsyncDurableResult<T>
    {
        /// <summary>
        /// Gets the query string associated with this result.
        /// </summary>
        public string Query { get; }

        /// <summary>
        /// Gets the asynchronous enumerable result containing items of type T.
        /// </summary>
        public IAsyncEnumerable<T> Result { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncDurableResult{T}"/> class.
        /// </summary>
        /// <param name="query">The query string associated with this result.</param>
        /// <param name="result">The asynchronous enumerable result containing items of type T.</param>
        public AsyncDurableResult(string query, IAsyncEnumerable<T> result)
        {
            Query = query;
            Result = result;
        }
    }
}