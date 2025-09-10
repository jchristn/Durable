namespace Durable
{
    using System.Collections.Generic;

    /// <summary>
    /// Represents the result of an asynchronous durable query operation.
    /// </summary>
    /// <typeparam name="T">The type of objects returned in the result.</typeparam>
    public interface IAsyncDurableResult<T>
    {
        /// <summary>
        /// Gets the SQL query string that was executed.
        /// </summary>
        string Query { get; }

        /// <summary>
        /// Gets the asynchronous enumerable result set containing the query results.
        /// </summary>
        IAsyncEnumerable<T> Result { get; }
    }
}