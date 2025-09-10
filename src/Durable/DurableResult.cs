namespace Durable
{
    using System.Collections.Generic;

    /// <summary>
    /// Represents the result of a database query operation containing both the query and its results.
    /// </summary>
    /// <typeparam name="T">The type of objects in the result set.</typeparam>
    public class DurableResult<T> : IDurableResult<T>
    {
        /// <summary>
        /// Gets the SQL query that was executed.
        /// </summary>
        public string Query { get; }
        /// <summary>
        /// Gets the collection of results returned by the query.
        /// </summary>
        public IEnumerable<T> Result { get; }

        /// <summary>
        /// Initializes a new instance of the DurableResult class.
        /// </summary>
        /// <param name="query">The SQL query that was executed.</param>
        /// <param name="result">The collection of results returned by the query.</param>
        public DurableResult(string query, IEnumerable<T> result)
        {
            Query = query;
            Result = result;
        }
    }
}