namespace Durable
{
    using System.Collections.Generic;

    /// <summary>
    /// Represents a query result that includes both the executed query and its results.
    /// </summary>
    /// <typeparam name="T">The type of entities in the result.</typeparam>
    public interface IDurableResult<T>
    {
        /// <summary>
        /// Gets the SQL query that was executed.
        /// </summary>
        string Query { get; }
        
        /// <summary>
        /// Gets the results of the query execution.
        /// </summary>
        IEnumerable<T> Result { get; }
    }
}