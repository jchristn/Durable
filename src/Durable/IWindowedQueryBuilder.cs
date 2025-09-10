namespace Durable
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Provides methods for building windowed queries with window functions.
    /// </summary>
    /// <typeparam name="T">The entity type being queried.</typeparam>
    public interface IWindowedQueryBuilder<T> where T : class, new()
    {
        // Window function configuration
        /// <summary>
        /// Adds a ROW_NUMBER() window function to the query.
        /// </summary>
        /// <param name="alias">The alias for the row number column.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IWindowedQueryBuilder<T> RowNumber(string alias = "row_number");
        /// <summary>
        /// Adds a RANK() window function to the query.
        /// </summary>
        /// <param name="alias">The alias for the rank column.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IWindowedQueryBuilder<T> Rank(string alias = "rank");
        /// <summary>
        /// Adds a DENSE_RANK() window function to the query.
        /// </summary>
        /// <param name="alias">The alias for the dense rank column.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IWindowedQueryBuilder<T> DenseRank(string alias = "dense_rank");
        /// <summary>
        /// Adds a LEAD() window function to access data from a subsequent row.
        /// </summary>
        /// <typeparam name="TKey">The type of the column value.</typeparam>
        /// <param name="column">The column to access from the subsequent row.</param>
        /// <param name="offset">The number of rows to look ahead.</param>
        /// <param name="defaultValue">The default value to return if no subsequent row is found.</param>
        /// <param name="alias">The alias for the lead column.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IWindowedQueryBuilder<T> Lead<TKey>(Expression<Func<T, TKey>> column, int offset = 1, object? defaultValue = null, string alias = "lead");
        /// <summary>
        /// Adds a LAG() window function to access data from a previous row.
        /// </summary>
        /// <typeparam name="TKey">The type of the column value.</typeparam>
        /// <param name="column">The column to access from the previous row.</param>
        /// <param name="offset">The number of rows to look back.</param>
        /// <param name="defaultValue">The default value to return if no previous row is found.</param>
        /// <param name="alias">The alias for the lag column.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IWindowedQueryBuilder<T> Lag<TKey>(Expression<Func<T, TKey>> column, int offset = 1, object? defaultValue = null, string alias = "lag");
        /// <summary>
        /// Adds a FIRST_VALUE() window function to get the first value in the window frame.
        /// </summary>
        /// <typeparam name="TKey">The type of the column value.</typeparam>
        /// <param name="column">The column to get the first value from.</param>
        /// <param name="alias">The alias for the first value column.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IWindowedQueryBuilder<T> FirstValue<TKey>(Expression<Func<T, TKey>> column, string alias = "first_value");
        /// <summary>
        /// Adds a LAST_VALUE() window function to get the last value in the window frame.
        /// </summary>
        /// <typeparam name="TKey">The type of the column value.</typeparam>
        /// <param name="column">The column to get the last value from.</param>
        /// <param name="alias">The alias for the last value column.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IWindowedQueryBuilder<T> LastValue<TKey>(Expression<Func<T, TKey>> column, string alias = "last_value");
        /// <summary>
        /// Adds a NTH_VALUE() window function to get the nth value in the window frame.
        /// </summary>
        /// <typeparam name="TKey">The type of the column value.</typeparam>
        /// <param name="column">The column to get the nth value from.</param>
        /// <param name="n">The position of the value to retrieve (1-based).</param>
        /// <param name="alias">The alias for the nth value column.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IWindowedQueryBuilder<T> NthValue<TKey>(Expression<Func<T, TKey>> column, int n, string alias = "nth_value");
        /// <summary>
        /// Adds a SUM() window function to calculate the sum of values in the window frame.
        /// </summary>
        /// <typeparam name="TKey">The type of the column value.</typeparam>
        /// <param name="column">The column to sum.</param>
        /// <param name="alias">The alias for the sum column.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IWindowedQueryBuilder<T> Sum<TKey>(Expression<Func<T, TKey>> column, string alias = "sum");
        /// <summary>
        /// Adds an AVG() window function to calculate the average of values in the window frame.
        /// </summary>
        /// <typeparam name="TKey">The type of the column value.</typeparam>
        /// <param name="column">The column to average.</param>
        /// <param name="alias">The alias for the average column.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IWindowedQueryBuilder<T> Avg<TKey>(Expression<Func<T, TKey>> column, string alias = "avg");
        /// <summary>
        /// Adds a COUNT() window function to count rows in the window frame.
        /// </summary>
        /// <param name="alias">The alias for the count column.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IWindowedQueryBuilder<T> Count(string alias = "count");
        /// <summary>
        /// Adds a MIN() window function to find the minimum value in the window frame.
        /// </summary>
        /// <typeparam name="TKey">The type of the column value.</typeparam>
        /// <param name="column">The column to find the minimum value from.</param>
        /// <param name="alias">The alias for the minimum column.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IWindowedQueryBuilder<T> Min<TKey>(Expression<Func<T, TKey>> column, string alias = "min");
        /// <summary>
        /// Adds a MAX() window function to find the maximum value in the window frame.
        /// </summary>
        /// <typeparam name="TKey">The type of the column value.</typeparam>
        /// <param name="column">The column to find the maximum value from.</param>
        /// <param name="alias">The alias for the maximum column.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IWindowedQueryBuilder<T> Max<TKey>(Expression<Func<T, TKey>> column, string alias = "max");

        // Window partitioning and ordering
        /// <summary>
        /// Partitions the result set by the specified key selector for the window function.
        /// </summary>
        /// <typeparam name="TKey">The type of the partition key.</typeparam>
        /// <param name="keySelector">The expression to partition by.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IWindowedQueryBuilder<T> PartitionBy<TKey>(Expression<Func<T, TKey>> keySelector);
        /// <summary>
        /// Orders the window frame by the specified key selector in ascending order.
        /// </summary>
        /// <typeparam name="TKey">The type of the order key.</typeparam>
        /// <param name="keySelector">The expression to order by.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IWindowedQueryBuilder<T> OrderBy<TKey>(Expression<Func<T, TKey>> keySelector);
        /// <summary>
        /// Orders the window frame by the specified key selector in descending order.
        /// </summary>
        /// <typeparam name="TKey">The type of the order key.</typeparam>
        /// <param name="keySelector">The expression to order by.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IWindowedQueryBuilder<T> OrderByDescending<TKey>(Expression<Func<T, TKey>> keySelector);

        // Window frame specification
        /// <summary>
        /// Specifies a ROWS window frame with the given preceding and following row counts.
        /// </summary>
        /// <param name="preceding">The number of preceding rows to include.</param>
        /// <param name="following">The number of following rows to include.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IWindowedQueryBuilder<T> Rows(int preceding, int following);
        /// <summary>
        /// Specifies a ROWS window frame from unbounded preceding to current row.
        /// </summary>
        /// <returns>The current query builder for method chaining.</returns>
        IWindowedQueryBuilder<T> RowsUnboundedPreceding();
        /// <summary>
        /// Specifies a ROWS window frame from current row to unbounded following.
        /// </summary>
        /// <returns>The current query builder for method chaining.</returns>
        IWindowedQueryBuilder<T> RowsUnboundedFollowing();
        /// <summary>
        /// Specifies a ROWS window frame between the given start and end boundaries.
        /// </summary>
        /// <param name="start">The start boundary of the window frame.</param>
        /// <param name="end">The end boundary of the window frame.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IWindowedQueryBuilder<T> RowsBetween(string start, string end);
        /// <summary>
        /// Specifies a RANGE window frame with the given preceding and following value ranges.
        /// </summary>
        /// <param name="preceding">The preceding value range to include.</param>
        /// <param name="following">The following value range to include.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IWindowedQueryBuilder<T> Range(int preceding, int following);
        /// <summary>
        /// Specifies a RANGE window frame from unbounded preceding to current row.
        /// </summary>
        /// <returns>The current query builder for method chaining.</returns>
        IWindowedQueryBuilder<T> RangeUnboundedPreceding();
        /// <summary>
        /// Specifies a RANGE window frame from current row to unbounded following.
        /// </summary>
        /// <returns>The current query builder for method chaining.</returns>
        IWindowedQueryBuilder<T> RangeUnboundedFollowing();
        /// <summary>
        /// Specifies a RANGE window frame between the given start and end boundaries.
        /// </summary>
        /// <param name="start">The start boundary of the window frame.</param>
        /// <param name="end">The end boundary of the window frame.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IWindowedQueryBuilder<T> RangeBetween(string start, string end);

        // Return to regular query builder
        /// <summary>
        /// Ends the window function configuration and returns to the regular query builder.
        /// </summary>
        /// <returns>The regular query builder for continued query construction.</returns>
        IQueryBuilder<T> EndWindow();

        // Execution methods (inherit from base query)
        /// <summary>
        /// Executes the windowed query and returns the results.
        /// </summary>
        /// <returns>An enumerable collection of query results.</returns>
        IEnumerable<T> Execute();
        /// <summary>
        /// Asynchronously executes the windowed query and returns the results.
        /// </summary>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that returns an enumerable collection of query results.</returns>
        Task<IEnumerable<T>> ExecuteAsync(CancellationToken token = default);
        /// <summary>
        /// Executes the windowed query and returns the results as an async enumerable.
        /// </summary>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>An async enumerable collection of query results.</returns>
        IAsyncEnumerable<T> ExecuteAsyncEnumerable(CancellationToken token = default);
    }
}