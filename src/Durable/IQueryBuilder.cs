namespace Durable
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Backend-neutral fluent query. Builder methods mutate and return the same instance.
    /// SQL providers return <c>Durable.Sql.ISqlQueryBuilder&lt;T&gt;</c>, which adds raw SQL, set operations, subqueries,
    /// CTEs and window functions.
    /// Thread safety: a query builder is not thread-safe; build and execute it on one flow.
    /// </summary>
    /// <typeparam name="T">Entity or result type.</typeparam>
    public interface IQueryBuilder<T> where T : class, new()
    {
        /// <summary>
        /// Adds a predicate; multiple calls are combined with AND.
        /// </summary>
        /// <param name="predicate">Predicate. Must not be null.</param>
        /// <returns>This builder.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null.</exception>
        IQueryBuilder<T> Where(Expression<Func<T, bool>> predicate);

        /// <summary>
        /// Sets the primary ascending sort, replacing earlier sorts.
        /// </summary>
        /// <typeparam name="TKey">Key type.</typeparam>
        /// <param name="keySelector">Key selector. Must not be null.</param>
        /// <returns>This builder.</returns>
        IQueryBuilder<T> OrderBy<TKey>(Expression<Func<T, TKey>> keySelector);

        /// <summary>
        /// Sets the primary descending sort, replacing earlier sorts.
        /// </summary>
        /// <typeparam name="TKey">Key type.</typeparam>
        /// <param name="keySelector">Key selector. Must not be null.</param>
        /// <returns>This builder.</returns>
        IQueryBuilder<T> OrderByDescending<TKey>(Expression<Func<T, TKey>> keySelector);

        /// <summary>
        /// Adds a secondary ascending sort.
        /// </summary>
        /// <typeparam name="TKey">Key type.</typeparam>
        /// <param name="keySelector">Key selector. Must not be null.</param>
        /// <returns>This builder.</returns>
        IQueryBuilder<T> ThenBy<TKey>(Expression<Func<T, TKey>> keySelector);

        /// <summary>
        /// Adds a secondary descending sort.
        /// </summary>
        /// <typeparam name="TKey">Key type.</typeparam>
        /// <param name="keySelector">Key selector. Must not be null.</param>
        /// <returns>This builder.</returns>
        IQueryBuilder<T> ThenByDescending<TKey>(Expression<Func<T, TKey>> keySelector);

        /// <summary>
        /// Skips rows. With includes, paging applies to root entities, not joined rows.
        /// </summary>
        /// <param name="count">Rows to skip. Minimum: 0.</param>
        /// <returns>This builder.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when count is negative.</exception>
        IQueryBuilder<T> Skip(int count);

        /// <summary>
        /// Limits rows. With includes, the limit applies to root entities, not joined rows.
        /// </summary>
        /// <param name="count">Maximum rows. Minimum: 0.</param>
        /// <returns>This builder.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when count is negative.</exception>
        IQueryBuilder<T> Take(int count);

        /// <summary>
        /// Returns distinct rows.
        /// </summary>
        /// <returns>This builder.</returns>
        IQueryBuilder<T> Distinct();

        /// <summary>
        /// Excludes the repository's global query filters and soft-delete filter from this query.
        /// </summary>
        /// <returns>This builder.</returns>
        IQueryBuilder<T> IgnoreQueryFilters();

        /// <summary>
        /// Projects to another type using a member-init or new expression, for example
        /// <c>x =&gt; new PersonSummary { Name = x.First + " " + x.Last }</c>.
        /// </summary>
        /// <typeparam name="TResult">Result type with a parameterless constructor.</typeparam>
        /// <param name="selector">Projection. Must not be null.</param>
        /// <returns>A builder producing <typeparamref name="TResult"/>.</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null.</exception>
        IQueryBuilder<TResult> Select<TResult>(Expression<Func<T, TResult>> selector) where TResult : class, new();

        /// <summary>
        /// Eagerly loads a navigation property. Related rows are loaded with separate queries keyed by the root results,
        /// so paging and row counts are unaffected.
        /// </summary>
        /// <typeparam name="TProperty">Navigation type.</typeparam>
        /// <param name="navigationProperty">Navigation selector. Must not be null.</param>
        /// <returns>This builder.</returns>
        /// <exception cref="ArgumentException">Thrown when the selector is not a mapped navigation property.</exception>
        IQueryBuilder<T> Include<TProperty>(Expression<Func<T, TProperty>> navigationProperty);

        /// <summary>
        /// Eagerly loads a navigation of the most recently included entity.
        /// </summary>
        /// <typeparam name="TPreviousProperty">Entity type of the previous include (the element type for collections).</typeparam>
        /// <typeparam name="TProperty">Navigation type.</typeparam>
        /// <param name="navigationProperty">Navigation selector. Must not be null.</param>
        /// <returns>This builder.</returns>
        /// <exception cref="InvalidOperationException">Thrown when no Include precedes this call.</exception>
        IQueryBuilder<T> ThenInclude<TPreviousProperty, TProperty>(Expression<Func<TPreviousProperty, TProperty>> navigationProperty);

        /// <summary>
        /// Groups by a key.
        /// </summary>
        /// <typeparam name="TKey">Key type.</typeparam>
        /// <param name="keySelector">Key selector. Must not be null.</param>
        /// <returns>A grouped builder.</returns>
        IGroupedQueryBuilder<T, TKey> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector);

        /// <summary>
        /// Executes the query and buffers results.
        /// </summary>
        /// <returns>Results. Never null.</returns>
        IEnumerable<T> Execute();

        /// <summary>
        /// Executes the query and buffers results.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Results. Never null.</returns>
        Task<IEnumerable<T>> ExecuteAsync(CancellationToken token = default);

        /// <summary>
        /// Executes the query and streams results as they are read. Includes are loaded per batch of streamed roots.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>An async stream of results.</returns>
        IAsyncEnumerable<T> ExecuteAsyncEnumerable(CancellationToken token = default);

        /// <summary>
        /// Executes the query and returns results with the native query text.
        /// </summary>
        /// <returns>Results and query text.</returns>
        IDurableResult<T> ExecuteWithQuery();

        /// <summary>
        /// Executes the query and returns results with the native query text.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Results and query text.</returns>
        Task<IDurableResult<T>> ExecuteWithQueryAsync(CancellationToken token = default);

        /// <summary>
        /// Streams results with the native query text.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Streamed results and query text.</returns>
        IAsyncDurableResult<T> ExecuteAsyncEnumerableWithQuery(CancellationToken token = default);

        /// <summary>
        /// Counts the rows the query returns, applying Skip/Take like LINQ (<c>Take(2).Count()</c> is at most 2).
        /// </summary>
        /// <returns>The count.</returns>
        long Count();

        /// <summary>
        /// Counts the rows the query returns, applying Skip/Take like LINQ (<c>Take(2).Count()</c> is at most 2).
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The count.</returns>
        Task<long> CountAsync(CancellationToken token = default);

        /// <summary>
        /// Determines whether any row matches.
        /// </summary>
        /// <returns>True when at least one row matches.</returns>
        bool Any();

        /// <summary>
        /// Determines whether any row matches.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>True when at least one row matches.</returns>
        Task<bool> AnyAsync(CancellationToken token = default);

        /// <summary>
        /// Sums a numeric value over matching rows.
        /// </summary>
        /// <typeparam name="TProperty">Numeric type.</typeparam>
        /// <param name="selector">Value selector. Must not be null.</param>
        /// <returns>The sum; zero when no rows match.</returns>
        decimal Sum<TProperty>(Expression<Func<T, TProperty>> selector);

        /// <summary>
        /// Sums a numeric value over matching rows.
        /// </summary>
        /// <typeparam name="TProperty">Numeric type.</typeparam>
        /// <param name="selector">Value selector. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The sum; zero when no rows match.</returns>
        Task<decimal> SumAsync<TProperty>(Expression<Func<T, TProperty>> selector, CancellationToken token = default);

        /// <summary>
        /// Averages a numeric value over matching rows.
        /// </summary>
        /// <typeparam name="TProperty">Numeric type.</typeparam>
        /// <param name="selector">Value selector. Must not be null.</param>
        /// <returns>The average; zero when no rows match.</returns>
        decimal Average<TProperty>(Expression<Func<T, TProperty>> selector);

        /// <summary>
        /// Averages a numeric value over matching rows.
        /// </summary>
        /// <typeparam name="TProperty">Numeric type.</typeparam>
        /// <param name="selector">Value selector. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The average; zero when no rows match.</returns>
        Task<decimal> AverageAsync<TProperty>(Expression<Func<T, TProperty>> selector, CancellationToken token = default);

        /// <summary>
        /// Returns the minimum of a value over matching rows.
        /// </summary>
        /// <typeparam name="TProperty">Value type.</typeparam>
        /// <param name="selector">Value selector. Must not be null.</param>
        /// <returns>The minimum; default when no rows match.</returns>
        TProperty Min<TProperty>(Expression<Func<T, TProperty>> selector);

        /// <summary>
        /// Returns the minimum of a value over matching rows.
        /// </summary>
        /// <typeparam name="TProperty">Value type.</typeparam>
        /// <param name="selector">Value selector. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The minimum; default when no rows match.</returns>
        Task<TProperty> MinAsync<TProperty>(Expression<Func<T, TProperty>> selector, CancellationToken token = default);

        /// <summary>
        /// Returns the maximum of a value over matching rows.
        /// </summary>
        /// <typeparam name="TProperty">Value type.</typeparam>
        /// <param name="selector">Value selector. Must not be null.</param>
        /// <returns>The maximum; default when no rows match.</returns>
        TProperty Max<TProperty>(Expression<Func<T, TProperty>> selector);

        /// <summary>
        /// Returns the maximum of a value over matching rows.
        /// </summary>
        /// <typeparam name="TProperty">Value type.</typeparam>
        /// <param name="selector">Value selector. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The maximum; default when no rows match.</returns>
        Task<TProperty> MaxAsync<TProperty>(Expression<Func<T, TProperty>> selector, CancellationToken token = default);

        /// <summary>
        /// Deletes matching rows (soft-deletes when the entity has a soft-delete column).
        /// </summary>
        /// <returns>The number of rows affected.</returns>
        int Delete();

        /// <summary>
        /// Deletes matching rows (soft-deletes when the entity has a soft-delete column).
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The number of rows affected.</returns>
        Task<int> DeleteAsync(CancellationToken token = default);

        /// <summary>
        /// Gets the native query text for the current definition (for SQL backends, the parameterized SQL).
        /// </summary>
        string Query { get; }
    }
}
