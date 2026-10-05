namespace Durable
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// A grouped query. Group keys and HAVING filters are evaluated by the backend.
    /// Thread safety: not thread-safe; build and execute on one flow.
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    /// <typeparam name="TKey">Group key type.</typeparam>
    public interface IGroupedQueryBuilder<T, TKey> where T : class, new()
    {
        /// <summary>
        /// Filters groups, for example <c>g =&gt; g.Count() &gt; 2 &amp;&amp; g.Sum(x =&gt; x.Salary) &gt; 1000</c>.
        /// Multiple calls are combined with AND.
        /// </summary>
        /// <param name="predicate">Group predicate. Must not be null.</param>
        /// <returns>This builder.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null.</exception>
        IGroupedQueryBuilder<T, TKey> Having(Expression<Func<IGrouping<TKey, T>, bool>> predicate);

        /// <summary>
        /// Projects each group to a result row computed by the backend, for example
        /// <c>g =&gt; new DepartmentStats { Department = g.Key, Headcount = g.Count(), Payroll = g.Sum(x =&gt; x.Salary) }</c>.
        /// </summary>
        /// <typeparam name="TResult">Result type with a parameterless constructor.</typeparam>
        /// <param name="selector">Group projection. Must not be null.</param>
        /// <returns>A query producing one <typeparamref name="TResult"/> per group.</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null.</exception>
        /// <exception cref="NotSupportedException">Thrown when the projection uses members other than the key and aggregates.</exception>
        IQueryBuilder<TResult> Select<TResult>(Expression<Func<IGrouping<TKey, T>, TResult>> selector) where TResult : class, new();

        /// <summary>
        /// Executes the query and returns groups with their member entities.
        /// </summary>
        /// <returns>Groups. Never null.</returns>
        IEnumerable<IGrouping<TKey, T>> Execute();

        /// <summary>
        /// Executes the query and returns groups with their member entities.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Groups. Never null.</returns>
        Task<IEnumerable<IGrouping<TKey, T>>> ExecuteAsync(CancellationToken token = default);

        /// <summary>
        /// Counts the groups (after HAVING).
        /// </summary>
        /// <returns>Number of groups.</returns>
        long Count();

        /// <summary>
        /// Counts the groups (after HAVING).
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Number of groups.</returns>
        Task<long> CountAsync(CancellationToken token = default);

        /// <summary>
        /// Sums a value over all rows belonging to the selected groups.
        /// </summary>
        /// <typeparam name="TProperty">Numeric type.</typeparam>
        /// <param name="selector">Value selector. Must not be null.</param>
        /// <returns>The sum; zero when no rows match.</returns>
        decimal Sum<TProperty>(Expression<Func<T, TProperty>> selector);

        /// <summary>
        /// Sums a value over all rows belonging to the selected groups.
        /// </summary>
        /// <typeparam name="TProperty">Numeric type.</typeparam>
        /// <param name="selector">Value selector. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The sum; zero when no rows match.</returns>
        Task<decimal> SumAsync<TProperty>(Expression<Func<T, TProperty>> selector, CancellationToken token = default);

        /// <summary>
        /// Averages a value over all rows belonging to the selected groups.
        /// </summary>
        /// <typeparam name="TProperty">Numeric type.</typeparam>
        /// <param name="selector">Value selector. Must not be null.</param>
        /// <returns>The average; zero when no rows match.</returns>
        decimal Average<TProperty>(Expression<Func<T, TProperty>> selector);

        /// <summary>
        /// Averages a value over all rows belonging to the selected groups.
        /// </summary>
        /// <typeparam name="TProperty">Numeric type.</typeparam>
        /// <param name="selector">Value selector. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The average; zero when no rows match.</returns>
        Task<decimal> AverageAsync<TProperty>(Expression<Func<T, TProperty>> selector, CancellationToken token = default);

        /// <summary>
        /// Returns the maximum value over all rows belonging to the selected groups.
        /// </summary>
        /// <typeparam name="TResult">Value type.</typeparam>
        /// <param name="selector">Value selector. Must not be null.</param>
        /// <returns>The maximum; default when no rows match.</returns>
        TResult Max<TResult>(Expression<Func<T, TResult>> selector);

        /// <summary>
        /// Returns the maximum value over all rows belonging to the selected groups.
        /// </summary>
        /// <typeparam name="TResult">Value type.</typeparam>
        /// <param name="selector">Value selector. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The maximum; default when no rows match.</returns>
        Task<TResult> MaxAsync<TResult>(Expression<Func<T, TResult>> selector, CancellationToken token = default);

        /// <summary>
        /// Returns the minimum value over all rows belonging to the selected groups.
        /// </summary>
        /// <typeparam name="TResult">Value type.</typeparam>
        /// <param name="selector">Value selector. Must not be null.</param>
        /// <returns>The minimum; default when no rows match.</returns>
        TResult Min<TResult>(Expression<Func<T, TResult>> selector);

        /// <summary>
        /// Returns the minimum value over all rows belonging to the selected groups.
        /// </summary>
        /// <typeparam name="TResult">Value type.</typeparam>
        /// <param name="selector">Value selector. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The minimum; default when no rows match.</returns>
        Task<TResult> MinAsync<TResult>(Expression<Func<T, TResult>> selector, CancellationToken token = default);
    }
}
