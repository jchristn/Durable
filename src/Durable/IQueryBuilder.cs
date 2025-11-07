namespace Durable
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Text;

    /// <summary>
    /// Provides methods for building and executing LINQ-style queries with fluent syntax.
    /// </summary>
    /// <typeparam name="T">The entity type being queried.</typeparam>
    public interface IQueryBuilder<T> where T : class, new()
    {
        /// <summary>
        /// Adds a WHERE clause to filter query results.
        /// </summary>
        /// <param name="predicate">The condition to apply to the query.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IQueryBuilder<T> Where(Expression<Func<T, bool>> predicate);
        /// <summary>
        /// Orders the query results in ascending order by the specified key.
        /// </summary>
        /// <typeparam name="TKey">The type of the ordering key.</typeparam>
        /// <param name="keySelector">The expression to extract the ordering key.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IQueryBuilder<T> OrderBy<TKey>(Expression<Func<T, TKey>> keySelector);
        /// <summary>
        /// Orders the query results in descending order by the specified key.
        /// </summary>
        /// <typeparam name="TKey">The type of the ordering key.</typeparam>
        /// <param name="keySelector">The expression to extract the ordering key.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IQueryBuilder<T> OrderByDescending<TKey>(Expression<Func<T, TKey>> keySelector);
        /// <summary>
        /// Performs a subsequent ordering of the query results in ascending order.
        /// </summary>
        /// <typeparam name="TKey">The type of the ordering key.</typeparam>
        /// <param name="keySelector">The expression to extract the ordering key.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IQueryBuilder<T> ThenBy<TKey>(Expression<Func<T, TKey>> keySelector);
        /// <summary>
        /// Performs a subsequent ordering of the query results in descending order.
        /// </summary>
        /// <typeparam name="TKey">The type of the ordering key.</typeparam>
        /// <param name="keySelector">The expression to extract the ordering key.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IQueryBuilder<T> ThenByDescending<TKey>(Expression<Func<T, TKey>> keySelector);
        /// <summary>
        /// Skips the specified number of elements in the query results.
        /// </summary>
        /// <param name="count">The number of elements to skip.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IQueryBuilder<T> Skip(int count);
        /// <summary>
        /// Takes only the specified number of elements from the query results.
        /// </summary>
        /// <param name="count">The number of elements to take.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IQueryBuilder<T> Take(int count);
        /// <summary>
        /// Returns distinct elements from the query results.
        /// </summary>
        /// <returns>The current query builder for method chaining.</returns>
        IQueryBuilder<T> Distinct();

        /// <summary>
        /// Projects each element of the query into a new form.
        /// </summary>
        /// <typeparam name="TResult">The type of the result after projection.</typeparam>
        /// <param name="selector">The projection expression.</param>
        /// <returns>A new query builder for the projected type.</returns>
        IQueryBuilder<TResult> Select<TResult>(Expression<Func<T, TResult>> selector) where TResult : class, new();

        /// <summary>
        /// Includes related data in the query results.
        /// </summary>
        /// <typeparam name="TProperty">The type of the navigation property.</typeparam>
        /// <param name="navigationProperty">The navigation property to include.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IQueryBuilder<T> Include<TProperty>(Expression<Func<T, TProperty>> navigationProperty);
        /// <summary>
        /// Includes additional related data based on a previously included navigation property.
        /// </summary>
        /// <typeparam name="TPreviousProperty">The type of the previously included property.</typeparam>
        /// <typeparam name="TProperty">The type of the navigation property to include.</typeparam>
        /// <param name="navigationProperty">The navigation property to include.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IQueryBuilder<T> ThenInclude<TPreviousProperty, TProperty>(Expression<Func<TPreviousProperty, TProperty>> navigationProperty);

        /// <summary>
        /// Groups the query results by the specified key selector.
        /// </summary>
        /// <typeparam name="TKey">The type of the grouping key.</typeparam>
        /// <param name="keySelector">The expression to extract the grouping key.</param>
        /// <returns>A grouped query builder for further operations.</returns>
        IGroupedQueryBuilder<T, TKey> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector);
        /// <summary>
        /// Adds a HAVING clause to filter grouped results.
        /// </summary>
        /// <param name="predicate">The condition to apply to grouped results.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IQueryBuilder<T> Having(Expression<Func<T, bool>> predicate);

        /// <summary>
        /// Performs a UNION operation with another query, combining results and removing duplicates.
        /// </summary>
        /// <param name="other">The other query builder to union with.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IQueryBuilder<T> Union(IQueryBuilder<T> other);
        /// <summary>
        /// Performs a UNION ALL operation with another query, combining results including duplicates.
        /// </summary>
        /// <param name="other">The other query builder to union with.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IQueryBuilder<T> UnionAll(IQueryBuilder<T> other);
        /// <summary>
        /// Performs an INTERSECT operation with another query, returning only common results.
        /// </summary>
        /// <param name="other">The other query builder to intersect with.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IQueryBuilder<T> Intersect(IQueryBuilder<T> other);
        /// <summary>
        /// Performs an EXCEPT operation with another query, returning results not in the other query.
        /// </summary>
        /// <param name="other">The other query builder to except with.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IQueryBuilder<T> Except(IQueryBuilder<T> other);

        /// <summary>
        /// Adds a WHERE IN clause using a subquery.
        /// </summary>
        /// <typeparam name="TKey">The type of the key to match.</typeparam>
        /// <param name="keySelector">The expression to extract the key from the main query.</param>
        /// <param name="subquery">The subquery to check membership against.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IQueryBuilder<T> WhereIn<TKey>(Expression<Func<T, TKey>> keySelector, IQueryBuilder<TKey> subquery) where TKey : class, new();
        /// <summary>
        /// Adds a WHERE NOT IN clause using a subquery.
        /// </summary>
        /// <typeparam name="TKey">The type of the key to match.</typeparam>
        /// <param name="keySelector">The expression to extract the key from the main query.</param>
        /// <param name="subquery">The subquery to check membership against.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IQueryBuilder<T> WhereNotIn<TKey>(Expression<Func<T, TKey>> keySelector, IQueryBuilder<TKey> subquery) where TKey : class, new();
        /// <summary>
        /// Adds a WHERE IN clause using raw SQL for the subquery.
        /// </summary>
        /// <typeparam name="TKey">The type of the key to match.</typeparam>
        /// <param name="keySelector">The expression to extract the key from the main query.</param>
        /// <param name="subquerySql">The raw SQL subquery string.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IQueryBuilder<T> WhereInRaw<TKey>(Expression<Func<T, TKey>> keySelector, string subquerySql);
        /// <summary>
        /// Adds a WHERE NOT IN clause using raw SQL for the subquery.
        /// </summary>
        /// <typeparam name="TKey">The type of the key to match.</typeparam>
        /// <param name="keySelector">The expression to extract the key from the main query.</param>
        /// <param name="subquerySql">The raw SQL subquery string.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IQueryBuilder<T> WhereNotInRaw<TKey>(Expression<Func<T, TKey>> keySelector, string subquerySql);
        /// <summary>
        /// Adds a WHERE EXISTS clause using a subquery.
        /// </summary>
        /// <typeparam name="TOther">The type of the subquery entity.</typeparam>
        /// <param name="subquery">The subquery to check for existence.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IQueryBuilder<T> WhereExists<TOther>(IQueryBuilder<TOther> subquery) where TOther : class, new();
        /// <summary>
        /// Adds a WHERE NOT EXISTS clause using a subquery.
        /// </summary>
        /// <typeparam name="TOther">The type of the subquery entity.</typeparam>
        /// <param name="subquery">The subquery to check for non-existence.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IQueryBuilder<T> WhereNotExists<TOther>(IQueryBuilder<TOther> subquery) where TOther : class, new();

        /// <summary>
        /// Adds a window function to the query.
        /// </summary>
        /// <param name="functionName">The name of the window function.</param>
        /// <param name="partitionBy">Optional PARTITION BY clause.</param>
        /// <param name="orderBy">Optional ORDER BY clause for the window.</param>
        /// <returns>A windowed query builder for further window operations.</returns>
        IWindowedQueryBuilder<T> WithWindowFunction(string functionName, string? partitionBy = null, string? orderBy = null);

        /// <summary>
        /// Adds a Common Table Expression (CTE) to the query.
        /// </summary>
        /// <param name="cteName">The name of the CTE.</param>
        /// <param name="cteQuery">The SQL query for the CTE.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IQueryBuilder<T> WithCte(string cteName, string cteQuery);
        /// <summary>
        /// Adds a recursive Common Table Expression (CTE) to the query.
        /// </summary>
        /// <param name="cteName">The name of the recursive CTE.</param>
        /// <param name="anchorQuery">The anchor query for the recursive CTE.</param>
        /// <param name="recursiveQuery">The recursive query for the CTE.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IQueryBuilder<T> WithRecursiveCte(string cteName, string anchorQuery, string recursiveQuery);

        /// <summary>
        /// Adds a raw SQL WHERE clause with optional parameters.
        /// </summary>
        /// <param name="sql">The raw SQL condition.</param>
        /// <param name="parameters">Optional parameters for the SQL.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IQueryBuilder<T> WhereRaw(string sql, params object[] parameters);
        /// <summary>
        /// Adds a raw SQL SELECT clause.
        /// </summary>
        /// <param name="sql">The raw SQL select statement.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IQueryBuilder<T> SelectRaw(string sql);
        /// <summary>
        /// Specifies a raw SQL FROM clause.
        /// </summary>
        /// <param name="sql">The raw SQL from statement.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IQueryBuilder<T> FromRaw(string sql);
        /// <summary>
        /// Adds a raw SQL JOIN clause.
        /// </summary>
        /// <param name="sql">The raw SQL join statement.</param>
        /// <returns>The current query builder for method chaining.</returns>
        IQueryBuilder<T> JoinRaw(string sql);

        /// <summary>
        /// Creates a CASE WHEN expression builder for conditional selections.
        /// </summary>
        /// <returns>A case expression builder for building conditional logic.</returns>
        ICaseExpressionBuilder<T> SelectCase();

        /// <summary>
        /// Executes the query and returns the results.
        /// </summary>
        /// <returns>The query results as an enumerable sequence.</returns>
        IEnumerable<T> Execute();
        /// <summary>
        /// Asynchronously executes the query and returns the results.
        /// </summary>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with query results.</returns>
        Task<IEnumerable<T>> ExecuteAsync(CancellationToken token = default);
        /// <summary>
        /// Executes the query and returns results as an asynchronous enumerable stream.
        /// </summary>
        /// <param name="token">The cancellation token.</param>
        /// <returns>The query results as an asynchronous enumerable sequence.</returns>
        IAsyncEnumerable<T> ExecuteAsyncEnumerable(CancellationToken token = default);

        /// <summary>
        /// Executes the query and returns both the results and the executed SQL query.
        /// </summary>
        /// <returns>A durable result containing both query and results.</returns>
        IDurableResult<T> ExecuteWithQuery();
        /// <summary>
        /// Asynchronously executes the query and returns both the results and the executed SQL query.
        /// </summary>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with durable result.</returns>
        Task<IDurableResult<T>> ExecuteWithQueryAsync(CancellationToken token = default);
        /// <summary>
        /// Executes the query as an asynchronous enumerable and exposes the executed SQL query.
        /// </summary>
        /// <param name="token">The cancellation token.</param>
        /// <returns>An asynchronous durable result containing both query and streaming results.</returns>
        IAsyncDurableResult<T> ExecuteAsyncEnumerableWithQuery(CancellationToken token = default);

        /// <summary>
        /// Counts the number of entities matching the query.
        /// </summary>
        /// <returns>The count of matching entities.</returns>
        long Count();
        /// <summary>
        /// Asynchronously counts the number of entities matching the query.
        /// </summary>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the count of matching entities.</returns>
        Task<long> CountAsync(CancellationToken token = default);

        /// <summary>
        /// Calculates the sum of a numeric property for entities matching the query.
        /// </summary>
        /// <typeparam name="TProperty">The type of the property to sum.</typeparam>
        /// <param name="selector">The expression to select the property to sum.</param>
        /// <returns>The sum of the property values.</returns>
        decimal Sum<TProperty>(Expression<Func<T, TProperty>> selector);
        /// <summary>
        /// Asynchronously calculates the sum of a numeric property for entities matching the query.
        /// </summary>
        /// <typeparam name="TProperty">The type of the property to sum.</typeparam>
        /// <param name="selector">The expression to select the property to sum.</param>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the sum of the property values.</returns>
        Task<decimal> SumAsync<TProperty>(Expression<Func<T, TProperty>> selector, CancellationToken token = default);

        /// <summary>
        /// Calculates the average of a numeric property for entities matching the query.
        /// </summary>
        /// <typeparam name="TProperty">The type of the property to average.</typeparam>
        /// <param name="selector">The expression to select the property to average.</param>
        /// <returns>The average of the property values.</returns>
        decimal Average<TProperty>(Expression<Func<T, TProperty>> selector);
        /// <summary>
        /// Asynchronously calculates the average of a numeric property for entities matching the query.
        /// </summary>
        /// <typeparam name="TProperty">The type of the property to average.</typeparam>
        /// <param name="selector">The expression to select the property to average.</param>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the average of the property values.</returns>
        Task<decimal> AverageAsync<TProperty>(Expression<Func<T, TProperty>> selector, CancellationToken token = default);

        /// <summary>
        /// Finds the minimum value of a property for entities matching the query.
        /// </summary>
        /// <typeparam name="TProperty">The type of the property.</typeparam>
        /// <param name="selector">The expression to select the property.</param>
        /// <returns>The minimum property value.</returns>
        TProperty Min<TProperty>(Expression<Func<T, TProperty>> selector);
        /// <summary>
        /// Asynchronously finds the minimum value of a property for entities matching the query.
        /// </summary>
        /// <typeparam name="TProperty">The type of the property.</typeparam>
        /// <param name="selector">The expression to select the property.</param>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the minimum property value.</returns>
        Task<TProperty> MinAsync<TProperty>(Expression<Func<T, TProperty>> selector, CancellationToken token = default);

        /// <summary>
        /// Finds the maximum value of a property for entities matching the query.
        /// </summary>
        /// <typeparam name="TProperty">The type of the property.</typeparam>
        /// <param name="selector">The expression to select the property.</param>
        /// <returns>The maximum property value.</returns>
        TProperty Max<TProperty>(Expression<Func<T, TProperty>> selector);
        /// <summary>
        /// Asynchronously finds the maximum value of a property for entities matching the query.
        /// </summary>
        /// <typeparam name="TProperty">The type of the property.</typeparam>
        /// <param name="selector">The expression to select the property.</param>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the maximum property value.</returns>
        Task<TProperty> MaxAsync<TProperty>(Expression<Func<T, TProperty>> selector, CancellationToken token = default);

        /// <summary>
        /// Deletes all entities matching the query.
        /// </summary>
        /// <returns>The number of entities deleted.</returns>
        int Delete();
        /// <summary>
        /// Asynchronously deletes all entities matching the query.
        /// </summary>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the number of entities deleted.</returns>
        Task<int> DeleteAsync(CancellationToken token = default);

        /// <summary>
        /// Gets the SQL query that will be or was executed.
        /// </summary>
        string Query { get; }

        /// <summary>
        /// Builds and returns the SQL query string for debugging purposes.
        /// </summary>
        /// <returns>The SQL query string.</returns>
        string BuildSql();
    }
}