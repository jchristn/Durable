namespace Durable.Postgres
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;

    /// <summary>
    /// Provides query building functionality for projected queries that transform entities from TEntity to TResult.
    /// This class handles SELECT operations with projection and supports ordering, pagination, and result transformation.
    /// </summary>
    /// <typeparam name="TEntity">The source entity type being queried from the database.</typeparam>
    /// <typeparam name="TResult">The projected result type that entities will be transformed into.</typeparam>
    public class PostgresProjectedQueryBuilder<TEntity, TResult> : IQueryBuilder<TResult>
        where TEntity : class, new()
        where TResult : class, new()
    {

        #region Private-Members

        private readonly PostgresRepository<TEntity> _Repository;
        private readonly ITransaction? _Transaction;
        private readonly Expression<Func<TEntity, TResult>> _Selector;
        private readonly List<string> _WhereClauses = new List<string>();
        private readonly List<OrderByClause> _OrderByClauses = new List<OrderByClause>();
        private readonly List<string> _Includes = new List<string>();
        private readonly List<string> _GroupByColumns = new List<string>();
        private readonly ISanitizer _Sanitizer;
        private int? _SkipCount;
        private int? _TakeCount;
        private bool _Distinct;
        private string? _CachedSql;
        private List<SelectMapping>? _SelectMappings;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the PostgresProjectedQueryBuilder class.
        /// </summary>
        /// <param name="repository">The repository instance for database operations.</param>
        /// <param name="selector">The projection expression that transforms TEntity to TResult.</param>
        /// <param name="sourceQueryBuilder">The source query builder to copy state from.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <exception cref="ArgumentNullException">Thrown when repository or selector is null.</exception>
        public PostgresProjectedQueryBuilder(
            PostgresRepository<TEntity> repository,
            Expression<Func<TEntity, TResult>> selector,
            PostgresQueryBuilder<TEntity> sourceQueryBuilder,
            ITransaction? transaction = null)
        {
            _Repository = repository ?? throw new ArgumentNullException(nameof(repository));
            _Selector = selector ?? throw new ArgumentNullException(nameof(selector));
            _Transaction = transaction;
            _Sanitizer = repository._Sanitizer;

            // Copy state from source query builder if provided
            if (sourceQueryBuilder != null)
            {
                _WhereClauses.AddRange(sourceQueryBuilder.GetWhereClauses());
                _GroupByColumns.AddRange(sourceQueryBuilder.GetGroupByColumns());
            }

            // Parse the selector expression to determine columns
            ParseSelector();
        }

        #endregion

        #region Public-Properties

        /// <summary>
        /// Gets the current SQL query being built.
        /// </summary>
        public string Query => BuildSql();

        #endregion

        #region Public-Methods

        /// <summary>
        /// Builds the SQL query string for the projected query.
        /// </summary>
        /// <returns>The complete SQL query string.</returns>
        public string BuildSql()
        {
            return BuildSqlInternal();
        }

        /// <summary>
        /// Applies a WHERE clause to the projected query.
        /// </summary>
        /// <param name="predicate">The predicate expression to filter results.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as WHERE clauses on projected queries are not supported.</exception>
        public IQueryBuilder<TResult> Where(Expression<Func<TResult, bool>> predicate)
        {
            // For projected queries, we need to translate the predicate back to TEntity
            // This is complex and may not be supported for all cases
            throw new NotSupportedException("Where clause on projected query is not supported. Apply Where before Select.");
        }

        /// <summary>
        /// Orders the query results by the specified key selector in ascending order.
        /// </summary>
        /// <typeparam name="TKey">The type of the key used for ordering.</typeparam>
        /// <param name="keySelector">The expression that selects the key for ordering.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        public IQueryBuilder<TResult> OrderBy<TKey>(Expression<Func<TResult, TKey>> keySelector)
        {
            string column = GetProjectedColumn(keySelector.Body);
            _OrderByClauses.Add(new OrderByClause { Column = column, Ascending = true });
            return this;
        }

        /// <summary>
        /// Orders the query results by the specified key selector in descending order.
        /// </summary>
        /// <typeparam name="TKey">The type of the key used for ordering.</typeparam>
        /// <param name="keySelector">The expression that selects the key for ordering.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        public IQueryBuilder<TResult> OrderByDescending<TKey>(Expression<Func<TResult, TKey>> keySelector)
        {
            string column = GetProjectedColumn(keySelector.Body);
            _OrderByClauses.Add(new OrderByClause { Column = column, Ascending = false });
            return this;
        }

        /// <summary>
        /// Performs a subsequent ordering of the query results by the specified key selector in ascending order.
        /// </summary>
        /// <typeparam name="TKey">The type of the key used for ordering.</typeparam>
        /// <param name="keySelector">The expression that selects the key for additional ordering.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="InvalidOperationException">Thrown when called without a preceding OrderBy or OrderByDescending.</exception>
        public IQueryBuilder<TResult> ThenBy<TKey>(Expression<Func<TResult, TKey>> keySelector)
        {
            if (_OrderByClauses.Count == 0)
                throw new InvalidOperationException("ThenBy can only be used after OrderBy or OrderByDescending");

            string column = GetProjectedColumn(keySelector.Body);
            _OrderByClauses.Add(new OrderByClause { Column = column, Ascending = true });
            return this;
        }

        /// <summary>
        /// Performs a subsequent ordering of the query results by the specified key selector in descending order.
        /// </summary>
        /// <typeparam name="TKey">The type of the key used for ordering.</typeparam>
        /// <param name="keySelector">The expression that selects the key for additional ordering.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="InvalidOperationException">Thrown when called without a preceding OrderBy or OrderByDescending.</exception>
        public IQueryBuilder<TResult> ThenByDescending<TKey>(Expression<Func<TResult, TKey>> keySelector)
        {
            if (_OrderByClauses.Count == 0)
                throw new InvalidOperationException("ThenByDescending can only be used after OrderBy or OrderByDescending");

            string column = GetProjectedColumn(keySelector.Body);
            _OrderByClauses.Add(new OrderByClause { Column = column, Ascending = false });
            return this;
        }

        /// <summary>
        /// Skips the specified number of results in the query.
        /// </summary>
        /// <param name="count">The number of results to skip.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        public IQueryBuilder<TResult> Skip(int count)
        {
            _SkipCount = count;
            return this;
        }

        /// <summary>
        /// Limits the query to return only the specified number of results.
        /// </summary>
        /// <param name="count">The maximum number of results to return.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        public IQueryBuilder<TResult> Take(int count)
        {
            _TakeCount = count;
            return this;
        }

        /// <summary>
        /// Ensures that the query returns only distinct results.
        /// </summary>
        /// <returns>The query builder instance for method chaining.</returns>
        public IQueryBuilder<TResult> Distinct()
        {
            _Distinct = true;
            return this;
        }

        /// <summary>
        /// Applies an additional projection to the query results.
        /// </summary>
        /// <typeparam name="TNewResult">The type of the new projection result.</typeparam>
        /// <param name="selector">The projection expression.</param>
        /// <returns>A new query builder for the projected type.</returns>
        /// <exception cref="NotSupportedException">Always thrown as chaining Select operations is not supported.</exception>
        public IQueryBuilder<TNewResult> Select<TNewResult>(Expression<Func<TResult, TNewResult>> selector) where TNewResult : class, new()
        {
            // Chaining projections would require composing the expressions
            throw new NotSupportedException("Chaining Select operations is not supported. Apply all projections in a single Select.");
        }

        /// <summary>
        /// Includes related data in the query results.
        /// </summary>
        /// <typeparam name="TProperty">The type of the navigation property.</typeparam>
        /// <param name="navigationProperty">The navigation property to include.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as Include on projected queries is not supported.</exception>
        public IQueryBuilder<TResult> Include<TProperty>(Expression<Func<TResult, TProperty>> navigationProperty)
        {
            throw new NotSupportedException("Include on projected query is not supported. Apply Include before Select.");
        }

        /// <summary>
        /// Includes related data in the query results using a nested navigation.
        /// </summary>
        /// <typeparam name="TPreviousProperty">The type of the previous navigation property.</typeparam>
        /// <typeparam name="TProperty">The type of the navigation property to include.</typeparam>
        /// <param name="navigationProperty">The navigation property to include.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as ThenInclude on projected queries is not supported.</exception>
        public IQueryBuilder<TResult> ThenInclude<TPreviousProperty, TProperty>(Expression<Func<TPreviousProperty, TProperty>> navigationProperty)
        {
            throw new NotSupportedException("ThenInclude on projected query is not supported. Apply ThenInclude before Select.");
        }

        /// <summary>
        /// Groups query results by the specified key selector expression.
        /// </summary>
        /// <typeparam name="TKey">The type of the grouping key.</typeparam>
        /// <param name="keySelector">The expression that selects the grouping key.</param>
        /// <returns>A grouped query builder for aggregate operations.</returns>
        /// <exception cref="NotSupportedException">Always thrown as GroupBy on projected queries is not supported.</exception>
        public IGroupedQueryBuilder<TResult, TKey> GroupBy<TKey>(Expression<Func<TResult, TKey>> keySelector)
        {
            throw new NotSupportedException("GroupBy on projected query is not supported. Apply GroupBy before Select.");
        }

        /// <summary>
        /// Adds a HAVING clause to the query.
        /// </summary>
        /// <param name="predicate">The HAVING condition.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as Having on projected queries is not supported.</exception>
        public IQueryBuilder<TResult> Having(Expression<Func<TResult, bool>> predicate)
        {
            throw new NotSupportedException("Having on projected query is not supported. Apply Having before Select.");
        }

        /// <summary>
        /// Performs a UNION operation with another query.
        /// </summary>
        /// <param name="other">The other query to union with.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as Union operations are not supported.</exception>
        public IQueryBuilder<TResult> Union(IQueryBuilder<TResult> other)
        {
            throw new NotSupportedException("Union operations are not supported on projected queries.");
        }

        /// <summary>
        /// Performs a UNION ALL operation with another query.
        /// </summary>
        /// <param name="other">The other query to union with.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as Union operations are not supported.</exception>
        public IQueryBuilder<TResult> UnionAll(IQueryBuilder<TResult> other)
        {
            throw new NotSupportedException("Union operations are not supported on projected queries.");
        }

        /// <summary>
        /// Performs an INTERSECT operation with another query.
        /// </summary>
        /// <param name="other">The other query to intersect with.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as Intersect operations are not supported.</exception>
        public IQueryBuilder<TResult> Intersect(IQueryBuilder<TResult> other)
        {
            throw new NotSupportedException("Intersect operations are not supported on projected queries.");
        }

        /// <summary>
        /// Performs an EXCEPT operation with another query.
        /// </summary>
        /// <param name="other">The other query to except with.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as Except operations are not supported.</exception>
        public IQueryBuilder<TResult> Except(IQueryBuilder<TResult> other)
        {
            throw new NotSupportedException("Except operations are not supported on projected queries.");
        }

        /// <summary>
        /// Adds a WHERE IN clause to the query.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <param name="keySelector">The key selector expression.</param>
        /// <param name="subquery">The subquery to check against.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as WhereIn operations are not supported.</exception>
        public IQueryBuilder<TResult> WhereIn<TKey>(Expression<Func<TResult, TKey>> keySelector, IQueryBuilder<TKey> subquery) where TKey : class, new()
        {
            throw new NotSupportedException("WhereIn operations are not supported on projected queries.");
        }

        /// <summary>
        /// Adds a WHERE NOT IN clause to the query.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <param name="keySelector">The key selector expression.</param>
        /// <param name="subquery">The subquery to check against.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as WhereNotIn operations are not supported.</exception>
        public IQueryBuilder<TResult> WhereNotIn<TKey>(Expression<Func<TResult, TKey>> keySelector, IQueryBuilder<TKey> subquery) where TKey : class, new()
        {
            throw new NotSupportedException("WhereNotIn operations are not supported on projected queries.");
        }

        /// <summary>
        /// Adds a WHERE IN clause with raw SQL to the query.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <param name="keySelector">The key selector expression.</param>
        /// <param name="rawSql">The raw SQL to check against.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as WhereInRaw operations are not supported.</exception>
        public IQueryBuilder<TResult> WhereInRaw<TKey>(Expression<Func<TResult, TKey>> keySelector, string rawSql)
        {
            throw new NotSupportedException("WhereInRaw operations are not supported on projected queries.");
        }

        /// <summary>
        /// Adds a WHERE NOT IN clause with raw SQL to the query.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <param name="keySelector">The key selector expression.</param>
        /// <param name="rawSql">The raw SQL to check against.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as WhereNotInRaw operations are not supported.</exception>
        public IQueryBuilder<TResult> WhereNotInRaw<TKey>(Expression<Func<TResult, TKey>> keySelector, string rawSql)
        {
            throw new NotSupportedException("WhereNotInRaw operations are not supported on projected queries.");
        }

        /// <summary>
        /// Adds a WHERE EXISTS clause to the query.
        /// </summary>
        /// <typeparam name="TOther">The type of the other query.</typeparam>
        /// <param name="subquery">The subquery to check for existence.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as WhereExists operations are not supported.</exception>
        public IQueryBuilder<TResult> WhereExists<TOther>(IQueryBuilder<TOther> subquery) where TOther : class, new()
        {
            throw new NotSupportedException("WhereExists operations are not supported on projected queries.");
        }

        /// <summary>
        /// Adds a WHERE NOT EXISTS clause to the query.
        /// </summary>
        /// <typeparam name="TOther">The type of the other query.</typeparam>
        /// <param name="subquery">The subquery to check for non-existence.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as WhereNotExists operations are not supported.</exception>
        public IQueryBuilder<TResult> WhereNotExists<TOther>(IQueryBuilder<TOther> subquery) where TOther : class, new()
        {
            throw new NotSupportedException("WhereNotExists operations are not supported on projected queries.");
        }

        /// <summary>
        /// Adds a window function to the query.
        /// </summary>
        /// <param name="functionName">The window function expression.</param>
        /// <param name="partitionBy">The PARTITION BY clause.</param>
        /// <param name="orderBy">The ORDER BY clause within the window.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as window functions are not supported.</exception>
        public IWindowedQueryBuilder<TResult> WithWindowFunction(string functionName, string? partitionBy = null, string? orderBy = null)
        {
            throw new NotSupportedException("Window functions are not supported on projected queries.");
        }

        /// <summary>
        /// Adds a Common Table Expression (CTE) to the query.
        /// </summary>
        /// <param name="name">The name of the CTE.</param>
        /// <param name="query">The CTE query definition.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as CTEs are not supported.</exception>
        public IQueryBuilder<TResult> WithCte(string name, string query)
        {
            throw new NotSupportedException("Common Table Expressions are not supported on projected queries.");
        }

        /// <summary>
        /// Adds a recursive Common Table Expression (CTE) to the query.
        /// </summary>
        /// <param name="name">The name of the recursive CTE.</param>
        /// <param name="anchor">The anchor query for the recursive CTE.</param>
        /// <param name="recursive">The recursive query definition.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as recursive CTEs are not supported.</exception>
        public IQueryBuilder<TResult> WithRecursiveCte(string name, string anchor, string recursive)
        {
            throw new NotSupportedException("Recursive Common Table Expressions are not supported on projected queries.");
        }

        /// <summary>
        /// Adds a raw WHERE clause to the query.
        /// </summary>
        /// <param name="whereClause">The raw WHERE clause.</param>
        /// <param name="parameters">The parameters for the WHERE clause.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as raw WHERE clauses are not supported.</exception>
        public IQueryBuilder<TResult> WhereRaw(string whereClause, params object[] parameters)
        {
            throw new NotSupportedException("Raw WHERE clauses are not supported on projected queries.");
        }

        /// <summary>
        /// Adds a raw SELECT clause to the query.
        /// </summary>
        /// <param name="selectClause">The raw SELECT clause.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as raw SELECT clauses are not supported.</exception>
        public IQueryBuilder<TResult> SelectRaw(string selectClause)
        {
            throw new NotSupportedException("Raw SELECT clauses are not supported on projected queries.");
        }

        /// <summary>
        /// Adds a raw FROM clause to the query.
        /// </summary>
        /// <param name="fromClause">The raw FROM clause.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as raw FROM clauses are not supported.</exception>
        public IQueryBuilder<TResult> FromRaw(string fromClause)
        {
            throw new NotSupportedException("Raw FROM clauses are not supported on projected queries.");
        }

        /// <summary>
        /// Adds a raw JOIN clause to the query.
        /// </summary>
        /// <param name="joinClause">The raw JOIN clause.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as raw JOIN clauses are not supported.</exception>
        public IQueryBuilder<TResult> JoinRaw(string joinClause)
        {
            throw new NotSupportedException("Raw JOIN clauses are not supported on projected queries.");
        }

        /// <summary>
        /// Creates a CASE statement builder for conditional logic.
        /// </summary>
        /// <returns>A CASE statement builder.</returns>
        /// <exception cref="NotSupportedException">Always thrown as CASE statements are not supported.</exception>
        public ICaseExpressionBuilder<TResult> SelectCase()
        {
            throw new NotSupportedException("CASE statements are not supported on projected queries.");
        }

        /// <summary>
        /// Executes the projected query and returns the results.
        /// </summary>
        /// <returns>The projected query results.</returns>
        /// <exception cref="InvalidOperationException">Thrown when query execution fails.</exception>
        public IEnumerable<TResult> Execute()
        {
            try
            {
                string sql = BuildSqlInternal();
                using NpgsqlConnection connection = (NpgsqlConnection)PooledConnectionHandle.Unwrap(_Repository._ConnectionFactory.GetConnection());
                if (connection.State != ConnectionState.Open)
                {
                    connection.Open();
                }

                using NpgsqlCommand command = connection.CreateCommand();
                command.CommandText = sql;

                _Repository.SetLastExecutedSql(sql);

                using NpgsqlDataReader reader = (NpgsqlDataReader)command.ExecuteReader();
                return MapResults(reader).ToList(); // Materialize to avoid connection disposal issues
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error executing projected query: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Asynchronously executes the projected query and returns the results.
        /// </summary>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with projected query results.</returns>
        /// <exception cref="InvalidOperationException">Thrown when query execution fails.</exception>
        public async Task<IEnumerable<TResult>> ExecuteAsync(CancellationToken token = default)
        {
            try
            {
                token.ThrowIfCancellationRequested();

                string sql = BuildSqlInternal();
                using NpgsqlConnection connection = (NpgsqlConnection)PooledConnectionHandle.Unwrap(_Repository._ConnectionFactory.GetConnection());
                if (connection.State != ConnectionState.Open)
                {
                    await ((NpgsqlConnection)connection).OpenAsync(token).ConfigureAwait(false);
                }

                using NpgsqlCommand command = connection.CreateCommand();
                command.CommandText = sql;

                _Repository.SetLastExecutedSql(sql);

                using NpgsqlDataReader reader = (NpgsqlDataReader)await command.ExecuteReaderAsync(token).ConfigureAwait(false);
                List<TResult> results = new List<TResult>();

                await foreach (TResult result in MapResultsAsync(reader, token).ConfigureAwait(false))
                {
                    results.Add(result);
                }

                return results;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error executing projected query: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Executes the projected query as an async enumerable for streaming results.
        /// </summary>
        /// <param name="token">The cancellation token.</param>
        /// <returns>An async enumerable of projected results.</returns>
        /// <exception cref="InvalidOperationException">Thrown when query execution fails.</exception>
        public async IAsyncEnumerable<TResult> ExecuteAsyncEnumerable([EnumeratorCancellation] CancellationToken token = default)
        {
            string sql = BuildSqlInternal();
            using NpgsqlConnection connection = (NpgsqlConnection)PooledConnectionHandle.Unwrap(_Repository._ConnectionFactory.GetConnection());

            try
            {
                if (connection.State != ConnectionState.Open)
                {
                    await ((NpgsqlConnection)connection).OpenAsync(token).ConfigureAwait(false);
                }

                using NpgsqlCommand command = connection.CreateCommand();
                command.CommandText = sql;

                _Repository.SetLastExecutedSql(sql);

                using NpgsqlDataReader reader = (NpgsqlDataReader)await command.ExecuteReaderAsync(token).ConfigureAwait(false);

                await foreach (TResult result in MapResultsAsync(reader, token).ConfigureAwait(false))
                {
                    yield return result;
                }
            }
            finally
            {
                _Repository._ConnectionFactory.ReturnConnection(connection);
            }
        }

        /// <summary>
        /// Executes the projected query and returns results with query information.
        /// </summary>
        /// <returns>A durable result containing the projected data and query information.</returns>
        public IDurableResult<TResult> ExecuteWithQuery()
        {
            string sql = BuildSqlInternal();
            IEnumerable<TResult> results = Execute();

            return new DurableResult<TResult>(sql, results);
        }

        /// <summary>
        /// Asynchronously executes the projected query and returns results with query information.
        /// </summary>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with durable result.</returns>
        public async Task<IDurableResult<TResult>> ExecuteWithQueryAsync(CancellationToken token = default)
        {
            string sql = BuildSqlInternal();
            IEnumerable<TResult> results = await ExecuteAsync(token).ConfigureAwait(false);

            return new DurableResult<TResult>(sql, results);
        }

        /// <summary>
        /// Executes the projected query as an async enumerable with query information.
        /// </summary>
        /// <param name="token">The cancellation token.</param>
        /// <returns>An async durable result for streaming projected data.</returns>
        public IAsyncDurableResult<TResult> ExecuteAsyncEnumerableWithQuery(CancellationToken token = default)
        {
            string sql = BuildSqlInternal();

            return new AsyncDurableResult<TResult>(sql, ExecuteAsyncEnumerable(token));
        }

        /// <summary>
        /// Counts the number of entities matching the query.
        /// </summary>
        /// <returns>The count of matching entities.</returns>
        /// <exception cref="NotSupportedException">Always thrown as Count operations are not supported on projected queries.</exception>
        public long Count()
        {
            throw new NotSupportedException("Count operations are not supported on projected queries.");
        }

        /// <summary>
        /// Asynchronously counts the number of entities matching the query.
        /// </summary>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the count of matching entities.</returns>
        /// <exception cref="NotSupportedException">Always thrown as Count operations are not supported on projected queries.</exception>
        public Task<long> CountAsync(CancellationToken token = default)
        {
            throw new NotSupportedException("Count operations are not supported on projected queries.");
        }

        /// <summary>
        /// Calculates the sum of a numeric property for entities matching the query.
        /// </summary>
        /// <typeparam name="TProperty">The type of the property to sum.</typeparam>
        /// <param name="selector">The expression to select the property to sum.</param>
        /// <returns>The sum of the property values.</returns>
        /// <exception cref="NotSupportedException">Always thrown as Sum operations are not supported on projected queries.</exception>
        public decimal Sum<TProperty>(Expression<Func<TResult, TProperty>> selector)
        {
            throw new NotSupportedException("Sum operations are not supported on projected queries.");
        }

        /// <summary>
        /// Asynchronously calculates the sum of a numeric property for entities matching the query.
        /// </summary>
        /// <typeparam name="TProperty">The type of the property to sum.</typeparam>
        /// <param name="selector">The expression to select the property to sum.</param>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the sum of the property values.</returns>
        /// <exception cref="NotSupportedException">Always thrown as Sum operations are not supported on projected queries.</exception>
        public Task<decimal> SumAsync<TProperty>(Expression<Func<TResult, TProperty>> selector, CancellationToken token = default)
        {
            throw new NotSupportedException("Sum operations are not supported on projected queries.");
        }

        /// <summary>
        /// Calculates the average of a numeric property for entities matching the query.
        /// </summary>
        /// <typeparam name="TProperty">The type of the property to average.</typeparam>
        /// <param name="selector">The expression to select the property to average.</param>
        /// <returns>The average of the property values.</returns>
        /// <exception cref="NotSupportedException">Always thrown as Average operations are not supported on projected queries.</exception>
        public decimal Average<TProperty>(Expression<Func<TResult, TProperty>> selector)
        {
            throw new NotSupportedException("Average operations are not supported on projected queries.");
        }

        /// <summary>
        /// Asynchronously calculates the average of a numeric property for entities matching the query.
        /// </summary>
        /// <typeparam name="TProperty">The type of the property to average.</typeparam>
        /// <param name="selector">The expression to select the property to average.</param>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the average of the property values.</returns>
        /// <exception cref="NotSupportedException">Always thrown as Average operations are not supported on projected queries.</exception>
        public Task<decimal> AverageAsync<TProperty>(Expression<Func<TResult, TProperty>> selector, CancellationToken token = default)
        {
            throw new NotSupportedException("Average operations are not supported on projected queries.");
        }

        /// <summary>
        /// Finds the minimum value of a property for entities matching the query.
        /// </summary>
        /// <typeparam name="TProperty">The type of the property.</typeparam>
        /// <param name="selector">The expression to select the property.</param>
        /// <returns>The minimum property value.</returns>
        /// <exception cref="NotSupportedException">Always thrown as Min operations are not supported on projected queries.</exception>
        public TProperty Min<TProperty>(Expression<Func<TResult, TProperty>> selector)
        {
            throw new NotSupportedException("Min operations are not supported on projected queries.");
        }

        /// <summary>
        /// Asynchronously finds the minimum value of a property for entities matching the query.
        /// </summary>
        /// <typeparam name="TProperty">The type of the property.</typeparam>
        /// <param name="selector">The expression to select the property.</param>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the minimum property value.</returns>
        /// <exception cref="NotSupportedException">Always thrown as Min operations are not supported on projected queries.</exception>
        public Task<TProperty> MinAsync<TProperty>(Expression<Func<TResult, TProperty>> selector, CancellationToken token = default)
        {
            throw new NotSupportedException("Min operations are not supported on projected queries.");
        }

        /// <summary>
        /// Finds the maximum value of a property for entities matching the query.
        /// </summary>
        /// <typeparam name="TProperty">The type of the property.</typeparam>
        /// <param name="selector">The expression to select the property.</param>
        /// <returns>The maximum property value.</returns>
        /// <exception cref="NotSupportedException">Always thrown as Max operations are not supported on projected queries.</exception>
        public TProperty Max<TProperty>(Expression<Func<TResult, TProperty>> selector)
        {
            throw new NotSupportedException("Max operations are not supported on projected queries.");
        }

        /// <summary>
        /// Asynchronously finds the maximum value of a property for entities matching the query.
        /// </summary>
        /// <typeparam name="TProperty">The type of the property.</typeparam>
        /// <param name="selector">The expression to select the property.</param>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the maximum property value.</returns>
        /// <exception cref="NotSupportedException">Always thrown as Max operations are not supported on projected queries.</exception>
        public Task<TProperty> MaxAsync<TProperty>(Expression<Func<TResult, TProperty>> selector, CancellationToken token = default)
        {
            throw new NotSupportedException("Max operations are not supported on projected queries.");
        }

        /// <summary>
        /// Deletes all entities matching the query.
        /// </summary>
        /// <returns>The number of entities deleted.</returns>
        /// <exception cref="NotSupportedException">Always thrown as Delete operations are not supported on projected queries.</exception>
        public int Delete()
        {
            throw new NotSupportedException("Delete operations are not supported on projected queries.");
        }

        /// <summary>
        /// Asynchronously deletes all entities matching the query.
        /// </summary>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the number of entities deleted.</returns>
        /// <exception cref="NotSupportedException">Always thrown as Delete operations are not supported on projected queries.</exception>
        public Task<int> DeleteAsync(CancellationToken token = default)
        {
            throw new NotSupportedException("Delete operations are not supported on projected queries.");
        }

        #endregion

        #region Private-Methods

        private void ParseSelector()
        {
            _SelectMappings = new List<SelectMapping>();

            if (_Selector.Body is NewExpression newExpression)
            {
                // Handle anonymous type or constructor projections
                for (int i = 0; i < newExpression.Arguments.Count; i++)
                {
                    Expression argument = newExpression.Arguments[i];
                    string? memberName = newExpression.Members?[i]?.Name;

                    if (memberName == null)
                        throw new InvalidOperationException($"Cannot determine member name for projection argument at index {i}");

                    string sourceColumn = GetSourceColumn(argument);
                    _SelectMappings.Add(new SelectMapping
                    {
                        SourceColumn = sourceColumn,
                        TargetProperty = memberName,
                        Expression = argument
                    });
                }
            }
            else if (_Selector.Body is MemberInitExpression memberInitExpression)
            {
                // Handle object initialization projections
                foreach (MemberBinding binding in memberInitExpression.Bindings)
                {
                    if (binding is MemberAssignment assignment)
                    {
                        string sourceColumn = GetSourceColumn(assignment.Expression);
                        _SelectMappings.Add(new SelectMapping
                        {
                            SourceColumn = sourceColumn,
                            TargetProperty = assignment.Member.Name,
                            Expression = assignment.Expression
                        });
                    }
                }
            }
            else
            {
                throw new NotSupportedException($"Projection expression type {_Selector.Body.GetType().Name} is not supported");
            }
        }

        private string GetSourceColumn(Expression expression)
        {
            if (expression is MemberExpression memberExpression)
            {
                return _Repository.GetColumnFromExpression(memberExpression);
            }

            // Handle UnaryExpression (typically nullable conversions like (decimal?)p.Salary)
            if (expression is UnaryExpression unaryExpression && unaryExpression.Operand is MemberExpression unaryMember)
            {
                return _Repository.GetColumnFromExpression(unaryMember);
            }

            // For complex expressions (like string concatenation, method calls, etc.),
            // parse them into SQL using the expression parser
            try
            {
                PostgresExpressionParser<TEntity> parser = new PostgresExpressionParser<TEntity>(_Repository._ColumnMappings, _Repository._Sanitizer);
                return parser.ParseExpression(expression);
            }
            catch (NotSupportedException ex) when (ex.Message.Contains("Method") && ex.Message.Contains("is not supported"))
            {
                // Provide better error message for unsupported methods in projections
                throw new NotSupportedException(
                    $"The expression '{expression}' contains method calls that cannot be translated to SQL in projections. " +
                    $"{ex.Message} " +
                    $"Consider using simpler expressions or supported methods (Contains, StartsWith, ToUpper, ToLower, etc.), " +
                    $"or perform the calculation after retrieving the data from the database.");
            }
            catch (Exception ex)
            {
                throw new NotSupportedException(
                    $"Expression type {expression.GetType().Name} is not supported in projections: {ex.Message} " +
                    $"Projections must use simple property access or expressions that can be translated to SQL.");
            }
        }

        private string GetProjectedColumn(Expression expression)
        {
            if (expression is MemberExpression memberExpression)
            {
                string memberName = memberExpression.Member.Name;
                SelectMapping? mapping = _SelectMappings?.FirstOrDefault(m => m.TargetProperty == memberName);

                if (mapping != null)
                {
                    return mapping.SourceColumn;
                }
            }

            throw new InvalidOperationException($"Cannot resolve projected column for expression: {expression}");
        }

        private string BuildSqlInternal()
        {
            if (_CachedSql != null)
                return _CachedSql;

            if (_SelectMappings == null || _SelectMappings.Count == 0)
                throw new InvalidOperationException("No select mappings available for projection");

            System.Text.StringBuilder sql = new System.Text.StringBuilder();

            // SELECT clause
            sql.Append("SELECT ");
            if (_Distinct)
                sql.Append("DISTINCT ");

            List<string> selectColumns = _SelectMappings.Select(m => m.SourceColumn).ToList();
            sql.Append(string.Join(", ", selectColumns));

            // FROM clause - PostgreSQL uses double quotes for identifiers
            sql.Append($" FROM \"{_Repository._TableName}\"");

            // WHERE clause
            if (_WhereClauses.Count > 0)
            {
                sql.Append(" WHERE ");
                sql.Append(string.Join(" AND ", _WhereClauses));
            }

            // GROUP BY clause
            if (_GroupByColumns.Count > 0)
            {
                sql.Append(" GROUP BY ");
                sql.Append(string.Join(", ", _GroupByColumns.Select(c => $"\"{c}\"")));
            }

            // ORDER BY clause
            if (_OrderByClauses.Count > 0)
            {
                sql.Append(" ORDER BY ");
                List<string> orderByParts = _OrderByClauses.Select(o => $"{o.Column} {(o.Ascending ? "ASC" : "DESC")}").ToList();
                sql.Append(string.Join(", ", orderByParts));
            }

            // LIMIT/OFFSET clause (PostgreSQL syntax)
            // PostgreSQL requires LIMIT when using OFFSET
            // Use "LIMIT ALL" to return all remaining rows
            if (_TakeCount.HasValue)
            {
                sql.Append($" LIMIT {_TakeCount.Value}");
            }
            else if (_SkipCount.HasValue)
            {
                sql.Append(" LIMIT ALL");
            }

            if (_SkipCount.HasValue)
            {
                sql.Append($" OFFSET {_SkipCount.Value}");
            }

            _CachedSql = sql.ToString();
            return _CachedSql;
        }

        private IEnumerable<TResult> MapResults(NpgsqlDataReader reader)
        {
            while (reader.Read())
            {
                TResult result = CreateResultInstance(reader);
                yield return result;
            }
        }

        private async IAsyncEnumerable<TResult> MapResultsAsync(NpgsqlDataReader reader, [EnumeratorCancellation] CancellationToken token)
        {
            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                token.ThrowIfCancellationRequested();
                TResult result = CreateResultInstance(reader);
                yield return result;
            }
        }

        private TResult CreateResultInstance(NpgsqlDataReader reader)
        {
            TResult result = new TResult();

            if (_SelectMappings == null)
                throw new InvalidOperationException("Select mappings not available");

            for (int i = 0; i < _SelectMappings.Count; i++)
            {
                SelectMapping mapping = _SelectMappings[i];
                PropertyInfo? targetProperty = typeof(TResult).GetProperty(mapping.TargetProperty);

                if (targetProperty == null)
                    continue;

                object? value = reader.GetValue(i);
                if (value != DBNull.Value)
                {
                    // Use repository's data type converter for consistent type handling
                    object convertedValue = _Repository._DataTypeConverter.ConvertFromDatabase(value, targetProperty.PropertyType)!;
                    targetProperty.SetValue(result, convertedValue);
                }
            }

            return result;
        }

        #endregion

        #region Helper-Classes

        private class SelectMapping
        {
            public string SourceColumn { get; set; } = string.Empty;
            public string TargetProperty { get; set; } = string.Empty;
            public Expression Expression { get; set; } = null!;
        }

        private class OrderByClause
        {
            public string Column { get; set; } = string.Empty;
            public bool Ascending { get; set; } = true;
        }

        #endregion
    }
}