namespace Durable.Sqlite
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
    using Microsoft.Data.Sqlite;

    /// <summary>
    /// Provides query building functionality for projected queries that transform entities from TEntity to TResult.
    /// This class handles SELECT operations with projection and supports ordering, pagination, and result transformation.
    /// </summary>
    /// <typeparam name="TEntity">The source entity type being queried from the database.</typeparam>
    /// <typeparam name="TResult">The projected result type that entities will be transformed into.</typeparam>
    public class SqliteProjectedQueryBuilder<TEntity, TResult> : IQueryBuilder<TResult>
        where TEntity : class, new()
        where TResult : class, new()
    {
        #region Private-Members

        private readonly SqliteRepository<TEntity> _Repository;
        private readonly ITransaction _Transaction;
        private readonly Expression<Func<TEntity, TResult>> _Selector;
        private readonly List<string> _WhereClauses = new List<string>();
        private readonly List<OrderByClause> _OrderByClauses = new List<OrderByClause>();
        private readonly List<string> _Includes = new List<string>();
        private readonly List<string> _GroupByColumns = new List<string>();
        private int? _SkipCount;
        private int? _TakeCount;
        private bool _Distinct;
        private string _CachedSql;
        private List<SelectMapping> _SelectMappings;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the SqliteProjectedQueryBuilder class.
        /// </summary>
        /// <param name="repository">The repository instance for database operations.</param>
        /// <param name="selector">The projection expression that transforms TEntity to TResult.</param>
        /// <param name="sourceQueryBuilder">The source query builder to copy state from.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <exception cref="ArgumentNullException">Thrown when repository or selector is null.</exception>
        public SqliteProjectedQueryBuilder(
            SqliteRepository<TEntity> repository,
            Expression<Func<TEntity, TResult>> selector,
            SqliteQueryBuilder<TEntity> sourceQueryBuilder,
            ITransaction transaction = null)
        {
            _Repository = repository ?? throw new ArgumentNullException(nameof(repository));
            _Selector = selector ?? throw new ArgumentNullException(nameof(selector));
            _Transaction = transaction;

            // Copy state from source query builder if provided
            if (sourceQueryBuilder != null)
            {
                _WhereClauses.AddRange(sourceQueryBuilder.GetWhereClauses());
                _GroupByColumns.AddRange(sourceQueryBuilder.GetGroupByColumns());
                // Note: Would need to expose more internal state from SqliteQueryBuilder if needed
            }

            // Parse the selector expression to determine columns
            ParseSelector();
        }

        #endregion

        #region Public-Methods

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
            // Including on projected results is complex and may not be meaningful
            throw new NotSupportedException("Include on projected query is not supported. Apply Include before Select.");
        }

        /// <summary>
        /// Includes additional related data based on a previously included navigation property.
        /// </summary>
        /// <typeparam name="TPreviousProperty">The type of the previously included property.</typeparam>
        /// <typeparam name="TProperty">The type of the navigation property to include.</typeparam>
        /// <param name="navigationProperty">The navigation property to include.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as ThenInclude on projected queries is not supported.</exception>
        public IQueryBuilder<TResult> ThenInclude<TPreviousProperty, TProperty>(Expression<Func<TPreviousProperty, TProperty>> navigationProperty)
        {
            throw new NotSupportedException("ThenInclude on projected query is not supported. Apply ThenInclude before Select.");
        }

        /// <summary>
        /// Groups the query results by the specified key selector.
        /// </summary>
        /// <typeparam name="TKey">The type of the grouping key.</typeparam>
        /// <param name="keySelector">The expression that selects the grouping key.</param>
        /// <returns>A grouped query builder for the specified key type.</returns>
        /// <exception cref="NotSupportedException">Always thrown as GroupBy on projected queries is not supported.</exception>
        public IGroupedQueryBuilder<TResult, TKey> GroupBy<TKey>(Expression<Func<TResult, TKey>> keySelector)
        {
            throw new NotSupportedException("GroupBy on projected query is not supported. Apply GroupBy before Select.");
        }

        /// <summary>
        /// Applies a HAVING clause to the grouped query results.
        /// </summary>
        /// <param name="predicate">The predicate expression for the HAVING clause.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as Having on projected queries is not supported.</exception>
        public IQueryBuilder<TResult> Having(Expression<Func<TResult, bool>> predicate)
        {
            throw new NotSupportedException("Having on projected query is not supported. Apply Having before Select.");
        }

        // Set operations
        /// <summary>
        /// Performs a UNION operation with another query.
        /// </summary>
        /// <param name="other">The other query to union with.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as Union on projected queries is not supported.</exception>
        public IQueryBuilder<TResult> Union(IQueryBuilder<TResult> other)
        {
            throw new NotSupportedException("Union on projected query is not supported. Apply Union before Select.");
        }

        /// <summary>
        /// Performs a UNION ALL operation with another query.
        /// </summary>
        /// <param name="other">The other query to union with.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as UnionAll on projected queries is not supported.</exception>
        public IQueryBuilder<TResult> UnionAll(IQueryBuilder<TResult> other)
        {
            throw new NotSupportedException("UnionAll on projected query is not supported. Apply UnionAll before Select.");
        }

        /// <summary>
        /// Performs an INTERSECT operation with another query.
        /// </summary>
        /// <param name="other">The other query to intersect with.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as Intersect on projected queries is not supported.</exception>
        public IQueryBuilder<TResult> Intersect(IQueryBuilder<TResult> other)
        {
            throw new NotSupportedException("Intersect on projected query is not supported. Apply Intersect before Select.");
        }

        /// <summary>
        /// Performs an EXCEPT operation with another query.
        /// </summary>
        /// <param name="other">The other query to exclude from results.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as Except on projected queries is not supported.</exception>
        public IQueryBuilder<TResult> Except(IQueryBuilder<TResult> other)
        {
            throw new NotSupportedException("Except on projected query is not supported. Apply Except before Select.");
        }

        // Subquery support
        /// <summary>
        /// Applies a WHERE IN clause using a subquery.
        /// </summary>
        /// <typeparam name="TKey">The type of the key for the IN operation.</typeparam>
        /// <param name="keySelector">The expression that selects the key for comparison.</param>
        /// <param name="subquery">The subquery that provides values for the IN clause.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as WhereIn on projected queries is not supported.</exception>
        public IQueryBuilder<TResult> WhereIn<TKey>(Expression<Func<TResult, TKey>> keySelector, IQueryBuilder<TKey> subquery) where TKey : class, new()
        {
            throw new NotSupportedException("WhereIn on projected query is not supported. Apply WhereIn before Select.");
        }

        /// <summary>
        /// Applies a WHERE NOT IN clause using a subquery.
        /// </summary>
        /// <typeparam name="TKey">The type of the key for the NOT IN operation.</typeparam>
        /// <param name="keySelector">The expression that selects the key for comparison.</param>
        /// <param name="subquery">The subquery that provides values for the NOT IN clause.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as WhereNotIn on projected queries is not supported.</exception>
        public IQueryBuilder<TResult> WhereNotIn<TKey>(Expression<Func<TResult, TKey>> keySelector, IQueryBuilder<TKey> subquery) where TKey : class, new()
        {
            throw new NotSupportedException("WhereNotIn on projected query is not supported. Apply WhereNotIn before Select.");
        }

        /// <summary>
        /// Applies a WHERE IN clause using raw SQL.
        /// </summary>
        /// <typeparam name="TKey">The type of the key for the IN operation.</typeparam>
        /// <param name="keySelector">The expression that selects the key for comparison.</param>
        /// <param name="subquerySql">The raw SQL string for the IN clause.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as WhereInRaw on projected queries is not supported.</exception>
        public IQueryBuilder<TResult> WhereInRaw<TKey>(Expression<Func<TResult, TKey>> keySelector, string subquerySql)
        {
            throw new NotSupportedException("WhereInRaw on projected query is not supported. Apply WhereInRaw before Select.");
        }

        /// <summary>
        /// Applies a WHERE NOT IN clause using raw SQL.
        /// </summary>
        /// <typeparam name="TKey">The type of the key for the NOT IN operation.</typeparam>
        /// <param name="keySelector">The expression that selects the key for comparison.</param>
        /// <param name="subquerySql">The raw SQL string for the NOT IN clause.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as WhereNotInRaw on projected queries is not supported.</exception>
        public IQueryBuilder<TResult> WhereNotInRaw<TKey>(Expression<Func<TResult, TKey>> keySelector, string subquerySql)
        {
            throw new NotSupportedException("WhereNotInRaw on projected query is not supported. Apply WhereNotInRaw before Select.");
        }

        /// <summary>
        /// Applies a WHERE EXISTS clause using a subquery.
        /// </summary>
        /// <typeparam name="TOther">The type of the subquery entity.</typeparam>
        /// <param name="subquery">The subquery for the EXISTS clause.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as WhereExists on projected queries is not supported.</exception>
        public IQueryBuilder<TResult> WhereExists<TOther>(IQueryBuilder<TOther> subquery) where TOther : class, new()
        {
            throw new NotSupportedException("WhereExists on projected query is not supported. Apply WhereExists before Select.");
        }

        /// <summary>
        /// Applies a WHERE NOT EXISTS clause using a subquery.
        /// </summary>
        /// <typeparam name="TOther">The type of the subquery entity.</typeparam>
        /// <param name="subquery">The subquery for the NOT EXISTS clause.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as WhereNotExists on projected queries is not supported.</exception>
        public IQueryBuilder<TResult> WhereNotExists<TOther>(IQueryBuilder<TOther> subquery) where TOther : class, new()
        {
            throw new NotSupportedException("WhereNotExists on projected query is not supported. Apply WhereNotExists before Select.");
        }

        // Window functions
        /// <summary>
        /// Adds a window function to the query.
        /// </summary>
        /// <param name="functionName">The name of the window function.</param>
        /// <param name="partitionBy">Optional PARTITION BY clause.</param>
        /// <param name="orderBy">Optional ORDER BY clause for the window.</param>
        /// <returns>A windowed query builder instance.</returns>
        /// <exception cref="NotSupportedException">Always thrown as window functions on projected queries are not supported.</exception>
        public IWindowedQueryBuilder<TResult> WithWindowFunction(string functionName, string partitionBy = null, string orderBy = null)
        {
            throw new NotSupportedException("Window functions on projected query is not supported. Apply window functions before Select.");
        }

        // CTEs
        /// <summary>
        /// Adds a Common Table Expression (CTE) to the query.
        /// </summary>
        /// <param name="cteName">The name of the CTE.</param>
        /// <param name="cteQuery">The SQL query for the CTE.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as CTEs on projected queries are not supported.</exception>
        public IQueryBuilder<TResult> WithCte(string cteName, string cteQuery)
        {
            throw new NotSupportedException("CTEs on projected query is not supported. Apply CTEs before Select.");
        }

        /// <summary>
        /// Adds a recursive Common Table Expression (CTE) to the query.
        /// </summary>
        /// <param name="cteName">The name of the recursive CTE.</param>
        /// <param name="anchorQuery">The anchor (base) query for the CTE.</param>
        /// <param name="recursiveQuery">The recursive query for the CTE.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as recursive CTEs on projected queries are not supported.</exception>
        public IQueryBuilder<TResult> WithRecursiveCte(string cteName, string anchorQuery, string recursiveQuery)
        {
            throw new NotSupportedException("Recursive CTEs on projected query is not supported. Apply recursive CTEs before Select.");
        }

        // Custom SQL fragments
        /// <summary>
        /// Applies a WHERE clause using raw SQL.
        /// </summary>
        /// <param name="sql">The raw SQL for the WHERE clause.</param>
        /// <param name="parameters">Parameters for the SQL query.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as WhereRaw on projected queries is not supported.</exception>
        public IQueryBuilder<TResult> WhereRaw(string sql, params object[] parameters)
        {
            throw new NotSupportedException("WhereRaw on projected query is not supported. Apply WhereRaw before Select.");
        }

        /// <summary>
        /// Adds raw SQL to the SELECT clause.
        /// </summary>
        /// <param name="sql">The raw SQL to add to the SELECT clause.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as SelectRaw on projected queries is not supported.</exception>
        public IQueryBuilder<TResult> SelectRaw(string sql)
        {
            throw new NotSupportedException("SelectRaw on projected query is not supported. Use a single Select with projection.");
        }

        /// <summary>
        /// Uses raw SQL for the FROM clause.
        /// </summary>
        /// <param name="sql">The raw SQL for the FROM clause.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as FromRaw on projected queries is not supported.</exception>
        public IQueryBuilder<TResult> FromRaw(string sql)
        {
            throw new NotSupportedException("FromRaw on projected query is not supported. Apply FromRaw before Select.");
        }

        /// <summary>
        /// Adds a raw SQL JOIN clause to the query.
        /// </summary>
        /// <param name="sql">The raw SQL for the JOIN clause.</param>
        /// <returns>The query builder instance for method chaining.</returns>
        /// <exception cref="NotSupportedException">Always thrown as JoinRaw on projected queries is not supported.</exception>
        public IQueryBuilder<TResult> JoinRaw(string sql)
        {
            throw new NotSupportedException("JoinRaw on projected query is not supported. Apply JoinRaw before Select.");
        }

        // CASE WHEN expressions
        /// <summary>
        /// Creates a CASE expression builder for conditional logic in the SELECT clause.
        /// </summary>
        /// <returns>A CASE expression builder instance.</returns>
        /// <exception cref="NotSupportedException">Always thrown as CASE expressions on projected queries are not supported.</exception>
        public ICaseExpressionBuilder<TResult> SelectCase()
        {
            throw new NotSupportedException("CASE expressions on projected query is not supported. Apply CASE expressions before Select.");
        }

        /// <summary>
        /// Executes the query synchronously and returns the results.
        /// </summary>
        /// <returns>An enumerable collection of projected results.</returns>
        public IEnumerable<TResult> Execute()
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = _Repository.GetConnectionAndCommand(_Transaction);
            ConnectionResult connectionResult = new ConnectionResult(result.Connection, result.Command, result.ShouldReturnToPool);
            try
            {
                connectionResult.Command.CommandText = BuildSql();
                using SqliteDataReader reader = connectionResult.Command.ExecuteReader();

                List<TResult> results = new List<TResult>();
                while (reader.Read())
                {
                    results.Add(MapReaderToResult(reader));
                }

                return results;
            }
            finally
            {
                connectionResult.Command?.Dispose();
                if (connectionResult.ShouldDispose)
                {
                    connectionResult.Connection?.Dispose();
                }
            }
        }

        /// <summary>
        /// Executes the query asynchronously and returns the results.
        /// </summary>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing an enumerable collection of projected results.</returns>
        public async Task<IEnumerable<TResult>> ExecuteAsync(CancellationToken token = default)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = await _Repository.GetConnectionAndCommandAsync(_Transaction, token);
            ConnectionResult connectionResult = new ConnectionResult(result.Connection, result.Command, result.ShouldReturnToPool);
            try
            {
                connectionResult.Command.CommandText = BuildSql();
                await using SqliteDataReader reader = await connectionResult.Command.ExecuteReaderAsync(token);

                List<TResult> results = new List<TResult>();
                while (await reader.ReadAsync(token))
                {
                    results.Add(MapReaderToResult(reader));
                }

                return results;
            }
            finally
            {
                if (connectionResult.Command != null) await connectionResult.Command.DisposeAsync();
                if (connectionResult.ShouldDispose && connectionResult.Connection != null)
                {
                    await connectionResult.Connection.DisposeAsync();
                }
            }
        }

        // Explicit interface implementations for CancellationToken overloads
        Task<IEnumerable<TResult>> IQueryBuilder<TResult>.ExecuteAsync(CancellationToken token)
        {
            return ExecuteAsync(token);
        }

        IAsyncEnumerable<TResult> IQueryBuilder<TResult>.ExecuteAsyncEnumerable(CancellationToken token)
        {
            return ExecuteAsyncEnumerable(token);
        }

        Task<IDurableResult<TResult>> IQueryBuilder<TResult>.ExecuteWithQueryAsync(CancellationToken token)
        {
            return ExecuteWithQueryAsync(token);
        }

        IAsyncDurableResult<TResult> IQueryBuilder<TResult>.ExecuteAsyncEnumerableWithQuery(CancellationToken token)
        {
            return ExecuteAsyncEnumerableWithQuery(token);
        }

        /// <summary>
        /// Executes the query asynchronously and returns the results as an async enumerable for streaming.
        /// </summary>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>An async enumerable collection of projected results that can be consumed with await foreach.</returns>
        public async IAsyncEnumerable<TResult> ExecuteAsyncEnumerable([EnumeratorCancellation] CancellationToken token = default)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = await _Repository.GetConnectionAndCommandAsync(_Transaction, token);
            ConnectionResult connectionResult = new ConnectionResult(result.Connection, result.Command, result.ShouldReturnToPool);
            try
            {
                connectionResult.Command.CommandText = BuildSql();
                await using SqliteDataReader reader = await connectionResult.Command.ExecuteReaderAsync(token);

                while (await reader.ReadAsync(token))
                {
                    yield return MapReaderToResult(reader);
                }
            }
            finally
            {
                if (connectionResult.Command != null) await connectionResult.Command.DisposeAsync();
                if (connectionResult.ShouldDispose && connectionResult.Connection != null)
                {
                    await connectionResult.Connection.DisposeAsync();
                }
            }
        }

        /// <summary>
        /// Gets the generated SQL query string.
        /// </summary>
        /// <value>The SQL query string that will be executed.</value>
        public string Query
        {
            get
            {
                if (_CachedSql == null)
                    _CachedSql = BuildSql();
                return _CachedSql;
            }
        }

        /// <summary>
        /// Executes the query synchronously and returns both the results and the SQL query.
        /// </summary>
        /// <returns>A durable result containing both the query string and the projected results.</returns>
        public IDurableResult<TResult> ExecuteWithQuery()
        {
            string query = Query;
            IEnumerable<TResult> results = Execute();
            return new DurableResult<TResult>(query, results);
        }

        /// <summary>
        /// Executes the query asynchronously and returns both the results and the SQL query.
        /// </summary>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing a durable result with both the query string and projected results.</returns>
        public async Task<IDurableResult<TResult>> ExecuteWithQueryAsync(CancellationToken token = default)
        {
            string query = Query;
            IEnumerable<TResult> results = await ExecuteAsync(token);
            return new DurableResult<TResult>(query, results);
        }

        /// <summary>
        /// Executes the query asynchronously and returns both the results as an async enumerable and the SQL query.
        /// </summary>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>An async durable result containing both the query string and an async enumerable of projected results.</returns>
        public IAsyncDurableResult<TResult> ExecuteAsyncEnumerableWithQuery(CancellationToken token = default)
        {
            string query = Query;
            IAsyncEnumerable<TResult> results = ExecuteAsyncEnumerable(token);
            return new AsyncDurableResult<TResult>(query, results);
        }

        /// <summary>
        /// Builds and returns the SQL query string for this projected query.
        /// </summary>
        /// <returns>The complete SQL query string including SELECT, FROM, WHERE, ORDER BY, and other clauses as applicable.</returns>
        public string BuildSql()
        {
            System.Text.StringBuilder sql = new System.Text.StringBuilder();

            sql.Append("SELECT ");
            if (_Distinct) sql.Append("DISTINCT ");

            // Build the column list from parsed mappings
            if (_SelectMappings != null && _SelectMappings.Count > 0)
            {
                List<string> columns = new List<string>();
                foreach (SelectMapping mapping in _SelectMappings)
                {
                    if (mapping.ColumnName != mapping.Alias)
                    {
                        columns.Add($"{_Repository._Sanitizer.SanitizeIdentifier(mapping.ColumnName)} AS {_Repository._Sanitizer.SanitizeIdentifier(mapping.Alias)}");
                    }
                    else
                    {
                        columns.Add(_Repository._Sanitizer.SanitizeIdentifier(mapping.ColumnName));
                    }
                }
                sql.Append(string.Join(", ", columns));
            }
            else
            {
                sql.Append("*");
            }

            sql.Append($" FROM {_Repository._Sanitizer.SanitizeIdentifier(_Repository._TableName)}");

            if (_WhereClauses.Count > 0)
            {
                sql.Append(" WHERE ");
                sql.Append(string.Join(" AND ", _WhereClauses));
            }

            if (_GroupByColumns.Count > 0)
            {
                sql.Append(" GROUP BY ");
                sql.Append(string.Join(", ", _GroupByColumns));
            }

            if (_OrderByClauses.Count > 0)
            {
                sql.Append(" ORDER BY ");
                IEnumerable<string> orderParts = _OrderByClauses.Select(o => $"{o.Column} {(o.Ascending ? "ASC" : "DESC")}");
                sql.Append(string.Join(", ", orderParts));
            }

            if (_TakeCount.HasValue)
            {
                sql.Append($" LIMIT {_TakeCount.Value}");
            }

            if (_SkipCount.HasValue)
            {
                sql.Append($" OFFSET {_SkipCount.Value}");
            }

            sql.Append(";");
            return sql.ToString();
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
            ExpressionParser<TEntity> parser = new ExpressionParser<TEntity>(_Repository._ColumnMappings, _Repository._Sanitizer);
            _SelectMappings = parser.ParseSelectExpression(_Selector);
        }

        private TResult MapReaderToResult(IDataReader reader)
        {
            TResult result = new TResult();
            Type resultType = typeof(TResult);

            // Map based on the parsed mappings
            if (_SelectMappings != null && _SelectMappings.Count > 0)
            {
                foreach (SelectMapping mapping in _SelectMappings)
                {
                    try
                    {
                        // Try to get by alias first, then by column name
                        int ordinal = -1;
                        try
                        {
                            ordinal = reader.GetOrdinal(mapping.Alias);
                        }
                        catch
                        {
                            ordinal = reader.GetOrdinal(mapping.ColumnName);
                        }

                        if (ordinal >= 0 && !reader.IsDBNull(ordinal))
                        {
                            object value = reader.GetValue(ordinal);
                            
                            // Find the target property on TResult
                            PropertyInfo targetProperty = resultType.GetProperty(mapping.Alias, 
                                BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
                            
                            if (targetProperty == null && mapping.TargetProperty != null)
                            {
                                targetProperty = mapping.TargetProperty;
                            }

                            if (targetProperty != null && targetProperty.CanWrite)
                            {
                                object convertedValue = _Repository._DataTypeConverter.ConvertFromDatabase(
                                    value, 
                                    targetProperty.PropertyType, 
                                    mapping.SourceProperty);
                                targetProperty.SetValue(result, convertedValue);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        // Log or handle mapping errors
                        System.Diagnostics.Debug.WriteLine($"Error mapping column {mapping.ColumnName}: {ex.Message}");
                    }
                }
            }
            else
            {
                // Fallback to property name matching
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    string columnName = reader.GetName(i);
                    PropertyInfo property = resultType.GetProperty(columnName, 
                        BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
                    
                    if (property != null && property.CanWrite && !reader.IsDBNull(i))
                    {
                        object value = reader.GetValue(i);
                        object convertedValue = _Repository._DataTypeConverter.ConvertFromDatabase(
                            value, 
                            property.PropertyType, 
                            property);
                        property.SetValue(result, convertedValue);
                    }
                }
            }

            return result;
        }

        private string GetProjectedColumn(Expression expression)
        {
            if (expression is MemberExpression memberExpr)
            {
                PropertyInfo propInfo = memberExpr.Member as PropertyInfo;
                if (propInfo != null)
                {
                    // Find the mapping for this property
                    if (_SelectMappings != null)
                    {
                        SelectMapping mapping = _SelectMappings.FirstOrDefault(m => m.Alias == propInfo.Name);
                        if (mapping != null && !string.IsNullOrEmpty(mapping.ColumnName))
                        {
                            return mapping.ColumnName;
                        }
                    }
                    return propInfo.Name;
                }
            }
            throw new ArgumentException($"Cannot get column from projected expression: {expression}");
        }

        #endregion
    }
}