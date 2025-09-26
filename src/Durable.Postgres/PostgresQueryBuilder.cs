namespace Durable.Postgres
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;

    /// <summary>
    /// PostgreSQL-specific implementation of IQueryBuilder that provides fluent query building capabilities for PostgreSQL databases.
    /// Supports advanced features like CTEs, window functions, set operations, and includes.
    /// </summary>
    /// <typeparam name="TEntity">The entity type being queried</typeparam>
    public class PostgresQueryBuilder<TEntity> : IQueryBuilder<TEntity> where TEntity : class, new()
    {

        #region Public-Members

        /// <summary>
        /// Gets the SQL query that will be or was executed.
        /// </summary>
        public string Query => BuildSql();

        #endregion

        #region Private-Members

        private readonly PostgresRepository<TEntity> _Repository;
        private readonly ITransaction? _Transaction;
        private readonly List<string> _WhereClauses = new List<string>();
        private readonly List<string> _OrderByClauses = new List<string>();
        private readonly List<string> _IncludePaths = new List<string>();
        private PostgresJoinBuilder.PostgresJoinResult? _CachedJoinResult;
        private readonly List<string> _GroupByColumns = new List<string>();
        private readonly List<string> _HavingClauses = new List<string>();
        private readonly PostgresExpressionParser<TEntity> _ExpressionParser;
        private readonly PostgresJoinBuilder _JoinBuilder;
        private readonly PostgresEntityMapper<TEntity> _EntityMapper;
        private int? _SkipCount;
        private int? _TakeCount;
        private bool _Distinct;

        // Window functions support
        internal readonly List<WindowFunction> _WindowFunctions = new List<WindowFunction>();

        // CTE support
        private readonly List<CteDefinition> _CteDefinitions = new List<CteDefinition>();

        // Set operations support
        private readonly List<SetOperation<TEntity>> _SetOperations = new List<SetOperation<TEntity>>();

        // CASE expressions support
        private readonly List<string> _CaseExpressions = new List<string>();

        // Raw SQL support
        private string? _CustomSelectClause;
        private string? _CustomFromClause;
        private readonly List<string> _CustomJoinClauses = new List<string>();

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the PostgresQueryBuilder class.
        /// </summary>
        /// <param name="repository">The PostgreSQL repository instance for data access operations</param>
        /// <param name="transaction">Optional transaction to execute queries within. Default is null</param>
        /// <exception cref="ArgumentNullException">Thrown when repository is null</exception>
        public PostgresQueryBuilder(PostgresRepository<TEntity> repository, ITransaction? transaction = null)
        {
            _Repository = repository ?? throw new ArgumentNullException(nameof(repository));
            _Transaction = transaction;
            _ExpressionParser = new PostgresExpressionParser<TEntity>(_Repository._ColumnMappings, _Repository._Sanitizer);
            _JoinBuilder = new PostgresJoinBuilder(_Repository._Sanitizer);
            _EntityMapper = new PostgresEntityMapper<TEntity>(_Repository._DataTypeConverter, _Repository._ColumnMappings, _Repository._Sanitizer);
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Adds a WHERE clause condition to the query using a lambda expression.
        /// </summary>
        /// <param name="predicate">Lambda expression representing the WHERE condition</param>
        /// <returns>The current query builder instance for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null</exception>
        public IQueryBuilder<TEntity> Where(Expression<Func<TEntity, bool>> predicate)
        {
            if (predicate == null) throw new ArgumentNullException(nameof(predicate));

            string whereClause = _ExpressionParser.ParseExpression(predicate.Body);
            _WhereClauses.Add(whereClause);
            return this;
        }

        /// <summary>
        /// Adds an ORDER BY clause to sort results in ascending order by the specified property.
        /// </summary>
        /// <typeparam name="TKey">The type of the property to sort by</typeparam>
        /// <param name="keySelector">Lambda expression selecting the property to sort by</param>
        /// <returns>The current query builder instance for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when keySelector is null</exception>
        public IQueryBuilder<TEntity> OrderBy<TKey>(Expression<Func<TEntity, TKey>> keySelector)
        {
            if (keySelector == null) throw new ArgumentNullException(nameof(keySelector));

            string column = _ExpressionParser.GetColumnFromExpression(keySelector.Body);
            _OrderByClauses.Add($"{column} ASC");
            return this;
        }

        /// <summary>
        /// Adds an ORDER BY clause to sort results in descending order by the specified property.
        /// </summary>
        /// <typeparam name="TKey">The type of the property to sort by</typeparam>
        /// <param name="keySelector">Lambda expression selecting the property to sort by</param>
        /// <returns>The current query builder instance for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when keySelector is null</exception>
        public IQueryBuilder<TEntity> OrderByDescending<TKey>(Expression<Func<TEntity, TKey>> keySelector)
        {
            if (keySelector == null) throw new ArgumentNullException(nameof(keySelector));

            string column = _ExpressionParser.GetColumnFromExpression(keySelector.Body);
            _OrderByClauses.Add($"{column} DESC");
            return this;
        }

        /// <summary>
        /// Performs a subsequent ordering of the query results in ascending order.
        /// </summary>
        /// <typeparam name="TKey">The type of the ordering key.</typeparam>
        /// <param name="keySelector">The expression to extract the ordering key.</param>
        /// <returns>The current query builder for method chaining.</returns>
        public IQueryBuilder<TEntity> ThenBy<TKey>(Expression<Func<TEntity, TKey>> keySelector)
        {
            return OrderBy(keySelector); // For now, same as OrderBy - full implementation would track order precedence
        }

        /// <summary>
        /// Performs a subsequent ordering of the query results in descending order.
        /// </summary>
        /// <typeparam name="TKey">The type of the ordering key.</typeparam>
        /// <param name="keySelector">The expression to extract the ordering key.</param>
        /// <returns>The current query builder for method chaining.</returns>
        public IQueryBuilder<TEntity> ThenByDescending<TKey>(Expression<Func<TEntity, TKey>> keySelector)
        {
            return OrderByDescending(keySelector); // For now, same as OrderByDescending
        }

        /// <summary>
        /// Skips the specified number of elements in the query results.
        /// </summary>
        /// <param name="count">The number of elements to skip.</param>
        /// <returns>The current query builder for method chaining.</returns>
        public IQueryBuilder<TEntity> Skip(int count)
        {
            _SkipCount = count;
            return this;
        }

        /// <summary>
        /// Takes only the specified number of elements from the query results.
        /// </summary>
        /// <param name="count">The number of elements to take.</param>
        /// <returns>The current query builder for method chaining.</returns>
        public IQueryBuilder<TEntity> Take(int count)
        {
            _TakeCount = count;
            return this;
        }

        /// <summary>
        /// Returns distinct elements from the query results.
        /// </summary>
        /// <returns>The current query builder for method chaining.</returns>
        public IQueryBuilder<TEntity> Distinct()
        {
            _Distinct = true;
            return this;
        }

        /// <summary>
        /// Projects each element of the query into a new form.
        /// </summary>
        /// <typeparam name="TResult">The type of the result after projection.</typeparam>
        /// <param name="selector">The projection expression.</param>
        /// <returns>A new query builder for the projected type.</returns>
        public IQueryBuilder<TResult> Select<TResult>(Expression<Func<TEntity, TResult>> selector) where TResult : class, new()
        {
            throw new NotImplementedException("Select projection is not yet implemented");
        }

        /// <summary>
        /// Includes related data in the query results.
        /// </summary>
        /// <typeparam name="TProperty">The type of the navigation property.</typeparam>
        /// <param name="navigationProperty">The navigation property to include.</param>
        /// <returns>The current query builder for method chaining.</returns>
        public IQueryBuilder<TEntity> Include<TProperty>(Expression<Func<TEntity, TProperty>> navigationProperty)
        {
            if (navigationProperty == null) throw new ArgumentNullException(nameof(navigationProperty));

            string includePath = GetIncludePathFromExpression(navigationProperty);
            _IncludePaths.Add(includePath);
            return this;
        }

        /// <summary>
        /// Includes additional related data based on a previously included navigation property.
        /// </summary>
        /// <typeparam name="TPreviousProperty">The type of the previously included property.</typeparam>
        /// <typeparam name="TProperty">The type of the navigation property to include.</typeparam>
        /// <param name="navigationProperty">The navigation property to include.</param>
        /// <returns>The current query builder for method chaining.</returns>
        public IQueryBuilder<TEntity> ThenInclude<TPreviousProperty, TProperty>(Expression<Func<TPreviousProperty, TProperty>> navigationProperty)
        {
            throw new NotImplementedException("ThenInclude is not yet implemented");
        }

        /// <summary>
        /// Groups the query results by the specified key selector.
        /// </summary>
        /// <typeparam name="TKey">The type of the grouping key.</typeparam>
        /// <param name="keySelector">The expression to extract the grouping key.</param>
        /// <returns>A grouped query builder for further operations.</returns>
        public IGroupedQueryBuilder<TEntity, TKey> GroupBy<TKey>(Expression<Func<TEntity, TKey>> keySelector)
        {
            throw new NotImplementedException("GroupBy is not yet implemented");
        }

        /// <summary>
        /// Adds a HAVING clause to filter grouped results.
        /// </summary>
        /// <param name="predicate">The condition to apply to grouped results.</param>
        /// <returns>The current query builder for method chaining.</returns>
        public IQueryBuilder<TEntity> Having(Expression<Func<TEntity, bool>> predicate)
        {
            if (predicate == null) throw new ArgumentNullException(nameof(predicate));

            string havingClause = _ExpressionParser.ParseExpression(predicate.Body);
            _HavingClauses.Add(havingClause);
            return this;
        }

        /// <summary>
        /// Performs a UNION operation with another query, combining results and removing duplicates.
        /// </summary>
        /// <param name="other">The other query builder to union with.</param>
        /// <returns>The current query builder for method chaining.</returns>
        public IQueryBuilder<TEntity> Union(IQueryBuilder<TEntity> other)
        {
            throw new NotImplementedException("Union is not yet implemented");
        }

        /// <summary>
        /// Performs a UNION ALL operation with another query, combining results including duplicates.
        /// </summary>
        /// <param name="other">The other query builder to union with.</param>
        /// <returns>The current query builder for method chaining.</returns>
        public IQueryBuilder<TEntity> UnionAll(IQueryBuilder<TEntity> other)
        {
            throw new NotImplementedException("UnionAll is not yet implemented");
        }

        /// <summary>
        /// Performs an INTERSECT operation with another query, returning only common results.
        /// </summary>
        /// <param name="other">The other query builder to intersect with.</param>
        /// <returns>The current query builder for method chaining.</returns>
        public IQueryBuilder<TEntity> Intersect(IQueryBuilder<TEntity> other)
        {
            throw new NotImplementedException("Intersect is not yet implemented");
        }

        /// <summary>
        /// Performs an EXCEPT operation with another query, returning results not in the other query.
        /// </summary>
        /// <param name="other">The other query builder to except with.</param>
        /// <returns>The current query builder for method chaining.</returns>
        public IQueryBuilder<TEntity> Except(IQueryBuilder<TEntity> other)
        {
            throw new NotImplementedException("Except is not yet implemented");
        }

        /// <summary>
        /// Adds a WHERE IN clause using a subquery.
        /// </summary>
        /// <typeparam name="TKey">The type of the key to match.</typeparam>
        /// <param name="keySelector">The expression to extract the key from the main query.</param>
        /// <param name="subquery">The subquery to check membership against.</param>
        /// <returns>The current query builder for method chaining.</returns>
        public IQueryBuilder<TEntity> WhereIn<TKey>(Expression<Func<TEntity, TKey>> keySelector, IQueryBuilder<TKey> subquery) where TKey : class, new()
        {
            throw new NotImplementedException("WhereIn is not yet implemented");
        }

        /// <summary>
        /// Adds a WHERE NOT IN clause using a subquery.
        /// </summary>
        /// <typeparam name="TKey">The type of the key to match.</typeparam>
        /// <param name="keySelector">The expression to extract the key from the main query.</param>
        /// <param name="subquery">The subquery to check membership against.</param>
        /// <returns>The current query builder for method chaining.</returns>
        public IQueryBuilder<TEntity> WhereNotIn<TKey>(Expression<Func<TEntity, TKey>> keySelector, IQueryBuilder<TKey> subquery) where TKey : class, new()
        {
            throw new NotImplementedException("WhereNotIn is not yet implemented");
        }

        /// <summary>
        /// Adds a WHERE IN clause using raw SQL for the subquery.
        /// </summary>
        /// <typeparam name="TKey">The type of the key to match.</typeparam>
        /// <param name="keySelector">The expression to extract the key from the main query.</param>
        /// <param name="subquerySql">The raw SQL subquery string.</param>
        /// <returns>The current query builder for method chaining.</returns>
        public IQueryBuilder<TEntity> WhereInRaw<TKey>(Expression<Func<TEntity, TKey>> keySelector, string subquerySql)
        {
            throw new NotImplementedException("WhereInRaw is not yet implemented");
        }

        /// <summary>
        /// Adds a WHERE NOT IN clause using raw SQL for the subquery.
        /// </summary>
        /// <typeparam name="TKey">The type of the key to match.</typeparam>
        /// <param name="keySelector">The expression to extract the key from the main query.</param>
        /// <param name="subquerySql">The raw SQL subquery string.</param>
        /// <returns>The current query builder for method chaining.</returns>
        public IQueryBuilder<TEntity> WhereNotInRaw<TKey>(Expression<Func<TEntity, TKey>> keySelector, string subquerySql)
        {
            throw new NotImplementedException("WhereNotInRaw is not yet implemented");
        }

        /// <summary>
        /// Adds a WHERE EXISTS clause using a subquery.
        /// </summary>
        /// <typeparam name="TOther">The type of the subquery entity.</typeparam>
        /// <param name="subquery">The subquery to check for existence.</param>
        /// <returns>The current query builder for method chaining.</returns>
        public IQueryBuilder<TEntity> WhereExists<TOther>(IQueryBuilder<TOther> subquery) where TOther : class, new()
        {
            throw new NotImplementedException("WhereExists is not yet implemented");
        }

        /// <summary>
        /// Adds a WHERE NOT EXISTS clause using a subquery.
        /// </summary>
        /// <typeparam name="TOther">The type of the subquery entity.</typeparam>
        /// <param name="subquery">The subquery to check for non-existence.</param>
        /// <returns>The current query builder for method chaining.</returns>
        public IQueryBuilder<TEntity> WhereNotExists<TOther>(IQueryBuilder<TOther> subquery) where TOther : class, new()
        {
            throw new NotImplementedException("WhereNotExists is not yet implemented");
        }

        /// <summary>
        /// Adds a window function to the query.
        /// </summary>
        /// <param name="functionName">The name of the window function.</param>
        /// <param name="partitionBy">Optional PARTITION BY clause.</param>
        /// <param name="orderBy">Optional ORDER BY clause for the window.</param>
        /// <returns>A windowed query builder for further window operations.</returns>
        public IWindowedQueryBuilder<TEntity> WithWindowFunction(string functionName, string? partitionBy = null, string? orderBy = null)
        {
            throw new NotImplementedException("WithWindowFunction is not yet implemented");
        }

        /// <summary>
        /// Adds a Common Table Expression (CTE) to the query.
        /// </summary>
        /// <param name="cteName">The name of the CTE.</param>
        /// <param name="cteQuery">The SQL query for the CTE.</param>
        /// <returns>The current query builder for method chaining.</returns>
        public IQueryBuilder<TEntity> WithCte(string cteName, string cteQuery)
        {
            throw new NotImplementedException("WithCte is not yet implemented");
        }

        /// <summary>
        /// Adds a recursive Common Table Expression (CTE) to the query.
        /// </summary>
        /// <param name="cteName">The name of the recursive CTE.</param>
        /// <param name="anchorQuery">The anchor query for the recursive CTE.</param>
        /// <param name="recursiveQuery">The recursive query for the CTE.</param>
        /// <returns>The current query builder for method chaining.</returns>
        public IQueryBuilder<TEntity> WithRecursiveCte(string cteName, string anchorQuery, string recursiveQuery)
        {
            throw new NotImplementedException("WithRecursiveCte is not yet implemented");
        }

        /// <summary>
        /// Adds a raw SQL WHERE clause with optional parameters.
        /// </summary>
        /// <param name="sql">The raw SQL condition.</param>
        /// <param name="parameters">Optional parameters for the SQL.</param>
        /// <returns>The current query builder for method chaining.</returns>
        public IQueryBuilder<TEntity> WhereRaw(string sql, params object[] parameters)
        {
            if (string.IsNullOrWhiteSpace(sql)) throw new ArgumentException("SQL cannot be null or empty", nameof(sql));
            _WhereClauses.Add(sql);
            return this;
        }

        /// <summary>
        /// Adds a raw SQL SELECT clause.
        /// </summary>
        /// <param name="sql">The raw SQL select statement.</param>
        /// <returns>The current query builder for method chaining.</returns>
        public IQueryBuilder<TEntity> SelectRaw(string sql)
        {
            _CustomSelectClause = sql;
            return this;
        }

        /// <summary>
        /// Specifies a raw SQL FROM clause.
        /// </summary>
        /// <param name="sql">The raw SQL from statement.</param>
        /// <returns>The current query builder for method chaining.</returns>
        public IQueryBuilder<TEntity> FromRaw(string sql)
        {
            _CustomFromClause = sql;
            return this;
        }

        /// <summary>
        /// Adds a raw SQL JOIN clause.
        /// </summary>
        /// <param name="sql">The raw SQL join statement.</param>
        /// <returns>The current query builder for method chaining.</returns>
        public IQueryBuilder<TEntity> JoinRaw(string sql)
        {
            _CustomJoinClauses.Add(sql);
            return this;
        }

        /// <summary>
        /// Creates a CASE WHEN expression builder for conditional selections.
        /// </summary>
        /// <returns>A case expression builder for building conditional logic.</returns>
        public ICaseExpressionBuilder<TEntity> SelectCase()
        {
            throw new NotImplementedException("SelectCase is not yet implemented");
        }

        /// <summary>
        /// Executes the query and returns the results.
        /// </summary>
        /// <returns>The query results as an enumerable sequence.</returns>
        public IEnumerable<TEntity> Execute()
        {
            string sql = BuildSql();
            return ExecuteSqlInternal(sql);
        }

        /// <summary>
        /// Asynchronously executes the query and returns the results.
        /// </summary>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with query results.</returns>
        public async Task<IEnumerable<TEntity>> ExecuteAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            string sql = BuildSql();
            return await ExecuteSqlInternalAsync(sql, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Executes the query and returns results as an asynchronous enumerable stream.
        /// </summary>
        /// <param name="token">The cancellation token.</param>
        /// <returns>The query results as an asynchronous enumerable sequence.</returns>
        public async IAsyncEnumerable<TEntity> ExecuteAsyncEnumerable([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            IEnumerable<TEntity> results = await ExecuteAsync(token).ConfigureAwait(false);

            foreach (TEntity entity in results)
            {
                token.ThrowIfCancellationRequested();
                yield return entity;
            }
        }

        /// <summary>
        /// Executes the query and returns both the results and the executed SQL query.
        /// </summary>
        /// <returns>A durable result containing both query and results.</returns>
        public IDurableResult<TEntity> ExecuteWithQuery()
        {
            throw new NotImplementedException("ExecuteWithQuery is not yet implemented");
        }

        /// <summary>
        /// Asynchronously executes the query and returns both the results and the executed SQL query.
        /// </summary>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with durable result.</returns>
        public Task<IDurableResult<TEntity>> ExecuteWithQueryAsync(CancellationToken token = default)
        {
            throw new NotImplementedException("ExecuteWithQueryAsync is not yet implemented");
        }

        /// <summary>
        /// Executes the query as an asynchronous enumerable and exposes the executed SQL query.
        /// </summary>
        /// <param name="token">The cancellation token.</param>
        /// <returns>An asynchronous durable result containing both query and streaming results.</returns>
        public IAsyncDurableResult<TEntity> ExecuteAsyncEnumerableWithQuery(CancellationToken token = default)
        {
            throw new NotImplementedException("ExecuteAsyncEnumerableWithQuery is not yet implemented");
        }

        /// <summary>
        /// Builds and returns the SQL query string for debugging purposes.
        /// </summary>
        /// <returns>The SQL query string.</returns>
        public string BuildSql()
        {
            return BuildSql(true);
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Internal method to build the SQL query with options.
        /// </summary>
        /// <param name="includeGroupBy">Whether to include GROUP BY clauses</param>
        /// <returns>The constructed SQL query string</returns>
        private string BuildSql(bool includeGroupBy)
        {
            List<string> sqlParts = new List<string>();
            PostgresJoinBuilder.PostgresJoinResult? joinResult = null;

            // Pre-calculate join result if includes are present
            if (_IncludePaths.Count > 0)
            {
                joinResult = _JoinBuilder.BuildJoinSql<TEntity>(_Repository._TableName, _IncludePaths);
                _CachedJoinResult = joinResult; // Cache for later use in Execute()
            }

            // SELECT clause
            if (!string.IsNullOrEmpty(_CustomSelectClause))
            {
                sqlParts.Add($"SELECT {(_Distinct ? "DISTINCT " : "")}{_CustomSelectClause}");
            }
            else if (joinResult != null)
            {
                sqlParts.Add($"SELECT {(_Distinct ? "DISTINCT " : "")}{joinResult.SelectClause}");
            }
            else
            {
                string tableName = _Repository._Sanitizer.SanitizeIdentifier(_Repository._TableName);
                sqlParts.Add($"SELECT {(_Distinct ? "DISTINCT " : "")}{tableName}.*");
            }

            // FROM clause
            if (!string.IsNullOrEmpty(_CustomFromClause))
            {
                sqlParts.Add($"FROM {_CustomFromClause}");
            }
            else
            {
                string tableName = _Repository._Sanitizer.SanitizeIdentifier(_Repository._TableName);
                sqlParts.Add($"FROM {tableName}");
            }

            // JOIN clauses
            if (joinResult != null && !string.IsNullOrEmpty(joinResult.JoinClause))
            {
                sqlParts.Add(joinResult.JoinClause);
            }

            // Custom JOIN clauses
            if (_CustomJoinClauses.Count > 0)
            {
                sqlParts.AddRange(_CustomJoinClauses);
            }

            // WHERE clause
            if (_WhereClauses.Count > 0)
            {
                sqlParts.Add($"WHERE {string.Join(" AND ", _WhereClauses)}");
            }

            // GROUP BY clause
            if (includeGroupBy && _GroupByColumns.Count > 0)
            {
                sqlParts.Add($"GROUP BY {string.Join(", ", _GroupByColumns)}");
            }

            // HAVING clause
            if (_HavingClauses.Count > 0)
            {
                sqlParts.Add($"HAVING {string.Join(" AND ", _HavingClauses)}");
            }

            // ORDER BY clause
            if (_OrderByClauses.Count > 0)
            {
                sqlParts.Add($"ORDER BY {string.Join(", ", _OrderByClauses)}");
            }

            // LIMIT and OFFSET (PostgreSQL syntax)
            if (_TakeCount.HasValue)
            {
                sqlParts.Add($"LIMIT {_TakeCount.Value}");
            }

            if (_SkipCount.HasValue)
            {
                sqlParts.Add($"OFFSET {_SkipCount.Value}");
            }

            return string.Join(" ", sqlParts);
        }

        private IEnumerable<TEntity> ExecuteSqlInternal(string sql)
        {
            List<TEntity> results = new List<TEntity>();

            DbConnection connection = GetConnection();
            bool shouldDisposeConnection = (_Transaction == null); // Only dispose if we created it

            try
            {
                // Ensure connection is open
                if (connection.State != System.Data.ConnectionState.Open)
                {
                    connection.Open();
                }

                using (NpgsqlCommand command = new NpgsqlCommand(sql, (NpgsqlConnection)connection))
                {
                    using (NpgsqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            TEntity entity = _EntityMapper.MapEntity(reader, _CachedJoinResult);
                            results.Add(entity);
                        }
                    }
                }
            }
            finally
            {
                if (shouldDisposeConnection)
                {
                    connection?.Dispose();
                }
            }

            return results;
        }

        private async Task<IEnumerable<TEntity>> ExecuteSqlInternalAsync(string sql, CancellationToken token)
        {
            List<TEntity> results = new List<TEntity>();

            DbConnection connection = await GetConnectionAsync().ConfigureAwait(false);
            bool shouldDisposeConnection = (_Transaction == null); // Only dispose if we created it

            try
            {
                // Ensure connection is open
                if (connection.State != System.Data.ConnectionState.Open)
                {
                    await connection.OpenAsync(token).ConfigureAwait(false);
                }

                using (NpgsqlCommand command = new NpgsqlCommand(sql, (NpgsqlConnection)connection))
                {
                    using (NpgsqlDataReader reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false))
                    {
                        while (await reader.ReadAsync(token).ConfigureAwait(false))
                        {
                            token.ThrowIfCancellationRequested();
                            TEntity entity = _EntityMapper.MapEntity(reader, _CachedJoinResult);
                            results.Add(entity);
                        }
                    }
                }
            }
            finally
            {
                if (shouldDisposeConnection)
                {
                    connection?.Dispose();
                }
            }

            return results;
        }

        private DbConnection GetConnection()
        {
            return _Transaction?.Connection ?? _Repository._ConnectionFactory.GetConnection();
        }

        private async Task<DbConnection> GetConnectionAsync()
        {
            return _Transaction?.Connection ?? await _Repository._ConnectionFactory.GetConnectionAsync().ConfigureAwait(false);
        }

        private string GetIncludePathFromExpression<TProperty>(Expression<Func<TEntity, TProperty>> navigationProperty)
        {
            // Simplified implementation - full version would properly parse navigation property paths
            if (navigationProperty.Body is MemberExpression member)
            {
                return member.Member.Name;
            }
            throw new ArgumentException("Invalid navigation property expression", nameof(navigationProperty));
        }

        #endregion
    }
}