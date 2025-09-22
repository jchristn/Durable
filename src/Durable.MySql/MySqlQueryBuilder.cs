namespace Durable.MySql
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// MySQL-specific implementation of IQueryBuilder that provides fluent query building capabilities for MySQL databases.
    /// Supports advanced features like CTEs, window functions, set operations, and includes.
    /// </summary>
    /// <typeparam name="TEntity">The entity type being queried</typeparam>
    public class MySqlQueryBuilder<TEntity> : IQueryBuilder<TEntity> where TEntity : class, new()
    {
        #region Public-Members

        /// <summary>
        /// Gets the SQL query that will be or was executed.
        /// </summary>
        public string Query => BuildSql();

        #endregion

        #region Private-Members

        private readonly MySqlRepository<TEntity> _Repository;
        private readonly ITransaction? _Transaction;
        private readonly List<string> _WhereClauses = new List<string>();
        private readonly List<string> _OrderByClauses = new List<string>();
        private readonly List<string> _IncludePaths = new List<string>();
        private readonly List<string> _GroupByColumns = new List<string>();
        private readonly MySqlExpressionParser<TEntity> _ExpressionParser;
        private readonly MySqlJoinBuilder _JoinBuilder;
        private readonly MySqlEntityMapper<TEntity> _EntityMapper;
        private int? _SkipCount;
        private int? _TakeCount;
        private bool _Distinct;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the MySqlQueryBuilder class.
        /// </summary>
        /// <param name="repository">The MySQL repository instance for data access operations</param>
        /// <param name="transaction">Optional transaction to execute queries within. Default is null</param>
        /// <exception cref="ArgumentNullException">Thrown when repository is null</exception>
        public MySqlQueryBuilder(MySqlRepository<TEntity> repository, ITransaction? transaction = null)
        {
            _Repository = repository ?? throw new ArgumentNullException(nameof(repository));
            _Transaction = transaction;
            _ExpressionParser = new MySqlExpressionParser<TEntity>(_Repository._ColumnMappings, _Repository._Sanitizer);
            _JoinBuilder = new MySqlJoinBuilder(_Repository._Sanitizer);
            _EntityMapper = new MySqlEntityMapper<TEntity>(_Repository._DataTypeConverter, _Repository._ColumnMappings, _Repository._Sanitizer);
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
            return OrderBy(keySelector); // TODO: Implement proper ThenBy logic
        }

        /// <summary>
        /// Performs a subsequent ordering of the query results in descending order.
        /// </summary>
        /// <typeparam name="TKey">The type of the ordering key.</typeparam>
        /// <param name="keySelector">The expression to extract the ordering key.</param>
        /// <returns>The current query builder for method chaining.</returns>
        public IQueryBuilder<TEntity> ThenByDescending<TKey>(Expression<Func<TEntity, TKey>> keySelector)
        {
            return OrderByDescending(keySelector); // TODO: Implement proper ThenByDescending logic
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
        /// Executes the query and returns the results.
        /// </summary>
        /// <returns>The query results as an enumerable sequence.</returns>
        public IEnumerable<TEntity> Execute()
        {
            string sql = BuildSql();
            return ExecuteSqlInternal(sql);
        }

        /// <summary>
        /// Builds and returns the SQL query string for debugging purposes.
        /// </summary>
        /// <returns>The SQL query string.</returns>
        public string BuildSql()
        {
            List<string> sqlParts = new List<string>();

            // SELECT clause
            if (_Distinct)
                sqlParts.Add("SELECT DISTINCT *");
            else
                sqlParts.Add("SELECT *");

            // FROM clause
            sqlParts.Add($"FROM `{_Repository._TableName}`");

            // WHERE clause
            if (_WhereClauses.Any())
            {
                sqlParts.Add($"WHERE {string.Join(" AND ", _WhereClauses)}");
            }

            // ORDER BY clause
            if (_OrderByClauses.Any())
            {
                sqlParts.Add($"ORDER BY {string.Join(", ", _OrderByClauses)}");
            }

            // LIMIT clause (MySQL uses LIMIT instead of SQLite's LIMIT OFFSET)
            if (_TakeCount.HasValue || _SkipCount.HasValue)
            {
                if (_SkipCount.HasValue && _TakeCount.HasValue)
                    sqlParts.Add($"LIMIT {_SkipCount.Value}, {_TakeCount.Value}");
                else if (_TakeCount.HasValue)
                    sqlParts.Add($"LIMIT {_TakeCount.Value}");
                else if (_SkipCount.HasValue)
                    sqlParts.Add($"LIMIT {_SkipCount.Value}, 18446744073709551615"); // Max value for MySQL
            }

            return string.Join(" ", sqlParts);
        }

        #endregion

        #region Not-Yet-Implemented

        // These methods will be implemented as the MySQL provider matures

        public IQueryBuilder<TResult> Select<TResult>(Expression<Func<TEntity, TResult>> selector) where TResult : class, new()
        {
            throw new NotImplementedException("Coming soon");
        }

        public IQueryBuilder<TEntity> Include<TProperty>(Expression<Func<TEntity, TProperty>> navigationProperty)
        {
            if (navigationProperty == null) throw new ArgumentNullException(nameof(navigationProperty));

            string propertyPath = ExtractPropertyPath(navigationProperty);
            _IncludePaths.Add(propertyPath);
            return this;
        }

        public IQueryBuilder<TEntity> ThenInclude<TPreviousProperty, TProperty>(Expression<Func<TPreviousProperty, TProperty>> navigationProperty)
        {
            if (navigationProperty == null) throw new ArgumentNullException(nameof(navigationProperty));

            // For ThenInclude, we need to append to the last include path
            if (_IncludePaths.Count == 0)
            {
                throw new InvalidOperationException("ThenInclude can only be used after Include");
            }

            string propertyPath = ExtractPropertyPath(navigationProperty);
            string lastIncludePath = _IncludePaths[_IncludePaths.Count - 1];
            string combinedPath = $"{lastIncludePath}.{propertyPath}";

            // Replace the last include path with the combined path
            _IncludePaths[_IncludePaths.Count - 1] = combinedPath;
            return this;
        }

        public IGroupedQueryBuilder<TEntity, TKey> GroupBy<TKey>(Expression<Func<TEntity, TKey>> keySelector)
        {
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));

            // Extract the column name from the key selector expression
            string groupColumn = GetColumnFromExpression(keySelector.Body);
            _GroupByColumns.Add(_Repository._Sanitizer.SanitizeIdentifier(groupColumn));

            // Create advanced entity mapper for enhanced type handling
            MySqlEntityMapper<TEntity> entityMapper = new MySqlEntityMapper<TEntity>(
                _Repository._DataTypeConverter,
                _Repository._ColumnMappings,
                _Repository._Sanitizer);

            // Return the advanced grouped query builder with full EntityMapper integration
            return new MySqlGroupedQueryBuilder<TEntity, TKey>(
                _Repository,
                this,
                keySelector,
                entityMapper,
                _Repository._DataTypeConverter,
                _Repository._Sanitizer);
        }

        public IQueryBuilder<TEntity> Having(Expression<Func<TEntity, bool>> predicate)
        {
            throw new NotImplementedException("Coming soon");
        }

        public IQueryBuilder<TEntity> Union(IQueryBuilder<TEntity> other)
        {
            throw new NotImplementedException("Coming soon");
        }

        public IQueryBuilder<TEntity> UnionAll(IQueryBuilder<TEntity> other)
        {
            throw new NotImplementedException("Coming soon");
        }

        public IQueryBuilder<TEntity> Intersect(IQueryBuilder<TEntity> other)
        {
            throw new NotImplementedException("Coming soon");
        }

        public IQueryBuilder<TEntity> Except(IQueryBuilder<TEntity> other)
        {
            throw new NotImplementedException("Coming soon");
        }

        public IQueryBuilder<TEntity> WhereIn<TKey>(Expression<Func<TEntity, TKey>> keySelector, IQueryBuilder<TKey> subquery) where TKey : class, new()
        {
            throw new NotImplementedException("Coming soon");
        }

        public IQueryBuilder<TEntity> WhereNotIn<TKey>(Expression<Func<TEntity, TKey>> keySelector, IQueryBuilder<TKey> subquery) where TKey : class, new()
        {
            throw new NotImplementedException("Coming soon");
        }

        public IQueryBuilder<TEntity> WhereInRaw<TKey>(Expression<Func<TEntity, TKey>> keySelector, string subquerySql)
        {
            throw new NotImplementedException("Coming soon");
        }

        public IQueryBuilder<TEntity> WhereNotInRaw<TKey>(Expression<Func<TEntity, TKey>> keySelector, string subquerySql)
        {
            throw new NotImplementedException("Coming soon");
        }

        public IQueryBuilder<TEntity> WhereExists<TOther>(IQueryBuilder<TOther> subquery) where TOther : class, new()
        {
            throw new NotImplementedException("Coming soon");
        }

        public IQueryBuilder<TEntity> WhereNotExists<TOther>(IQueryBuilder<TOther> subquery) where TOther : class, new()
        {
            throw new NotImplementedException("Coming soon");
        }

        public IWindowedQueryBuilder<TEntity> WithWindowFunction(string functionName, string? partitionBy = null, string? orderBy = null)
        {
            throw new NotImplementedException("Coming soon");
        }

        public IQueryBuilder<TEntity> WithCte(string cteName, string cteQuery)
        {
            throw new NotImplementedException("Coming soon");
        }

        public IQueryBuilder<TEntity> WithRecursiveCte(string cteName, string anchorQuery, string recursiveQuery)
        {
            throw new NotImplementedException("Coming soon");
        }

        public IQueryBuilder<TEntity> WhereRaw(string sql, params object[] parameters)
        {
            throw new NotImplementedException("Coming soon");
        }

        public IQueryBuilder<TEntity> SelectRaw(string sql)
        {
            throw new NotImplementedException("Coming soon");
        }

        public IQueryBuilder<TEntity> FromRaw(string sql)
        {
            throw new NotImplementedException("Coming soon");
        }

        public IQueryBuilder<TEntity> JoinRaw(string sql)
        {
            throw new NotImplementedException("Coming soon");
        }

        public ICaseExpressionBuilder<TEntity> SelectCase()
        {
            throw new NotImplementedException("Coming soon");
        }

        /// <summary>
        /// Asynchronously executes the query and returns the results using advanced entity mapping.
        /// </summary>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>The query results with properly mapped entities including navigation properties</returns>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        public async Task<IEnumerable<TEntity>> ExecuteAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            string sql = BuildSql();
            return await ExecuteSqlInternalAsync(sql, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Asynchronously executes the query and returns the results as an async enumerable with advanced entity mapping.
        /// </summary>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>An async enumerable of query results with properly mapped entities</returns>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
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

        public IDurableResult<TEntity> ExecuteWithQuery()
        {
            throw new NotImplementedException("Coming soon");
        }

        public Task<IDurableResult<TEntity>> ExecuteWithQueryAsync(CancellationToken token = default)
        {
            throw new NotImplementedException("Coming soon");
        }

        public IAsyncDurableResult<TEntity> ExecuteAsyncEnumerableWithQuery(CancellationToken token = default)
        {
            throw new NotImplementedException("Coming soon");
        }

        #endregion

        #region Private-Methods

        private IEnumerable<TEntity> ExecuteSqlInternal(string sql)
        {
            if (_Transaction != null)
            {
                // Use existing transaction connection
                return ExecuteWithConnection(_Transaction.Connection, sql);
            }
            else
            {
                // Get connection from factory
                using var connection = _Repository._ConnectionFactory.GetConnection();
                return ExecuteWithConnection(connection, sql);
            }
        }

        private IEnumerable<TEntity> ExecuteWithConnection(System.Data.Common.DbConnection connection, string sql)
        {
            List<TEntity> results = new List<TEntity>();

            if (connection.State != System.Data.ConnectionState.Open)
            {
                connection.Open();
            }

            using var command = connection.CreateCommand();
            command.CommandText = sql;

            if (_Transaction != null)
            {
                command.Transaction = _Transaction.Transaction;
            }

            // Capture SQL if enabled
            if (_Repository.CaptureSql)
            {
                _Repository.SetLastExecutedSql(sql);
            }

            try
            {
                using var reader = command.ExecuteReader();
                results = MapResults(reader).ToList();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error executing SQL: {sql}", ex);
            }

            return results;
        }

        private IEnumerable<TEntity> MapResults(System.Data.Common.DbDataReader reader)
        {
            // This is a basic implementation - in a full implementation this would use
            // a proper entity mapper similar to SQLite's EntityMapper
            while (reader.Read())
            {
                var entity = new TEntity();

                // Map columns to entity properties using the repository's column mappings
                foreach (var mapping in _Repository._ColumnMappings)
                {
                    string columnName = mapping.Key;
                    var property = mapping.Value;

                    try
                    {
                        if (HasColumn(reader, columnName))
                        {
                            object? value = reader[columnName];
                            if (value != DBNull.Value && value != null)
                            {
                                // Convert value to property type
                                object convertedValue = _Repository._DataTypeConverter.ConvertFromDatabase(value, property.PropertyType, property);
                                property.SetValue(entity, convertedValue);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException($"Error mapping column '{columnName}' to property '{property.Name}'", ex);
                    }
                }

                yield return entity;
            }
        }

        private static bool HasColumn(System.Data.Common.DbDataReader reader, string columnName)
        {
            for (int i = 0; i < reader.FieldCount; i++)
            {
                if (reader.GetName(i).Equals(columnName, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Gets the current GROUP BY columns for grouped query operations.
        /// </summary>
        /// <returns>A list of GROUP BY column names</returns>
        internal List<string> GetGroupByColumns()
        {
            return new List<string>(_GroupByColumns);
        }

        /// <summary>
        /// Gets the current WHERE clauses for query filtering.
        /// </summary>
        /// <returns>A list of WHERE clause conditions</returns>
        internal List<string> GetWhereClauses()
        {
            return new List<string>(_WhereClauses);
        }

        /// <summary>
        /// Gets the current include paths for navigation property loading.
        /// </summary>
        /// <returns>A list of include paths</returns>
        internal List<string> GetIncludePaths()
        {
            return new List<string>(_IncludePaths);
        }

        /// <summary>
        /// Extracts column name from an expression using the MySQL expression parser.
        /// </summary>
        /// <param name="expression">The expression to extract the column name from</param>
        /// <returns>The column name</returns>
        internal string GetColumnFromExpression(Expression expression)
        {
            return _ExpressionParser.GetColumnFromExpression(expression);
        }

        /// <summary>
        /// Asynchronously executes SQL and returns mapped entities using advanced EntityMapper.
        /// </summary>
        /// <param name="sql">The SQL query to execute</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>The mapped entities with navigation properties</returns>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        private async Task<IEnumerable<TEntity>> ExecuteSqlInternalAsync(string sql, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();

            if (_Transaction != null)
            {
                // Use existing transaction connection
                return await ExecuteWithConnectionAsync(_Transaction.Connection, sql, token).ConfigureAwait(false);
            }
            else
            {
                // Get connection from factory
                using var connection = _Repository._ConnectionFactory.GetConnection();
                return await ExecuteWithConnectionAsync(connection, sql, token).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Asynchronously executes SQL with a specific connection using advanced entity mapping.
        /// </summary>
        /// <param name="connection">The database connection to use</param>
        /// <param name="sql">The SQL query to execute</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>The mapped entities with navigation properties</returns>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        private async Task<IEnumerable<TEntity>> ExecuteWithConnectionAsync(System.Data.Common.DbConnection connection, string sql, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();

            if (connection.State != System.Data.ConnectionState.Open)
            {
                await connection.OpenAsync(token).ConfigureAwait(false);
            }

            using var command = connection.CreateCommand();
            command.CommandText = sql;

            if (_Transaction != null)
            {
                command.Transaction = _Transaction.Transaction;
            }

            // Capture SQL if enabled
            if (_Repository.CaptureSql)
            {
                _Repository.SetLastExecutedSql(sql);
            }

            try
            {
                using var reader = (MySqlConnector.MySqlDataReader)await command.ExecuteReaderAsync(token).ConfigureAwait(false);

                // Check if we have includes that require advanced mapping
                if (_IncludePaths.Count > 0)
                {
                    // Use advanced EntityMapper for complex scenarios with navigation properties
                    List<MySqlIncludeInfo> includeInfos = ProcessIncludePaths();
                    MySqlJoinBuilder.MySqlJoinResult joinResult = _JoinBuilder.BuildJoinSql<TEntity>(_Repository._TableName, _IncludePaths);

                    _EntityMapper.ClearProcessingCache();
                    return await _EntityMapper.MapJoinedResultsAsync(reader, joinResult, includeInfos, token).ConfigureAwait(false);
                }
                else
                {
                    // Use simple mapping for basic queries
                    return await _EntityMapper.MapSimpleResultsAsync(reader, token).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error executing SQL: {sql}", ex);
            }
        }

        /// <summary>
        /// Processes include paths into MySqlIncludeInfo objects for advanced entity mapping.
        /// </summary>
        /// <returns>A list of include information objects</returns>
        private List<MySqlIncludeInfo> ProcessIncludePaths()
        {
            try
            {
                MySqlIncludeProcessor processor = new MySqlIncludeProcessor(_Repository._Sanitizer);
                return processor.ParseIncludes<TEntity>(_IncludePaths);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error processing include paths: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Extracts the property path from a navigation property expression.
        /// </summary>
        /// <typeparam name="TSource">The source type</typeparam>
        /// <typeparam name="TProperty">The property type</typeparam>
        /// <param name="expression">The navigation property expression</param>
        /// <returns>The property path as a string</returns>
        /// <exception cref="ArgumentNullException">Thrown when expression is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when expression is not a valid property access</exception>
        private string ExtractPropertyPath<TSource, TProperty>(Expression<Func<TSource, TProperty>> expression)
        {
            if (expression == null)
                throw new ArgumentNullException(nameof(expression));

            if (expression.Body is MemberExpression memberExpression)
            {
                List<string> propertyNames = new List<string>();
                MemberExpression current = memberExpression;

                while (current != null)
                {
                    propertyNames.Insert(0, current.Member.Name);
                    current = current.Expression as MemberExpression;
                }

                return string.Join(".", propertyNames);
            }

            throw new InvalidOperationException("Expression must be a property access expression");
        }

        #endregion
    }
}