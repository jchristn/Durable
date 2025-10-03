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
            string column = _ExpressionParser.GetColumnFromExpression(keySelector.Body);
            _OrderByClauses.Add($"{column} ASC");
            return this;
        }

        /// <summary>
        /// Performs a subsequent ordering of the query results in descending order.
        /// </summary>
        /// <typeparam name="TKey">The type of the ordering key.</typeparam>
        /// <param name="keySelector">The expression to extract the ordering key.</param>
        /// <returns>The current query builder for method chaining.</returns>
        public IQueryBuilder<TEntity> ThenByDescending<TKey>(Expression<Func<TEntity, TKey>> keySelector)
        {
            string column = _ExpressionParser.GetColumnFromExpression(keySelector.Body);
            _OrderByClauses.Add($"{column} DESC");
            return this;
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
            return new PostgresProjectedQueryBuilder<TEntity, TResult>(_Repository, selector, this, _Transaction);
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

        /// <summary>
        /// Groups the query results by the specified key selector.
        /// </summary>
        /// <typeparam name="TKey">The type of the grouping key.</typeparam>
        /// <param name="keySelector">The expression to extract the grouping key.</param>
        /// <returns>A grouped query builder for further operations.</returns>
        /// <exception cref="ArgumentNullException">Thrown when keySelector is null</exception>
        public IGroupedQueryBuilder<TEntity, TKey> GroupBy<TKey>(Expression<Func<TEntity, TKey>> keySelector)
        {
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));

            // Try to extract column name from the key selector expression
            // For complex expressions (anonymous types, computed fields), we'll skip SQL GROUP BY
            // and let PostgresGroupedQueryBuilder handle it in-memory
            try
            {
                string groupColumn = _ExpressionParser.GetColumnFromExpression(keySelector.Body);
                // Remove double quotes if present to store raw column name
                string rawColumn = groupColumn.Trim('"');
                _GroupByColumns.Add(rawColumn);
            }
            catch (ArgumentException)
            {
                // Complex expression that can't be translated to SQL column
                // PostgresGroupedQueryBuilder will handle this with in-memory grouping
                // Don't add anything to _GroupByColumns so SQL GROUP BY is skipped
            }

            // Create advanced entity mapper for enhanced type handling
            PostgresEntityMapper<TEntity> entityMapper = new PostgresEntityMapper<TEntity>(
                _Repository._DataTypeConverter,
                _Repository._ColumnMappings,
                _Repository._Sanitizer);

            // Return the advanced grouped query builder with full EntityMapper integration
            return new PostgresGroupedQueryBuilder<TEntity, TKey>(
                _Repository,
                this,
                keySelector,
                entityMapper,
                _Repository._DataTypeConverter,
                _Repository._Sanitizer);
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
        /// <exception cref="ArgumentNullException">Thrown when other is null</exception>
        public IQueryBuilder<TEntity> Union(IQueryBuilder<TEntity> other)
        {
            if (other == null)
                throw new ArgumentNullException(nameof(other));
            _SetOperations.Add(new SetOperation<TEntity>(SetOperationType.Union, other));
            return this;
        }

        /// <summary>
        /// Performs a UNION ALL operation with another query, combining results including duplicates.
        /// </summary>
        /// <param name="other">The other query builder to union with.</param>
        /// <returns>The current query builder for method chaining.</returns>
        /// <exception cref="ArgumentNullException">Thrown when other is null</exception>
        public IQueryBuilder<TEntity> UnionAll(IQueryBuilder<TEntity> other)
        {
            if (other == null)
                throw new ArgumentNullException(nameof(other));
            _SetOperations.Add(new SetOperation<TEntity>(SetOperationType.UnionAll, other));
            return this;
        }

        /// <summary>
        /// Performs an INTERSECT operation with another query, returning only common results.
        /// PostgreSQL natively supports INTERSECT operations.
        /// </summary>
        /// <param name="other">The other query builder to intersect with.</param>
        /// <returns>The current query builder for method chaining.</returns>
        /// <exception cref="ArgumentNullException">Thrown when other is null</exception>
        public IQueryBuilder<TEntity> Intersect(IQueryBuilder<TEntity> other)
        {
            if (other == null)
                throw new ArgumentNullException(nameof(other));
            _SetOperations.Add(new SetOperation<TEntity>(SetOperationType.Intersect, other));
            return this;
        }

        /// <summary>
        /// Performs an EXCEPT operation with another query, returning results not in the other query.
        /// PostgreSQL natively supports EXCEPT operations.
        /// </summary>
        /// <param name="other">The other query builder to except with.</param>
        /// <returns>The current query builder for method chaining.</returns>
        /// <exception cref="ArgumentNullException">Thrown when other is null</exception>
        public IQueryBuilder<TEntity> Except(IQueryBuilder<TEntity> other)
        {
            if (other == null)
                throw new ArgumentNullException(nameof(other));
            _SetOperations.Add(new SetOperation<TEntity>(SetOperationType.Except, other));
            return this;
        }

        /// <summary>
        /// Adds a WHERE IN clause using a subquery.
        /// </summary>
        /// <typeparam name="TKey">The type of the key to match.</typeparam>
        /// <param name="keySelector">The expression to extract the key from the main query.</param>
        /// <param name="subquery">The subquery to check membership against.</param>
        /// <returns>The current query builder for method chaining.</returns>
        /// <exception cref="ArgumentNullException">Thrown when keySelector or subquery is null</exception>
        public IQueryBuilder<TEntity> WhereIn<TKey>(Expression<Func<TEntity, TKey>> keySelector, IQueryBuilder<TKey> subquery) where TKey : class, new()
        {
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));
            if (subquery == null)
                throw new ArgumentNullException(nameof(subquery));
            string column = _ExpressionParser.GetColumnFromExpression(keySelector.Body);
            string subquerySql = subquery.BuildSql().TrimEnd(';');
            _WhereClauses.Add($"{column} IN ({subquerySql})");
            return this;
        }

        /// <summary>
        /// Adds a WHERE NOT IN clause using a subquery.
        /// </summary>
        /// <typeparam name="TKey">The type of the key to match.</typeparam>
        /// <param name="keySelector">The expression to extract the key from the main query.</param>
        /// <param name="subquery">The subquery to check membership against.</param>
        /// <returns>The current query builder for method chaining.</returns>
        /// <exception cref="ArgumentNullException">Thrown when keySelector or subquery is null</exception>
        public IQueryBuilder<TEntity> WhereNotIn<TKey>(Expression<Func<TEntity, TKey>> keySelector, IQueryBuilder<TKey> subquery) where TKey : class, new()
        {
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));
            if (subquery == null)
                throw new ArgumentNullException(nameof(subquery));
            string column = _ExpressionParser.GetColumnFromExpression(keySelector.Body);
            string subquerySql = subquery.BuildSql().TrimEnd(';');
            _WhereClauses.Add($"{column} NOT IN ({subquerySql})");
            return this;
        }

        /// <summary>
        /// Adds a WHERE IN clause using raw SQL for the subquery.
        /// </summary>
        /// <typeparam name="TKey">The type of the key to match.</typeparam>
        /// <param name="keySelector">The expression to extract the key from the main query.</param>
        /// <param name="subquerySql">The raw SQL subquery string.</param>
        /// <returns>The current query builder for method chaining.</returns>
        /// <exception cref="ArgumentNullException">Thrown when keySelector is null</exception>
        /// <exception cref="ArgumentException">Thrown when subquerySql is null, empty, or whitespace</exception>
        public IQueryBuilder<TEntity> WhereInRaw<TKey>(Expression<Func<TEntity, TKey>> keySelector, string subquerySql)
        {
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));
            if (string.IsNullOrWhiteSpace(subquerySql))
                throw new ArgumentException("Subquery SQL cannot be null or empty", nameof(subquerySql));
            string column = _ExpressionParser.GetColumnFromExpression(keySelector.Body);
            _WhereClauses.Add($"{column} IN ({subquerySql})");
            return this;
        }

        /// <summary>
        /// Adds a WHERE NOT IN clause using raw SQL for the subquery.
        /// </summary>
        /// <typeparam name="TKey">The type of the key to match.</typeparam>
        /// <param name="keySelector">The expression to extract the key from the main query.</param>
        /// <param name="subquerySql">The raw SQL subquery string.</param>
        /// <returns>The current query builder for method chaining.</returns>
        /// <exception cref="ArgumentNullException">Thrown when keySelector is null</exception>
        /// <exception cref="ArgumentException">Thrown when subquerySql is null, empty, or whitespace</exception>
        public IQueryBuilder<TEntity> WhereNotInRaw<TKey>(Expression<Func<TEntity, TKey>> keySelector, string subquerySql)
        {
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));
            if (string.IsNullOrWhiteSpace(subquerySql))
                throw new ArgumentException("Subquery SQL cannot be null or empty", nameof(subquerySql));
            string column = _ExpressionParser.GetColumnFromExpression(keySelector.Body);
            _WhereClauses.Add($"{column} NOT IN ({subquerySql})");
            return this;
        }

        /// <summary>
        /// Adds a WHERE EXISTS clause using a subquery.
        /// </summary>
        /// <typeparam name="TOther">The type of the subquery entity.</typeparam>
        /// <param name="subquery">The subquery to check for existence.</param>
        /// <returns>The current query builder for method chaining.</returns>
        /// <exception cref="ArgumentNullException">Thrown when subquery is null</exception>
        public IQueryBuilder<TEntity> WhereExists<TOther>(IQueryBuilder<TOther> subquery) where TOther : class, new()
        {
            if (subquery == null)
                throw new ArgumentNullException(nameof(subquery));
            string subquerySql = subquery.BuildSql().TrimEnd(';');
            _WhereClauses.Add($"EXISTS ({subquerySql})");
            return this;
        }

        /// <summary>
        /// Adds a WHERE NOT EXISTS clause using a subquery.
        /// </summary>
        /// <typeparam name="TOther">The type of the subquery entity.</typeparam>
        /// <param name="subquery">The subquery to check for non-existence.</param>
        /// <returns>The current query builder for method chaining.</returns>
        /// <exception cref="ArgumentNullException">Thrown when subquery is null</exception>
        public IQueryBuilder<TEntity> WhereNotExists<TOther>(IQueryBuilder<TOther> subquery) where TOther : class, new()
        {
            if (subquery == null)
                throw new ArgumentNullException(nameof(subquery));
            string subquerySql = subquery.BuildSql().TrimEnd(';');
            _WhereClauses.Add($"NOT EXISTS ({subquerySql})");
            return this;
        }

        /// <summary>
        /// Adds a window function to the query.
        /// PostgreSQL has excellent support for window functions with sophisticated frame specifications.
        /// </summary>
        /// <param name="functionName">The name of the window function.</param>
        /// <param name="partitionBy">Optional PARTITION BY clause.</param>
        /// <param name="orderBy">Optional ORDER BY clause for the window.</param>
        /// <returns>A windowed query builder for further window operations.</returns>
        /// <exception cref="ArgumentException">Thrown when functionName is null or empty</exception>
        public IWindowedQueryBuilder<TEntity> WithWindowFunction(string functionName, string? partitionBy = null, string? orderBy = null)
        {
            if (string.IsNullOrWhiteSpace(functionName))
                throw new ArgumentException("Function name cannot be null or empty", nameof(functionName));

            return new PostgresWindowedQueryBuilder<TEntity>(
                this,
                _Repository,
                null, // Transaction will be handled by the repository
                functionName,
                partitionBy,
                orderBy);
        }

        /// <summary>
        /// Adds a Common Table Expression (CTE) to the query.
        /// PostgreSQL has excellent support for CTEs including recursive CTEs.
        /// </summary>
        /// <param name="cteName">The name of the CTE.</param>
        /// <param name="cteQuery">The SQL query for the CTE.</param>
        /// <returns>The current query builder for method chaining.</returns>
        /// <exception cref="ArgumentException">Thrown when cteName or cteQuery is null or empty</exception>
        public IQueryBuilder<TEntity> WithCte(string cteName, string cteQuery)
        {
            if (string.IsNullOrWhiteSpace(cteName))
                throw new ArgumentException("CTE name cannot be null or empty", nameof(cteName));
            if (string.IsNullOrWhiteSpace(cteQuery))
                throw new ArgumentException("CTE query cannot be null or empty", nameof(cteQuery));

            _CteDefinitions.Add(new CteDefinition(cteName, cteQuery));
            return this;
        }

        /// <summary>
        /// Adds a recursive Common Table Expression (CTE) to the query.
        /// PostgreSQL has excellent support for recursive CTEs for hierarchical data processing.
        /// </summary>
        /// <param name="cteName">The name of the recursive CTE.</param>
        /// <param name="anchorQuery">The anchor query for the recursive CTE.</param>
        /// <param name="recursiveQuery">The recursive query for the CTE.</param>
        /// <returns>The current query builder for method chaining.</returns>
        /// <exception cref="ArgumentException">Thrown when cteName, anchorQuery, or recursiveQuery is null or empty</exception>
        public IQueryBuilder<TEntity> WithRecursiveCte(string cteName, string anchorQuery, string recursiveQuery)
        {
            if (string.IsNullOrWhiteSpace(cteName))
                throw new ArgumentException("CTE name cannot be null or empty", nameof(cteName));
            if (string.IsNullOrWhiteSpace(anchorQuery))
                throw new ArgumentException("Anchor query cannot be null or empty", nameof(anchorQuery));
            if (string.IsNullOrWhiteSpace(recursiveQuery))
                throw new ArgumentException("Recursive query cannot be null or empty", nameof(recursiveQuery));

            _CteDefinitions.Add(new CteDefinition(cteName, anchorQuery, recursiveQuery));
            return this;
        }

        /// <summary>
        /// Adds a raw SQL WHERE clause with optional parameters.
        /// </summary>
        /// <param name="sql">The raw SQL condition.</param>
        /// <param name="parameters">Optional parameters for the SQL.</param>
        /// <returns>The current query builder for method chaining.</returns>
        /// <exception cref="ArgumentException">Thrown when sql is empty or whitespace</exception>
        public IQueryBuilder<TEntity> WhereRaw(string sql, params object[] parameters)
        {
            if (string.IsNullOrWhiteSpace(sql))
                throw new ArgumentException("Raw SQL cannot be null or empty", nameof(sql));

            if (parameters != null && parameters.Length > 0)
            {
                sql = string.Format(sql, parameters.Select(p => _Repository._Sanitizer.FormatValue(p)).ToArray());
            }
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
        /// Begins building a CASE expression for conditional logic in the SELECT clause.
        /// </summary>
        /// <returns>A case expression builder for constructing WHEN/THEN/ELSE logic</returns>
        public ICaseExpressionBuilder<TEntity> SelectCase()
        {
            return new PostgresCaseExpressionBuilder<TEntity>(this, _Repository);
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
            string sql = BuildSql();
            IEnumerable<TEntity> results = ExecuteSqlInternal(sql);
            return new DurableResult<TEntity>(sql, results);
        }

        /// <summary>
        /// Asynchronously executes the query and returns both the results and the executed SQL query.
        /// </summary>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with durable result.</returns>
        public async Task<IDurableResult<TEntity>> ExecuteWithQueryAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            string sql = BuildSql();
            IEnumerable<TEntity> results = await ExecuteSqlInternalAsync(sql, token).ConfigureAwait(false);
            return new DurableResult<TEntity>(sql, results);
        }

        /// <summary>
        /// Executes the query as an asynchronous enumerable and exposes the executed SQL query.
        /// </summary>
        /// <param name="token">The cancellation token.</param>
        /// <returns>An asynchronous durable result containing both query and streaming results.</returns>
        public IAsyncDurableResult<TEntity> ExecuteAsyncEnumerableWithQuery(CancellationToken token = default)
        {
            string sql = BuildSql();
            IAsyncEnumerable<TEntity> results = ExecuteAsyncEnumerable(token);
            return new AsyncDurableResult<TEntity>(sql, results);
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

            // CTEs (Common Table Expressions) at the beginning
            if (_CteDefinitions.Count > 0)
            {
                List<string> cteDeclarations = new List<string>();
                bool hasRecursiveCte = _CteDefinitions.Any(c => c.IsRecursive);

                // Use WITH RECURSIVE if any CTE is recursive
                string withKeyword = hasRecursiveCte ? "WITH RECURSIVE" : "WITH";

                foreach (CteDefinition cte in _CteDefinitions)
                {
                    if (cte.IsRecursive)
                    {
                        string cteSql = $"{_Repository._Sanitizer.SanitizeIdentifier(cte.Name)} AS ({cte.AnchorQuery} UNION ALL {cte.RecursiveQuery})";
                        cteDeclarations.Add(cteSql);
                    }
                    else
                    {
                        string cteSql = $"{_Repository._Sanitizer.SanitizeIdentifier(cte.Name)} AS ({cte.Query})";
                        cteDeclarations.Add(cteSql);
                    }
                }

                sqlParts.Add($"{withKeyword} {string.Join(", ", cteDeclarations)}");
            }

            // Pre-calculate join result if includes are present
            if (_IncludePaths.Count > 0)
            {
                joinResult = _JoinBuilder.BuildJoinSql<TEntity>(_Repository._TableName, _IncludePaths);
                _CachedJoinResult = joinResult; // Cache for later use in Execute()
            }

            // SELECT clause with window functions
            if (!string.IsNullOrEmpty(_CustomSelectClause))
            {
                string selectClause = _CustomSelectClause;
                if (_WindowFunctions.Count > 0)
                {
                    selectClause += ", " + BuildWindowFunctionsClause();
                }
                if (_CaseExpressions.Count > 0)
                {
                    selectClause += ", " + BuildCaseExpressionsClause();
                }
                sqlParts.Add($"SELECT {(_Distinct ? "DISTINCT " : "")}{selectClause}");
            }
            else if (joinResult != null)
            {
                string selectClause = joinResult.SelectClause;
                if (_WindowFunctions.Count > 0)
                {
                    selectClause += ", " + BuildWindowFunctionsClause();
                }
                if (_CaseExpressions.Count > 0)
                {
                    selectClause += ", " + BuildCaseExpressionsClause();
                }
                sqlParts.Add($"SELECT {(_Distinct ? "DISTINCT " : "")}{selectClause}");
            }
            else
            {
                string tableName = _Repository._Sanitizer.SanitizeIdentifier(_Repository._TableName);
                string selectClause = $"{tableName}.*";
                if (_WindowFunctions.Count > 0)
                {
                    selectClause += ", " + BuildWindowFunctionsClause();
                }
                if (_CaseExpressions.Count > 0)
                {
                    selectClause += ", " + BuildCaseExpressionsClause();
                }
                sqlParts.Add($"SELECT {(_Distinct ? "DISTINCT " : "")}{selectClause}");
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

            // Handle set operations (UNION, INTERSECT, EXCEPT)
            if (_SetOperations.Count > 0)
            {
                string baseQuery = string.Join(" ", sqlParts);
                StringBuilder setOperationSql = new StringBuilder(baseQuery);

                foreach (SetOperation<TEntity> setOp in _SetOperations)
                {
                    switch (setOp.Type)
                    {
                        case SetOperationType.Union:
                            setOperationSql.Append(" UNION ");
                            break;
                        case SetOperationType.UnionAll:
                            setOperationSql.Append(" UNION ALL ");
                            break;
                        case SetOperationType.Intersect:
                            setOperationSql.Append(" INTERSECT ");
                            break;
                        case SetOperationType.Except:
                            setOperationSql.Append(" EXCEPT ");
                            break;
                    }

                    string otherQuerySql = setOp.OtherQuery.BuildSql().TrimEnd(';');
                    setOperationSql.Append($"({otherQuerySql})");
                }

                return setOperationSql.ToString();
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
            if (navigationProperty == null)
                throw new ArgumentNullException(nameof(navigationProperty));

            if (navigationProperty.Body is MemberExpression memberExpression)
            {
                List<string> propertyNames = new List<string>();
                MemberExpression? current = memberExpression;

                while (current != null)
                {
                    propertyNames.Insert(0, current.Member.Name);
                    current = current.Expression as MemberExpression;
                }

                return string.Join(".", propertyNames);
            }

            throw new InvalidOperationException("Expression must be a property access expression");
        }

        private string ExtractPropertyPath<TSource, TProperty>(Expression<Func<TSource, TProperty>> expression)
        {
            if (expression == null)
                throw new ArgumentNullException(nameof(expression));

            if (expression.Body is MemberExpression memberExpression)
            {
                List<string> propertyNames = new List<string>();
                MemberExpression? current = memberExpression;

                while (current != null)
                {
                    propertyNames.Insert(0, current.Member.Name);
                    current = current.Expression as MemberExpression;
                }

                return string.Join(".", propertyNames);
            }

            throw new InvalidOperationException("Expression must be a property access expression");
        }

        internal List<string> GetWhereClauses()
        {
            return new List<string>(_WhereClauses);
        }

        internal List<string> GetGroupByColumns()
        {
            return new List<string>(_GroupByColumns);
        }

        /// <summary>
        /// Gets the current include paths for navigation property loading.
        /// Used by PostgresGroupedQueryBuilder for entity fetching after group filtering.
        /// </summary>
        /// <returns>A list of include paths</returns>
        internal List<string> GetIncludePaths()
        {
            return new List<string>(_IncludePaths);
        }

        /// <summary>
        /// Executes the query without GROUP BY clauses for grouped query operations.
        /// Used by PostgresGroupedQueryBuilder for entity fetching after group filtering.
        /// </summary>
        /// <returns>An enumerable of query results</returns>
        internal IEnumerable<TEntity> ExecuteWithoutGroupBy()
        {
            string sql = BuildSql(false);
            return ExecuteSqlInternal(sql);
        }

        /// <summary>
        /// Asynchronously executes the query without GROUP BY clauses for grouped query operations.
        /// Used by PostgresGroupedQueryBuilder for entity fetching after group filtering.
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>An enumerable of query results</returns>
        internal async Task<IEnumerable<TEntity>> ExecuteWithoutGroupByAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            string sql = BuildSql(false);
            return await ExecuteSqlInternalAsync(sql, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Adds a window function to the query.
        /// Used by PostgresWindowedQueryBuilder to register window functions.
        /// </summary>
        /// <param name="windowFunction">The window function to add</param>
        /// <exception cref="ArgumentNullException">Thrown when windowFunction is null</exception>
        internal void AddWindowFunction(WindowFunction windowFunction)
        {
            if (windowFunction == null)
                throw new ArgumentNullException(nameof(windowFunction));

            _WindowFunctions.Add(windowFunction);
        }

        /// <summary>
        /// Adds a CASE expression to the query.
        /// Used by PostgresCaseExpressionBuilder to register CASE expressions.
        /// </summary>
        /// <param name="caseExpressionSql">The complete CASE expression SQL string to add</param>
        /// <exception cref="ArgumentNullException">Thrown when caseExpressionSql is null</exception>
        /// <exception cref="ArgumentException">Thrown when caseExpressionSql is empty or whitespace</exception>
        internal void AddCaseExpression(string caseExpressionSql)
        {
            if (string.IsNullOrWhiteSpace(caseExpressionSql))
                throw new ArgumentException("CASE expression SQL cannot be null or empty", nameof(caseExpressionSql));

            _CaseExpressions.Add(caseExpressionSql);
        }

        private string BuildWindowFunctionsClause()
        {
            List<string> windowFunctionsSql = new List<string>();

            foreach (WindowFunction wf in _WindowFunctions)
            {
                string functionSql = BuildWindowFunctionSql(wf);
                windowFunctionsSql.Add(functionSql);
            }

            return string.Join(", ", windowFunctionsSql);
        }

        private string BuildCaseExpressionsClause()
        {
            return string.Join(", ", _CaseExpressions);
        }

        private string BuildWindowFunctionSql(WindowFunction wf)
        {
            StringBuilder sql = new StringBuilder();

            // Function name and column
            if (!string.IsNullOrEmpty(wf.Column) && wf.Column != "*")
            {
                if (wf.FunctionName == "LEAD" || wf.FunctionName == "LAG")
                {
                    // LEAD/LAG with parameters
                    string columnName = _Repository._Sanitizer.SanitizeIdentifier(wf.Column);
                    sql.Append($"{wf.FunctionName}({columnName}");

                    if (wf.Parameters.ContainsKey("offset"))
                    {
                        sql.Append($", {wf.Parameters["offset"]}");
                    }
                    if (wf.Parameters.ContainsKey("default"))
                    {
                        sql.Append($", {FormatParameterValue(wf.Parameters["default"])}");
                    }
                    sql.Append(")");
                }
                else if (wf.FunctionName == "NTH_VALUE")
                {
                    // NTH_VALUE with position parameter
                    string columnName = _Repository._Sanitizer.SanitizeIdentifier(wf.Column);
                    int n = (int)wf.Parameters["n"];
                    sql.Append($"{wf.FunctionName}({columnName}, {n})");
                }
                else
                {
                    // Regular functions with column
                    string columnName = _Repository._Sanitizer.SanitizeIdentifier(wf.Column);
                    sql.Append($"{wf.FunctionName}({columnName})");
                }
            }
            else if (wf.FunctionName == "ROW_NUMBER" || wf.FunctionName == "RANK" || wf.FunctionName == "DENSE_RANK")
            {
                // Ranking functions without parameters
                sql.Append($"{wf.FunctionName}()");
            }
            else
            {
                // COUNT(*) and similar
                sql.Append($"{wf.FunctionName}(*)");
            }

            // OVER clause
            sql.Append(" OVER (");

            List<string> overClauses = new List<string>();

            // PARTITION BY
            if (wf.PartitionByColumns.Count > 0)
            {
                List<string> partitionColumns = wf.PartitionByColumns
                    .Select(col => _Repository._Sanitizer.SanitizeIdentifier(col))
                    .ToList();
                overClauses.Add($"PARTITION BY {string.Join(", ", partitionColumns)}");
            }

            // ORDER BY
            if (wf.OrderByColumns.Count > 0)
            {
                List<string> orderColumns = wf.OrderByColumns
                    .Select(col => $"{_Repository._Sanitizer.SanitizeIdentifier(col.Column)} {(col.Ascending ? "ASC" : "DESC")}")
                    .ToList();
                overClauses.Add($"ORDER BY {string.Join(", ", orderColumns)}");
            }

            // Frame specification (only add if bounds are explicitly set)
            if (wf.Frame.StartBound != null && wf.Frame.EndBound != null &&
                (wf.Frame.StartBound.Type != WindowFrameBoundType.CurrentRow ||
                 wf.Frame.EndBound.Type != WindowFrameBoundType.CurrentRow))
            {
                overClauses.Add(BuildWindowFrameClause(wf.Frame));
            }

            sql.Append(string.Join(" ", overClauses));
            sql.Append(")");

            // Alias
            if (!string.IsNullOrEmpty(wf.Alias))
            {
                sql.Append($" AS {_Repository._Sanitizer.SanitizeIdentifier(wf.Alias)}");
            }

            return sql.ToString();
        }

        private string BuildWindowFrameClause(WindowFrame frame)
        {
            string frameType = frame.Type == WindowFrameType.Rows ? "ROWS" : "RANGE";

            if (frame.StartBound != null && frame.EndBound != null)
            {
                string startBound = FormatWindowFrameBound(frame.StartBound);
                string endBound = FormatWindowFrameBound(frame.EndBound);
                return $"{frameType} BETWEEN {startBound} AND {endBound}";
            }
            else if (frame.StartBound != null)
            {
                string startBound = FormatWindowFrameBound(frame.StartBound);
                return $"{frameType} {startBound}";
            }

            return "";
        }

        private string FormatWindowFrameBound(WindowFrameBound bound)
        {
            return bound.Type switch
            {
                WindowFrameBoundType.UnboundedPreceding => "UNBOUNDED PRECEDING",
                WindowFrameBoundType.UnboundedFollowing => "UNBOUNDED FOLLOWING",
                WindowFrameBoundType.CurrentRow => "CURRENT ROW",
                WindowFrameBoundType.Preceding => $"{bound.Offset} PRECEDING",
                WindowFrameBoundType.Following => $"{bound.Offset} FOLLOWING",
                _ => "CURRENT ROW"
            };
        }

        private string FormatParameterValue(object value)
        {
            if (value == null) return "NULL";
            if (value is string str) return $"'{str.Replace("'", "''")}'";
            if (value is DateTime dt) return $"'{dt:yyyy-MM-dd HH:mm:ss}'";
            if (value is bool b) return b ? "true" : "false";
            return value.ToString() ?? "NULL";
        }

        #endregion
    }
}