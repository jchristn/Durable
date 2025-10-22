namespace Durable.SqlServer
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
    using Microsoft.Data.SqlClient;

    /// <summary>
    /// SQL Server-specific implementation of IQueryBuilder that provides fluent query building capabilities for SQL Server databases.
    /// Supports advanced features like CTEs, window functions, set operations, and includes.
    /// </summary>
    /// <typeparam name="TEntity">The entity type being queried</typeparam>
    public class SqlServerQueryBuilder<TEntity> : IQueryBuilder<TEntity> where TEntity : class, new()
    {
        #region Public-Members

        /// <summary>
        /// Gets the SQL query that will be or was executed.
        /// </summary>
        public string Query => BuildSql();

        #endregion

        #region Private-Members

        private readonly SqlServerRepository<TEntity> _Repository;
        private readonly ITransaction? _Transaction;
        private readonly List<string> _WhereClauses = new List<string>();
        private readonly List<string> _OrderByClauses = new List<string>();
        private readonly List<string> _IncludePaths = new List<string>();
        private SqlServerJoinBuilder.SqlServerJoinResult? _CachedJoinResult;
        private readonly List<string> _GroupByColumns = new List<string>();
        private readonly List<string> _HavingClauses = new List<string>();
        private readonly SqlServerExpressionParser<TEntity> _ExpressionParser;
        private readonly SqlServerJoinBuilder _JoinBuilder;
        private readonly SqlServerEntityMapper<TEntity> _EntityMapper;
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
        /// Initializes a new instance of the SqlServerQueryBuilder class.
        /// </summary>
        /// <param name="repository">The MySQL repository instance for data access operations</param>
        /// <param name="transaction">Optional transaction to execute queries within. Default is null</param>
        /// <exception cref="ArgumentNullException">Thrown when repository is null</exception>
        public SqlServerQueryBuilder(SqlServerRepository<TEntity> repository, ITransaction? transaction = null)
        {
            _Repository = repository ?? throw new ArgumentNullException(nameof(repository));
            _Transaction = transaction;
            _ExpressionParser = new SqlServerExpressionParser<TEntity>(_Repository._ColumnMappings, _Repository._Sanitizer);
            _JoinBuilder = new SqlServerJoinBuilder(_Repository._Sanitizer);
            _EntityMapper = new SqlServerEntityMapper<TEntity>(_Repository._DataTypeConverter, _Repository._ColumnMappings, _Repository._Sanitizer);
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
        /// Executes the query and returns the results.
        /// </summary>
        /// <returns>The query results as an enumerable sequence.</returns>
        public IEnumerable<TEntity> Execute()
        {
            string sql = BuildSql();
            return ExecuteSqlInternal(sql);
        }

        /// <summary>
        /// Executes the query without GROUP BY clause and returns the results.
        /// Used by SqlServerGroupedQueryBuilder for entity fetching after group filtering.
        /// </summary>
        /// <returns>An enumerable of query results</returns>
        internal IEnumerable<TEntity> ExecuteWithoutGroupBy()
        {
            string sql = BuildSql(false);
            return ExecuteSqlInternal(sql);
        }

        /// <summary>
        /// Builds and returns the SQL query string for debugging purposes.
        /// </summary>
        /// <returns>The SQL query string.</returns>
        public string BuildSql()
        {
            return BuildSql(true);
        }

        /// <summary>
        /// Builds the SQL query string from the current query configuration.
        /// </summary>
        /// <param name="includeGroupBy">Whether to include GROUP BY clause in the generated SQL.</param>
        /// <returns>The generated SQL query string.</returns>
        public string BuildSql(bool includeGroupBy)
        {
            List<string> sqlParts = new List<string>();
            SqlServerJoinBuilder.SqlServerJoinResult? joinResult = null;

            // Pre-calculate join result if includes are present
            if (_IncludePaths.Count > 0)
            {
                joinResult = _JoinBuilder.BuildJoinSql<TEntity>(_Repository._TableName, _IncludePaths);
                _CachedJoinResult = joinResult; // Cache for later use in Execute()
            }

            // Handle CTEs first
            if (_CteDefinitions.Count > 0)
            {
                StringBuilder cteBuilder = new StringBuilder();
                cteBuilder.Append("WITH ");

                if (_CteDefinitions.Any(c => c.IsRecursive))
                {
                    cteBuilder.Append("RECURSIVE ");
                }

                List<string> cteStrings = new List<string>();
                foreach (CteDefinition cte in _CteDefinitions)
                {
                    if (cte.IsRecursive)
                    {
                        cteStrings.Add($"[{cte.Name}] AS ({cte.AnchorQuery} UNION ALL {cte.RecursiveQuery})");
                    }
                    else
                    {
                        cteStrings.Add($"[{cte.Name}] AS ({cte.Query})");
                    }
                }

                cteBuilder.Append(string.Join(", ", cteStrings));
                sqlParts.Add(cteBuilder.ToString());
            }

            // SELECT clause
            if (!string.IsNullOrWhiteSpace(_CustomSelectClause))
            {
                // Use custom SELECT clause
                if (_Distinct)
                    sqlParts.Add($"SELECT DISTINCT {_CustomSelectClause}");
                else
                    sqlParts.Add($"SELECT {_CustomSelectClause}");
            }
            else if (joinResult != null)
            {
                // Use the SELECT clause generated by the JOIN builder for includes
                if (_Distinct)
                    sqlParts.Add($"SELECT DISTINCT {joinResult.SelectClause}");
                else
                    sqlParts.Add($"SELECT {joinResult.SelectClause}");
            }
            else if (_WindowFunctions.Count > 0 || _CaseExpressions.Count > 0)
            {
                List<string> selectParts = new List<string>();
                selectParts.Add("t0.*");

                foreach (WindowFunction windowFunc in _WindowFunctions)
                {
                    selectParts.Add(BuildWindowFunctionSql(windowFunc));
                }

                foreach (string caseExpr in _CaseExpressions)
                {
                    selectParts.Add(caseExpr);
                }

                if (_Distinct)
                    sqlParts.Add($"SELECT DISTINCT {string.Join(", ", selectParts)}");
                else
                    sqlParts.Add($"SELECT {string.Join(", ", selectParts)}");
            }
            else
            {
                if (_Distinct)
                    sqlParts.Add("SELECT DISTINCT *");
                else
                    sqlParts.Add("SELECT *");
            }

            // FROM clause
            if (!string.IsNullOrWhiteSpace(_CustomFromClause))
            {
                // Use custom FROM clause
                sqlParts.Add($"FROM {_CustomFromClause}");
            }
            else if (_WindowFunctions.Count > 0 || _CaseExpressions.Count > 0 || _IncludePaths.Count > 0)
            {
                sqlParts.Add($"FROM [{_Repository._TableName}] t0");
            }
            else
            {
                sqlParts.Add($"FROM [{_Repository._TableName}]");
            }

            // JOIN clauses (includes and custom joins)
            if (joinResult != null && !string.IsNullOrWhiteSpace(joinResult.JoinClause))
            {
                // Add JOIN clauses for Include operations
                sqlParts.Add(joinResult.JoinClause);
            }

            // Custom JOIN clauses
            if (_CustomJoinClauses.Any())
            {
                foreach (string joinClause in _CustomJoinClauses)
                {
                    sqlParts.Add(joinClause);
                }
            }

            // WHERE clause
            if (_WhereClauses.Any())
            {
                sqlParts.Add($"WHERE {string.Join(" AND ", _WhereClauses)}");
            }

            // GROUP BY clause
            if (includeGroupBy && _GroupByColumns.Any())
            {
                sqlParts.Add($"GROUP BY {string.Join(", ", _GroupByColumns.Select(c => $"[{c.Replace("]", "]]")}]"))}");
            }

            // HAVING clause
            if (_HavingClauses.Any())
            {
                sqlParts.Add($"HAVING {string.Join(" AND ", _HavingClauses)}");
            }

            // ORDER BY clause
            if (_OrderByClauses.Any())
            {
                sqlParts.Add($"ORDER BY {string.Join(", ", _OrderByClauses)}");
            }

            // OFFSET/FETCH clause (SQL Server pagination)
            if (_SkipCount.HasValue || _TakeCount.HasValue)
            {
                // SQL Server requires ORDER BY when using OFFSET/FETCH
                // However, for simple Take(1) queries (like EXISTS), we can use TOP instead
                if (!_OrderByClauses.Any())
                {
                    // If it's just Take() without Skip(), we can use TOP in the SELECT clause instead
                    // This is handled by rewriting the query to use TOP
                    if (!_SkipCount.HasValue && _TakeCount.HasValue)
                    {
                        // For Take() without ORDER BY, use TOP instead of OFFSET/FETCH
                        // We'll need to modify the SELECT clause instead
                        // This is a workaround for EXISTS-style queries
                        // Note: The SELECT clause was already built above, so we'll use OFFSET 0 with a dummy ORDER BY

                        // Add a dummy ORDER BY (SELECT NULL) to satisfy SQL Server's requirement
                        sqlParts.Add("ORDER BY (SELECT NULL)");
                        sqlParts.Add($"OFFSET 0 ROWS");
                        sqlParts.Add($"FETCH NEXT {_TakeCount.Value} ROWS ONLY");
                    }
                    else
                    {
                        throw new InvalidOperationException("ORDER BY is required when using Skip() or Take() in SQL Server queries");
                    }
                }
                else
                {
                    if (_SkipCount.HasValue)
                    {
                        sqlParts.Add($"OFFSET {_SkipCount.Value} ROWS");

                        if (_TakeCount.HasValue)
                        {
                        sqlParts.Add($"FETCH NEXT {_TakeCount.Value} ROWS ONLY");
                        }
                    }
                    else if (_TakeCount.HasValue)
                    {
                        // Take without Skip requires OFFSET 0
                        sqlParts.Add($"OFFSET 0 ROWS");
                        sqlParts.Add($"FETCH NEXT {_TakeCount.Value} ROWS ONLY");
                    }
                }
            }

            // Handle set operations
            if (_SetOperations.Count > 0)
            {
                StringBuilder setOperationSql = new StringBuilder();

                // Extract CTE definitions (WITH clause) if present - they must be at the start, not wrapped
                string cteClause = string.Empty;
                List<string> queryParts = new List<string>(sqlParts);
                if (queryParts.Count > 0 && queryParts[0].StartsWith("WITH "))
                {
                    cteClause = queryParts[0];
                    queryParts.RemoveAt(0);
                }

                // Add CTE at the very beginning if present
                if (!string.IsNullOrEmpty(cteClause))
                {
                    setOperationSql.Append(cteClause);
                    setOperationSql.Append(" ");
                }

                // Wrap the main query (without CTE) in parentheses
                setOperationSql.Append("(");
                setOperationSql.Append(string.Join(" ", queryParts));
                setOperationSql.Append(")");

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
                            // SQL Server has native INTERSECT support
                            setOperationSql.Append(" INTERSECT ");
                            break;
                        case SetOperationType.Except:
                            // SQL Server has native EXCEPT support
                            setOperationSql.Append(" EXCEPT ");
                            break;
                    }
                    setOperationSql.Append("(");
                    setOperationSql.Append(setOp.OtherQuery.BuildSql());
                    setOperationSql.Append(")");
                }

                return setOperationSql.ToString();
            }

            return string.Join(" ", sqlParts);
        }

        #endregion

        #region Not-Yet-Implemented

        // These methods will be implemented as the MySQL provider matures

        /// <summary>
        /// Projects the query results to a different type using the specified selector expression.
        /// </summary>
        /// <typeparam name="TResult">The type to project the results to.</typeparam>
        /// <param name="selector">An expression that specifies the projection.</param>
        /// <returns>A query builder for the projected type.</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null.</exception>
        public IQueryBuilder<TResult> Select<TResult>(Expression<Func<TEntity, TResult>> selector) where TResult : class, new()
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            return new SqlServerProjectedQueryBuilder<TEntity, TResult>(_Repository, selector, this, null);
        }

        /// <summary>
        /// Specifies a related entity to include in the query results.
        /// </summary>
        /// <typeparam name="TProperty">The type of the navigation property.</typeparam>
        /// <param name="navigationProperty">An expression representing the navigation property to include.</param>
        /// <returns>The query builder for further configuration.</returns>
        /// <exception cref="ArgumentNullException">Thrown when navigationProperty is null.</exception>
        public IQueryBuilder<TEntity> Include<TProperty>(Expression<Func<TEntity, TProperty>> navigationProperty)
        {
            if (navigationProperty == null) throw new ArgumentNullException(nameof(navigationProperty));

            string propertyPath = ExtractPropertyPath(navigationProperty);
            _IncludePaths.Add(propertyPath);
            return this;
        }

        /// <summary>
        /// Specifies an additional related entity to include in the query results following a previous Include.
        /// </summary>
        /// <typeparam name="TPreviousProperty">The type of the previous navigation property.</typeparam>
        /// <typeparam name="TProperty">The type of the navigation property to include.</typeparam>
        /// <param name="navigationProperty">An expression representing the navigation property to include.</param>
        /// <returns>The query builder for further configuration.</returns>
        /// <exception cref="ArgumentNullException">Thrown when navigationProperty is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when ThenInclude is called without a prior Include.</exception>
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
        /// Groups the query results by the specified key selector expression.
        /// </summary>
        /// <typeparam name="TKey">The type of the grouping key.</typeparam>
        /// <param name="keySelector">An expression that specifies the property to group by.</param>
        /// <returns>A grouped query builder for further configuration.</returns>
        /// <exception cref="ArgumentNullException">Thrown when keySelector is null.</exception>
        public IGroupedQueryBuilder<TEntity, TKey> GroupBy<TKey>(Expression<Func<TEntity, TKey>> keySelector)
        {
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));

            // Try to extract column name from the key selector expression
            // For complex expressions (anonymous types, computed fields), we'll skip SQL GROUP BY
            // and let SqlServerGroupedQueryBuilder handle it in-memory
            try
            {
                string groupColumn = GetColumnFromExpression(keySelector.Body);
                // Remove square brackets if present to store raw column name
                string rawColumn = groupColumn.Trim('[', ']');
                _GroupByColumns.Add(rawColumn);
            }
            catch (ArgumentException)
            {
                // Complex expression that can't be translated to SQL column
                // SqlServerGroupedQueryBuilder will handle this with in-memory grouping
                // Don't add anything to _GroupByColumns so SQL GROUP BY is skipped
            }

            // Create advanced entity mapper for enhanced type handling
            SqlServerEntityMapper<TEntity> entityMapper = new SqlServerEntityMapper<TEntity>(
                _Repository._DataTypeConverter,
                _Repository._ColumnMappings,
                _Repository._Sanitizer);

            // Return the advanced grouped query builder with full EntityMapper integration
            return new SqlServerGroupedQueryBuilder<TEntity, TKey>(
                _Repository,
                this,
                keySelector,
                entityMapper,
                _Repository._DataTypeConverter,
                _Repository._Sanitizer);
        }

        /// <summary>
        /// Adds a HAVING clause to filter grouped results. Can only be used with GROUP BY.
        /// </summary>
        /// <param name="predicate">Expression representing the HAVING condition</param>
        /// <returns>The current query builder for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when used without GROUP BY</exception>
        public IQueryBuilder<TEntity> Having(Expression<Func<TEntity, bool>> predicate)
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));

            if (_GroupByColumns.Count == 0)
            {
                throw new InvalidOperationException("HAVING clause can only be used with GROUP BY");
            }

            string havingClause = _ExpressionParser.ParseExpression(predicate.Body);
            _HavingClauses.Add(havingClause);
            return this;
        }

        /// <summary>
        /// Performs a UNION operation with another query, combining results and removing duplicates.
        /// </summary>
        /// <param name="other">The other query builder to union with</param>
        /// <returns>The current query builder instance for method chaining</returns>
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
        /// <param name="other">The other query builder to union with</param>
        /// <returns>The current query builder instance for method chaining</returns>
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
        /// Implemented using INNER JOIN since MySQL does not natively support INTERSECT.
        /// </summary>
        /// <param name="other">The other query builder to intersect with</param>
        /// <returns>The current query builder instance for method chaining</returns>
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
        /// Implemented using LEFT JOIN with NULL check since MySQL does not natively support EXCEPT.
        /// </summary>
        /// <param name="other">The other query builder to except against</param>
        /// <returns>The current query builder instance for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when other is null</exception>
        public IQueryBuilder<TEntity> Except(IQueryBuilder<TEntity> other)
        {
            if (other == null)
                throw new ArgumentNullException(nameof(other));

            _SetOperations.Add(new SetOperation<TEntity>(SetOperationType.Except, other));
            return this;
        }

        /// <summary>
        /// Adds a WHERE IN clause to the query using a subquery to check if the specified property value exists in the subquery results.
        /// </summary>
        /// <typeparam name="TKey">The type of the property to check</typeparam>
        /// <param name="keySelector">Lambda expression selecting the property to check</param>
        /// <param name="subquery">The subquery that returns values to check against</param>
        /// <returns>The current query builder instance for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when keySelector or subquery is null</exception>
        public IQueryBuilder<TEntity> WhereIn<TKey>(Expression<Func<TEntity, TKey>> keySelector, IQueryBuilder<TKey> subquery) where TKey : class, new()
        {
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));
            if (subquery == null)
                throw new ArgumentNullException(nameof(subquery));

            string column = GetColumnFromExpression(keySelector.Body);
            string subquerySql = subquery.BuildSql().TrimEnd(';');
            _WhereClauses.Add($"[{column}] IN ({subquerySql})");
            return this;
        }

        /// <summary>
        /// Adds a WHERE NOT IN clause to the query using a subquery to check if the specified property value does not exist in the subquery results.
        /// </summary>
        /// <typeparam name="TKey">The type of the property to check</typeparam>
        /// <param name="keySelector">Lambda expression selecting the property to check</param>
        /// <param name="subquery">The subquery that returns values to check against</param>
        /// <returns>The current query builder instance for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when keySelector or subquery is null</exception>
        public IQueryBuilder<TEntity> WhereNotIn<TKey>(Expression<Func<TEntity, TKey>> keySelector, IQueryBuilder<TKey> subquery) where TKey : class, new()
        {
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));
            if (subquery == null)
                throw new ArgumentNullException(nameof(subquery));

            string column = GetColumnFromExpression(keySelector.Body);
            string subquerySql = subquery.BuildSql().TrimEnd(';');
            _WhereClauses.Add($"[{column}] NOT IN ({subquerySql})");
            return this;
        }

        /// <summary>
        /// Adds a WHERE IN clause to the query using raw SQL for the subquery.
        /// </summary>
        /// <typeparam name="TKey">The type of the property to check</typeparam>
        /// <param name="keySelector">Lambda expression selecting the property to check</param>
        /// <param name="subquerySql">The raw SQL subquery string that returns values to check against</param>
        /// <returns>The current query builder instance for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when keySelector is null</exception>
        /// <exception cref="ArgumentException">Thrown when subquerySql is null, empty, or whitespace</exception>
        public IQueryBuilder<TEntity> WhereInRaw<TKey>(Expression<Func<TEntity, TKey>> keySelector, string subquerySql)
        {
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));
            if (string.IsNullOrWhiteSpace(subquerySql))
                throw new ArgumentException("Subquery SQL cannot be null or empty", nameof(subquerySql));

            string column = GetColumnFromExpression(keySelector.Body);
            _WhereClauses.Add($"[{column}] IN ({subquerySql})");
            return this;
        }

        /// <summary>
        /// Adds a WHERE NOT IN clause to the query using raw SQL for the subquery.
        /// </summary>
        /// <typeparam name="TKey">The type of the property to check</typeparam>
        /// <param name="keySelector">Lambda expression selecting the property to check</param>
        /// <param name="subquerySql">The raw SQL subquery string that returns values to check against</param>
        /// <returns>The current query builder instance for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when keySelector is null</exception>
        /// <exception cref="ArgumentException">Thrown when subquerySql is null, empty, or whitespace</exception>
        public IQueryBuilder<TEntity> WhereNotInRaw<TKey>(Expression<Func<TEntity, TKey>> keySelector, string subquerySql)
        {
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));
            if (string.IsNullOrWhiteSpace(subquerySql))
                throw new ArgumentException("Subquery SQL cannot be null or empty", nameof(subquerySql));

            string column = GetColumnFromExpression(keySelector.Body);
            _WhereClauses.Add($"[{column}] NOT IN ({subquerySql})");
            return this;
        }

        /// <summary>
        /// Adds a WHERE EXISTS clause to the query using a subquery.
        /// </summary>
        /// <typeparam name="TOther">The entity type of the subquery</typeparam>
        /// <param name="subquery">The subquery to check for existence</param>
        /// <returns>The current query builder instance for method chaining</returns>
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
        /// Adds a WHERE NOT EXISTS clause to the query using a subquery.
        /// </summary>
        /// <typeparam name="TOther">The entity type of the subquery</typeparam>
        /// <param name="subquery">The subquery to check for non-existence</param>
        /// <returns>The current query builder instance for method chaining</returns>
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
        /// Adds a window function to the query with optional partitioning and ordering.
        /// </summary>
        /// <param name="functionName">The name of the window function (e.g., ROW_NUMBER, RANK, LAG)</param>
        /// <param name="partitionBy">Optional partition clause for the window function. Default is null</param>
        /// <param name="orderBy">Optional order clause for the window function. Default is null</param>
        /// <returns>A windowed query builder for additional window function configuration</returns>
        /// <exception cref="ArgumentNullException">Thrown when functionName is null</exception>
        /// <exception cref="ArgumentException">Thrown when functionName is empty or whitespace</exception>
        public IWindowedQueryBuilder<TEntity> WithWindowFunction(string functionName, string? partitionBy = null, string? orderBy = null)
        {
            if (string.IsNullOrWhiteSpace(functionName))
                throw new ArgumentException("Function name cannot be null or empty", nameof(functionName));

            return new SqlServerWindowedQueryBuilder<TEntity>(this, _Repository, _Transaction, functionName, partitionBy, orderBy);
        }

        /// <summary>
        /// Adds a configured window function to the query. This method is called internally by SqlServerWindowedQueryBuilder.
        /// </summary>
        /// <param name="windowFunction">The window function configuration to add</param>
        /// <exception cref="ArgumentNullException">Thrown when windowFunction is null</exception>
        internal void AddWindowFunction(WindowFunction windowFunction)
        {
            if (windowFunction == null)
                throw new ArgumentNullException(nameof(windowFunction));

            _WindowFunctions.Add(windowFunction);
        }

        /// <summary>
        /// Adds a CASE expression to the SELECT clause. This method is called internally by SqlServerCaseExpressionBuilder.
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

        /// <summary>
        /// Adds a Common Table Expression (CTE) to the query.
        /// </summary>
        /// <param name="cteName">The name for the CTE</param>
        /// <param name="cteQuery">The SQL query defining the CTE</param>
        /// <returns>The current query builder instance for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when cteName or cteQuery is null</exception>
        /// <exception cref="ArgumentException">Thrown when cteName or cteQuery is empty or whitespace</exception>
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
        /// </summary>
        /// <param name="cteName">The name for the recursive CTE</param>
        /// <param name="anchorQuery">The anchor (base) query for the recursive CTE</param>
        /// <param name="recursiveQuery">The recursive query that references the CTE</param>
        /// <returns>The current query builder instance for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when cteName, anchorQuery, or recursiveQuery is null</exception>
        /// <exception cref="ArgumentException">Thrown when cteName, anchorQuery, or recursiveQuery is empty or whitespace</exception>
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
        /// <param name="sql">Raw SQL string for the WHERE condition</param>
        /// <param name="parameters">Optional parameters to format into the SQL string</param>
        /// <returns>The current query builder instance for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null</exception>
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
        /// Sets a custom raw SQL SELECT clause, overriding the default column selection.
        /// </summary>
        /// <param name="sql">Raw SQL string for the SELECT clause</param>
        /// <returns>The current query builder instance for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null</exception>
        /// <exception cref="ArgumentException">Thrown when sql is empty or whitespace</exception>
        public IQueryBuilder<TEntity> SelectRaw(string sql)
        {
            if (string.IsNullOrWhiteSpace(sql))
                throw new ArgumentException("Raw SQL cannot be null or empty", nameof(sql));

            _CustomSelectClause = sql;
            return this;
        }

        /// <summary>
        /// Sets a custom raw SQL FROM clause, overriding the default table name.
        /// </summary>
        /// <param name="sql">Raw SQL string for the FROM clause</param>
        /// <returns>The current query builder instance for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null</exception>
        /// <exception cref="ArgumentException">Thrown when sql is empty or whitespace</exception>
        public IQueryBuilder<TEntity> FromRaw(string sql)
        {
            if (string.IsNullOrWhiteSpace(sql))
                throw new ArgumentException("Raw SQL cannot be null or empty", nameof(sql));

            _CustomFromClause = sql;
            return this;
        }

        /// <summary>
        /// Adds a custom raw SQL JOIN clause to the query.
        /// </summary>
        /// <param name="sql">Raw SQL string for the JOIN clause</param>
        /// <returns>The current query builder instance for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null</exception>
        /// <exception cref="ArgumentException">Thrown when sql is empty or whitespace</exception>
        public IQueryBuilder<TEntity> JoinRaw(string sql)
        {
            if (string.IsNullOrWhiteSpace(sql))
                throw new ArgumentException("Raw SQL cannot be null or empty", nameof(sql));

            _CustomJoinClauses.Add(sql);
            return this;
        }

        /// <summary>
        /// Creates a CASE expression builder for conditional logic in SELECT statements.
        /// </summary>
        /// <returns>A CASE expression builder instance for building conditional SQL expressions</returns>
        public ICaseExpressionBuilder<TEntity> SelectCase()
        {
            return new SqlServerCaseExpressionBuilder<TEntity>(this, _Repository);
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
        /// Executes the query without GROUP BY clause and returns the results asynchronously.
        /// Used by SqlServerGroupedQueryBuilder for entity fetching after group filtering.
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

        /// <summary>
        /// Executes the query and returns both the results and the SQL query string.
        /// </summary>
        /// <returns>A durable result containing both the query results and the SQL string.</returns>
        public IDurableResult<TEntity> ExecuteWithQuery()
        {
            string sql = BuildSql();
            IEnumerable<TEntity> results = ExecuteSqlInternal(sql);
            return new DurableResult<TEntity>(sql, results);
        }

        /// <summary>
        /// Asynchronously executes the query and returns both the results and the SQL query string.
        /// </summary>
        /// <param name="token">Cancellation token for the async operation.</param>
        /// <returns>A task containing a durable result with both the query results and the SQL string.</returns>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
        public async Task<IDurableResult<TEntity>> ExecuteWithQueryAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            string sql = BuildSql();
            IEnumerable<TEntity> results = await ExecuteSqlInternalAsync(sql, token).ConfigureAwait(false);
            return new DurableResult<TEntity>(sql, results);
        }

        /// <summary>
        /// Executes the query as an async enumerable and returns both the results and the SQL query string.
        /// </summary>
        /// <param name="token">Cancellation token for the async operation.</param>
        /// <returns>An async durable result containing both the query results stream and the SQL string.</returns>
        public IAsyncDurableResult<TEntity> ExecuteAsyncEnumerableWithQuery(CancellationToken token = default)
        {
            string sql = BuildSql();
            IAsyncEnumerable<TEntity> results = ExecuteAsyncEnumerable(token);
            return new AsyncDurableResult<TEntity>(sql, results);
        }

        /// <summary>
        /// Counts the number of entities matching the query.
        /// </summary>
        /// <returns>The count of matching entities.</returns>
        public long Count()
        {
            string sql = BuildCountSql();
            return ExecuteScalarInternal<long>(sql);
        }

        /// <summary>
        /// Asynchronously counts the number of entities matching the query.
        /// </summary>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the count of matching entities.</returns>
        public async Task<long> CountAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            string sql = BuildCountSql();
            return await ExecuteScalarInternalAsync<long>(sql, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Calculates the sum of a numeric property for entities matching the query.
        /// </summary>
        /// <typeparam name="TProperty">The type of the property to sum.</typeparam>
        /// <param name="selector">The expression to select the property to sum.</param>
        /// <returns>The sum of the property values.</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null.</exception>
        public decimal Sum<TProperty>(Expression<Func<TEntity, TProperty>> selector)
        {
            if (selector == null) throw new ArgumentNullException(nameof(selector));

            string column = _ExpressionParser.GetColumnFromExpression(selector.Body);
            string sql = BuildAggregateSql("SUM", column);
            return ExecuteScalarInternal<decimal>(sql);
        }

        /// <summary>
        /// Asynchronously calculates the sum of a numeric property for entities matching the query.
        /// </summary>
        /// <typeparam name="TProperty">The type of the property to sum.</typeparam>
        /// <param name="selector">The expression to select the property to sum.</param>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the sum of the property values.</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
        public async Task<decimal> SumAsync<TProperty>(Expression<Func<TEntity, TProperty>> selector, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            if (selector == null) throw new ArgumentNullException(nameof(selector));

            string column = _ExpressionParser.GetColumnFromExpression(selector.Body);
            string sql = BuildAggregateSql("SUM", column);
            return await ExecuteScalarInternalAsync<decimal>(sql, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Calculates the average of a numeric property for entities matching the query.
        /// </summary>
        /// <typeparam name="TProperty">The type of the property to average.</typeparam>
        /// <param name="selector">The expression to select the property to average.</param>
        /// <returns>The average of the property values.</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null.</exception>
        public decimal Average<TProperty>(Expression<Func<TEntity, TProperty>> selector)
        {
            if (selector == null) throw new ArgumentNullException(nameof(selector));

            string column = _ExpressionParser.GetColumnFromExpression(selector.Body);
            string sql = BuildAggregateSql("AVG", column);
            return ExecuteScalarInternal<decimal>(sql);
        }

        /// <summary>
        /// Asynchronously calculates the average of a numeric property for entities matching the query.
        /// </summary>
        /// <typeparam name="TProperty">The type of the property to average.</typeparam>
        /// <param name="selector">The expression to select the property to average.</param>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the average of the property values.</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
        public async Task<decimal> AverageAsync<TProperty>(Expression<Func<TEntity, TProperty>> selector, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            if (selector == null) throw new ArgumentNullException(nameof(selector));

            string column = _ExpressionParser.GetColumnFromExpression(selector.Body);
            string sql = BuildAggregateSql("AVG", column);
            return await ExecuteScalarInternalAsync<decimal>(sql, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Finds the minimum value of a property for entities matching the query.
        /// </summary>
        /// <typeparam name="TProperty">The type of the property.</typeparam>
        /// <param name="selector">The expression to select the property.</param>
        /// <returns>The minimum property value.</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null.</exception>
        public TProperty Min<TProperty>(Expression<Func<TEntity, TProperty>> selector)
        {
            if (selector == null) throw new ArgumentNullException(nameof(selector));

            string column = _ExpressionParser.GetColumnFromExpression(selector.Body);
            string sql = BuildAggregateSql("MIN", column);
            return ExecuteScalarInternal<TProperty>(sql);
        }

        /// <summary>
        /// Asynchronously finds the minimum value of a property for entities matching the query.
        /// </summary>
        /// <typeparam name="TProperty">The type of the property.</typeparam>
        /// <param name="selector">The expression to select the property.</param>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the minimum property value.</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
        public async Task<TProperty> MinAsync<TProperty>(Expression<Func<TEntity, TProperty>> selector, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            if (selector == null) throw new ArgumentNullException(nameof(selector));

            string column = _ExpressionParser.GetColumnFromExpression(selector.Body);
            string sql = BuildAggregateSql("MIN", column);
            return await ExecuteScalarInternalAsync<TProperty>(sql, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Finds the maximum value of a property for entities matching the query.
        /// </summary>
        /// <typeparam name="TProperty">The type of the property.</typeparam>
        /// <param name="selector">The expression to select the property.</param>
        /// <returns>The maximum property value.</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null.</exception>
        public TProperty Max<TProperty>(Expression<Func<TEntity, TProperty>> selector)
        {
            if (selector == null) throw new ArgumentNullException(nameof(selector));

            string column = _ExpressionParser.GetColumnFromExpression(selector.Body);
            string sql = BuildAggregateSql("MAX", column);
            return ExecuteScalarInternal<TProperty>(sql);
        }

        /// <summary>
        /// Asynchronously finds the maximum value of a property for entities matching the query.
        /// </summary>
        /// <typeparam name="TProperty">The type of the property.</typeparam>
        /// <param name="selector">The expression to select the property.</param>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the maximum property value.</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
        public async Task<TProperty> MaxAsync<TProperty>(Expression<Func<TEntity, TProperty>> selector, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            if (selector == null) throw new ArgumentNullException(nameof(selector));

            string column = _ExpressionParser.GetColumnFromExpression(selector.Body);
            string sql = BuildAggregateSql("MAX", column);
            return await ExecuteScalarInternalAsync<TProperty>(sql, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Deletes all entities matching the query.
        /// </summary>
        /// <returns>The number of entities deleted.</returns>
        public int Delete()
        {
            string sql = BuildDeleteSql();
            return ExecuteNonQueryInternal(sql);
        }

        /// <summary>
        /// Asynchronously deletes all entities matching the query.
        /// </summary>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the number of entities deleted.</returns>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
        public async Task<int> DeleteAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            string sql = BuildDeleteSql();
            return await ExecuteNonQueryInternalAsync(sql, token).ConfigureAwait(false);
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
                DbConnection connection = _Repository._ConnectionFactory.GetConnection();
                try
                {
                    return ExecuteWithConnection(connection, sql);
                }
                finally
                {
                    _Repository._ConnectionFactory.ReturnConnection(connection);
                }
            }
        }

        private IEnumerable<TEntity> ExecuteWithConnection(DbConnection connection, string sql)
        {
            List<TEntity> results = new List<TEntity>();

            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }

            using SqlCommand command = (SqlCommand)connection.CreateCommand();
            command.CommandText = sql;

            if (_Transaction != null)
            {
                command.Transaction = (SqlTransaction)_Transaction.Transaction;
            }

            // Capture SQL if enabled
            if (_Repository.CaptureSql)
            {
                _Repository.SetLastExecutedSql(sql);
            }

            try
            {
                using SqlDataReader reader = (SqlDataReader)command.ExecuteReader();

                // Check if we have includes that require advanced mapping
                if (_IncludePaths.Count > 0)
                {
                    // Use advanced EntityMapper for complex scenarios with navigation properties
                    SqlServerJoinBuilder.SqlServerJoinResult joinResult = _CachedJoinResult ?? _JoinBuilder.BuildJoinSql<TEntity>(_Repository._TableName, _IncludePaths);

                    _EntityMapper.ClearProcessingCache();
                    results = _EntityMapper.MapJoinedResults(reader, joinResult, joinResult.Includes).ToList();
                }
                else
                {
                    // Use simple mapping for basic queries
                    results = _EntityMapper.MapSimpleResults(reader).ToList();
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error executing SQL: {sql}", ex);
            }

            return results;
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
                DbConnection connection = _Repository._ConnectionFactory.GetConnection();
                try
                {
                    return await ExecuteWithConnectionAsync(connection, sql, token).ConfigureAwait(false);
                }
                finally
                {
                    await _Repository._ConnectionFactory.ReturnConnectionAsync(connection);
                }
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

            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync(token).ConfigureAwait(false);
            }

            using SqlCommand command = (SqlCommand)connection.CreateCommand();
            command.CommandText = sql;

            if (_Transaction != null)
            {
                command.Transaction = (SqlTransaction)_Transaction.Transaction;
            }

            // Capture SQL if enabled
            if (_Repository.CaptureSql)
            {
                _Repository.SetLastExecutedSql(sql);
            }

            try
            {
                using SqlDataReader reader = (SqlDataReader)await command.ExecuteReaderAsync(token).ConfigureAwait(false);

                // Check if we have includes that require advanced mapping
                if (_IncludePaths.Count > 0)
                {
                    // Use advanced EntityMapper for complex scenarios with navigation properties
                    SqlServerJoinBuilder.SqlServerJoinResult joinResult = _CachedJoinResult ?? _JoinBuilder.BuildJoinSql<TEntity>(_Repository._TableName, _IncludePaths);

                    _EntityMapper.ClearProcessingCache();
                    return await _EntityMapper.MapJoinedResultsAsync(reader, joinResult, joinResult.Includes, token).ConfigureAwait(false);
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
        /// Processes include paths into SqlServerIncludeInfo objects for advanced entity mapping.
        /// </summary>
        /// <returns>A list of include information objects</returns>
        private List<SqlServerIncludeInfo> ProcessIncludePaths()
        {
            try
            {
                SqlServerIncludeProcessor processor = new SqlServerIncludeProcessor(_Repository._Sanitizer);
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

        private string BuildWindowFunctionSql(WindowFunction windowFunc)
        {
            StringBuilder funcSql = new StringBuilder();

            funcSql.Append(windowFunc.FunctionName);
            funcSql.Append("(");

            // Handle special functions with parameters like LEAD/LAG
            if (windowFunc.FunctionName == "LEAD" || windowFunc.FunctionName == "LAG")
            {
                if (!string.IsNullOrEmpty(windowFunc.Column))
                {
                    funcSql.Append($"[{windowFunc.Column}]");
                }

                if (windowFunc.Parameters.ContainsKey("offset"))
                {
                    funcSql.Append(", ");
                    funcSql.Append(windowFunc.Parameters["offset"]);

                    if (windowFunc.Parameters.ContainsKey("default"))
                    {
                        funcSql.Append(", ");
                        object? defaultValue = windowFunc.Parameters["default"];
                        if (defaultValue is string)
                        {
                            funcSql.Append($"'{defaultValue}'");
                        }
                        else
                        {
                            funcSql.Append(defaultValue);
                        }
                    }
                }
            }
            else if (windowFunc.FunctionName == "NTH_VALUE")
            {
                if (!string.IsNullOrEmpty(windowFunc.Column))
                {
                    funcSql.Append($"[{windowFunc.Column}]");
                }

                if (windowFunc.Parameters.ContainsKey("n"))
                {
                    funcSql.Append(", ");
                    funcSql.Append(windowFunc.Parameters["n"]);
                }
            }
            else
            {
                // Regular window functions
                if (!string.IsNullOrEmpty(windowFunc.Column))
                {
                    if (windowFunc.Column == "*")
                        funcSql.Append("*");
                    else
                        funcSql.Append($"[{windowFunc.Column}]");
                }
            }

            funcSql.Append(") OVER (");

            // PARTITION BY clause
            if (windowFunc.PartitionByColumns.Count > 0)
            {
                funcSql.Append("PARTITION BY ");
                List<string> partitions = new List<string>();
                foreach (string partition in windowFunc.PartitionByColumns)
                {
                    partitions.Add($"[{partition}]");
                }
                funcSql.Append(string.Join(", ", partitions));
            }

            // ORDER BY clause
            if (windowFunc.OrderByColumns.Count > 0)
            {
                if (windowFunc.PartitionByColumns.Count > 0)
                    funcSql.Append(" ");

                funcSql.Append("ORDER BY ");
                List<string> orderBys = new List<string>();
                foreach (WindowOrderByClause orderBy in windowFunc.OrderByColumns)
                {
                    orderBys.Add($"[{orderBy.Column}] {(orderBy.Ascending ? "ASC" : "DESC")}");
                }
                funcSql.Append(string.Join(", ", orderBys));
            }

            // Window frame specification
            if (windowFunc.Frame != null)
            {
                if (windowFunc.PartitionByColumns.Count > 0 || windowFunc.OrderByColumns.Count > 0)
                    funcSql.Append(" ");

                funcSql.Append(windowFunc.Frame.Type.ToString().ToUpper());

                if (windowFunc.Frame.StartBound != null || windowFunc.Frame.EndBound != null)
                {
                    funcSql.Append(" BETWEEN ");

                    if (windowFunc.Frame.StartBound != null)
                    {
                        funcSql.Append(FormatWindowFrameBound(windowFunc.Frame.StartBound));
                    }
                    else
                    {
                        funcSql.Append("UNBOUNDED PRECEDING");
                    }

                    funcSql.Append(" AND ");

                    if (windowFunc.Frame.EndBound != null)
                    {
                        funcSql.Append(FormatWindowFrameBound(windowFunc.Frame.EndBound));
                    }
                    else
                    {
                        funcSql.Append("CURRENT ROW");
                    }
                }
            }

            funcSql.Append(")");

            // Add alias
            if (!string.IsNullOrEmpty(windowFunc.Alias))
            {
                funcSql.Append($" AS [{windowFunc.Alias}]");
            }

            return funcSql.ToString();
        }

        private string FormatWindowFrameBound(WindowFrameBound bound)
        {
            switch (bound.Type)
            {
                case WindowFrameBoundType.UnboundedPreceding:
                    return "UNBOUNDED PRECEDING";
                case WindowFrameBoundType.UnboundedFollowing:
                    return "UNBOUNDED FOLLOWING";
                case WindowFrameBoundType.CurrentRow:
                    return "CURRENT ROW";
                case WindowFrameBoundType.Preceding:
                    return $"{bound.Offset} PRECEDING";
                case WindowFrameBoundType.Following:
                    return $"{bound.Offset} FOLLOWING";
                default:
                    return "CURRENT ROW";
            }
        }

        private string BuildCountSql()
        {
            List<string> sqlParts = new List<string>();

            sqlParts.Add("SELECT COUNT(*)");
            sqlParts.Add($"FROM [{_Repository._TableName}]");

            if (_WhereClauses.Any())
            {
                sqlParts.Add($"WHERE {string.Join(" AND ", _WhereClauses)}");
            }

            return string.Join(" ", sqlParts);
        }

        private string BuildAggregateSql(string function, string column)
        {
            List<string> sqlParts = new List<string>();

            sqlParts.Add($"SELECT {function}({column})");
            sqlParts.Add($"FROM [{_Repository._TableName}]");

            if (_WhereClauses.Any())
            {
                sqlParts.Add($"WHERE {string.Join(" AND ", _WhereClauses)}");
            }

            return string.Join(" ", sqlParts);
        }

        private string BuildDeleteSql()
        {
            List<string> sqlParts = new List<string>();

            sqlParts.Add($"DELETE FROM [{_Repository._TableName}]");

            if (_WhereClauses.Any())
            {
                sqlParts.Add($"WHERE {string.Join(" AND ", _WhereClauses)}");
            }

            return string.Join(" ", sqlParts);
        }

        private TResult ExecuteScalarInternal<TResult>(string sql)
        {
            if (_Transaction != null)
            {
                return ExecuteScalarWithConnection<TResult>(_Transaction.Connection, sql);
            }
            else
            {
                DbConnection connection = _Repository._ConnectionFactory.GetConnection();
                try
                {
                    return ExecuteScalarWithConnection<TResult>(connection, sql);
                }
                finally
                {
                    _Repository._ConnectionFactory.ReturnConnection(connection);
                }
            }
        }

        private TResult ExecuteScalarWithConnection<TResult>(DbConnection connection, string sql)
        {
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }

            using SqlCommand command = (SqlCommand)connection.CreateCommand();
            command.CommandText = sql;

            if (_Transaction != null)
            {
                command.Transaction = (SqlTransaction)_Transaction.Transaction;
            }

            if (_Repository.CaptureSql)
            {
                _Repository.SetLastExecutedSql(sql);
            }

            try
            {
                object? result = command.ExecuteScalar();

                if (result == null || result == DBNull.Value)
                {
                    return default(TResult)!;
                }

                if (typeof(TResult) == typeof(long))
                {
                    return (TResult)(object)Convert.ToInt64(result);
                }
                else if (typeof(TResult) == typeof(decimal))
                {
                    return (TResult)(object)Convert.ToDecimal(result);
                }
                else if (typeof(TResult) == typeof(int))
                {
                    return (TResult)(object)Convert.ToInt32(result);
                }
                else if (typeof(TResult) == typeof(double))
                {
                    return (TResult)(object)Convert.ToDouble(result);
                }
                else if (typeof(TResult) == typeof(float))
                {
                    return (TResult)(object)Convert.ToSingle(result);
                }
                else if (typeof(TResult) == typeof(DateTime))
                {
                    return (TResult)(object)Convert.ToDateTime(result);
                }
                else if (typeof(TResult) == typeof(string))
                {
                    return (TResult)(object)Convert.ToString(result)!;
                }
                else
                {
                    return (TResult)result;
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error executing scalar SQL: {sql}", ex);
            }
        }

        private async Task<TResult> ExecuteScalarInternalAsync<TResult>(string sql, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();

            if (_Transaction != null)
            {
                return await ExecuteScalarWithConnectionAsync<TResult>(_Transaction.Connection, sql, token).ConfigureAwait(false);
            }
            else
            {
                DbConnection connection = _Repository._ConnectionFactory.GetConnection();
                try
                {
                    return await ExecuteScalarWithConnectionAsync<TResult>(connection, sql, token).ConfigureAwait(false);
                }
                finally
                {
                    _Repository._ConnectionFactory.ReturnConnection(connection);
                }
            }
        }

        private async Task<TResult> ExecuteScalarWithConnectionAsync<TResult>(DbConnection connection, string sql, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();

            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync(token).ConfigureAwait(false);
            }

            using SqlCommand command = (SqlCommand)connection.CreateCommand();
            command.CommandText = sql;

            if (_Transaction != null)
            {
                command.Transaction = (SqlTransaction)_Transaction.Transaction;
            }

            if (_Repository.CaptureSql)
            {
                _Repository.SetLastExecutedSql(sql);
            }

            try
            {
                object? result = await command.ExecuteScalarAsync(token).ConfigureAwait(false);

                if (result == null || result == DBNull.Value)
                {
                    return default(TResult)!;
                }

                if (typeof(TResult) == typeof(long))
                {
                    return (TResult)(object)Convert.ToInt64(result);
                }
                else if (typeof(TResult) == typeof(decimal))
                {
                    return (TResult)(object)Convert.ToDecimal(result);
                }
                else if (typeof(TResult) == typeof(int))
                {
                    return (TResult)(object)Convert.ToInt32(result);
                }
                else if (typeof(TResult) == typeof(double))
                {
                    return (TResult)(object)Convert.ToDouble(result);
                }
                else if (typeof(TResult) == typeof(float))
                {
                    return (TResult)(object)Convert.ToSingle(result);
                }
                else if (typeof(TResult) == typeof(DateTime))
                {
                    return (TResult)(object)Convert.ToDateTime(result);
                }
                else if (typeof(TResult) == typeof(string))
                {
                    return (TResult)(object)Convert.ToString(result)!;
                }
                else
                {
                    return (TResult)result;
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error executing scalar SQL: {sql}", ex);
            }
        }

        private int ExecuteNonQueryInternal(string sql)
        {
            if (_Transaction != null)
            {
                return ExecuteNonQueryWithConnection(_Transaction.Connection, sql);
            }
            else
            {
                DbConnection connection = _Repository._ConnectionFactory.GetConnection();
                try
                {
                    return ExecuteNonQueryWithConnection(connection, sql);
                }
                finally
                {
                    _Repository._ConnectionFactory.ReturnConnection(connection);
                }
            }
        }

        private int ExecuteNonQueryWithConnection(DbConnection connection, string sql)
        {
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }

            using SqlCommand command = (SqlCommand)connection.CreateCommand();
            command.CommandText = sql;

            if (_Transaction != null)
            {
                command.Transaction = (SqlTransaction)_Transaction.Transaction;
            }

            if (_Repository.CaptureSql)
            {
                _Repository.SetLastExecutedSql(sql);
            }

            try
            {
                int rowsAffected = command.ExecuteNonQuery();
                return rowsAffected;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error executing non-query SQL: {sql}", ex);
            }
        }

        private async Task<int> ExecuteNonQueryInternalAsync(string sql, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();

            if (_Transaction != null)
            {
                return await ExecuteNonQueryWithConnectionAsync(_Transaction.Connection, sql, token).ConfigureAwait(false);
            }
            else
            {
                DbConnection connection = _Repository._ConnectionFactory.GetConnection();
                try
                {
                    return await ExecuteNonQueryWithConnectionAsync(connection, sql, token).ConfigureAwait(false);
                }
                finally
                {
                    _Repository._ConnectionFactory.ReturnConnection(connection);
                }
            }
        }

        private async Task<int> ExecuteNonQueryWithConnectionAsync(DbConnection connection, string sql, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();

            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync(token).ConfigureAwait(false);
            }

            using SqlCommand command = (SqlCommand)connection.CreateCommand();
            command.CommandText = sql;

            if (_Transaction != null)
            {
                command.Transaction = (SqlTransaction)_Transaction.Transaction;
            }

            if (_Repository.CaptureSql)
            {
                _Repository.SetLastExecutedSql(sql);
            }

            try
            {
                int rowsAffected = await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
                return rowsAffected;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error executing non-query SQL: {sql}", ex);
            }
        }

        #endregion
    }
}