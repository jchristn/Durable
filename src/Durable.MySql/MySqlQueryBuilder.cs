namespace Durable.MySql
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
    using MySqlConnector;

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
        private MySqlJoinBuilder.MySqlJoinResult? _CachedJoinResult;
        private readonly List<string> _GroupByColumns = new List<string>();
        private readonly List<string> _HavingClauses = new List<string>();
        private readonly MySqlExpressionParser<TEntity> _ExpressionParser;
        private readonly MySqlJoinBuilder _JoinBuilder;
        private readonly MySqlEntityMapper<TEntity> _EntityMapper;
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
        /// Executes the query without GROUP BY clause and returns the results.
        /// Used by MySqlGroupedQueryBuilder for entity fetching after group filtering.
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

        public string BuildSql(bool includeGroupBy)
        {
            List<string> sqlParts = new List<string>();
            MySqlJoinBuilder.MySqlJoinResult? joinResult = null;

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
                        cteStrings.Add($"`{cte.Name}` AS ({cte.AnchorQuery} UNION ALL {cte.RecursiveQuery})");
                    }
                    else
                    {
                        cteStrings.Add($"`{cte.Name}` AS ({cte.Query})");
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
                sqlParts.Add($"FROM `{_Repository._TableName}` t0");
            }
            else
            {
                sqlParts.Add($"FROM `{_Repository._TableName}`");
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
                sqlParts.Add($"GROUP BY {string.Join(", ", _GroupByColumns.Select(c => $"`{c.Replace("`", "``")}`"))}");
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

            // Handle set operations
            if (_SetOperations.Count > 0)
            {
                StringBuilder setOperationSql = new StringBuilder();
                setOperationSql.Append("(");
                setOperationSql.Append(string.Join(" ", sqlParts));
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
                            // MySQL doesn't support INTERSECT natively, so we implement it using INNER JOIN
                            {
                                string baseQuery = string.Join(" ", sqlParts);
                                string otherQuerySql = setOp.OtherQuery.BuildSql().TrimEnd(';');
                                string primaryKey = _Repository._PrimaryKeyColumn;
                                return $"SELECT DISTINCT t1.* FROM ({baseQuery}) t1 INNER JOIN ({otherQuerySql}) t2 ON t1.`{primaryKey}` = t2.`{primaryKey}`";
                            }
                        case SetOperationType.Except:
                            // MySQL doesn't support EXCEPT natively, so we implement it using LEFT JOIN with NULL check
                            {
                                string baseQuery = string.Join(" ", sqlParts);
                                string otherQuerySql = setOp.OtherQuery.BuildSql().TrimEnd(';');
                                string primaryKey = _Repository._PrimaryKeyColumn;
                                return $"SELECT t1.* FROM ({baseQuery}) t1 LEFT JOIN ({otherQuerySql}) t2 ON t1.`{primaryKey}` = t2.`{primaryKey}` WHERE t2.`{primaryKey}` IS NULL";
                            }
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

        public IQueryBuilder<TResult> Select<TResult>(Expression<Func<TEntity, TResult>> selector) where TResult : class, new()
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            return new MySqlProjectedQueryBuilder<TEntity, TResult>(_Repository, selector, this, null);
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

            // Try to extract column name from the key selector expression
            // For complex expressions (anonymous types, computed fields), we'll skip SQL GROUP BY
            // and let MySqlGroupedQueryBuilder handle it in-memory
            try
            {
                string groupColumn = GetColumnFromExpression(keySelector.Body);
                // Remove backticks if present to store raw column name
                string rawColumn = groupColumn.Trim('`');
                _GroupByColumns.Add(rawColumn);
            }
            catch (ArgumentException ex)
            {
                // Complex expression that can't be translated to SQL column
                // MySqlGroupedQueryBuilder will handle this with in-memory grouping
                // Don't add anything to _GroupByColumns so SQL GROUP BY is skipped
            }

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
            _WhereClauses.Add($"`{column}` IN ({subquerySql})");
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
            _WhereClauses.Add($"`{column}` NOT IN ({subquerySql})");
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
            _WhereClauses.Add($"`{column}` IN ({subquerySql})");
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
            _WhereClauses.Add($"`{column}` NOT IN ({subquerySql})");
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

            return new MySqlWindowedQueryBuilder<TEntity>(this, _Repository, _Transaction, functionName, partitionBy, orderBy);
        }

        /// <summary>
        /// Adds a configured window function to the query. This method is called internally by MySqlWindowedQueryBuilder.
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
        /// Adds a CASE expression to the SELECT clause. This method is called internally by MySqlCaseExpressionBuilder.
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
            return new MySqlCaseExpressionBuilder<TEntity>(this, _Repository);
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
        /// Used by MySqlGroupedQueryBuilder for entity fetching after group filtering.
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

        private IEnumerable<TEntity> ExecuteWithConnection(DbConnection connection, string sql)
        {
            List<TEntity> results = new List<TEntity>();

            if (connection.State != ConnectionState.Open)
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
                using var reader = (MySqlDataReader)command.ExecuteReader();

                // Check if we have includes that require advanced mapping
                if (_IncludePaths.Count > 0)
                {
                    // Use advanced EntityMapper for complex scenarios with navigation properties
                    MySqlJoinBuilder.MySqlJoinResult joinResult = _CachedJoinResult ?? _JoinBuilder.BuildJoinSql<TEntity>(_Repository._TableName, _IncludePaths);

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

            if (connection.State != ConnectionState.Open)
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
                    MySqlJoinBuilder.MySqlJoinResult joinResult = _CachedJoinResult ?? _JoinBuilder.BuildJoinSql<TEntity>(_Repository._TableName, _IncludePaths);

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
                    funcSql.Append($"`{windowFunc.Column}`");
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
                    funcSql.Append($"`{windowFunc.Column}`");
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
                        funcSql.Append($"`{windowFunc.Column}`");
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
                    partitions.Add($"`{partition}`");
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
                    orderBys.Add($"`{orderBy.Column}` {(orderBy.Ascending ? "ASC" : "DESC")}");
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
                funcSql.Append($" AS `{windowFunc.Alias}`");
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

        #endregion
    }
}