namespace Durable.Sqlite
{
    using Microsoft.Data.Sqlite;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    public class SqliteQueryBuilder<TEntity> : IQueryBuilder<TEntity> where TEntity : class, new()
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private readonly SqliteRepository<TEntity> _Repository;
        private readonly ITransaction _Transaction;
        private readonly List<string> _WhereClauses = new List<string>();
        private readonly List<OrderByClause> _OrderByClauses = new List<OrderByClause>();
        private readonly List<string> _Includes = new List<string>();
        private readonly List<string> _GroupByColumns = new List<string>();
        private readonly JoinBuilder _JoinBuilder;
        private readonly HashSet<string> _ProcessedIncludes = new HashSet<string>();
        private int? _SkipCount;
        private int? _TakeCount;
        private bool _Distinct;
        private string _CachedSql;
        private JoinBuilder.JoinResult _CachedJoinResult;
        
        // Advanced query features
        private readonly List<SetOperation<TEntity>> _SetOperations = new List<SetOperation<TEntity>>();
        private readonly List<CteDefinition> _CteDefinitions = new List<CteDefinition>();
        internal readonly List<WindowFunction> _WindowFunctions = new List<WindowFunction>();
        private string _CustomFromClause;
        private string _CustomSelectClause;
        private readonly List<string> _CustomJoinClauses = new List<string>();
        private readonly List<string> _HavingClauses = new List<string>();

        #endregion

        #region Constructors-and-Factories

        public SqliteQueryBuilder(SqliteRepository<TEntity> repository, ITransaction transaction = null)
        {
            _Repository = repository;
            _Transaction = transaction;
            _JoinBuilder = new JoinBuilder(_Repository._Sanitizer);
        }

        #endregion

        #region Public-Methods

        public IQueryBuilder<TEntity> Where(Expression<Func<TEntity, bool>> predicate)
        {
            string whereClause = _Repository.BuildWhereClause(predicate);
            _WhereClauses.Add(whereClause);
            return this;
        }

        public IQueryBuilder<TEntity> OrderBy<TKey>(Expression<Func<TEntity, TKey>> keySelector)
        {
            string column = _Repository.GetColumnFromExpression(keySelector.Body);
            _OrderByClauses.Add(new OrderByClause { Column = column, Ascending = true });
            return this;
        }

        public IQueryBuilder<TEntity> OrderByDescending<TKey>(Expression<Func<TEntity, TKey>> keySelector)
        {
            string column = _Repository.GetColumnFromExpression(keySelector.Body);
            _OrderByClauses.Add(new OrderByClause { Column = column, Ascending = false });
            return this;
        }

        public IQueryBuilder<TEntity> ThenBy<TKey>(Expression<Func<TEntity, TKey>> keySelector)
        {
            if (_OrderByClauses.Count == 0)
                throw new InvalidOperationException("ThenBy can only be used after OrderBy or OrderByDescending");

            string column = _Repository.GetColumnFromExpression(keySelector.Body);
            _OrderByClauses.Add(new OrderByClause { Column = column, Ascending = true });
            return this;
        }

        public IQueryBuilder<TEntity> ThenByDescending<TKey>(Expression<Func<TEntity, TKey>> keySelector)
        {
            if (_OrderByClauses.Count == 0)
                throw new InvalidOperationException("ThenByDescending can only be used after OrderBy or OrderByDescending");

            string column = _Repository.GetColumnFromExpression(keySelector.Body);
            _OrderByClauses.Add(new OrderByClause { Column = column, Ascending = false });
            return this;
        }

        public IQueryBuilder<TEntity> Skip(int count)
        {
            _SkipCount = count;
            return this;
        }

        public IQueryBuilder<TEntity> Take(int count)
        {
            _TakeCount = count;
            return this;
        }

        public IQueryBuilder<TEntity> Distinct()
        {
            _Distinct = true;
            return this;
        }

        public IQueryBuilder<TResult> Select<TResult>(Expression<Func<TEntity, TResult>> selector) where TResult : class, new()
        {
            return new SqliteProjectedQueryBuilder<TEntity, TResult>(_Repository, selector, this, _Transaction);
        }

        public IQueryBuilder<TEntity> Include<TProperty>(Expression<Func<TEntity, TProperty>> navigationProperty)
        {
            // Store include information for later processing
            string propertyName = GetPropertyName(navigationProperty);
            _Includes.Add(propertyName);
            return this;
        }

        public IQueryBuilder<TEntity> ThenInclude<TPreviousProperty, TProperty>(Expression<Func<TPreviousProperty, TProperty>> navigationProperty)
        {
            // Store nested include information
            string propertyName = GetPropertyNameFromExpression(navigationProperty);
            if (_Includes.Count > 0)
            {
                _Includes[_Includes.Count - 1] += "." + propertyName;
            }
            return this;
        }

        public IGroupedQueryBuilder<TEntity, TKey> GroupBy<TKey>(Expression<Func<TEntity, TKey>> keySelector)
        {
            string column = _Repository.GetColumnFromExpression(keySelector.Body);
            _GroupByColumns.Add(column);
            return new SqliteGroupedQueryBuilder<TEntity, TKey>(_Repository, this, keySelector);
        }

        public IQueryBuilder<TEntity> Having(Expression<Func<TEntity, bool>> predicate)
        {
            if (_GroupByColumns.Count == 0)
            {
                throw new InvalidOperationException("HAVING clause can only be used with GROUP BY");
            }
            
            string havingClause = _Repository.BuildWhereClause(predicate);
            _HavingClauses.Add(havingClause);
            _CachedSql = null;
            return this;
        }

        // Set operations
        public IQueryBuilder<TEntity> Union(IQueryBuilder<TEntity> other)
        {
            _SetOperations.Add(new SetOperation<TEntity>(SetOperationType.Union, other));
            _CachedSql = null; // Invalidate cached SQL
            return this;
        }

        public IQueryBuilder<TEntity> UnionAll(IQueryBuilder<TEntity> other)
        {
            _SetOperations.Add(new SetOperation<TEntity>(SetOperationType.UnionAll, other));
            _CachedSql = null;
            return this;
        }

        public IQueryBuilder<TEntity> Intersect(IQueryBuilder<TEntity> other)
        {
            _SetOperations.Add(new SetOperation<TEntity>(SetOperationType.Intersect, other));
            _CachedSql = null;
            return this;
        }

        public IQueryBuilder<TEntity> Except(IQueryBuilder<TEntity> other)
        {
            _SetOperations.Add(new SetOperation<TEntity>(SetOperationType.Except, other));
            _CachedSql = null;
            return this;
        }

        // Subquery support
        public IQueryBuilder<TEntity> WhereIn<TKey>(Expression<Func<TEntity, TKey>> keySelector, IQueryBuilder<TKey> subquery) where TKey : class, new()
        {
            string column = _Repository.GetColumnFromExpression(keySelector.Body);
            string subquerySql = subquery.BuildSql().TrimEnd(';');
            _WhereClauses.Add($"{column} IN ({subquerySql})");
            _CachedSql = null;
            return this;
        }

        public IQueryBuilder<TEntity> WhereNotIn<TKey>(Expression<Func<TEntity, TKey>> keySelector, IQueryBuilder<TKey> subquery) where TKey : class, new()
        {
            string column = _Repository.GetColumnFromExpression(keySelector.Body);
            string subquerySql = subquery.BuildSql().TrimEnd(';');
            _WhereClauses.Add($"{column} NOT IN ({subquerySql})");
            _CachedSql = null;
            return this;
        }

        public IQueryBuilder<TEntity> WhereInRaw<TKey>(Expression<Func<TEntity, TKey>> keySelector, string subquerySql)
        {
            string column = _Repository.GetColumnFromExpression(keySelector.Body);
            _WhereClauses.Add($"{column} IN ({subquerySql})");
            _CachedSql = null;
            return this;
        }

        public IQueryBuilder<TEntity> WhereNotInRaw<TKey>(Expression<Func<TEntity, TKey>> keySelector, string subquerySql)
        {
            string column = _Repository.GetColumnFromExpression(keySelector.Body);
            _WhereClauses.Add($"{column} NOT IN ({subquerySql})");
            _CachedSql = null;
            return this;
        }

        public IQueryBuilder<TEntity> WhereExists<TOther>(IQueryBuilder<TOther> subquery) where TOther : class, new()
        {
            string subquerySql = subquery.BuildSql().TrimEnd(';');
            _WhereClauses.Add($"EXISTS ({subquerySql})");
            _CachedSql = null;
            return this;
        }

        public IQueryBuilder<TEntity> WhereNotExists<TOther>(IQueryBuilder<TOther> subquery) where TOther : class, new()
        {
            string subquerySql = subquery.BuildSql().TrimEnd(';');
            _WhereClauses.Add($"NOT EXISTS ({subquerySql})");
            _CachedSql = null;
            return this;
        }

        // Window functions
        public IWindowedQueryBuilder<TEntity> WithWindowFunction(string functionName, string partitionBy = null, string orderBy = null)
        {
            return new SqliteWindowedQueryBuilder<TEntity>(this, _Repository, _Transaction, functionName, partitionBy, orderBy);
        }

        // CTEs
        public IQueryBuilder<TEntity> WithCte(string cteName, string cteQuery)
        {
            _CteDefinitions.Add(new CteDefinition(cteName, cteQuery));
            _CachedSql = null;
            return this;
        }

        public IQueryBuilder<TEntity> WithRecursiveCte(string cteName, string anchorQuery, string recursiveQuery)
        {
            _CteDefinitions.Add(new CteDefinition(cteName, anchorQuery, recursiveQuery));
            _CachedSql = null;
            return this;
        }

        // Custom SQL fragments
        public IQueryBuilder<TEntity> WhereRaw(string sql, params object[] parameters)
        {
            if (parameters != null && parameters.Length > 0)
            {
                sql = string.Format(sql, parameters.Select(p => _Repository._Sanitizer.FormatValue(p)).ToArray());
            }
            _WhereClauses.Add(sql);
            _CachedSql = null;
            return this;
        }

        public IQueryBuilder<TEntity> SelectRaw(string sql)
        {
            _CustomSelectClause = sql;
            _CachedSql = null;
            return this;
        }

        public IQueryBuilder<TEntity> FromRaw(string sql)
        {
            _CustomFromClause = sql;
            _CachedSql = null;
            return this;
        }

        public IQueryBuilder<TEntity> JoinRaw(string sql)
        {
            _CustomJoinClauses.Add(sql);
            _CachedSql = null;
            return this;
        }

        // CASE WHEN expressions
        public ICaseExpressionBuilder<TEntity> SelectCase()
        {
            return new SqliteCaseExpressionBuilder<TEntity>(this, _Repository);
        }

        // Internal helper methods
        internal string GetCustomSelectClause()
        {
            return _CustomSelectClause;
        }

        public IEnumerable<TEntity> Execute()
        {
            (SqliteConnection connection, SqliteCommand command, bool shouldReturnToPool) = _Repository.GetConnectionAndCommand(_Transaction);
            ConnectionResult connectionResult = new ConnectionResult(connection, command, shouldReturnToPool);
            try
            {
                connectionResult.Command.CommandText = BuildSql();
                using SqliteDataReader reader = connectionResult.Command.ExecuteReader();

                List<TEntity> results;
                
                if (_Includes.Count > 0)
                {
                    // Use EntityMapper for joined results
                    EntityMapper<TEntity> mapper = new EntityMapper<TEntity>(
                        _Repository._DataTypeConverter,
                        _Repository._ColumnMappings);
                    
                    IncludeProcessor processor = new IncludeProcessor(_Repository._Sanitizer);
                    List<IncludeInfo> includeInfos = processor.ParseIncludes<TEntity>(_Includes);
                    
                    results = mapper.MapJoinedResults(reader, GetOrBuildJoinResult(), includeInfos);
                    
                    // Load collections if needed
                    CollectionLoader<TEntity> collectionLoader = new CollectionLoader<TEntity>(
                        _Repository._Sanitizer,
                        _Repository._DataTypeConverter);
                    collectionLoader.LoadCollections(results, includeInfos, connectionResult.Connection, _Transaction);
                }
                else
                {
                    results = new List<TEntity>();
                    while (reader.Read())
                    {
                        results.Add(_Repository.MapReaderToEntity(reader));
                    }
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

        public async Task<IEnumerable<TEntity>> ExecuteAsync(CancellationToken token = default)
        {
            (SqliteConnection connection, SqliteCommand command, bool shouldReturnToPool) = await _Repository.GetConnectionAndCommandAsync(_Transaction, token);
            ConnectionResult connectionResult = new ConnectionResult(connection, command, shouldReturnToPool);
            try
            {
                connectionResult.Command.CommandText = BuildSql();
                await using SqliteDataReader reader = await connectionResult.Command.ExecuteReaderAsync(token);

                List<TEntity> results;
                
                if (_Includes.Count > 0)
                {
                    // Use EntityMapper for joined results
                    EntityMapper<TEntity> mapper = new EntityMapper<TEntity>(
                        _Repository._DataTypeConverter,
                        _Repository._ColumnMappings);
                    
                    IncludeProcessor processor = new IncludeProcessor(_Repository._Sanitizer);
                    List<IncludeInfo> includeInfos = processor.ParseIncludes<TEntity>(_Includes);
                    
                    // Read all results into memory for joined processing
                    List<Dictionary<string, object>> rawResults = new List<Dictionary<string, object>>();
                    while (await reader.ReadAsync(token))
                    {
                        Dictionary<string, object> row = new Dictionary<string, object>();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                        }
                        rawResults.Add(row);
                    }
                    
                    // Process the raw results
                    results = ProcessRawJoinResults(rawResults, includeInfos);
                    
                    // Load collections if needed
                    CollectionLoader<TEntity> collectionLoader = new CollectionLoader<TEntity>(
                        _Repository._Sanitizer,
                        _Repository._DataTypeConverter);
                    collectionLoader.LoadCollections(results, includeInfos, connectionResult.Connection, _Transaction);
                }
                else
                {
                    results = new List<TEntity>();
                    while (await reader.ReadAsync(token))
                    {
                        results.Add(_Repository.MapReaderToEntity(reader));
                    }
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

        public async IAsyncEnumerable<TEntity> ExecuteAsyncEnumerable([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken token = default)
        {
            (SqliteConnection connection, SqliteCommand command, bool shouldReturnToPool) = await _Repository.GetConnectionAndCommandAsync(_Transaction, token);
            ConnectionResult connectionResult = new ConnectionResult(connection, command, shouldReturnToPool);
            try
            {
                connectionResult.Command.CommandText = BuildSql();
                await using SqliteDataReader reader = await connectionResult.Command.ExecuteReaderAsync(token);

                while (await reader.ReadAsync(token))
                {
                    yield return _Repository.MapReaderToEntity(reader);
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

        public string Query
        {
            get
            {
                if (_CachedSql == null)
                    _CachedSql = BuildSql();
                return _CachedSql;
            }
        }

        public IDurableResult<TEntity> ExecuteWithQuery()
        {
            string query = Query;
            IEnumerable<TEntity> results = Execute();
            return new DurableResult<TEntity>(query, results);
        }

        public async Task<IDurableResult<TEntity>> ExecuteWithQueryAsync(CancellationToken token = default)
        {
            string query = Query;
            IEnumerable<TEntity> results = await ExecuteAsync(token);
            return new DurableResult<TEntity>(query, results);
        }

        public IAsyncDurableResult<TEntity> ExecuteAsyncEnumerableWithQuery(CancellationToken token = default)
        {
            string query = Query;
            IAsyncEnumerable<TEntity> results = ExecuteAsyncEnumerable(token);
            return new AsyncDurableResult<TEntity>(query, results);
        }

        public string BuildSql()
        {
            StringBuilder sql = new StringBuilder();

            // Handle CTEs
            if (_CteDefinitions.Count > 0)
            {
                sql.Append("WITH ");
                if (_CteDefinitions.Any(c => c.IsRecursive))
                {
                    sql.Append("RECURSIVE ");
                }
                
                List<string> cteStrings = new List<string>();
                foreach (CteDefinition cte in _CteDefinitions)
                {
                    if (cte.IsRecursive)
                    {
                        cteStrings.Add($"{cte.Name} AS ({cte.AnchorQuery} UNION ALL {cte.RecursiveQuery})");
                    }
                    else
                    {
                        cteStrings.Add($"{cte.Name} AS ({cte.Query})");
                    }
                }
                sql.Append(string.Join(", ", cteStrings));
                sql.Append(" ");
            }

            // Build main query
            string mainQuery = BuildMainQuery();
            
            // Handle set operations
            if (_SetOperations.Count > 0)
            {
                sql.Append("(");
                sql.Append(mainQuery);
                sql.Append(")");
                
                foreach (SetOperation<TEntity> setOp in _SetOperations)
                {
                    switch (setOp.Type)
                    {
                        case SetOperationType.Union:
                            sql.Append(" UNION ");
                            break;
                        case SetOperationType.UnionAll:
                            sql.Append(" UNION ALL ");
                            break;
                        case SetOperationType.Intersect:
                            sql.Append(" INTERSECT ");
                            break;
                        case SetOperationType.Except:
                            sql.Append(" EXCEPT ");
                            break;
                    }
                    sql.Append("(");
                    sql.Append(setOp.OtherQuery.BuildSql().TrimEnd(';'));
                    sql.Append(")");
                }
            }
            else
            {
                sql.Append(mainQuery);
            }

            sql.Append(";");
            return sql.ToString();
        }

        private string BuildMainQuery()
        {
            StringBuilder sql = new StringBuilder();

            // SELECT clause
            sql.Append("SELECT ");
            if (_Distinct) sql.Append("DISTINCT ");
            
            if (!string.IsNullOrEmpty(_CustomSelectClause))
            {
                sql.Append(_CustomSelectClause);
            }
            else if (_WindowFunctions.Count > 0)
            {
                List<string> selectParts = new List<string>();
                selectParts.Add("t0.*");
                
                foreach (WindowFunction windowFunc in _WindowFunctions)
                {
                    selectParts.Add(BuildWindowFunctionSql(windowFunc));
                }
                
                sql.Append(string.Join(", ", selectParts));
            }
            else if (_Includes.Count > 0)
            {
                JoinBuilder.JoinResult joinResult = GetOrBuildJoinResult();
                sql.Append(joinResult.SelectClause);
            }
            else
            {
                sql.Append("t0.*");
            }

            // FROM clause
            if (!string.IsNullOrEmpty(_CustomFromClause))
            {
                sql.Append($" FROM {_CustomFromClause}");
            }
            else
            {
                sql.Append($" FROM {_Repository._Sanitizer.SanitizeIdentifier(_Repository._TableName)} t0");
            }

            // JOINs
            if (_Includes.Count > 0)
            {
                JoinBuilder.JoinResult joinResult = GetOrBuildJoinResult();
                sql.Append(joinResult.JoinClause);
            }
            
            if (_CustomJoinClauses.Count > 0)
            {
                foreach (string customJoin in _CustomJoinClauses)
                {
                    sql.Append(" ");
                    sql.Append(customJoin);
                }
            }

            // WHERE clause
            if (_WhereClauses.Count > 0)
            {
                sql.Append(" WHERE ");
                List<string> updatedWhereClauses = _WhereClauses.Select(w => 
                    UpdateWhereClauseWithAlias(w, "t0")).ToList();
                sql.Append(string.Join(" AND ", updatedWhereClauses));
            }

            // GROUP BY clause
            if (_GroupByColumns.Count > 0)
            {
                sql.Append(" GROUP BY ");
                List<string> updatedGroupByColumns = _GroupByColumns.Select(c => $"t0.{c}").ToList();
                sql.Append(string.Join(", ", updatedGroupByColumns));
            }

            // HAVING clause
            if (_HavingClauses.Count > 0)
            {
                sql.Append(" HAVING ");
                sql.Append(string.Join(" AND ", _HavingClauses));
            }

            // ORDER BY clause
            if (_OrderByClauses.Count > 0)
            {
                sql.Append(" ORDER BY ");
                IEnumerable<string> orderParts = _OrderByClauses.Select(o => 
                    $"t0.{o.Column} {(o.Ascending ? "ASC" : "DESC")}");
                sql.Append(string.Join(", ", orderParts));
            }

            // LIMIT/OFFSET
            if (_TakeCount.HasValue)
            {
                sql.Append($" LIMIT {_TakeCount.Value}");
            }

            if (_SkipCount.HasValue)
            {
                sql.Append($" OFFSET {_SkipCount.Value}");
            }

            return sql.ToString();
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
                    funcSql.Append(windowFunc.Column);
                }
                
                if (windowFunc.Parameters.ContainsKey("offset"))
                {
                    funcSql.Append(", ");
                    funcSql.Append(windowFunc.Parameters["offset"]);
                }
                
                if (windowFunc.Parameters.ContainsKey("default"))
                {
                    funcSql.Append(", ");
                    object defaultValue = windowFunc.Parameters["default"];
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
            else if (windowFunc.FunctionName == "NTH_VALUE")
            {
                if (!string.IsNullOrEmpty(windowFunc.Column))
                {
                    funcSql.Append(windowFunc.Column);
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
                    funcSql.Append(windowFunc.Column);
                }
            }
            
            funcSql.Append(") OVER (");
            
            if (windowFunc.PartitionByColumns.Count > 0)
            {
                funcSql.Append("PARTITION BY ");
                funcSql.Append(string.Join(", ", windowFunc.PartitionByColumns));
            }
            
            if (windowFunc.OrderByColumns.Count > 0)
            {
                if (windowFunc.PartitionByColumns.Count > 0)
                {
                    funcSql.Append(" ");
                }
                funcSql.Append("ORDER BY ");
                IEnumerable<string> orderParts = windowFunc.OrderByColumns.Select(o => 
                    $"{o.Column} {(o.Ascending ? "ASC" : "DESC")}");
                funcSql.Append(string.Join(", ", orderParts));
            }
            
            if (windowFunc.Frame != null)
            {
                funcSql.Append(" ");
                funcSql.Append(BuildWindowFrameSql(windowFunc.Frame));
            }
            
            funcSql.Append(") AS ");
            funcSql.Append(windowFunc.Alias);
            
            return funcSql.ToString();
        }

        private string BuildWindowFrameSql(WindowFrame frame)
        {
            StringBuilder frameSql = new StringBuilder();
            
            switch (frame.Type)
            {
                case WindowFrameType.Rows:
                    frameSql.Append("ROWS ");
                    break;
                case WindowFrameType.Range:
                    frameSql.Append("RANGE ");
                    break;
                case WindowFrameType.Groups:
                    frameSql.Append("GROUPS ");
                    break;
            }
            
            if (frame.EndBound != null)
            {
                frameSql.Append("BETWEEN ");
                frameSql.Append(BuildWindowBoundSql(frame.StartBound));
                frameSql.Append(" AND ");
                frameSql.Append(BuildWindowBoundSql(frame.EndBound));
            }
            else
            {
                frameSql.Append(BuildWindowBoundSql(frame.StartBound));
            }
            
            return frameSql.ToString();
        }

        private string BuildWindowBoundSql(WindowFrameBound bound)
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

        internal List<string> GetGroupByColumns() => _GroupByColumns;
        internal List<string> GetWhereClauses() => _WhereClauses;

        #endregion

        #region Private-Methods

        private string GetPropertyName<TProperty>(Expression<Func<TEntity, TProperty>> expression)
        {
            if (expression.Body is MemberExpression memberExpression)
            {
                return memberExpression.Member.Name;
            }
            throw new ArgumentException("Expression must be a member expression");
        }
        
        private string GetPropertyNameFromExpression<TSource, TProperty>(Expression<Func<TSource, TProperty>> expression)
        {
            if (expression.Body is MemberExpression memberExpression)
            {
                return memberExpression.Member.Name;
            }
            throw new ArgumentException("Expression must be a member expression");
        }

        private JoinBuilder.JoinResult GetOrBuildJoinResult()
        {
            if (_CachedJoinResult == null && _Includes.Count > 0)
            {
                _CachedJoinResult = _JoinBuilder.BuildJoinSql<TEntity>(_Repository._TableName, _Includes);
            }
            return _CachedJoinResult;
        }

        private string UpdateWhereClauseWithAlias(string whereClause, string alias)
        {
            // Update column references in WHERE clause to include table alias
            // This handles common patterns for column names in WHERE clauses
            
            // Simple column names that are not already aliased
            // Look for patterns like "id = " or "name LIKE " etc.
            string result = whereClause;
            
            // Get all column names from the entity
            foreach (KeyValuePair<string, PropertyInfo> mapping in _Repository._ColumnMappings)
            {
                string columnName = mapping.Key;
                
                // Replace unaliased column references with aliased ones
                // Match column name followed by space and operator (=, <, >, LIKE, IN, etc)
                string pattern = $@"\b{System.Text.RegularExpressions.Regex.Escape(columnName)}\b(?=\s*[=<>!]|\s+LIKE|\s+IN|\s+NOT|\s+IS)";
                result = System.Text.RegularExpressions.Regex.Replace(
                    result, 
                    pattern, 
                    $"{alias}.{columnName}",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase
                );
            }
            
            return result;
        }

        private List<TEntity> ProcessRawJoinResults(List<Dictionary<string, object>> rawResults, List<IncludeInfo> includeInfos)
        {
            Dictionary<object, TEntity> entityMap = new Dictionary<object, TEntity>();
            List<TEntity> results = new List<TEntity>();

            foreach (Dictionary<string, object> row in rawResults)
            {
                TEntity entity = MapRowToEntity(row);
                object entityKey = GetEntityKey(entity);

                if (!entityMap.ContainsKey(entityKey))
                {
                    entityMap[entityKey] = entity;
                    results.Add(entity);
                }
                else
                {
                    entity = entityMap[entityKey];
                }

                MapRelatedEntitiesFromRow(entity, row, includeInfos);
            }

            return results;
        }

        private TEntity MapRowToEntity(Dictionary<string, object> row)
        {
            TEntity entity = new TEntity();

            foreach (KeyValuePair<string, PropertyInfo> kvp in _Repository._ColumnMappings)
            {
                if (row.ContainsKey(kvp.Key) && row[kvp.Key] != null)
                {
                    object convertedValue = _Repository._DataTypeConverter.ConvertFromDatabase(
                        row[kvp.Key], kvp.Value.PropertyType, kvp.Value);
                    kvp.Value.SetValue(entity, convertedValue);
                }
            }

            return entity;
        }

        private void MapRelatedEntitiesFromRow(TEntity entity, Dictionary<string, object> row, List<IncludeInfo> includeInfos)
        {
            // Map related entities from the joined row data
            // This would use the column aliases generated by JoinBuilder
            foreach (IncludeInfo includeInfo in includeInfos)
            {
                if (!includeInfo.IsCollection)
                {
                    object relatedEntity = MapRelatedEntity(row, includeInfo);
                    if (relatedEntity != null)
                    {
                        includeInfo.NavigationProperty.SetValue(entity, relatedEntity);
                    }
                }
            }
        }

        private object MapRelatedEntity(Dictionary<string, object> row, IncludeInfo includeInfo)
        {
            object entity = Activator.CreateInstance(includeInfo.RelatedEntityType);
            bool hasValue = false;

            IncludeProcessor processor = new IncludeProcessor(_Repository._Sanitizer);
            Dictionary<string, PropertyInfo> columnMappings = processor.GetColumnMappings(includeInfo.RelatedEntityType);

            foreach (KeyValuePair<string, PropertyInfo> kvp in columnMappings)
            {
                string columnAlias = $"{includeInfo.JoinAlias}_{kvp.Key}";
                if (row.ContainsKey(columnAlias) && row[columnAlias] != null)
                {
                    hasValue = true;
                    object convertedValue = _Repository._DataTypeConverter.ConvertFromDatabase(
                        row[columnAlias], kvp.Value.PropertyType, kvp.Value);
                    kvp.Value.SetValue(entity, convertedValue);
                }
            }

            return hasValue ? entity : null;
        }

        private object GetEntityKey(TEntity entity)
        {
            PropertyInfo primaryKeyProp = _Repository._PrimaryKeyProperty;
            return primaryKeyProp.GetValue(entity);
        }

        #endregion
    }
}
