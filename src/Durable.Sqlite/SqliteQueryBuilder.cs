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

            sql.Append("SELECT ");
            if (_Distinct) sql.Append("DISTINCT ");
            
            if (_Includes.Count > 0)
            {
                JoinBuilder.JoinResult joinResult = GetOrBuildJoinResult();
                sql.Append(joinResult.SelectClause);
            }
            else
            {
                sql.Append("t0.*");
            }

            sql.Append($" FROM {_Repository._Sanitizer.SanitizeIdentifier(_Repository._TableName)} t0");

            // Add JOINs for includes
            if (_Includes.Count > 0)
            {
                JoinBuilder.JoinResult joinResult = GetOrBuildJoinResult();
                sql.Append(joinResult.JoinClause);
            }

            if (_WhereClauses.Count > 0)
            {
                sql.Append(" WHERE ");
                // Update WHERE clauses to use t0 alias
                List<string> updatedWhereClauses = _WhereClauses.Select(w => 
                    UpdateWhereClauseWithAlias(w, "t0")).ToList();
                sql.Append(string.Join(" AND ", updatedWhereClauses));
            }

            if (_GroupByColumns.Count > 0)
            {
                sql.Append(" GROUP BY ");
                // Update GROUP BY columns to use t0 alias
                List<string> updatedGroupByColumns = _GroupByColumns.Select(c => $"t0.{c}").ToList();
                sql.Append(string.Join(", ", updatedGroupByColumns));
            }

            if (_OrderByClauses.Count > 0)
            {
                sql.Append(" ORDER BY ");
                IEnumerable<string> orderParts = _OrderByClauses.Select(o => 
                    $"t0.{o.Column} {(o.Ascending ? "ASC" : "DESC")}");
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
            // This is a simplified implementation
            // A more robust implementation would parse the WHERE clause properly
            // For now, we'll just prepend the alias to column names that aren't already aliased
            return whereClause;
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
