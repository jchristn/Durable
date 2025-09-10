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

        public IQueryBuilder<TResult> Where(Expression<Func<TResult, bool>> predicate)
        {
            // For projected queries, we need to translate the predicate back to TEntity
            // This is complex and may not be supported for all cases
            throw new NotSupportedException("Where clause on projected query is not supported. Apply Where before Select.");
        }

        public IQueryBuilder<TResult> OrderBy<TKey>(Expression<Func<TResult, TKey>> keySelector)
        {
            string column = GetProjectedColumn(keySelector.Body);
            _OrderByClauses.Add(new OrderByClause { Column = column, Ascending = true });
            return this;
        }

        public IQueryBuilder<TResult> OrderByDescending<TKey>(Expression<Func<TResult, TKey>> keySelector)
        {
            string column = GetProjectedColumn(keySelector.Body);
            _OrderByClauses.Add(new OrderByClause { Column = column, Ascending = false });
            return this;
        }

        public IQueryBuilder<TResult> ThenBy<TKey>(Expression<Func<TResult, TKey>> keySelector)
        {
            if (_OrderByClauses.Count == 0)
                throw new InvalidOperationException("ThenBy can only be used after OrderBy or OrderByDescending");

            string column = GetProjectedColumn(keySelector.Body);
            _OrderByClauses.Add(new OrderByClause { Column = column, Ascending = true });
            return this;
        }

        public IQueryBuilder<TResult> ThenByDescending<TKey>(Expression<Func<TResult, TKey>> keySelector)
        {
            if (_OrderByClauses.Count == 0)
                throw new InvalidOperationException("ThenByDescending can only be used after OrderBy or OrderByDescending");

            string column = GetProjectedColumn(keySelector.Body);
            _OrderByClauses.Add(new OrderByClause { Column = column, Ascending = false });
            return this;
        }

        public IQueryBuilder<TResult> Skip(int count)
        {
            _SkipCount = count;
            return this;
        }

        public IQueryBuilder<TResult> Take(int count)
        {
            _TakeCount = count;
            return this;
        }

        public IQueryBuilder<TResult> Distinct()
        {
            _Distinct = true;
            return this;
        }

        public IQueryBuilder<TNewResult> Select<TNewResult>(Expression<Func<TResult, TNewResult>> selector) where TNewResult : class, new()
        {
            // Chaining projections would require composing the expressions
            throw new NotSupportedException("Chaining Select operations is not supported. Apply all projections in a single Select.");
        }

        public IQueryBuilder<TResult> Include<TProperty>(Expression<Func<TResult, TProperty>> navigationProperty)
        {
            // Including on projected results is complex and may not be meaningful
            throw new NotSupportedException("Include on projected query is not supported. Apply Include before Select.");
        }

        public IQueryBuilder<TResult> ThenInclude<TPreviousProperty, TProperty>(Expression<Func<TPreviousProperty, TProperty>> navigationProperty)
        {
            throw new NotSupportedException("ThenInclude on projected query is not supported. Apply ThenInclude before Select.");
        }

        public IGroupedQueryBuilder<TResult, TKey> GroupBy<TKey>(Expression<Func<TResult, TKey>> keySelector)
        {
            throw new NotSupportedException("GroupBy on projected query is not supported. Apply GroupBy before Select.");
        }

        public IQueryBuilder<TResult> Having(Expression<Func<TResult, bool>> predicate)
        {
            throw new NotSupportedException("Having on projected query is not supported. Apply Having before Select.");
        }

        // Set operations
        public IQueryBuilder<TResult> Union(IQueryBuilder<TResult> other)
        {
            throw new NotSupportedException("Union on projected query is not supported. Apply Union before Select.");
        }

        public IQueryBuilder<TResult> UnionAll(IQueryBuilder<TResult> other)
        {
            throw new NotSupportedException("UnionAll on projected query is not supported. Apply UnionAll before Select.");
        }

        public IQueryBuilder<TResult> Intersect(IQueryBuilder<TResult> other)
        {
            throw new NotSupportedException("Intersect on projected query is not supported. Apply Intersect before Select.");
        }

        public IQueryBuilder<TResult> Except(IQueryBuilder<TResult> other)
        {
            throw new NotSupportedException("Except on projected query is not supported. Apply Except before Select.");
        }

        // Subquery support
        public IQueryBuilder<TResult> WhereIn<TKey>(Expression<Func<TResult, TKey>> keySelector, IQueryBuilder<TKey> subquery) where TKey : class, new()
        {
            throw new NotSupportedException("WhereIn on projected query is not supported. Apply WhereIn before Select.");
        }

        public IQueryBuilder<TResult> WhereNotIn<TKey>(Expression<Func<TResult, TKey>> keySelector, IQueryBuilder<TKey> subquery) where TKey : class, new()
        {
            throw new NotSupportedException("WhereNotIn on projected query is not supported. Apply WhereNotIn before Select.");
        }

        public IQueryBuilder<TResult> WhereInRaw<TKey>(Expression<Func<TResult, TKey>> keySelector, string subquerySql)
        {
            throw new NotSupportedException("WhereInRaw on projected query is not supported. Apply WhereInRaw before Select.");
        }

        public IQueryBuilder<TResult> WhereNotInRaw<TKey>(Expression<Func<TResult, TKey>> keySelector, string subquerySql)
        {
            throw new NotSupportedException("WhereNotInRaw on projected query is not supported. Apply WhereNotInRaw before Select.");
        }

        public IQueryBuilder<TResult> WhereExists<TOther>(IQueryBuilder<TOther> subquery) where TOther : class, new()
        {
            throw new NotSupportedException("WhereExists on projected query is not supported. Apply WhereExists before Select.");
        }

        public IQueryBuilder<TResult> WhereNotExists<TOther>(IQueryBuilder<TOther> subquery) where TOther : class, new()
        {
            throw new NotSupportedException("WhereNotExists on projected query is not supported. Apply WhereNotExists before Select.");
        }

        // Window functions
        public IWindowedQueryBuilder<TResult> WithWindowFunction(string functionName, string partitionBy = null, string orderBy = null)
        {
            throw new NotSupportedException("Window functions on projected query is not supported. Apply window functions before Select.");
        }

        // CTEs
        public IQueryBuilder<TResult> WithCte(string cteName, string cteQuery)
        {
            throw new NotSupportedException("CTEs on projected query is not supported. Apply CTEs before Select.");
        }

        public IQueryBuilder<TResult> WithRecursiveCte(string cteName, string anchorQuery, string recursiveQuery)
        {
            throw new NotSupportedException("Recursive CTEs on projected query is not supported. Apply recursive CTEs before Select.");
        }

        // Custom SQL fragments
        public IQueryBuilder<TResult> WhereRaw(string sql, params object[] parameters)
        {
            throw new NotSupportedException("WhereRaw on projected query is not supported. Apply WhereRaw before Select.");
        }

        public IQueryBuilder<TResult> SelectRaw(string sql)
        {
            throw new NotSupportedException("SelectRaw on projected query is not supported. Use a single Select with projection.");
        }

        public IQueryBuilder<TResult> FromRaw(string sql)
        {
            throw new NotSupportedException("FromRaw on projected query is not supported. Apply FromRaw before Select.");
        }

        public IQueryBuilder<TResult> JoinRaw(string sql)
        {
            throw new NotSupportedException("JoinRaw on projected query is not supported. Apply JoinRaw before Select.");
        }

        // CASE WHEN expressions
        public ICaseExpressionBuilder<TResult> SelectCase()
        {
            throw new NotSupportedException("CASE expressions on projected query is not supported. Apply CASE expressions before Select.");
        }

        public IEnumerable<TResult> Execute()
        {
            (SqliteConnection connection, SqliteCommand command, bool shouldReturnToPool) = _Repository.GetConnectionAndCommand(_Transaction);
            ConnectionResult connectionResult = new ConnectionResult(connection, command, shouldReturnToPool);
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

        public async Task<IEnumerable<TResult>> ExecuteAsync(CancellationToken token = default)
        {
            (SqliteConnection connection, SqliteCommand command, bool shouldReturnToPool) = await _Repository.GetConnectionAndCommandAsync(_Transaction, token);
            ConnectionResult connectionResult = new ConnectionResult(connection, command, shouldReturnToPool);
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

        public async IAsyncEnumerable<TResult> ExecuteAsyncEnumerable([EnumeratorCancellation] CancellationToken token = default)
        {
            (SqliteConnection connection, SqliteCommand command, bool shouldReturnToPool) = await _Repository.GetConnectionAndCommandAsync(_Transaction, token);
            ConnectionResult connectionResult = new ConnectionResult(connection, command, shouldReturnToPool);
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

        public string Query
        {
            get
            {
                if (_CachedSql == null)
                    _CachedSql = BuildSql();
                return _CachedSql;
            }
        }

        public IDurableResult<TResult> ExecuteWithQuery()
        {
            string query = Query;
            IEnumerable<TResult> results = Execute();
            return new DurableResult<TResult>(query, results);
        }

        public async Task<IDurableResult<TResult>> ExecuteWithQueryAsync(CancellationToken token = default)
        {
            string query = Query;
            IEnumerable<TResult> results = await ExecuteAsync(token);
            return new DurableResult<TResult>(query, results);
        }

        public IAsyncDurableResult<TResult> ExecuteAsyncEnumerableWithQuery(CancellationToken token = default)
        {
            string query = Query;
            IAsyncEnumerable<TResult> results = ExecuteAsyncEnumerable(token);
            return new AsyncDurableResult<TResult>(query, results);
        }

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