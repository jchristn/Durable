namespace Durable.Sqlite
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;

    internal class SqliteGroupedQueryBuilder<TEntity, TKey> : IGroupedQueryBuilder<TEntity, TKey> where TEntity : class, new()
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private readonly SqliteRepository<TEntity> _Repository;
        private readonly SqliteQueryBuilder<TEntity> _QueryBuilder;
        private readonly List<string> _HavingClauses = new List<string>();
        private readonly Expression<Func<TEntity, TKey>> _KeySelector;
        private readonly List<string> _SelectColumns = new List<string>();
        private readonly List<AggregateInfo> _Aggregates = new List<AggregateInfo>();

        #endregion

        #region Constructors-and-Factories

        public SqliteGroupedQueryBuilder(SqliteRepository<TEntity> repository, SqliteQueryBuilder<TEntity> queryBuilder, Expression<Func<TEntity, TKey>> keySelector)
        {
            _Repository = repository;
            _QueryBuilder = queryBuilder;
            _KeySelector = keySelector;
        }

        #endregion

        #region Public-Methods

        public IGroupedQueryBuilder<TEntity, TKey> Having(Expression<Func<IGrouping<TKey, TEntity>, bool>> predicate)
        {
            string havingClause = ParseHavingExpression(predicate.Body);
            _HavingClauses.Add(havingClause);
            return this;
        }

        public IGroupedQueryBuilder<TEntity, TKey> Select<TResult>(Expression<Func<IGrouping<TKey, TEntity>, TResult>> selector)
        {
            ParseSelectExpression(selector.Body);
            return this;
        }

        public int Count(Expression<Func<TEntity, bool>> predicate = null)
        {
            (var connection, var command, var shouldReturnToPool) = _Repository.GetConnectionAndCommand(null);
            ConnectionResult connectionResult = new ConnectionResult(connection, command, shouldReturnToPool);
            try
            {
                connectionResult.Command.CommandText = BuildAggregateQuery("COUNT(*)", null, predicate);
                object result = connectionResult.Command.ExecuteScalar();
                return result == DBNull.Value || result == null ? 0 : Convert.ToInt32(result);
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

        public async Task<int> CountAsync(Expression<Func<TEntity, bool>> predicate = null, CancellationToken token = default)
        {
            (var connection, var command, var shouldReturnToPool) = await _Repository.GetConnectionAndCommandAsync(null, token);
            ConnectionResult connectionResult = new ConnectionResult(connection, command, shouldReturnToPool);
            try
            {
                connectionResult.Command.CommandText = BuildAggregateQuery("COUNT(*)", null, predicate);
                object result = await connectionResult.Command.ExecuteScalarAsync(token);
                return result == DBNull.Value || result == null ? 0 : Convert.ToInt32(result);
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

        public decimal Sum(Expression<Func<TEntity, decimal>> selector)
        {
            string column = _Repository.GetColumnFromExpression(selector.Body);
            (var connection, var command, var shouldReturnToPool) = _Repository.GetConnectionAndCommand(null);
            ConnectionResult connectionResult = new ConnectionResult(connection, command, shouldReturnToPool);
            try
            {
                connectionResult.Command.CommandText = BuildAggregateQuery("SUM", column, null);
                object result = connectionResult.Command.ExecuteScalar();
                return result == DBNull.Value || result == null ? 0m : Convert.ToDecimal(result);
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

        public async Task<decimal> SumAsync(Expression<Func<TEntity, decimal>> selector, CancellationToken token = default)
        {
            string column = _Repository.GetColumnFromExpression(selector.Body);
            (var connection, var command, var shouldReturnToPool) = await _Repository.GetConnectionAndCommandAsync(null, token);
            ConnectionResult connectionResult = new ConnectionResult(connection, command, shouldReturnToPool);
            try
            {
                connectionResult.Command.CommandText = BuildAggregateQuery("SUM", column, null);
                object result = await connectionResult.Command.ExecuteScalarAsync(token);
                return result == DBNull.Value || result == null ? 0m : Convert.ToDecimal(result);
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

        public decimal Average(Expression<Func<TEntity, decimal>> selector)
        {
            string column = _Repository.GetColumnFromExpression(selector.Body);
            (var connection, var command, var shouldReturnToPool) = _Repository.GetConnectionAndCommand(null);
            ConnectionResult connectionResult = new ConnectionResult(connection, command, shouldReturnToPool);
            try
            {
                connectionResult.Command.CommandText = BuildAggregateQuery("AVG", column, null);
                object result = connectionResult.Command.ExecuteScalar();
                return result == DBNull.Value || result == null ? 0m : Convert.ToDecimal(result);
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

        public async Task<decimal> AverageAsync(Expression<Func<TEntity, decimal>> selector, CancellationToken token = default)
        {
            string column = _Repository.GetColumnFromExpression(selector.Body);
            (var connection, var command, var shouldReturnToPool) = await _Repository.GetConnectionAndCommandAsync(null, token);
            ConnectionResult connectionResult = new ConnectionResult(connection, command, shouldReturnToPool);
            try
            {
                connectionResult.Command.CommandText = BuildAggregateQuery("AVG", column, null);
                object result = await connectionResult.Command.ExecuteScalarAsync(token);
                return result == DBNull.Value || result == null ? 0m : Convert.ToDecimal(result);
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

        public TResult Max<TResult>(Expression<Func<TEntity, TResult>> selector)
        {
            string column = _Repository.GetColumnFromExpression(selector.Body);
            (var connection, var command, var shouldReturnToPool) = _Repository.GetConnectionAndCommand(null);
            ConnectionResult connectionResult = new ConnectionResult(connection, command, shouldReturnToPool);
            try
            {
                connectionResult.Command.CommandText = BuildAggregateQuery("MAX", column, null);
                object result = connectionResult.Command.ExecuteScalar();
                return result == DBNull.Value || result == null ? default(TResult) : (TResult)Convert.ChangeType(result, typeof(TResult));
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

        public async Task<TResult> MaxAsync<TResult>(Expression<Func<TEntity, TResult>> selector, CancellationToken token = default)
        {
            string column = _Repository.GetColumnFromExpression(selector.Body);
            (var connection, var command, var shouldReturnToPool) = await _Repository.GetConnectionAndCommandAsync(null, token);
            ConnectionResult connectionResult = new ConnectionResult(connection, command, shouldReturnToPool);
            try
            {
                connectionResult.Command.CommandText = BuildAggregateQuery("MAX", column, null);
                object result = await connectionResult.Command.ExecuteScalarAsync(token);
                return result == DBNull.Value || result == null ? default(TResult) : (TResult)Convert.ChangeType(result, typeof(TResult));
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

        public TResult Min<TResult>(Expression<Func<TEntity, TResult>> selector)
        {
            string column = _Repository.GetColumnFromExpression(selector.Body);
            (var connection, var command, var shouldReturnToPool) = _Repository.GetConnectionAndCommand(null);
            ConnectionResult connectionResult = new ConnectionResult(connection, command, shouldReturnToPool);
            try
            {
                connectionResult.Command.CommandText = BuildAggregateQuery("MIN", column, null);
                object result = connectionResult.Command.ExecuteScalar();
                return result == DBNull.Value || result == null ? default(TResult) : (TResult)Convert.ChangeType(result, typeof(TResult));
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

        public async Task<TResult> MinAsync<TResult>(Expression<Func<TEntity, TResult>> selector, CancellationToken token = default)
        {
            string column = _Repository.GetColumnFromExpression(selector.Body);
            (var connection, var command, var shouldReturnToPool) = await _Repository.GetConnectionAndCommandAsync(null, token);
            ConnectionResult connectionResult = new ConnectionResult(connection, command, shouldReturnToPool);
            try
            {
                connectionResult.Command.CommandText = BuildAggregateQuery("MIN", column, null);
                object result = await connectionResult.Command.ExecuteScalarAsync(token);
                return result == DBNull.Value || result == null ? default(TResult) : (TResult)Convert.ChangeType(result, typeof(TResult));
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

        public IEnumerable<IGrouping<TKey, TEntity>> Execute()
        {
            if (_HavingClauses.Count > 0)
            {
                // When HAVING clauses exist, we need to use SQL GROUP BY to filter groups
                // then fetch entities for the qualifying groups
                return ExecuteWithSqlGroupBy();
            }
            else
            {
                // Without HAVING clauses, we can fetch all entities and group in memory
                // This allows us to return the full entity data in each group
                IEnumerable<TEntity> allEntities = _QueryBuilder.Execute();
                Func<TEntity, TKey> keyExtractor = _KeySelector.Compile();
                
                return allEntities.GroupBy(keyExtractor)
                                 .Select(g => new Grouping<TKey, TEntity>(g.Key, g));
            }
        }

        public async Task<IEnumerable<IGrouping<TKey, TEntity>>> ExecuteAsync(CancellationToken token = default)
        {
            if (_HavingClauses.Count > 0)
            {
                // When HAVING clauses exist, we need to use SQL GROUP BY to filter groups
                // then fetch entities for the qualifying groups
                return await ExecuteWithSqlGroupByAsync(token);
            }
            else
            {
                // Without HAVING clauses, we can fetch all entities and group in memory
                // This allows us to return the full entity data in each group
                IEnumerable<TEntity> allEntities = await _QueryBuilder.ExecuteAsync(token);
                Func<TEntity, TKey> keyExtractor = _KeySelector.Compile();
                
                return allEntities.GroupBy(keyExtractor)
                                 .Select(g => new Grouping<TKey, TEntity>(g.Key, g));
            }
        }

        #endregion

        #region Private-Methods

        private IEnumerable<IGrouping<TKey, TEntity>> ExecuteWithSqlGroupBy()
        {
            // First, get the qualifying group keys using SQL GROUP BY with HAVING
            HashSet<TKey> qualifyingKeys = GetQualifyingGroupKeys();
            
            if (qualifyingKeys.Count == 0)
            {
                return new List<IGrouping<TKey, TEntity>>();
            }
            
            // Then fetch all entities and filter to only qualifying groups
            IEnumerable<TEntity> allEntities = _QueryBuilder.Execute();
            Func<TEntity, TKey> keyExtractor = _KeySelector.Compile();
            
            return allEntities.GroupBy(keyExtractor)
                             .Where(g => qualifyingKeys.Contains(g.Key))
                             .Select(g => new Grouping<TKey, TEntity>(g.Key, g));
        }

        private async Task<IEnumerable<IGrouping<TKey, TEntity>>> ExecuteWithSqlGroupByAsync(CancellationToken token)
        {
            // First, get the qualifying group keys using SQL GROUP BY with HAVING
            HashSet<TKey> qualifyingKeys = await GetQualifyingGroupKeysAsync(token);
            
            if (qualifyingKeys.Count == 0)
            {
                return new List<IGrouping<TKey, TEntity>>();
            }
            
            // Then fetch all entities and filter to only qualifying groups
            IEnumerable<TEntity> allEntities = await _QueryBuilder.ExecuteAsync(token);
            Func<TEntity, TKey> keyExtractor = _KeySelector.Compile();
            
            return allEntities.GroupBy(keyExtractor)
                             .Where(g => qualifyingKeys.Contains(g.Key))
                             .Select(g => new Grouping<TKey, TEntity>(g.Key, g));
        }

        private HashSet<TKey> GetQualifyingGroupKeys()
        {
            (var connection, var command, var shouldReturnToPool) = _Repository.GetConnectionAndCommand(null);
            ConnectionResult connectionResult = new ConnectionResult(connection, command, shouldReturnToPool);
            try
            {
                connectionResult.Command.CommandText = BuildGroupKeysSql();
                using SqliteDataReader reader = connectionResult.Command.ExecuteReader();
                
                HashSet<TKey> keys = new HashSet<TKey>();
                while (reader.Read())
                {
                    TKey key = (TKey)reader.GetValue(0);
                    keys.Add(key);
                }
                return keys;
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

        private async Task<HashSet<TKey>> GetQualifyingGroupKeysAsync(CancellationToken token)
        {
            (var connection, var command, var shouldReturnToPool) = await _Repository.GetConnectionAndCommandAsync(null, token);
            ConnectionResult connectionResult = new ConnectionResult(connection, command, shouldReturnToPool);
            try
            {
                connectionResult.Command.CommandText = BuildGroupKeysSql();
                await using SqliteDataReader reader = await connectionResult.Command.ExecuteReaderAsync(token);
                
                HashSet<TKey> keys = new HashSet<TKey>();
                while (await reader.ReadAsync(token))
                {
                    TKey key = (TKey)reader.GetValue(0);
                    keys.Add(key);
                }
                return keys;
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

        private string BuildGroupKeysSql()
        {
            StringBuilder sql = new StringBuilder();
            List<string> groupByColumns = _QueryBuilder.GetGroupByColumns();
            
            if (groupByColumns.Count == 0)
            {
                throw new InvalidOperationException("Cannot use HAVING clause without GROUP BY columns");
            }
            
            sql.Append("SELECT ");
            sql.Append(string.Join(", ", groupByColumns));
            sql.Append($" FROM {_Repository._Sanitizer.SanitizeIdentifier(_Repository._TableName)}");
            
            List<string> whereClauses = _QueryBuilder.GetWhereClauses();
            if (whereClauses.Count > 0)
            {
                sql.Append(" WHERE ");
                sql.Append(string.Join(" AND ", whereClauses));
            }
            
            sql.Append(" GROUP BY ");
            sql.Append(string.Join(", ", groupByColumns));
            
            if (_HavingClauses.Count > 0)
            {
                sql.Append(" HAVING ");
                sql.Append(string.Join(" AND ", _HavingClauses));
            }
            
            sql.Append(";");
            return sql.ToString();
        }

        private string BuildGroupBySql()
        {
            StringBuilder sql = new StringBuilder();
            sql.Append("SELECT ");
            
            List<string> groupByColumns = _QueryBuilder.GetGroupByColumns();
            
            if (_SelectColumns.Count > 0)
            {
                sql.Append(string.Join(", ", _SelectColumns));
            }
            else if (groupByColumns.Count > 0)
            {
                sql.Append(string.Join(", ", groupByColumns));
            }
            else
            {
                sql.Append("*");
            }
            
            sql.Append($" FROM {_Repository._Sanitizer.SanitizeIdentifier(_Repository._TableName)}");
            
            List<string> whereClauses = _QueryBuilder.GetWhereClauses();
            if (whereClauses.Count > 0)
            {
                sql.Append(" WHERE ");
                sql.Append(string.Join(" AND ", whereClauses));
            }
            
            if (groupByColumns.Count > 0)
            {
                sql.Append(" GROUP BY ");
                sql.Append(string.Join(", ", groupByColumns));
            }
            
            if (_HavingClauses.Count > 0)
            {
                sql.Append(" HAVING ");
                sql.Append(string.Join(" AND ", _HavingClauses));
            }
            
            sql.Append(";");
            return sql.ToString();
        }

        private string ParseHavingExpression(Expression expression)
        {
            if (expression is BinaryExpression binaryExpr)
            {
                string left = ParseHavingExpression(binaryExpr.Left);
                string right = ParseHavingExpression(binaryExpr.Right);
                string op = GetOperator(binaryExpr.NodeType);
                
                if (binaryExpr.NodeType == ExpressionType.AndAlso || binaryExpr.NodeType == ExpressionType.OrElse)
                {
                    return $"({left} {op} {right})";
                }
                return $"{left} {op} {right}";
            }
            else if (expression is MethodCallExpression methodCall)
            {
                if (methodCall.Method.Name == "Count" && methodCall.Arguments.Count == 1)
                {
                    return "COUNT(*)";
                }
                else if (methodCall.Method.Name == "Sum" && methodCall.Arguments.Count == 2)
                {
                    if (methodCall.Arguments[1] is UnaryExpression unary && unary.Operand is LambdaExpression lambda)
                    {
                        string column = _Repository.GetColumnFromExpression(lambda.Body);
                        return $"SUM({column})";
                    }
                }
                else if (methodCall.Method.Name == "Average" && methodCall.Arguments.Count == 2)
                {
                    if (methodCall.Arguments[1] is UnaryExpression unary && unary.Operand is LambdaExpression lambda)
                    {
                        string column = _Repository.GetColumnFromExpression(lambda.Body);
                        return $"AVG({column})";
                    }
                }
                else if (methodCall.Method.Name == "Max" && methodCall.Arguments.Count == 2)
                {
                    if (methodCall.Arguments[1] is UnaryExpression unary && unary.Operand is LambdaExpression lambda)
                    {
                        string column = _Repository.GetColumnFromExpression(lambda.Body);
                        return $"MAX({column})";
                    }
                }
                else if (methodCall.Method.Name == "Min" && methodCall.Arguments.Count == 2)
                {
                    if (methodCall.Arguments[1] is UnaryExpression unary && unary.Operand is LambdaExpression lambda)
                    {
                        string column = _Repository.GetColumnFromExpression(lambda.Body);
                        return $"MIN({column})";
                    }
                }
            }
            else if (expression is ConstantExpression constant)
            {
                return FormatValue(constant.Value);
            }
            else if (expression is MemberExpression member)
            {
                if (member.Member.Name == "Key")
                {
                    List<string> groupByColumns = _QueryBuilder.GetGroupByColumns();
                    if (groupByColumns.Count > 0)
                    {
                        return groupByColumns[0];
                    }
                }
            }
            
            throw new NotSupportedException($"Expression type {expression.GetType().Name} is not supported in HAVING clause");
        }

        private string GetOperator(ExpressionType nodeType)
        {
            switch (nodeType)
            {
                case ExpressionType.Equal:
                    return "=";
                case ExpressionType.NotEqual:
                    return "!=";
                case ExpressionType.LessThan:
                    return "<";
                case ExpressionType.LessThanOrEqual:
                    return "<=";
                case ExpressionType.GreaterThan:
                    return ">";
                case ExpressionType.GreaterThanOrEqual:
                    return ">=";
                case ExpressionType.AndAlso:
                    return "AND";
                case ExpressionType.OrElse:
                    return "OR";
                default:
                    throw new NotSupportedException($"Operator {nodeType} is not supported");
            }
        }

        private string FormatValue(object value)
        {
            if (value == null)
                return "NULL";
            if (value is string str)
                return $"'{str.Replace("'", "''")}'"; 
            if (value is DateTime dt)
                return $"'{dt:yyyy-MM-dd HH:mm:ss}'";
            if (value is bool b)
                return b ? "1" : "0";
            return value.ToString();
        }

        private TEntity MapReaderToEntity(SqliteDataReader reader)
        {
            return _Repository.MapReaderToEntity(reader);
        }

        private string BuildAggregateQuery(string aggregateFunction, string column, Expression<Func<TEntity, bool>> predicate)
        {
            // Aggregate methods return single values across the entire dataset that would be grouped
            // They don't use GROUP BY clauses since they return totals, not per-group values
            StringBuilder sql = new StringBuilder();
            
            sql.Append("SELECT ");
            
            if (column != null)
            {
                sql.Append($"{aggregateFunction}({column})");
            }
            else
            {
                sql.Append(aggregateFunction);
            }
            
            sql.Append($" FROM {_Repository._Sanitizer.SanitizeIdentifier(_Repository._TableName)}");
            
            List<string> whereClauses = _QueryBuilder.GetWhereClauses();
            if (predicate != null)
            {
                string predicateClause = _Repository.BuildWhereClause(predicate);
                whereClauses.Add(predicateClause);
            }
            
            if (whereClauses.Count > 0)
            {
                sql.Append(" WHERE ");
                sql.Append(string.Join(" AND ", whereClauses));
            }
            
            // Note: No GROUP BY or HAVING for aggregate methods
            // They return single values across all data that matches WHERE conditions
            
            sql.Append(";");
            return sql.ToString();
        }

        private void ParseSelectExpression(Expression expression)
        {
            if (expression is NewExpression newExpr)
            {
                _SelectColumns.Clear();
                List<string> groupByColumns = _QueryBuilder.GetGroupByColumns();
                
                if (groupByColumns.Count > 0)
                {
                    _SelectColumns.AddRange(groupByColumns);
                }
                
                foreach (Expression arg in newExpr.Arguments)
                {
                    if (arg is MethodCallExpression methodCall)
                    {
                        string aggregate = ParseAggregateMethod(methodCall);
                        if (aggregate != null && !_SelectColumns.Contains(aggregate))
                        {
                            _SelectColumns.Add(aggregate);
                        }
                    }
                    else if (arg is MemberExpression member && member.Member.Name == "Key")
                    {
                        // Key is already included in groupByColumns
                    }
                }
            }
            else if (expression is MemberInitExpression memberInit)
            {
                _SelectColumns.Clear();
                List<string> groupByColumns = _QueryBuilder.GetGroupByColumns();
                
                if (groupByColumns.Count > 0)
                {
                    _SelectColumns.AddRange(groupByColumns);
                }
                
                foreach (MemberBinding binding in memberInit.Bindings)
                {
                    if (binding is MemberAssignment assignment)
                    {
                        if (assignment.Expression is MethodCallExpression methodCall)
                        {
                            string aggregate = ParseAggregateMethod(methodCall);
                            if (aggregate != null)
                            {
                                string alias = assignment.Member.Name;
                                _SelectColumns.Add($"{aggregate} AS {alias}");
                                _Aggregates.Add(new AggregateInfo { Function = aggregate, Alias = alias });
                            }
                        }
                    }
                }
            }
        }

        private string ParseAggregateMethod(MethodCallExpression methodCall)
        {
            if (methodCall.Method.Name == "Count")
            {
                return "COUNT(*)";
            }
            else if (methodCall.Method.Name == "Sum" && methodCall.Arguments.Count == 2)
            {
                if (methodCall.Arguments[1] is UnaryExpression unary && unary.Operand is LambdaExpression lambda)
                {
                    string column = _Repository.GetColumnFromExpression(lambda.Body);
                    return $"SUM({column})";
                }
            }
            else if (methodCall.Method.Name == "Average" && methodCall.Arguments.Count == 2)
            {
                if (methodCall.Arguments[1] is UnaryExpression unary && unary.Operand is LambdaExpression lambda)
                {
                    string column = _Repository.GetColumnFromExpression(lambda.Body);
                    return $"AVG({column})";
                }
            }
            else if (methodCall.Method.Name == "Max" && methodCall.Arguments.Count == 2)
            {
                if (methodCall.Arguments[1] is UnaryExpression unary && unary.Operand is LambdaExpression lambda)
                {
                    string column = _Repository.GetColumnFromExpression(lambda.Body);
                    return $"MAX({column})";
                }
            }
            else if (methodCall.Method.Name == "Min" && methodCall.Arguments.Count == 2)
            {
                if (methodCall.Arguments[1] is UnaryExpression unary && unary.Operand is LambdaExpression lambda)
                {
                    string column = _Repository.GetColumnFromExpression(lambda.Body);
                    return $"MIN({column})";
                }
            }
            return null;
        }

        private struct AggregateInfo
        {
            public string Function { get; set; }
            public string Column { get; set; }
            public string Alias { get; set; }
        }

        #endregion
    }

    internal class Grouping<TKey, TElement> : IGrouping<TKey, TElement>
    {
        private readonly TKey _Key;
        private readonly List<TElement> _Elements;

        public Grouping(TKey key, IEnumerable<TElement> elements)
        {
            _Key = key;
            _Elements = new List<TElement>(elements);
        }

        public TKey Key => _Key;

        public IEnumerator<TElement> GetEnumerator()
        {
            return _Elements.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
