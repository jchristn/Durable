namespace Durable.Sqlite
{
    using Microsoft.Data.Sqlite;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
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
        private Expression _SelectExpression;
        private Type _SelectType;
        private int? _SkipCount;
        private int? _TakeCount;
        private bool _Distinct;
        private string _CachedSql;

        #endregion

        #region Constructors-and-Factories

        public SqliteQueryBuilder(SqliteRepository<TEntity> repository, ITransaction transaction = null)
        {
            _Repository = repository;
            _Transaction = transaction;
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
            _SelectExpression = selector;
            _SelectType = typeof(TResult);
            // Note: This would require a more complex implementation to properly handle projection
            // For now, we'll throw a not implemented exception
            throw new NotImplementedException("Select projection is not yet implemented");
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
            string propertyName = GetPropertyName(navigationProperty);
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
            (var connection, var command, var shouldReturnToPool) = _Repository.GetConnectionAndCommand(_Transaction);
            ConnectionResult connectionResult = new ConnectionResult(connection, command, shouldReturnToPool);
            try
            {
                connectionResult.Command.CommandText = BuildSql();
                using SqliteDataReader reader = connectionResult.Command.ExecuteReader();

                List<TEntity> results = new List<TEntity>();
                while (reader.Read())
                {
                    results.Add(_Repository.MapReaderToEntity(reader));
                }

                // Process includes if any (simplified version - full implementation would require multiple queries)
                if (_Includes.Count > 0 && connectionResult.ShouldDispose)
                {
                    ProcessIncludes(results, connectionResult.Connection);
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
            (var connection, var command, var shouldReturnToPool) = await _Repository.GetConnectionAndCommandAsync(_Transaction, token);
            ConnectionResult connectionResult = new ConnectionResult(connection, command, shouldReturnToPool);
            try
            {
                connectionResult.Command.CommandText = BuildSql();
                await using SqliteDataReader reader = await connectionResult.Command.ExecuteReaderAsync(token);

                List<TEntity> results = new List<TEntity>();
                while (await reader.ReadAsync(token))
                {
                    results.Add(_Repository.MapReaderToEntity(reader));
                }

                // Process includes if any
                if (_Includes.Count > 0 && connectionResult.ShouldDispose)
                {
                    await ProcessIncludesAsync(results, connectionResult.Connection, token);
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
            (var connection, var command, var shouldReturnToPool) = await _Repository.GetConnectionAndCommandAsync(_Transaction, token);
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

            if (_SelectExpression != null)
            {
                // Build projection columns - simplified version
                sql.Append("*");
            }
            else
            {
                sql.Append("*");
            }

            sql.Append($" FROM {_Repository._Sanitizer.SanitizeIdentifier(_Repository._TableName)}");

            // Add JOINs for includes
            foreach (string include in _Includes)
            {
                // Simplified - full implementation would generate proper JOIN clauses
            }

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

        internal List<string> GetGroupByColumns() => _GroupByColumns;
        internal List<string> GetWhereClauses() => _WhereClauses;

        #endregion

        #region Private-Methods

        private string GetPropertyName<TProp, TProperty>(Expression<Func<TProp, TProperty>> expression)
        {
            if (expression.Body is MemberExpression memberExpression)
            {
                return memberExpression.Member.Name;
            }
            throw new ArgumentException("Expression must be a member expression");
        }

        private void ProcessIncludes(List<TEntity> entities, SqliteConnection connection)
        {
            // Simplified include processing - full implementation would need to:
            // 1. Parse navigation properties
            // 2. Generate appropriate JOIN queries
            // 3. Map related entities
            // This is a placeholder for the complex logic required
        }

        private async Task ProcessIncludesAsync(List<TEntity> entities, SqliteConnection connection, CancellationToken token)
        {
            // Simplified async include processing
            await Task.CompletedTask;
        }

        #endregion
    }
}
