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
        private readonly SqliteRepository<TEntity> _repository;
        private readonly ITransaction _transaction;
        private readonly List<string> _whereClauses = new List<string>();
        private readonly List<OrderByClause> _orderByClauses = new List<OrderByClause>();
        private readonly List<string> _includes = new List<string>();
        private readonly List<string> _groupByColumns = new List<string>();
        private Expression _selectExpression;
        private Type _selectType;
        private int? _skipCount;
        private int? _takeCount;
        private bool _distinct;

        public SqliteQueryBuilder(SqliteRepository<TEntity> repository, ITransaction transaction = null)
        {
            _repository = repository;
            _transaction = transaction;
        }

        public IQueryBuilder<TEntity> Where(Expression<Func<TEntity, bool>> predicate)
        {
            var whereClause = _repository.BuildWhereClause(predicate);
            _whereClauses.Add(whereClause);
            return this;
        }

        public IQueryBuilder<TEntity> OrderBy<TKey>(Expression<Func<TEntity, TKey>> keySelector)
        {
            var column = _repository.GetColumnFromExpression(keySelector.Body);
            _orderByClauses.Add(new OrderByClause { Column = column, Ascending = true });
            return this;
        }

        public IQueryBuilder<TEntity> OrderByDescending<TKey>(Expression<Func<TEntity, TKey>> keySelector)
        {
            var column = _repository.GetColumnFromExpression(keySelector.Body);
            _orderByClauses.Add(new OrderByClause { Column = column, Ascending = false });
            return this;
        }

        public IQueryBuilder<TEntity> ThenBy<TKey>(Expression<Func<TEntity, TKey>> keySelector)
        {
            if (_orderByClauses.Count == 0)
                throw new InvalidOperationException("ThenBy can only be used after OrderBy or OrderByDescending");

            var column = _repository.GetColumnFromExpression(keySelector.Body);
            _orderByClauses.Add(new OrderByClause { Column = column, Ascending = true });
            return this;
        }

        public IQueryBuilder<TEntity> ThenByDescending<TKey>(Expression<Func<TEntity, TKey>> keySelector)
        {
            if (_orderByClauses.Count == 0)
                throw new InvalidOperationException("ThenByDescending can only be used after OrderBy or OrderByDescending");

            var column = _repository.GetColumnFromExpression(keySelector.Body);
            _orderByClauses.Add(new OrderByClause { Column = column, Ascending = false });
            return this;
        }

        public IQueryBuilder<TEntity> Skip(int count)
        {
            _skipCount = count;
            return this;
        }

        public IQueryBuilder<TEntity> Take(int count)
        {
            _takeCount = count;
            return this;
        }

        public IQueryBuilder<TEntity> Distinct()
        {
            _distinct = true;
            return this;
        }

        public IQueryBuilder<TResult> Select<TResult>(Expression<Func<TEntity, TResult>> selector) where TResult : class, new()
        {
            _selectExpression = selector;
            _selectType = typeof(TResult);
            // Note: This would require a more complex implementation to properly handle projection
            // For now, we'll throw a not implemented exception
            throw new NotImplementedException("Select projection is not yet implemented");
        }

        public IQueryBuilder<TEntity> Include<TProperty>(Expression<Func<TEntity, TProperty>> navigationProperty)
        {
            // Store include information for later processing
            var propertyName = GetPropertyName(navigationProperty);
            _includes.Add(propertyName);
            return this;
        }

        public IQueryBuilder<TEntity> ThenInclude<TPreviousProperty, TProperty>(Expression<Func<TPreviousProperty, TProperty>> navigationProperty)
        {
            // Store nested include information
            var propertyName = GetPropertyName(navigationProperty);
            if (_includes.Count > 0)
            {
                _includes[_includes.Count - 1] += "." + propertyName;
            }
            return this;
        }

        public IGroupedQueryBuilder<TEntity, TKey> GroupBy<TKey>(Expression<Func<TEntity, TKey>> keySelector)
        {
            var column = _repository.GetColumnFromExpression(keySelector.Body);
            _groupByColumns.Add(column);
            return new SqliteGroupedQueryBuilder<TEntity, TKey>(_repository, this);
        }

        private string GetPropertyName<TProp, TProperty>(Expression<Func<TProp, TProperty>> expression)
        {
            if (expression.Body is MemberExpression memberExpression)
            {
                return memberExpression.Member.Name;
            }
            throw new ArgumentException("Expression must be a member expression");
        }

        public IEnumerable<TEntity> Execute()
        {
            var (connection, command, shouldDispose) = _repository.GetConnectionAndCommand(_transaction);
            try
            {
                command.CommandText = BuildSql();
                using var reader = command.ExecuteReader();

                var results = new List<TEntity>();
                while (reader.Read())
                {
                    results.Add(_repository.MapReaderToEntity(reader));
                }

                // Process includes if any (simplified version - full implementation would require multiple queries)
                if (_includes.Count > 0 && shouldDispose)
                {
                    ProcessIncludes(results, connection);
                }

                return results;
            }
            finally
            {
                command?.Dispose();
                if (shouldDispose)
                {
                    connection?.Dispose();
                }
            }
        }

        public async Task<IEnumerable<TEntity>> ExecuteAsync(CancellationToken token = default)
        {
            var (connection, command, shouldDispose) = await _repository.GetConnectionAndCommandAsync(_transaction, token);
            try
            {
                command.CommandText = BuildSql();
                await using var reader = await command.ExecuteReaderAsync(token);

                var results = new List<TEntity>();
                while (await reader.ReadAsync(token))
                {
                    results.Add(_repository.MapReaderToEntity(reader));
                }

                // Process includes if any
                if (_includes.Count > 0 && shouldDispose)
                {
                    await ProcessIncludesAsync(results, connection, token);
                }

                return results;
            }
            finally
            {
                if (command != null) await command.DisposeAsync();
                if (shouldDispose && connection != null)
                {
                    await connection.DisposeAsync();
                }
            }
        }

        public async IAsyncEnumerable<TEntity> ExecuteAsyncEnumerable([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken token = default)
        {
            var (connection, command, shouldDispose) = await _repository.GetConnectionAndCommandAsync(_transaction, token);
            try
            {
                command.CommandText = BuildSql();
                await using var reader = await command.ExecuteReaderAsync(token);

                while (await reader.ReadAsync(token))
                {
                    yield return _repository.MapReaderToEntity(reader);
                }
            }
            finally
            {
                if (command != null) await command.DisposeAsync();
                if (shouldDispose && connection != null)
                {
                    await connection.DisposeAsync();
                }
            }
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

        public string BuildSql()
        {
            var sql = new StringBuilder();

            sql.Append("SELECT ");
            if (_distinct) sql.Append("DISTINCT ");

            if (_selectExpression != null)
            {
                // Build projection columns - simplified version
                sql.Append("*");
            }
            else
            {
                sql.Append("*");
            }

            sql.Append($" FROM {_repository._tableName}");

            // Add JOINs for includes
            foreach (var include in _includes)
            {
                // Simplified - full implementation would generate proper JOIN clauses
            }

            if (_whereClauses.Count > 0)
            {
                sql.Append(" WHERE ");
                sql.Append(string.Join(" AND ", _whereClauses));
            }

            if (_groupByColumns.Count > 0)
            {
                sql.Append(" GROUP BY ");
                sql.Append(string.Join(", ", _groupByColumns));
            }

            if (_orderByClauses.Count > 0)
            {
                sql.Append(" ORDER BY ");
                var orderParts = _orderByClauses.Select(o => $"{o.Column} {(o.Ascending ? "ASC" : "DESC")}");
                sql.Append(string.Join(", ", orderParts));
            }

            if (_takeCount.HasValue)
            {
                sql.Append($" LIMIT {_takeCount.Value}");
            }

            if (_skipCount.HasValue)
            {
                sql.Append($" OFFSET {_skipCount.Value}");
            }

            sql.Append(";");
            return sql.ToString();
        }

        internal List<string> GetGroupByColumns() => _groupByColumns;
        internal List<string> GetWhereClauses() => _whereClauses;
    }
}
