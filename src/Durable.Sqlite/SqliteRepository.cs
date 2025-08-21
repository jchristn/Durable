namespace Durable.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Runtime.CompilerServices;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;

// SQLite Repository Implementation with Full Transaction Support and Connection Pooling
    public class SqliteRepository<T> : IRepository<T>, IBatchInsertConfiguration, IDisposable where T : class, new()
    {
        #region Public-Members

        #endregion

        #region Private-Members

        internal readonly IConnectionFactory _ConnectionFactory;
        internal readonly string _TableName;
        internal readonly string _PrimaryKeyColumn;
        internal readonly PropertyInfo _PrimaryKeyProperty;
        internal readonly Dictionary<string, PropertyInfo> _ColumnMappings;
        internal readonly Dictionary<PropertyInfo, ForeignKeyAttribute> _ForeignKeys;
        internal readonly Dictionary<PropertyInfo, NavigationPropertyAttribute> _NavigationProperties;
        internal readonly IBatchInsertConfiguration _BatchConfig;

        #endregion

        #region Constructors-and-Factories

        public SqliteRepository(string connectionString, IBatchInsertConfiguration batchConfig = null)
        {
            _ConnectionFactory = new SqliteConnectionFactory(connectionString);
            _TableName = GetEntityName();
            (_PrimaryKeyColumn, _PrimaryKeyProperty) = GetPrimaryKeyInfo();
            _ColumnMappings = GetColumnMappings();
            _ForeignKeys = GetForeignKeys();
            _NavigationProperties = GetNavigationProperties();
            _BatchConfig = batchConfig ?? BatchInsertConfiguration.Default;
        }

        public SqliteRepository(IConnectionFactory connectionFactory, IBatchInsertConfiguration batchConfig = null)
        {
            _ConnectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
            _TableName = GetEntityName();
            (_PrimaryKeyColumn, _PrimaryKeyProperty) = GetPrimaryKeyInfo();
            _ColumnMappings = GetColumnMappings();
            _ForeignKeys = GetForeignKeys();
            _NavigationProperties = GetNavigationProperties();
            _BatchConfig = batchConfig ?? BatchInsertConfiguration.Default;
        }

        #endregion

        #region Public-Methods

        // Read operations
        public T ReadFirst(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null)
        {
            var query = Query(transaction);
            if (predicate != null) query = query.Where(predicate);
            return query.Take(1).Execute().FirstOrDefault();
        }

        public async Task<T> ReadFirstAsync(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default)
        {
            var query = Query(transaction);
            if (predicate != null) query.Where(predicate);
            query.Take(1);

            var results = await query.ExecuteAsync(token);
            return results.FirstOrDefault();
        }

        public T ReadFirstOrDefault(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null)
        {
            return ReadFirst(predicate, transaction);
        }

        public Task<T> ReadFirstOrDefaultAsync(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default)
        {
            return ReadFirstAsync(predicate, transaction, token);
        }

        public T ReadSingle(Expression<Func<T, bool>> predicate, ITransaction transaction = null)
        {
            var results = Query(transaction).Where(predicate).Take(2).Execute().ToList();
            if (results.Count != 1)
                throw new InvalidOperationException($"Expected exactly 1 result but found {results.Count}");
            return results[0];
        }

        public async Task<T> ReadSingleAsync(Expression<Func<T, bool>> predicate, ITransaction transaction = null, CancellationToken token = default)
        {
            var results = new List<T>();
            await foreach (var item in ReadManyAsync(predicate, transaction, token).ConfigureAwait(false))
            {
                results.Add(item);
                if (results.Count > 1)
                    throw new InvalidOperationException($"Expected exactly 1 result but found more");
            }

            if (results.Count != 1)
                throw new InvalidOperationException($"Expected exactly 1 result but found {results.Count}");

            return results[0];
        }

        public T ReadSingleOrDefault(Expression<Func<T, bool>> predicate, ITransaction transaction = null)
        {
            var results = Query(transaction).Where(predicate).Take(2).Execute().ToList();
            if (results.Count > 1)
                throw new InvalidOperationException($"Expected at most 1 result but found {results.Count}");
            return results.FirstOrDefault();
        }

        public async Task<T> ReadSingleOrDefaultAsync(Expression<Func<T, bool>> predicate, ITransaction transaction = null, CancellationToken token = default)
        {
            var results = new List<T>();
            await foreach (var item in ReadManyAsync(predicate, transaction, token).ConfigureAwait(false))
            {
                results.Add(item);
                if (results.Count > 1)
                    throw new InvalidOperationException($"Expected at most 1 result but found {results.Count}");
            }

            return results.FirstOrDefault();
        }

        public IEnumerable<T> ReadMany(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null)
        {
            var query = Query(transaction);
            if (predicate != null) query = query.Where(predicate);
            return query.Execute();
        }

        public async IAsyncEnumerable<T> ReadManyAsync(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, [EnumeratorCancellation] CancellationToken token = default)
        {
            var query = Query(transaction);
            if (predicate != null) query.Where(predicate);

            await foreach (var item in query.ExecuteAsyncEnumerable(token))
            {
                yield return item;
            }
        }

        public IEnumerable<T> ReadAll(ITransaction transaction = null)
        {
            return ReadMany(null, transaction);
        }

        public IAsyncEnumerable<T> ReadAllAsync(ITransaction transaction = null, CancellationToken token = default)
        {
            return ReadManyAsync(null, transaction, token);
        }

        public T ReadById(object id, ITransaction transaction = null)
        {
            var (connection, command, shouldReturnToPool) = GetConnectionAndCommand(transaction);
            try
            {
                command.CommandText = $"SELECT * FROM {_TableName} WHERE {_PrimaryKeyColumn} = @id;";
                command.Parameters.AddWithValue("@id", id);

                using var reader = command.ExecuteReader();
                if (reader.Read())
                {
                    return MapReaderToEntity(reader);
                }

                return null;
            }
            finally
            {
                CleanupConnection(connection, command, shouldReturnToPool);
            }
        }

        public async Task<T> ReadByIdAsync(object id, ITransaction transaction = null, CancellationToken token = default)
        {
            var (connection, command, shouldReturnToPool) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                command.CommandText = $"SELECT * FROM {_TableName} WHERE {_PrimaryKeyColumn} = @id;";
                command.Parameters.AddWithValue("@id", id);

                await using var reader = await command.ExecuteReaderAsync(token);
                if (await reader.ReadAsync(token))
                {
                    return MapReaderToEntity(reader);
                }

                return null;
            }
            finally
            {
                await CleanupConnectionAsync(connection, command, shouldReturnToPool);
            }
        }

        // Aggregate operations
        public TResult Max<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null)
        {
            var (connection, command, shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                var column = GetColumnFromExpression(selector.Body);
                var sql = new StringBuilder($"SELECT MAX({column}) FROM {_TableName}");

                if (predicate != null)
                {
                    var whereClause = BuildWhereClause(predicate);
                    sql.Append($" WHERE {whereClause}");
                }

                sql.Append(";");
                command.CommandText = sql.ToString();

                var result = command.ExecuteScalar();
                return result == DBNull.Value || result == null ? default(TResult) : (TResult)Convert.ChangeType(result, typeof(TResult));
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<TResult> MaxAsync<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default)
        {
            var (connection, command, shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                var column = GetColumnFromExpression(selector.Body);
                var sql = new StringBuilder($"SELECT MAX({column}) FROM {_TableName}");

                if (predicate != null)
                {
                    var whereClause = BuildWhereClause(predicate);
                    sql.Append($" WHERE {whereClause}");
                }

                sql.Append(";");
                command.CommandText = sql.ToString();

                var result = await command.ExecuteScalarAsync(token);
                return result == DBNull.Value || result == null ? default(TResult) : (TResult)Convert.ChangeType(result, typeof(TResult));
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

        public TResult Min<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null)
        {
            var (connection, command, shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                var column = GetColumnFromExpression(selector.Body);
                var sql = new StringBuilder($"SELECT MIN({column}) FROM {_TableName}");

                if (predicate != null)
                {
                    var whereClause = BuildWhereClause(predicate);
                    sql.Append($" WHERE {whereClause}");
                }

                sql.Append(";");
                command.CommandText = sql.ToString();

                var result = command.ExecuteScalar();
                return result == DBNull.Value || result == null ? default(TResult) : (TResult)Convert.ChangeType(result, typeof(TResult));
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<TResult> MinAsync<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default)
        {
            var (connection, command, shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                var column = GetColumnFromExpression(selector.Body);
                var sql = new StringBuilder($"SELECT MIN({column}) FROM {_TableName}");

                if (predicate != null)
                {
                    var whereClause = BuildWhereClause(predicate);
                    sql.Append($" WHERE {whereClause}");
                }

                sql.Append(";");
                command.CommandText = sql.ToString();

                var result = await command.ExecuteScalarAsync(token);
                return result == DBNull.Value || result == null ? default(TResult) : (TResult)Convert.ChangeType(result, typeof(TResult));
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

        public decimal Average(Expression<Func<T, decimal>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null)
        {
            var (connection, command, shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                var column = GetColumnFromExpression(selector.Body);
                var sql = new StringBuilder($"SELECT AVG(CAST({column} AS REAL)) FROM {_TableName}");

                if (predicate != null)
                {
                    var whereClause = BuildWhereClause(predicate);
                    sql.Append($" WHERE {whereClause}");
                }

                sql.Append(";");
                command.CommandText = sql.ToString();

                var result = command.ExecuteScalar();
                return result == DBNull.Value || result == null ? 0m : Convert.ToDecimal(result);
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<decimal> AverageAsync(Expression<Func<T, decimal>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default)
        {
            var (connection, command, shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                var column = GetColumnFromExpression(selector.Body);
                var sql = new StringBuilder($"SELECT AVG(CAST({column} AS REAL)) FROM {_TableName}");

                if (predicate != null)
                {
                    var whereClause = BuildWhereClause(predicate);
                    sql.Append($" WHERE {whereClause}");
                }

                sql.Append(";");
                command.CommandText = sql.ToString();

                var result = await command.ExecuteScalarAsync(token);
                return result == DBNull.Value || result == null ? 0m : Convert.ToDecimal(result);
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

        public decimal Sum(Expression<Func<T, decimal>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null)
        {
            var (connection, command, shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                var column = GetColumnFromExpression(selector.Body);
                var sql = new StringBuilder($"SELECT COALESCE(SUM({column}), 0) FROM {_TableName}");

                if (predicate != null)
                {
                    var whereClause = BuildWhereClause(predicate);
                    sql.Append($" WHERE {whereClause}");
                }

                sql.Append(";");
                command.CommandText = sql.ToString();

                var result = command.ExecuteScalar();
                return Convert.ToDecimal(result);
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<decimal> SumAsync(Expression<Func<T, decimal>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default)
        {
            var (connection, command, shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                var column = GetColumnFromExpression(selector.Body);
                var sql = new StringBuilder($"SELECT COALESCE(SUM({column}), 0) FROM {_TableName}");

                if (predicate != null)
                {
                    var whereClause = BuildWhereClause(predicate);
                    sql.Append($" WHERE {whereClause}");
                }

                sql.Append(";");
                command.CommandText = sql.ToString();

                var result = await command.ExecuteScalarAsync(token);
                return Convert.ToDecimal(result);
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

        // Batch operations
        public int BatchUpdate(Expression<Func<T, bool>> predicate, Expression<Func<T, T>> updateExpression, ITransaction transaction = null)
        {
            var (connection, command, shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                var parser = new ExpressionParser<T>(_ColumnMappings);
                var whereClause = BuildWhereClause(predicate);
                var setPairs = parser.ParseUpdateExpression(updateExpression);

                command.CommandText = $"UPDATE {_TableName} SET {setPairs} WHERE {whereClause};";
                return command.ExecuteNonQuery();
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<int> BatchUpdateAsync(Expression<Func<T, bool>> predicate, Expression<Func<T, T>> updateExpression, ITransaction transaction = null, CancellationToken token = default)
        {
            var (connection, command, shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                var parser = new ExpressionParser<T>(_ColumnMappings);
                var whereClause = BuildWhereClause(predicate);
                var setPairs = parser.ParseUpdateExpression(updateExpression);

                command.CommandText = $"UPDATE {_TableName} SET {setPairs} WHERE {whereClause};";
                return await command.ExecuteNonQueryAsync(token);
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

        public int BatchDelete(Expression<Func<T, bool>> predicate, ITransaction transaction = null)
        {
            return DeleteMany(predicate, transaction);
        }

        public Task<int> BatchDeleteAsync(Expression<Func<T, bool>> predicate, ITransaction transaction = null, CancellationToken token = default)
        {
            return DeleteManyAsync(predicate, transaction, token);
        }

        // Raw SQL operations
        public IEnumerable<T> FromSql(string sql, ITransaction transaction = null, params object[] parameters)
        {
            var (connection, command, shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                command.CommandText = sql;
                for (int i = 0; i < parameters.Length; i++)
                {
                    command.Parameters.AddWithValue($"@p{i}", parameters[i] ?? DBNull.Value);
                }

                using var reader = command.ExecuteReader();
                var results = new List<T>();
                while (reader.Read())
                {
                    results.Add(MapReaderToEntity(reader));
                }

                return results;
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async IAsyncEnumerable<T> FromSqlAsync(string sql, ITransaction transaction = null, [EnumeratorCancellation] CancellationToken token = default, params object[] parameters)
        {
            var (connection, command, shouldDispose) = await GetConnectionAndCommandAsync(transaction, CancellationToken.None);
            try
            {
                command.CommandText = sql;
                for (int i = 0; i < parameters.Length; i++)
                {
                    command.Parameters.AddWithValue($"@p{i}", parameters[i] ?? DBNull.Value);
                }

                await using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    yield return MapReaderToEntity(reader);
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

        public async IAsyncEnumerable<TResult> FromSqlAsync<TResult>(string sql, ITransaction transaction = null, [EnumeratorCancellation] CancellationToken token = default, params object[] parameters) where TResult : new()
        {
            var (connection, command, shouldDispose) = await GetConnectionAndCommandAsync(transaction, CancellationToken.None);
            try
            {
                command.CommandText = sql;
                for (int i = 0; i < parameters.Length; i++)
                {
                    command.Parameters.AddWithValue($"@p{i}", parameters[i] ?? DBNull.Value);
                }

                await using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    yield return MapReaderToType<TResult>(reader);
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

        public IEnumerable<TResult> FromSql<TResult>(string sql, ITransaction transaction = null, params object[] parameters) where TResult : new()
        {
            var (connection, command, shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                command.CommandText = sql;
                for (int i = 0; i < parameters.Length; i++)
                {
                    command.Parameters.AddWithValue($"@p{i}", parameters[i] ?? DBNull.Value);
                }

                using var reader = command.ExecuteReader();
                var results = new List<TResult>();
                while (reader.Read())
                {
                    results.Add(MapReaderToType<TResult>(reader));
                }

                return results;
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public int ExecuteSql(string sql, ITransaction transaction = null, params object[] parameters)
        {
            var (connection, command, shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                command.CommandText = sql;
                for (int i = 0; i < parameters.Length; i++)
                {
                    command.Parameters.AddWithValue($"@p{i}", parameters[i] ?? DBNull.Value);
                }

                return command.ExecuteNonQuery();
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<int> ExecuteSqlAsync(string sql, ITransaction transaction = null, CancellationToken token = default, params object[] parameters)
        {
            var (connection, command, shouldDispose) = await GetConnectionAndCommandAsync(transaction, CancellationToken.None);
            try
            {
                command.CommandText = sql;
                for (int i = 0; i < parameters.Length; i++)
                {
                    command.Parameters.AddWithValue($"@p{i}", parameters[i] ?? DBNull.Value);
                }

                return await command.ExecuteNonQueryAsync();
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

        // Transaction support
        public ITransaction BeginTransaction()
        {
            var connection = GetConnection();
            connection.Open();
            var transaction = connection.BeginTransaction();
            return new SqliteRepositoryTransaction(connection, transaction);
        }

        public async Task<ITransaction> BeginTransactionAsync(CancellationToken token = default)
        {
            var connection = GetConnection();
            await connection.OpenAsync(token);
            var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(token);
            return new SqliteRepositoryTransaction(connection, transaction);
        }

        // Existence checks
        public bool Exists(Expression<Func<T, bool>> predicate, ITransaction transaction = null)
        {
            var (connection, command, shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                var whereClause = BuildWhereClause(predicate);
                command.CommandText = $"SELECT EXISTS(SELECT 1 FROM {_TableName} WHERE {whereClause} LIMIT 1);";
                return Convert.ToBoolean(command.ExecuteScalar());
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<bool> ExistsAsync(Expression<Func<T, bool>> predicate, ITransaction transaction = null, CancellationToken token = default)
        {
            var (connection, command, shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                var whereClause = BuildWhereClause(predicate);
                command.CommandText = $"SELECT EXISTS(SELECT 1 FROM {_TableName} WHERE {whereClause} LIMIT 1);";
                var result = await command.ExecuteScalarAsync(token);
                return Convert.ToBoolean(result);
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

        public bool ExistsById(object id, ITransaction transaction = null)
        {
            var (connection, command, shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                command.CommandText = $"SELECT EXISTS(SELECT 1 FROM {_TableName} WHERE {_PrimaryKeyColumn} = @id LIMIT 1);";
                command.Parameters.AddWithValue("@id", id);
                return Convert.ToBoolean(command.ExecuteScalar());
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<bool> ExistsByIdAsync(object id, ITransaction transaction = null, CancellationToken token = default)
        {
            var (connection, command, shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                command.CommandText = $"SELECT EXISTS(SELECT 1 FROM {_TableName} WHERE {_PrimaryKeyColumn} = @id LIMIT 1);";
                command.Parameters.AddWithValue("@id", id);
                var result = await command.ExecuteScalarAsync(token);
                return Convert.ToBoolean(result);
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

        // Count operations
        public int Count(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null)
        {
            var (connection, command, shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                var sql = new StringBuilder($"SELECT COUNT(*) FROM {_TableName}");

                if (predicate != null)
                {
                    var whereClause = BuildWhereClause(predicate);
                    sql.Append($" WHERE {whereClause}");
                }

                sql.Append(";");
                command.CommandText = sql.ToString();
                return Convert.ToInt32(command.ExecuteScalar());
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<int> CountAsync(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default)
        {
            var (connection, command, shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                var sql = new StringBuilder($"SELECT COUNT(*) FROM {_TableName}");

                if (predicate != null)
                {
                    var whereClause = BuildWhereClause(predicate);
                    sql.Append($" WHERE {whereClause}");
                }

                sql.Append(";");
                command.CommandText = sql.ToString();
                var result = await command.ExecuteScalarAsync(token);
                return Convert.ToInt32(result);
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

        // Create operations
        public T Create(T entity, ITransaction transaction = null)
        {
            var (connection, command, shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                var columns = new List<string>();
                var parameters = new List<string>();

                foreach (var kvp in _ColumnMappings)
                {
                    var columnName = kvp.Key;
                    var property = kvp.Value;
                    var columnAttr = property.GetCustomAttribute<PropertyAttribute>();

                    // Skip auto-increment primary keys
                    if (columnAttr != null &&
                        (columnAttr.PropertyFlags & Flags.PrimaryKey) == Flags.PrimaryKey &&
                        (columnAttr.PropertyFlags & Flags.AutoIncrement) == Flags.AutoIncrement)
                    {
                        continue;
                    }

                    columns.Add(columnName);
                    parameters.Add($"@{columnName}");
                    var value = property.GetValue(entity);
                    command.Parameters.AddWithValue($"@{columnName}", value ?? DBNull.Value);
                }

                command.CommandText = $"INSERT INTO {_TableName} ({string.Join(", ", columns)}) VALUES ({string.Join(", ", parameters)}); SELECT last_insert_rowid();";

                var insertedId = Convert.ToInt64(command.ExecuteScalar());

                // Set the ID back on the entity if it's auto-increment
                if (_PrimaryKeyProperty != null)
                {
                    var columnAttr = _PrimaryKeyProperty.GetCustomAttribute<PropertyAttribute>();
                    if (columnAttr != null && (columnAttr.PropertyFlags & Flags.AutoIncrement) == Flags.AutoIncrement)
                    {
                        _PrimaryKeyProperty.SetValue(entity, Convert.ChangeType(insertedId, _PrimaryKeyProperty.PropertyType));
                    }
                }

                return entity;
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<T> CreateAsync(T entity, ITransaction transaction = null, CancellationToken token = default)
        {
            var (connection, command, shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                var columns = new List<string>();
                var parameters = new List<string>();

                foreach (var kvp in _ColumnMappings)
                {
                    var columnName = kvp.Key;
                    var property = kvp.Value;
                    var columnAttr = property.GetCustomAttribute<PropertyAttribute>();

                    // Skip auto-increment primary keys
                    if (columnAttr != null &&
                        (columnAttr.PropertyFlags & Flags.PrimaryKey) == Flags.PrimaryKey &&
                        (columnAttr.PropertyFlags & Flags.AutoIncrement) == Flags.AutoIncrement)
                    {
                        continue;
                    }

                    columns.Add(columnName);
                    parameters.Add($"@{columnName}");
                    var value = property.GetValue(entity);
                    command.Parameters.AddWithValue($"@{columnName}", value ?? DBNull.Value);
                }

                command.CommandText = $"INSERT INTO {_TableName} ({string.Join(", ", columns)}) VALUES ({string.Join(", ", parameters)}); SELECT last_insert_rowid();";

                var insertedId = Convert.ToInt64(await command.ExecuteScalarAsync(token));

                // Set the ID back on the entity if it's auto-increment
                if (_PrimaryKeyProperty != null)
                {
                    var columnAttr = _PrimaryKeyProperty.GetCustomAttribute<PropertyAttribute>();
                    if (columnAttr != null && (columnAttr.PropertyFlags & Flags.AutoIncrement) == Flags.AutoIncrement)
                    {
                        _PrimaryKeyProperty.SetValue(entity, Convert.ChangeType(insertedId, _PrimaryKeyProperty.PropertyType));
                    }
                }

                return entity;
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

        public IEnumerable<T> CreateMany(IEnumerable<T> entities, ITransaction transaction = null)
        {
            var entitiesList = entities.ToList();
            if (!entitiesList.Any()) return entitiesList;

            bool ownTransaction = transaction == null;
            SqliteConnection connection = null;
            SqliteTransaction localTransaction = null;

            try
            {
                if (ownTransaction)
                {
                    connection = GetConnection();
                    connection.Open();
                    localTransaction = connection.BeginTransaction();
                    transaction = new SqliteRepositoryTransaction(connection, localTransaction);
                }

                var results = new List<T>();
                
                if (_BatchConfig.EnableMultiRowInsert && entitiesList.Count > 1)
                {
                    results.AddRange(CreateManyOptimized(entitiesList, transaction));
                }
                else
                {
                    // Fallback to individual inserts
                    foreach (var entity in entitiesList)
                    {
                        results.Add(Create(entity, transaction));
                    }
                }

                if (ownTransaction)
                {
                    localTransaction.Commit();
                }

                return results;
            }
            catch
            {
                if (ownTransaction)
                {
                    localTransaction?.Rollback();
                }
                throw;
            }
            finally
            {
                if (ownTransaction)
                {
                    localTransaction?.Dispose();
                    _ConnectionFactory.ReturnConnection(connection);
                }
            }
        }

        public async Task<IEnumerable<T>> CreateManyAsync(IEnumerable<T> entities, ITransaction transaction = null, CancellationToken token = default)
        {
            var entitiesList = entities.ToList();
            if (!entitiesList.Any()) return entitiesList;

            bool ownTransaction = transaction == null;
            SqliteConnection connection = null;
            SqliteTransaction localTransaction = null;

            try
            {
                if (ownTransaction)
                {
                    connection = await GetConnectionAsync(token);
                    await connection.OpenAsync(token);
                    localTransaction = (SqliteTransaction)await connection.BeginTransactionAsync(token);
                    transaction = new SqliteRepositoryTransaction(connection, localTransaction);
                }

                var results = new List<T>();
                
                if (_BatchConfig.EnableMultiRowInsert && entitiesList.Count > 1)
                {
                    results.AddRange(await CreateManyOptimizedAsync(entitiesList, transaction, token));
                }
                else
                {
                    // Fallback to individual inserts
                    foreach (var entity in entitiesList)
                    {
                        results.Add(await CreateAsync(entity, transaction, token));
                    }
                }

                if (ownTransaction)
                {
                    await localTransaction.CommitAsync(token);
                }

                return results;
            }
            catch
            {
                if (ownTransaction)
                {
                    if (localTransaction != null) await localTransaction.RollbackAsync(token);
                }
                throw;
            }
            finally
            {
                if (ownTransaction)
                {
                    if (localTransaction != null) await localTransaction.DisposeAsync();
                    if (connection != null) await _ConnectionFactory.ReturnConnectionAsync(connection);
                }
            }
        }

        // Update operations
        public T Update(T entity, ITransaction transaction = null)
        {
            var (connection, command, shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                var setPairs = new List<string>();
                object idValue = null;

                foreach (var kvp in _ColumnMappings)
                {
                    var columnName = kvp.Key;
                    var property = kvp.Value;
                    var value = property.GetValue(entity);

                    if (columnName == _PrimaryKeyColumn)
                    {
                        idValue = value;
                    }
                    else
                    {
                        setPairs.Add($"{columnName} = @{columnName}");
                        command.Parameters.AddWithValue($"@{columnName}", value ?? DBNull.Value);
                    }
                }

                command.Parameters.AddWithValue("@id", idValue);
                command.CommandText = $"UPDATE {_TableName} SET {string.Join(", ", setPairs)} WHERE {_PrimaryKeyColumn} = @id;";

                var rowsAffected = command.ExecuteNonQuery();

                if (rowsAffected == 0)
                    throw new InvalidOperationException($"No rows were updated for entity with {_PrimaryKeyColumn} = {idValue}");

                return entity;
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<T> UpdateAsync(T entity, ITransaction transaction = null, CancellationToken token = default)
        {
            var (connection, command, shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                var setPairs = new List<string>();
                object idValue = null;

                foreach (var kvp in _ColumnMappings)
                {
                    var columnName = kvp.Key;
                    var property = kvp.Value;
                    var value = property.GetValue(entity);

                    if (columnName == _PrimaryKeyColumn)
                    {
                        idValue = value;
                    }
                    else
                    {
                        setPairs.Add($"{columnName} = @{columnName}");
                        command.Parameters.AddWithValue($"@{columnName}", value ?? DBNull.Value);
                    }
                }

                command.Parameters.AddWithValue("@id", idValue);
                command.CommandText = $"UPDATE {_TableName} SET {string.Join(", ", setPairs)} WHERE {_PrimaryKeyColumn} = @id;";

                var rowsAffected = await command.ExecuteNonQueryAsync(token);

                if (rowsAffected == 0)
                    throw new InvalidOperationException($"No rows were updated for entity with {_PrimaryKeyColumn} = {idValue}");

                return entity;
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

        public int UpdateMany(Expression<Func<T, bool>> predicate, Action<T> updateAction, ITransaction transaction = null)
        {
            // For simplicity, fetch and update each entity individually
            var entities = ReadMany(predicate, transaction).ToList();

            bool ownTransaction = transaction == null;
            SqliteConnection connection = null;
            SqliteTransaction localTransaction = null;

            try
            {
                if (ownTransaction)
                {
                    connection = GetConnection();
                    connection.Open();
                    localTransaction = connection.BeginTransaction();
                    transaction = new SqliteRepositoryTransaction(connection, localTransaction);
                }

                foreach (var entity in entities)
                {
                    updateAction(entity);
                    Update(entity, transaction);
                }

                if (ownTransaction)
                {
                    localTransaction.Commit();
                }

                return entities.Count;
            }
            catch
            {
                if (ownTransaction)
                {
                    localTransaction?.Rollback();
                }
                throw;
            }
            finally
            {
                if (ownTransaction)
                {
                    localTransaction?.Dispose();
                    connection?.Dispose();
                }
            }
        }

        public async Task<int> UpdateManyAsync(Expression<Func<T, bool>> predicate, Func<T, Task> updateAction, ITransaction transaction = null, CancellationToken token = default)
        {
            // Fetch all entities matching the predicate
            var entities = new List<T>();
            await foreach (var entity in ReadManyAsync(predicate, transaction, token))
            {
                entities.Add(entity);
            }

            bool ownTransaction = transaction == null;
            SqliteConnection connection = null;
            SqliteTransaction localTransaction = null;

            try
            {
                if (ownTransaction)
                {
                    connection = GetConnection();
                    await connection.OpenAsync(token);
                    localTransaction = (SqliteTransaction)await connection.BeginTransactionAsync(token);
                    transaction = new SqliteRepositoryTransaction(connection, localTransaction);
                }

                foreach (var entity in entities)
                {
                    await updateAction(entity);
                    await UpdateAsync(entity, transaction, token);
                }

                if (ownTransaction)
                {
                    await localTransaction.CommitAsync(token);
                }

                return entities.Count;
            }
            catch
            {
                if (ownTransaction)
                {
                    if (localTransaction != null) await localTransaction.RollbackAsync(token);
                }
                throw;
            }
            finally
            {
                if (ownTransaction)
                {
                    if (localTransaction != null) await localTransaction.DisposeAsync();
                    if (connection != null) await connection.DisposeAsync();
                }
            }
        }

        public int UpdateField<TField>(Expression<Func<T, bool>> predicate, Expression<Func<T, TField>> field, TField value, ITransaction transaction = null)
        {
            var (connection, command, shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                var whereClause = BuildWhereClause(predicate);
                var columnName = GetColumnFromExpression(field.Body);

                command.CommandText = $"UPDATE {_TableName} SET {columnName} = @value WHERE {whereClause};";
                command.Parameters.AddWithValue("@value", value != null ? (object)value : DBNull.Value);

                return command.ExecuteNonQuery();
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<int> UpdateFieldAsync<TField>(Expression<Func<T, bool>> predicate, Expression<Func<T, TField>> field, TField value, ITransaction transaction = null, CancellationToken token = default)
        {
            var (connection, command, shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                var whereClause = BuildWhereClause(predicate);
                var columnName = GetColumnFromExpression(field.Body);

                command.CommandText = $"UPDATE {_TableName} SET {columnName} = @value WHERE {whereClause};";
                command.Parameters.AddWithValue("@value", value != null ? (object)value : DBNull.Value);

                return await command.ExecuteNonQueryAsync(token);
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

        // Delete operations
        public bool Delete(T entity, ITransaction transaction = null)
        {
            var idValue = GetPrimaryKeyValue(entity);
            return DeleteById(idValue, transaction);
        }

        public async Task<bool> DeleteAsync(T entity, ITransaction transaction = null, CancellationToken token = default)
        {
            var idValue = GetPrimaryKeyValue(entity);
            return await DeleteByIdAsync(idValue, transaction, token);
        }

        public bool DeleteById(object id, ITransaction transaction = null)
        {
            var (connection, command, shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                command.CommandText = $"DELETE FROM {_TableName} WHERE {_PrimaryKeyColumn} = @id;";
                command.Parameters.AddWithValue("@id", id);

                return command.ExecuteNonQuery() > 0;
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<bool> DeleteByIdAsync(object id, ITransaction transaction = null, CancellationToken token = default)
        {
            var (connection, command, shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                command.CommandText = $"DELETE FROM {_TableName} WHERE {_PrimaryKeyColumn} = @id;";
                command.Parameters.AddWithValue("@id", id);

                return await command.ExecuteNonQueryAsync(token) > 0;
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

        public int DeleteMany(Expression<Func<T, bool>> predicate, ITransaction transaction = null)
        {
            var (connection, command, shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                var whereClause = BuildWhereClause(predicate);
                command.CommandText = $"DELETE FROM {_TableName} WHERE {whereClause};";
                return command.ExecuteNonQuery();
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<int> DeleteManyAsync(Expression<Func<T, bool>> predicate, ITransaction transaction = null, CancellationToken token = default)
        {
            var (connection, command, shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                var whereClause = BuildWhereClause(predicate);
                command.CommandText = $"DELETE FROM {_TableName} WHERE {whereClause};";
                return await command.ExecuteNonQueryAsync(token);
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

        public int DeleteAll(ITransaction transaction = null)
        {
            var (connection, command, shouldReturnToPool) = GetConnectionAndCommand(transaction);
            try
            {
                command.CommandText = $"DELETE FROM {_TableName};";
                return command.ExecuteNonQuery();
            }
            finally
            {
                CleanupConnection(connection, command, shouldReturnToPool);
            }
        }

        public async Task<int> DeleteAllAsync(ITransaction transaction = null, CancellationToken token = default)
        {
            var (connection, command, shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                command.CommandText = $"DELETE FROM {_TableName};";
                return await command.ExecuteNonQueryAsync(token);
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

        // Upsert operations
        public T Upsert(T entity, ITransaction transaction = null)
        {
            var (connection, command, shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                var columns = new List<string>();
                var parameters = new List<string>();
                var updatePairs = new List<string>();

                foreach (var kvp in _ColumnMappings)
                {
                    var columnName = kvp.Key;
                    var property = kvp.Value;
                    var value = property.GetValue(entity);

                    columns.Add(columnName);
                    parameters.Add($"@{columnName}");
                    command.Parameters.AddWithValue($"@{columnName}", value ?? DBNull.Value);

                    if (columnName != _PrimaryKeyColumn)
                    {
                        updatePairs.Add($"{columnName} = excluded.{columnName}");
                    }
                }

                var sql = new StringBuilder();
                sql.Append($"INSERT INTO {_TableName} ({string.Join(", ", columns)}) ");
                sql.Append($"VALUES ({string.Join(", ", parameters)}) ");
                sql.Append($"ON CONFLICT({_PrimaryKeyColumn}) DO UPDATE SET ");
                sql.Append(string.Join(", ", updatePairs));
                sql.Append(";");

                command.CommandText = sql.ToString();
                command.ExecuteNonQuery();

                return entity;
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<T> UpsertAsync(T entity, ITransaction transaction = null, CancellationToken token = default)
        {
            var (connection, command, shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                var columns = new List<string>();
                var parameters = new List<string>();
                var updatePairs = new List<string>();

                foreach (var kvp in _ColumnMappings)
                {
                    var columnName = kvp.Key;
                    var property = kvp.Value;
                    var value = property.GetValue(entity);

                    columns.Add(columnName);
                    parameters.Add($"@{columnName}");
                    command.Parameters.AddWithValue($"@{columnName}", value ?? DBNull.Value);

                    if (columnName != _PrimaryKeyColumn)
                    {
                        updatePairs.Add($"{columnName} = excluded.{columnName}");
                    }
                }

                var sql = new StringBuilder();
                sql.Append($"INSERT INTO {_TableName} ({string.Join(", ", columns)}) ");
                sql.Append($"VALUES ({string.Join(", ", parameters)}) ");
                sql.Append($"ON CONFLICT({_PrimaryKeyColumn}) DO UPDATE SET ");
                sql.Append(string.Join(", ", updatePairs));
                sql.Append(";");

                command.CommandText = sql.ToString();
                await command.ExecuteNonQueryAsync(token);

                return entity;
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

        public IEnumerable<T> UpsertMany(IEnumerable<T> entities, ITransaction transaction = null)
        {
            bool ownTransaction = transaction == null;
            SqliteConnection connection = null;
            SqliteTransaction localTransaction = null;

            try
            {
                if (ownTransaction)
                {
                    connection = GetConnection();
                    connection.Open();
                    localTransaction = connection.BeginTransaction();
                    transaction = new SqliteRepositoryTransaction(connection, localTransaction);
                }

                var results = new List<T>();
                foreach (var entity in entities)
                {
                    results.Add(Upsert(entity, transaction));
                }

                if (ownTransaction)
                {
                    localTransaction.Commit();
                }

                return results;
            }
            catch
            {
                if (ownTransaction)
                {
                    localTransaction?.Rollback();
                }
                throw;
            }
            finally
            {
                if (ownTransaction)
                {
                    localTransaction?.Dispose();
                    connection?.Dispose();
                }
            }
        }

        public async Task<IEnumerable<T>> UpsertManyAsync(IEnumerable<T> entities, ITransaction transaction = null, CancellationToken token = default)
        {
            bool ownTransaction = transaction == null;
            SqliteConnection connection = null;
            SqliteTransaction localTransaction = null;

            try
            {
                if (ownTransaction)
                {
                    connection = GetConnection();
                    await connection.OpenAsync(token);
                    localTransaction = (SqliteTransaction)await connection.BeginTransactionAsync(token);
                    transaction = new SqliteRepositoryTransaction(connection, localTransaction);
                }

                var results = new List<T>();
                foreach (var entity in entities)
                {
                    results.Add(await UpsertAsync(entity, transaction, token));
                }

                if (ownTransaction)
                {
                    await localTransaction.CommitAsync(token);
                }

                return results;
            }
            catch
            {
                if (ownTransaction)
                {
                    if (localTransaction != null) await localTransaction.RollbackAsync(token);
                }
                throw;
            }
            finally
            {
                if (ownTransaction)
                {
                    if (localTransaction != null) await localTransaction.DisposeAsync();
                    if (connection != null) await connection.DisposeAsync();
                }
            }
        }

        // Query builder
        public IQueryBuilder<T> Query(ITransaction transaction = null)
        {
            return new SqliteQueryBuilder<T>(this, transaction);
        }

        // IBatchInsertConfiguration implementation
        public int MaxRowsPerBatch => _BatchConfig.MaxRowsPerBatch;
        public int MaxParametersPerStatement => _BatchConfig.MaxParametersPerStatement;
        public bool EnablePreparedStatementReuse => _BatchConfig.EnablePreparedStatementReuse;
        public bool EnableMultiRowInsert => _BatchConfig.EnableMultiRowInsert;

        public void Dispose()
        {
            _ConnectionFactory?.Dispose();
        }

        #endregion

        #region Private-Methods

        protected SqliteConnection GetConnection()
        {
            return (SqliteConnection)_ConnectionFactory.GetConnection();
        }

        protected async Task<SqliteConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
        {
            return (SqliteConnection)await _ConnectionFactory.GetConnectionAsync(cancellationToken);
        }

        private void CleanupConnection(SqliteConnection connection, SqliteCommand command, bool shouldReturnToPool)
        {
            command?.Dispose();
            if (shouldReturnToPool && connection != null)
            {
                _ConnectionFactory.ReturnConnection(connection);
            }
        }

        private async Task CleanupConnectionAsync(SqliteConnection connection, SqliteCommand command, bool shouldReturnToPool)
        {
            if (command != null) await command.DisposeAsync();
            if (shouldReturnToPool && connection != null)
            {
                await _ConnectionFactory.ReturnConnectionAsync(connection);
            }
        }

        internal (SqliteConnection connection, SqliteCommand command, bool shouldReturnToPool) GetConnectionAndCommand(ITransaction transaction)
        {
            if (transaction != null)
            {
                var command = new SqliteCommand();
                command.Connection = (SqliteConnection)transaction.Connection;
                command.Transaction = (SqliteTransaction)transaction.Transaction;
                return ((SqliteConnection)transaction.Connection, command, false);
            }
            else
            {
                var connection = GetConnection();
                if (connection.State != ConnectionState.Open)
                {
                    connection.Open();
                }
                var command = new SqliteCommand();
                command.Connection = connection;
                return (connection, command, true);
            }
        }

        internal async Task<(SqliteConnection connection, SqliteCommand command, bool shouldReturnToPool)> GetConnectionAndCommandAsync(ITransaction transaction, CancellationToken token)
        {
            if (transaction != null)
            {
                var command = new SqliteCommand();
                command.Connection = (SqliteConnection)transaction.Connection;
                command.Transaction = (SqliteTransaction)transaction.Transaction;
                return ((SqliteConnection)transaction.Connection, command, false);
            }
            else
            {
                var connection = await GetConnectionAsync(token);
                if (connection.State != ConnectionState.Open)
                {
                    await connection.OpenAsync(token);
                }
                var command = new SqliteCommand();
                command.Connection = connection;
                return (connection, command, true);
            }
        }

        protected string GetEntityName()
        {
            var entityAttr = typeof(T).GetCustomAttribute<EntityAttribute>();
            if (entityAttr == null)
                throw new InvalidOperationException($"Type {typeof(T).Name} must have an Entity attribute");
            return entityAttr.Name;
        }

        protected (string columnName, PropertyInfo property) GetPrimaryKeyInfo()
        {
            foreach (var prop in typeof(T).GetProperties())
            {
                var attr = prop.GetCustomAttribute<PropertyAttribute>();
                if (attr != null && (attr.PropertyFlags & Flags.PrimaryKey) == Flags.PrimaryKey)
                {
                    return (attr.Name, prop);
                }
            }

            throw new InvalidOperationException($"Type {typeof(T).Name} must have a primary key column");
        }

        protected Dictionary<string, PropertyInfo> GetColumnMappings()
        {
            var mappings = new Dictionary<string, PropertyInfo>();

            foreach (var prop in typeof(T).GetProperties())
            {
                var attr = prop.GetCustomAttribute<PropertyAttribute>();
                if (attr != null)
                {
                    mappings[attr.Name] = prop;
                }
            }

            return mappings;
        }

        protected Dictionary<PropertyInfo, ForeignKeyAttribute> GetForeignKeys()
        {
            var foreignKeys = new Dictionary<PropertyInfo, ForeignKeyAttribute>();

            foreach (var prop in typeof(T).GetProperties())
            {
                var fkAttr = prop.GetCustomAttribute<ForeignKeyAttribute>();
                if (fkAttr != null)
                {
                    foreignKeys[prop] = fkAttr;
                }
            }

            return foreignKeys;
        }

        protected Dictionary<PropertyInfo, NavigationPropertyAttribute> GetNavigationProperties()
        {
            var navProps = new Dictionary<PropertyInfo, NavigationPropertyAttribute>();

            foreach (var prop in typeof(T).GetProperties())
            {
                var navAttr = prop.GetCustomAttribute<NavigationPropertyAttribute>();
                if (navAttr != null)
                {
                    navProps[prop] = navAttr;
                }
            }

            return navProps;
        }

        protected object GetPrimaryKeyValue(T entity)
        {
            return _PrimaryKeyProperty.GetValue(entity);
        }

        internal string BuildWhereClause(Expression<Func<T, bool>> predicate)
        {
            var parser = new ExpressionParser<T>(_ColumnMappings);
            return parser.ParseExpression(predicate.Body);
        }

        internal string GetColumnFromExpression(Expression expression)
        {
            var parser = new ExpressionParser<T>(_ColumnMappings);
            return parser.GetColumnFromExpression(expression);
        }

        internal T MapReaderToEntity(IDataReader reader)
        {
            var entity = new T();

            foreach (var kvp in _ColumnMappings)
            {
                var columnName = kvp.Key;
                var property = kvp.Value;

                var ordinal = reader.GetOrdinal(columnName);
                if (!reader.IsDBNull(ordinal))
                {
                    var value = reader.GetValue(ordinal);
                    property.SetValue(entity, Convert.ChangeType(value, property.PropertyType));
                }
            }

            return entity;
        }

        protected TResult MapReaderToType<TResult>(IDataReader reader) where TResult : new()
        {
            var result = new TResult();
            var resultType = typeof(TResult);

            for (int i = 0; i < reader.FieldCount; i++)
            {
                var columnName = reader.GetName(i);
                var property = resultType.GetProperty(columnName, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);

                if (property != null && !reader.IsDBNull(i))
                {
                    var value = reader.GetValue(i);
                    property.SetValue(result, Convert.ChangeType(value, property.PropertyType));
                }
            }

            return result;
        }

        private IEnumerable<T> CreateManyOptimized(IList<T> entities, ITransaction transaction)
        {
            var (connection, _, shouldReturnToPool) = GetConnectionAndCommand(transaction);
            var results = new List<T>();
            
            try
            {
                var batches = CreateBatches(entities);
                var preparedCommands = new Dictionary<int, SqliteCommand>();
                
                foreach (var batch in batches)
                {
                    var batchSize = batch.Count;
                    
                    if (EnablePreparedStatementReuse && preparedCommands.TryGetValue(batchSize, out var preparedCommand))
                    {
                        preparedCommand.Parameters.Clear();
                        AddParametersForBatch(preparedCommand, batch);
                        ExecuteBatchInsert(preparedCommand, batch);
                    }
                    else
                    {
                        using var command = new SqliteCommand();
                        command.Connection = connection;
                        if (transaction != null)
                            command.Transaction = (SqliteTransaction)transaction.Transaction;
                        
                        BuildBatchInsertCommand(command, batch);
                        ExecuteBatchInsert(command, batch);
                        
                        if (EnablePreparedStatementReuse && !preparedCommands.ContainsKey(batchSize))
                        {
                            var newPreparedCommand = new SqliteCommand(command.CommandText, connection);
                            if (transaction != null)
                                newPreparedCommand.Transaction = (SqliteTransaction)transaction.Transaction;
                            preparedCommands[batchSize] = newPreparedCommand;
                        }
                    }
                    
                    results.AddRange(batch);
                }
                
                foreach (var preparedCommand in preparedCommands.Values)
                {
                    preparedCommand?.Dispose();
                }
                
                return results;
            }
            finally
            {
                if (shouldReturnToPool)
                {
                    _ConnectionFactory.ReturnConnection(connection);
                }
            }
        }
        
        private async Task<IEnumerable<T>> CreateManyOptimizedAsync(IList<T> entities, ITransaction transaction, CancellationToken token)
        {
            var (connection, _, shouldReturnToPool) = await GetConnectionAndCommandAsync(transaction, token);
            var results = new List<T>();
            
            try
            {
                var batches = CreateBatches(entities);
                var preparedCommands = new Dictionary<int, SqliteCommand>();
                
                foreach (var batch in batches)
                {
                    var batchSize = batch.Count;
                    
                    if (EnablePreparedStatementReuse && preparedCommands.TryGetValue(batchSize, out var preparedCommand))
                    {
                        preparedCommand.Parameters.Clear();
                        AddParametersForBatch(preparedCommand, batch);
                        await ExecuteBatchInsertAsync(preparedCommand, batch, token);
                    }
                    else
                    {
                        using var command = new SqliteCommand();
                        command.Connection = connection;
                        if (transaction != null)
                            command.Transaction = (SqliteTransaction)transaction.Transaction;
                        
                        BuildBatchInsertCommand(command, batch);
                        await ExecuteBatchInsertAsync(command, batch, token);
                        
                        if (EnablePreparedStatementReuse && !preparedCommands.ContainsKey(batchSize))
                        {
                            var newPreparedCommand = new SqliteCommand(command.CommandText, connection);
                            if (transaction != null)
                                newPreparedCommand.Transaction = (SqliteTransaction)transaction.Transaction;
                            preparedCommands[batchSize] = newPreparedCommand;
                        }
                    }
                    
                    results.AddRange(batch);
                }
                
                foreach (var preparedCommand in preparedCommands.Values)
                {
                    if (preparedCommand != null)
                        await preparedCommand.DisposeAsync();
                }
                
                return results;
            }
            finally
            {
                if (shouldReturnToPool)
                {
                    await _ConnectionFactory.ReturnConnectionAsync(connection);
                }
            }
        }
        
        private IEnumerable<IList<T>> CreateBatches(IList<T> entities)
        {
            var nonAutoIncrementColumns = GetNonAutoIncrementColumns();
            var parametersPerEntity = nonAutoIncrementColumns.Count;
            var maxEntitiesPerBatch = Math.Min(
                MaxRowsPerBatch,
                MaxParametersPerStatement / parametersPerEntity);
                
            if (maxEntitiesPerBatch <= 0)
                maxEntitiesPerBatch = 1;
            
            for (int i = 0; i < entities.Count; i += maxEntitiesPerBatch)
            {
                var batchSize = Math.Min(maxEntitiesPerBatch, entities.Count - i);
                var batch = new List<T>(batchSize);
                for (int j = 0; j < batchSize; j++)
                {
                    batch.Add(entities[i + j]);
                }
                yield return batch;
            }
        }
        
        private List<string> GetNonAutoIncrementColumns()
        {
            var columns = new List<string>();
            foreach (var kvp in _ColumnMappings)
            {
                var columnName = kvp.Key;
                var property = kvp.Value;
                var columnAttr = property.GetCustomAttribute<PropertyAttribute>();

                if (columnAttr != null &&
                    (columnAttr.PropertyFlags & Flags.PrimaryKey) == Flags.PrimaryKey &&
                    (columnAttr.PropertyFlags & Flags.AutoIncrement) == Flags.AutoIncrement)
                {
                    continue;
                }
                
                columns.Add(columnName);
            }
            return columns;
        }
        
        private void BuildBatchInsertCommand(SqliteCommand command, IList<T> entities)
        {
            var columns = GetNonAutoIncrementColumns();
            var valuesList = new List<string>();
            
            for (int i = 0; i < entities.Count; i++)
            {
                var parameters = new List<string>();
                foreach (var column in columns)
                {
                    parameters.Add($"@{column}_{i}");
                }
                valuesList.Add($"({string.Join(", ", parameters)})");
            }
            
            command.CommandText = $"INSERT INTO {_TableName} ({string.Join(", ", columns)}) VALUES {string.Join(", ", valuesList)};";
            AddParametersForBatch(command, entities);
        }
        
        private void AddParametersForBatch(SqliteCommand command, IList<T> entities)
        {
            var columns = GetNonAutoIncrementColumns();
            
            for (int i = 0; i < entities.Count; i++)
            {
                var entity = entities[i];
                foreach (var column in columns)
                {
                    var property = _ColumnMappings[column];
                    var value = property.GetValue(entity);
                    command.Parameters.AddWithValue($"@{column}_{i}", value ?? DBNull.Value);
                }
            }
        }
        
        private void ExecuteBatchInsert(SqliteCommand command, IList<T> entities)
        {
            var rowsAffected = command.ExecuteNonQuery();
            
            if (_PrimaryKeyProperty != null)
            {
                var columnAttr = _PrimaryKeyProperty.GetCustomAttribute<PropertyAttribute>();
                if (columnAttr != null && (columnAttr.PropertyFlags & Flags.AutoIncrement) == Flags.AutoIncrement)
                {
                    using var idCommand = new SqliteCommand("SELECT last_insert_rowid();", command.Connection, command.Transaction);
                    var lastId = Convert.ToInt64(idCommand.ExecuteScalar());
                    
                    for (int i = entities.Count - 1; i >= 0; i--)
                    {
                        var id = lastId - (entities.Count - 1 - i);
                        _PrimaryKeyProperty.SetValue(entities[i], Convert.ChangeType(id, _PrimaryKeyProperty.PropertyType));
                    }
                }
            }
        }
        
        private async Task ExecuteBatchInsertAsync(SqliteCommand command, IList<T> entities, CancellationToken token)
        {
            var rowsAffected = await command.ExecuteNonQueryAsync(token);
            
            if (_PrimaryKeyProperty != null)
            {
                var columnAttr = _PrimaryKeyProperty.GetCustomAttribute<PropertyAttribute>();
                if (columnAttr != null && (columnAttr.PropertyFlags & Flags.AutoIncrement) == Flags.AutoIncrement)
                {
                    using var idCommand = new SqliteCommand("SELECT last_insert_rowid();", command.Connection, command.Transaction);
                    var lastId = Convert.ToInt64(await idCommand.ExecuteScalarAsync(token));
                    
                    for (int i = entities.Count - 1; i >= 0; i--)
                    {
                        var id = lastId - (entities.Count - 1 - i);
                        _PrimaryKeyProperty.SetValue(entities[i], Convert.ChangeType(id, _PrimaryKeyProperty.PropertyType));
                    }
                }
            }
        }

        #endregion
    }
}