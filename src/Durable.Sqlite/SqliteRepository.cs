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
    using Durable.ConcurrencyConflictResolvers;

// SQLite Repository Implementation with Full Transaction Support and Connection Pooling
    public class SqliteRepository<T> : IRepository<T>, IBatchInsertConfiguration, ISqlCapture, ISqlTrackingConfiguration, IDisposable where T : class, new()
    {
        #region Public-Members

        /// <summary>
        /// Gets the last SQL statement that was executed by this repository instance.
        /// Returns null if no SQL has been executed or SQL capture is disabled.
        /// </summary>
        public string LastExecutedSql
        {
            get => _LastExecutedSql.Value;
        }

        /// <summary>
        /// Gets or sets whether SQL statements should be captured and stored.
        /// Default value is false for performance reasons.
        /// </summary>
        public bool CaptureSql
        {
            get => _CaptureSql;
            set => _CaptureSql = value;
        }

        /// <summary>
        /// Gets or sets whether query results should automatically include the executed SQL statement.
        /// When true, repository operations will return IDurableResult objects containing both results and SQL.
        /// When false, repository operations return standard result types without SQL information.
        /// Default value is false for performance and backward compatibility.
        /// </summary>
        public bool IncludeQueryInResults
        {
            get => _IncludeQueryInResults;
            set => _IncludeQueryInResults = value;
        }

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
        internal readonly ISanitizer _Sanitizer;
        internal readonly IDataTypeConverter _DataTypeConverter;
        internal readonly VersionColumnInfo _VersionColumnInfo;
        internal readonly IConcurrencyConflictResolver<T> _ConflictResolver;
        internal readonly IChangeTracker<T> _ChangeTracker;

        private readonly AsyncLocal<string> _LastExecutedSql = new AsyncLocal<string>();
        private bool _CaptureSql;
        private bool _IncludeQueryInResults;

        #endregion

        #region Constructors-and-Factories

        public SqliteRepository(string connectionString, IBatchInsertConfiguration batchConfig = null, IDataTypeConverter dataTypeConverter = null, IConcurrencyConflictResolver<T> conflictResolver = null)
        {
            _ConnectionFactory = new SqliteConnectionFactory(connectionString);
            _Sanitizer = new SqliteSanitizer();
            _DataTypeConverter = dataTypeConverter ?? new DataTypeConverter();
            _TableName = GetEntityName();
            (_PrimaryKeyColumn, _PrimaryKeyProperty) = GetPrimaryKeyInfo();
            _ColumnMappings = GetColumnMappings();
            _ForeignKeys = GetForeignKeys();
            _NavigationProperties = GetNavigationProperties();
            _BatchConfig = batchConfig ?? BatchInsertConfiguration.Default;
            _VersionColumnInfo = GetVersionColumnInfo();
            _ConflictResolver = conflictResolver ?? new DefaultConflictResolver<T>(ConflictResolutionStrategy.ThrowException);
            _ChangeTracker = new SimpleChangeTracker<T>(_ColumnMappings);
        }

        public SqliteRepository(IConnectionFactory connectionFactory, IBatchInsertConfiguration batchConfig = null, IDataTypeConverter dataTypeConverter = null, IConcurrencyConflictResolver<T> conflictResolver = null)
        {
            _ConnectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
            _Sanitizer = new SqliteSanitizer();
            _DataTypeConverter = dataTypeConverter ?? new DataTypeConverter();
            _TableName = GetEntityName();
            (_PrimaryKeyColumn, _PrimaryKeyProperty) = GetPrimaryKeyInfo();
            _ColumnMappings = GetColumnMappings();
            _ForeignKeys = GetForeignKeys();
            _NavigationProperties = GetNavigationProperties();
            _BatchConfig = batchConfig ?? BatchInsertConfiguration.Default;
            _VersionColumnInfo = GetVersionColumnInfo();
            _ConflictResolver = conflictResolver ?? new DefaultConflictResolver<T>(ConflictResolutionStrategy.ThrowException);
            _ChangeTracker = new SimpleChangeTracker<T>(_ColumnMappings);
        }

        #endregion

        #region Public-Methods

        // Read operations
        public T ReadFirst(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null)
        {
            IQueryBuilder<T> query = Query(transaction);
            if (predicate != null) query = query.Where(predicate);
            return query.Take(1).Execute().FirstOrDefault();
        }

        public async Task<T> ReadFirstAsync(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default)
        {
            IQueryBuilder<T> query = Query(transaction);
            if (predicate != null) query.Where(predicate);
            query.Take(1);

            IEnumerable<T> results = await query.ExecuteAsync(token);
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
            List<T> results = Query(transaction).Where(predicate).Take(2).Execute().ToList();
            if (results.Count != 1)
                throw new InvalidOperationException($"Expected exactly 1 result but found {results.Count}");
            return results[0];
        }

        public async Task<T> ReadSingleAsync(Expression<Func<T, bool>> predicate, ITransaction transaction = null, CancellationToken token = default)
        {
            List<T> results = new List<T>();
            await foreach (T item in ReadManyAsync(predicate, transaction, token).ConfigureAwait(false))
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
            List<T> results = Query(transaction).Where(predicate).Take(2).Execute().ToList();
            if (results.Count > 1)
                throw new InvalidOperationException($"Expected at most 1 result but found {results.Count}");
            return results.FirstOrDefault();
        }

        public async Task<T> ReadSingleOrDefaultAsync(Expression<Func<T, bool>> predicate, ITransaction transaction = null, CancellationToken token = default)
        {
            List<T> results = new List<T>();
            await foreach (T item in ReadManyAsync(predicate, transaction, token).ConfigureAwait(false))
            {
                results.Add(item);
                if (results.Count > 1)
                    throw new InvalidOperationException($"Expected at most 1 result but found {results.Count}");
            }

            return results.FirstOrDefault();
        }

        public IEnumerable<T> ReadMany(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null)
        {
            IQueryBuilder<T> query = Query(transaction);
            if (predicate != null) query = query.Where(predicate);
            return query.Execute();
        }

        public async IAsyncEnumerable<T> ReadManyAsync(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, [EnumeratorCancellation] CancellationToken token = default)
        {
            IQueryBuilder<T> query = Query(transaction);
            if (predicate != null) query.Where(predicate);

            await foreach (T item in query.ExecuteAsyncEnumerable(token))
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
            (SqliteConnection connection, SqliteCommand command, bool shouldReturnToPool) = GetConnectionAndCommand(transaction);
            try
            {
                command.CommandText = $"SELECT * FROM {_Sanitizer.SanitizeIdentifier(_TableName)} WHERE {_Sanitizer.SanitizeIdentifier(_PrimaryKeyColumn)} = @id;";
                command.Parameters.AddWithValue("@id", id);
                CaptureSqlFromCommand(command);

                using DbDataReader reader = command.ExecuteReader();
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
            (SqliteConnection connection, SqliteCommand command, bool shouldReturnToPool) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                command.CommandText = $"SELECT * FROM {_Sanitizer.SanitizeIdentifier(_TableName)} WHERE {_Sanitizer.SanitizeIdentifier(_PrimaryKeyColumn)} = @id;";
                command.Parameters.AddWithValue("@id", id);
                CaptureSqlFromCommand(command);

                await using DbDataReader reader = await command.ExecuteReaderAsync(token);
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
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                string column = GetColumnFromExpression(selector.Body);
                StringBuilder sql = new StringBuilder($"SELECT MAX({column}) FROM {_Sanitizer.SanitizeIdentifier(_TableName)}");

                if (predicate != null)
                {
                    string whereClause = BuildWhereClause(predicate);
                    sql.Append($" WHERE {whereClause}");
                }

                sql.Append(";");
                command.CommandText = sql.ToString();
                CaptureSqlFromCommand(command);

                object result = command.ExecuteScalar();
                return result == DBNull.Value || result == null ? default(TResult) : (TResult)Convert.ChangeType(result, typeof(TResult));
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<TResult> MaxAsync<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default)
        {
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                string column = GetColumnFromExpression(selector.Body);
                StringBuilder sql = new StringBuilder($"SELECT MAX({column}) FROM {_Sanitizer.SanitizeIdentifier(_TableName)}");

                if (predicate != null)
                {
                    string whereClause = BuildWhereClause(predicate);
                    sql.Append($" WHERE {whereClause}");
                }

                sql.Append(";");
                command.CommandText = sql.ToString();
                CaptureSqlFromCommand(command);

                object result = await command.ExecuteScalarAsync(token);
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
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                string column = GetColumnFromExpression(selector.Body);
                StringBuilder sql = new StringBuilder($"SELECT MIN({column}) FROM {_Sanitizer.SanitizeIdentifier(_TableName)}");

                if (predicate != null)
                {
                    string whereClause = BuildWhereClause(predicate);
                    sql.Append($" WHERE {whereClause}");
                }

                sql.Append(";");
                command.CommandText = sql.ToString();
                CaptureSqlFromCommand(command);

                object result = command.ExecuteScalar();
                return result == DBNull.Value || result == null ? default(TResult) : (TResult)Convert.ChangeType(result, typeof(TResult));
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<TResult> MinAsync<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default)
        {
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                string column = GetColumnFromExpression(selector.Body);
                StringBuilder sql = new StringBuilder($"SELECT MIN({column}) FROM {_Sanitizer.SanitizeIdentifier(_TableName)}");

                if (predicate != null)
                {
                    string whereClause = BuildWhereClause(predicate);
                    sql.Append($" WHERE {whereClause}");
                }

                sql.Append(";");
                command.CommandText = sql.ToString();
                CaptureSqlFromCommand(command);

                object result = await command.ExecuteScalarAsync(token);
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
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                string column = GetColumnFromExpression(selector.Body);
                StringBuilder sql = new StringBuilder($"SELECT AVG(CAST({column} AS REAL)) FROM {_Sanitizer.SanitizeIdentifier(_TableName)}");

                if (predicate != null)
                {
                    string whereClause = BuildWhereClause(predicate);
                    sql.Append($" WHERE {whereClause}");
                }

                sql.Append(";");
                command.CommandText = sql.ToString();
                CaptureSqlFromCommand(command);

                object result = command.ExecuteScalar();
                return result == DBNull.Value || result == null ? 0m : Convert.ToDecimal(result);
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<decimal> AverageAsync(Expression<Func<T, decimal>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default)
        {
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                string column = GetColumnFromExpression(selector.Body);
                StringBuilder sql = new StringBuilder($"SELECT AVG(CAST({column} AS REAL)) FROM {_Sanitizer.SanitizeIdentifier(_TableName)}");

                if (predicate != null)
                {
                    string whereClause = BuildWhereClause(predicate);
                    sql.Append($" WHERE {whereClause}");
                }

                sql.Append(";");
                command.CommandText = sql.ToString();
                CaptureSqlFromCommand(command);

                object result = await command.ExecuteScalarAsync(token);
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
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                string column = GetColumnFromExpression(selector.Body);
                StringBuilder sql = new StringBuilder($"SELECT COALESCE(SUM({column}), 0) FROM {_Sanitizer.SanitizeIdentifier(_TableName)}");

                if (predicate != null)
                {
                    string whereClause = BuildWhereClause(predicate);
                    sql.Append($" WHERE {whereClause}");
                }

                sql.Append(";");
                command.CommandText = sql.ToString();
                CaptureSqlFromCommand(command);

                object result = command.ExecuteScalar();
                return Convert.ToDecimal(result);
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<decimal> SumAsync(Expression<Func<T, decimal>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default)
        {
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                string column = GetColumnFromExpression(selector.Body);
                StringBuilder sql = new StringBuilder($"SELECT COALESCE(SUM({column}), 0) FROM {_Sanitizer.SanitizeIdentifier(_TableName)}");

                if (predicate != null)
                {
                    string whereClause = BuildWhereClause(predicate);
                    sql.Append($" WHERE {whereClause}");
                }

                sql.Append(";");
                command.CommandText = sql.ToString();
                CaptureSqlFromCommand(command);

                object result = await command.ExecuteScalarAsync(token);
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
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                ExpressionParser<T> parser = new ExpressionParser<T>(_ColumnMappings, _Sanitizer);
                string whereClause = BuildWhereClause(predicate);
                string setPairs = parser.ParseUpdateExpression(updateExpression);

                command.CommandText = $"UPDATE {_Sanitizer.SanitizeIdentifier(_TableName)} SET {setPairs} WHERE {whereClause};";
                CaptureSqlFromCommand(command);
                return command.ExecuteNonQuery();
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<int> BatchUpdateAsync(Expression<Func<T, bool>> predicate, Expression<Func<T, T>> updateExpression, ITransaction transaction = null, CancellationToken token = default)
        {
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                ExpressionParser<T> parser = new ExpressionParser<T>(_ColumnMappings, _Sanitizer);
                string whereClause = BuildWhereClause(predicate);
                string setPairs = parser.ParseUpdateExpression(updateExpression);

                command.CommandText = $"UPDATE {_Sanitizer.SanitizeIdentifier(_TableName)} SET {setPairs} WHERE {whereClause};";
                CaptureSqlFromCommand(command);
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
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                command.CommandText = sql;
                for (int i = 0; i < parameters.Length; i++)
                {
                    command.Parameters.AddWithValue($"@p{i}", parameters[i] ?? DBNull.Value);
                }
                CaptureSqlFromCommand(command);

                using DbDataReader reader = command.ExecuteReader();
                List<T> results = new List<T>();
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
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = await GetConnectionAndCommandAsync(transaction, CancellationToken.None);
            try
            {
                command.CommandText = sql;
                for (int i = 0; i < parameters.Length; i++)
                {
                    command.Parameters.AddWithValue($"@p{i}", parameters[i] ?? DBNull.Value);
                }
                CaptureSqlFromCommand(command);

                await using DbDataReader reader = await command.ExecuteReaderAsync();
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
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = await GetConnectionAndCommandAsync(transaction, CancellationToken.None);
            try
            {
                command.CommandText = sql;
                for (int i = 0; i < parameters.Length; i++)
                {
                    command.Parameters.AddWithValue($"@p{i}", parameters[i] ?? DBNull.Value);
                }
                CaptureSqlFromCommand(command);

                await using DbDataReader reader = await command.ExecuteReaderAsync();
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
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                command.CommandText = sql;
                for (int i = 0; i < parameters.Length; i++)
                {
                    command.Parameters.AddWithValue($"@p{i}", parameters[i] ?? DBNull.Value);
                }
                CaptureSqlFromCommand(command);

                using DbDataReader reader = command.ExecuteReader();
                List<TResult> results = new List<TResult>();
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
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                command.CommandText = sql;
                for (int i = 0; i < parameters.Length; i++)
                {
                    command.Parameters.AddWithValue($"@p{i}", parameters[i] ?? DBNull.Value);
                }
                CaptureSqlFromCommand(command);

                return command.ExecuteNonQuery();
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<int> ExecuteSqlAsync(string sql, ITransaction transaction = null, CancellationToken token = default, params object[] parameters)
        {
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = await GetConnectionAndCommandAsync(transaction, CancellationToken.None);
            try
            {
                command.CommandText = sql;
                for (int i = 0; i < parameters.Length; i++)
                {
                    command.Parameters.AddWithValue($"@p{i}", parameters[i] ?? DBNull.Value);
                }
                CaptureSqlFromCommand(command);

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
            SqliteConnection connection = GetConnection();
            connection.Open();
            SqliteTransaction transaction = connection.BeginTransaction();
            return new SqliteRepositoryTransaction(connection, transaction);
        }

        public async Task<ITransaction> BeginTransactionAsync(CancellationToken token = default)
        {
            SqliteConnection connection = GetConnection();
            await connection.OpenAsync(token);
            SqliteTransaction transaction = (SqliteTransaction)await connection.BeginTransactionAsync(token);
            return new SqliteRepositoryTransaction(connection, transaction);
        }

        // Existence checks
        public bool Exists(Expression<Func<T, bool>> predicate, ITransaction transaction = null)
        {
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                string whereClause = BuildWhereClause(predicate);
                command.CommandText = $"SELECT EXISTS(SELECT 1 FROM {_Sanitizer.SanitizeIdentifier(_TableName)} WHERE {whereClause} LIMIT 1);";
                CaptureSqlFromCommand(command);
                return Convert.ToBoolean(command.ExecuteScalar());
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<bool> ExistsAsync(Expression<Func<T, bool>> predicate, ITransaction transaction = null, CancellationToken token = default)
        {
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                string whereClause = BuildWhereClause(predicate);
                command.CommandText = $"SELECT EXISTS(SELECT 1 FROM {_Sanitizer.SanitizeIdentifier(_TableName)} WHERE {whereClause} LIMIT 1);";
                CaptureSqlFromCommand(command);
                object result = await command.ExecuteScalarAsync(token);
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
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                command.CommandText = $"SELECT EXISTS(SELECT 1 FROM {_Sanitizer.SanitizeIdentifier(_TableName)} WHERE {_Sanitizer.SanitizeIdentifier(_PrimaryKeyColumn)} = @id LIMIT 1);";
                command.Parameters.AddWithValue("@id", id);
                CaptureSqlFromCommand(command);
                return Convert.ToBoolean(command.ExecuteScalar());
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<bool> ExistsByIdAsync(object id, ITransaction transaction = null, CancellationToken token = default)
        {
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                command.CommandText = $"SELECT EXISTS(SELECT 1 FROM {_Sanitizer.SanitizeIdentifier(_TableName)} WHERE {_Sanitizer.SanitizeIdentifier(_PrimaryKeyColumn)} = @id LIMIT 1);";
                command.Parameters.AddWithValue("@id", id);
                CaptureSqlFromCommand(command);
                object result = await command.ExecuteScalarAsync(token);
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
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                StringBuilder sql = new StringBuilder($"SELECT COUNT(*) FROM {_Sanitizer.SanitizeIdentifier(_TableName)}");

                if (predicate != null)
                {
                    string whereClause = BuildWhereClause(predicate);
                    sql.Append($" WHERE {whereClause}");
                }

                sql.Append(";");
                command.CommandText = sql.ToString();
                CaptureSqlFromCommand(command);
                return Convert.ToInt32(command.ExecuteScalar());
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<int> CountAsync(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default)
        {
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                StringBuilder sql = new StringBuilder($"SELECT COUNT(*) FROM {_Sanitizer.SanitizeIdentifier(_TableName)}");

                if (predicate != null)
                {
                    string whereClause = BuildWhereClause(predicate);
                    sql.Append($" WHERE {whereClause}");
                }

                sql.Append(";");
                command.CommandText = sql.ToString();
                CaptureSqlFromCommand(command);
                object result = await command.ExecuteScalarAsync(token);
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
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                List<string> columns = new List<string>();
                List<string> parameters = new List<string>();

                foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
                {
                    string columnName = kvp.Key;
                    PropertyInfo property = kvp.Value;
                    PropertyAttribute columnAttr = property.GetCustomAttribute<PropertyAttribute>();

                    // Skip auto-increment primary keys
                    if (columnAttr != null &&
                        (columnAttr.PropertyFlags & Flags.PrimaryKey) == Flags.PrimaryKey &&
                        (columnAttr.PropertyFlags & Flags.AutoIncrement) == Flags.AutoIncrement)
                    {
                        continue;
                    }

                    columns.Add(_Sanitizer.SanitizeIdentifier(columnName));
                    parameters.Add($"@{columnName}");
                    object value = property.GetValue(entity);
                    
                    // Set default version for version columns if not already set
                    if (_VersionColumnInfo != null && columnName == _VersionColumnInfo.ColumnName)
                    {
                        if (value == null || (value is int intValue && intValue == 0) || 
                            (value is DateTime dtValue && dtValue == DateTime.MinValue) ||
                            (value is Guid guidValue && guidValue == Guid.Empty))
                        {
                            value = _VersionColumnInfo.GetDefaultVersion();
                            _VersionColumnInfo.SetValue(entity, value);
                        }
                    }
                    
                    object convertedValue = _DataTypeConverter.ConvertToDatabase(value, property.PropertyType, property);
                    command.Parameters.AddWithValue($"@{columnName}", convertedValue);
                }

                command.CommandText = $"INSERT INTO {_Sanitizer.SanitizeIdentifier(_TableName)} ({string.Join(", ", columns)}) VALUES ({string.Join(", ", parameters)}); SELECT last_insert_rowid();";
                CaptureSqlFromCommand(command);

                long insertedId = Convert.ToInt64(command.ExecuteScalar());

                // Set the ID back on the entity if it's auto-increment
                if (_PrimaryKeyProperty != null)
                {
                    PropertyAttribute columnAttr = _PrimaryKeyProperty.GetCustomAttribute<PropertyAttribute>();
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
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                List<string> columns = new List<string>();
                List<string> parameters = new List<string>();

                foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
                {
                    string columnName = kvp.Key;
                    PropertyInfo property = kvp.Value;
                    PropertyAttribute columnAttr = property.GetCustomAttribute<PropertyAttribute>();

                    // Skip auto-increment primary keys
                    if (columnAttr != null &&
                        (columnAttr.PropertyFlags & Flags.PrimaryKey) == Flags.PrimaryKey &&
                        (columnAttr.PropertyFlags & Flags.AutoIncrement) == Flags.AutoIncrement)
                    {
                        continue;
                    }

                    columns.Add(_Sanitizer.SanitizeIdentifier(columnName));
                    parameters.Add($"@{columnName}");
                    object value = property.GetValue(entity);
                    
                    // Set default version for version columns if not already set
                    if (_VersionColumnInfo != null && columnName == _VersionColumnInfo.ColumnName)
                    {
                        if (value == null || (value is int intValue && intValue == 0) || 
                            (value is DateTime dtValue && dtValue == DateTime.MinValue) ||
                            (value is Guid guidValue && guidValue == Guid.Empty))
                        {
                            value = _VersionColumnInfo.GetDefaultVersion();
                            _VersionColumnInfo.SetValue(entity, value);
                        }
                    }
                    
                    object convertedValue = _DataTypeConverter.ConvertToDatabase(value, property.PropertyType, property);
                    command.Parameters.AddWithValue($"@{columnName}", convertedValue);
                }

                command.CommandText = $"INSERT INTO {_Sanitizer.SanitizeIdentifier(_TableName)} ({string.Join(", ", columns)}) VALUES ({string.Join(", ", parameters)}); SELECT last_insert_rowid();";
                CaptureSqlFromCommand(command);

                long insertedId = Convert.ToInt64(await command.ExecuteScalarAsync(token));

                // Set the ID back on the entity if it's auto-increment
                if (_PrimaryKeyProperty != null)
                {
                    PropertyAttribute columnAttr = _PrimaryKeyProperty.GetCustomAttribute<PropertyAttribute>();
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
            List<T> entitiesList = entities.ToList();
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

                List<T> results = new List<T>();
                
                if (_BatchConfig.EnableMultiRowInsert && entitiesList.Count > 1)
                {
                    results.AddRange(CreateManyOptimized(entitiesList, transaction));
                }
                else
                {
                    // Fallback to individual inserts
                    foreach (T entity in entitiesList)
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
            List<T> entitiesList = entities.ToList();
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

                List<T> results = new List<T>();
                
                if (_BatchConfig.EnableMultiRowInsert && entitiesList.Count > 1)
                {
                    results.AddRange(await CreateManyOptimizedAsync(entitiesList, transaction, token));
                }
                else
                {
                    // Fallback to individual inserts
                    foreach (T entity in entitiesList)
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
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                List<string> setPairs = new List<string>();
                object idValue = null;
                object currentVersion = null;

                foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
                {
                    string columnName = kvp.Key;
                    PropertyInfo property = kvp.Value;
                    object value = property.GetValue(entity);

                    if (columnName == _PrimaryKeyColumn)
                    {
                        idValue = value;
                    }
                    else if (_VersionColumnInfo != null && columnName == _VersionColumnInfo.ColumnName)
                    {
                        currentVersion = value;
                        object newVersion = _VersionColumnInfo.IncrementVersion(currentVersion);
                        setPairs.Add($"{_Sanitizer.SanitizeIdentifier(columnName)} = @new_version");
                        object convertedNewVersion = _DataTypeConverter.ConvertToDatabase(newVersion, _VersionColumnInfo.PropertyType, property);
                        command.Parameters.AddWithValue("@new_version", convertedNewVersion);
                        _VersionColumnInfo.SetValue(entity, newVersion);
                    }
                    else
                    {
                        setPairs.Add($"{_Sanitizer.SanitizeIdentifier(columnName)} = @{columnName}");
                        object convertedValue = _DataTypeConverter.ConvertToDatabase(value, property.PropertyType, property);
                        command.Parameters.AddWithValue($"@{columnName}", convertedValue);
                    }
                }

                command.Parameters.AddWithValue("@id", idValue);

                string whereClause = $"{_Sanitizer.SanitizeIdentifier(_PrimaryKeyColumn)} = @id";
                if (_VersionColumnInfo != null)
                {
                    object convertedCurrentVersion = _DataTypeConverter.ConvertToDatabase(currentVersion, _VersionColumnInfo.PropertyType, _VersionColumnInfo.Property);
                    command.Parameters.AddWithValue("@current_version", convertedCurrentVersion ?? DBNull.Value);
                    whereClause += $" AND {_Sanitizer.SanitizeIdentifier(_VersionColumnInfo.ColumnName)} = @current_version";
                }

                command.CommandText = $"UPDATE {_Sanitizer.SanitizeIdentifier(_TableName)} SET {string.Join(", ", setPairs)} WHERE {whereClause};";
                CaptureSqlFromCommand(command);

                int rowsAffected = command.ExecuteNonQuery();

                if (rowsAffected == 0)
                {
                    if (_VersionColumnInfo != null)
                    {
                        T currentDbEntity = ReadById(idValue, transaction);
                        if (currentDbEntity == null)
                        {
                            throw new InvalidOperationException($"Entity with {_PrimaryKeyColumn} = {idValue} not found in database");
                        }
                        
                        object actualVersion = _VersionColumnInfo.GetValue(currentDbEntity);
                        
                        // Create original entity by copying incoming entity but with the expected version
                        // This represents the entity state as the user originally loaded it
                        T originalEntity = CreateCopyOfEntity(entity);
                        _VersionColumnInfo.SetValue(originalEntity, currentVersion);
                        
                        // Try to resolve the conflict
                        ConflictResolutionStrategy strategy = _ConflictResolver.DefaultStrategy;
                        bool resolved = _ConflictResolver.TryResolveConflict(currentDbEntity, entity, originalEntity, strategy, out T resolvedEntity);
                        
                        if (resolved && resolvedEntity != null)
                        {
                            // Copy the current version from the database to the resolved entity
                            _VersionColumnInfo.SetValue(resolvedEntity, actualVersion);
                            // Retry the update with the resolved entity
                            return Update(resolvedEntity, transaction);
                        }
                        else
                        {
                            throw new OptimisticConcurrencyException(entity, currentVersion, actualVersion);
                        }
                    }
                    else
                    {
                        throw new InvalidOperationException($"No rows were updated for entity with {_PrimaryKeyColumn} = {idValue}");
                    }
                }

                return entity;
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<T> UpdateAsync(T entity, ITransaction transaction = null, CancellationToken token = default)
        {
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                List<string> setPairs = new List<string>();
                object idValue = null;
                object currentVersion = null;

                foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
                {
                    string columnName = kvp.Key;
                    PropertyInfo property = kvp.Value;
                    object value = property.GetValue(entity);

                    if (columnName == _PrimaryKeyColumn)
                    {
                        idValue = value;
                    }
                    else if (_VersionColumnInfo != null && columnName == _VersionColumnInfo.ColumnName)
                    {
                        currentVersion = value;
                        object newVersion = _VersionColumnInfo.IncrementVersion(currentVersion);
                        setPairs.Add($"{_Sanitizer.SanitizeIdentifier(columnName)} = @new_version");
                        object convertedNewVersion = _DataTypeConverter.ConvertToDatabase(newVersion, _VersionColumnInfo.PropertyType, property);
                        command.Parameters.AddWithValue("@new_version", convertedNewVersion);
                        _VersionColumnInfo.SetValue(entity, newVersion);
                    }
                    else
                    {
                        setPairs.Add($"{_Sanitizer.SanitizeIdentifier(columnName)} = @{columnName}");
                        object convertedValue = _DataTypeConverter.ConvertToDatabase(value, property.PropertyType, property);
                        command.Parameters.AddWithValue($"@{columnName}", convertedValue);
                    }
                }

                command.Parameters.AddWithValue("@id", idValue);

                string whereClause = $"{_Sanitizer.SanitizeIdentifier(_PrimaryKeyColumn)} = @id";
                if (_VersionColumnInfo != null)
                {
                    object convertedCurrentVersion = _DataTypeConverter.ConvertToDatabase(currentVersion, _VersionColumnInfo.PropertyType, _VersionColumnInfo.Property);
                    command.Parameters.AddWithValue("@current_version", convertedCurrentVersion ?? DBNull.Value);
                    whereClause += $" AND {_Sanitizer.SanitizeIdentifier(_VersionColumnInfo.ColumnName)} = @current_version";
                }

                command.CommandText = $"UPDATE {_Sanitizer.SanitizeIdentifier(_TableName)} SET {string.Join(", ", setPairs)} WHERE {whereClause};";
                CaptureSqlFromCommand(command);

                int rowsAffected = await command.ExecuteNonQueryAsync(token);

                if (rowsAffected == 0)
                {
                    if (_VersionColumnInfo != null)
                    {
                        T currentDbEntity = await ReadByIdAsync(idValue, transaction, token);
                        if (currentDbEntity == null)
                        {
                            throw new InvalidOperationException($"Entity with {_PrimaryKeyColumn} = {idValue} not found in database");
                        }
                        
                        object actualVersion = _VersionColumnInfo.GetValue(currentDbEntity);
                        
                        // Create original entity by copying incoming entity but with the expected version
                        // This represents the entity state as the user originally loaded it
                        T originalEntity = CreateCopyOfEntity(entity);
                        _VersionColumnInfo.SetValue(originalEntity, currentVersion);
                        
                        // Try to resolve the conflict
                        ConflictResolutionStrategy strategy = _ConflictResolver.DefaultStrategy;
                        TryResolveConflictResult<T> result = await _ConflictResolver.TryResolveConflictAsync(currentDbEntity, entity, originalEntity, strategy);
                        
                        if (result.Success && result.ResolvedEntity != null)
                        {
                            // Copy the current version from the database to the resolved entity
                            _VersionColumnInfo.SetValue(result.ResolvedEntity, actualVersion);
                            // Retry the update with the resolved entity
                            return await UpdateAsync(result.ResolvedEntity, transaction, token);
                        }
                        else
                        {
                            throw new OptimisticConcurrencyException(entity, currentVersion, actualVersion);
                        }
                    }
                    else
                    {
                        throw new InvalidOperationException($"No rows were updated for entity with {_PrimaryKeyColumn} = {idValue}");
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

        public int UpdateMany(Expression<Func<T, bool>> predicate, Action<T> updateAction, ITransaction transaction = null)
        {
            // For simplicity, fetch and update each entity individually
            List<T> entities = ReadMany(predicate, transaction).ToList();

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

                foreach (T entity in entities)
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
            List<T> entities = new List<T>();
            await foreach (T entity in ReadManyAsync(predicate, transaction, token))
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

                foreach (T entity in entities)
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
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                string whereClause = BuildWhereClause(predicate);
                string columnName = GetColumnFromExpression(field.Body);
                PropertyInfo propertyInfo = GetPropertyInfoFromColumnName(columnName);

                command.CommandText = $"UPDATE {_Sanitizer.SanitizeIdentifier(_TableName)} SET {columnName} = @value WHERE {whereClause};";
                object convertedValue = _DataTypeConverter.ConvertToDatabase(value, typeof(TField), propertyInfo);
                command.Parameters.AddWithValue("@value", convertedValue);
                CaptureSqlFromCommand(command);

                return command.ExecuteNonQuery();
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<int> UpdateFieldAsync<TField>(Expression<Func<T, bool>> predicate, Expression<Func<T, TField>> field, TField value, ITransaction transaction = null, CancellationToken token = default)
        {
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                string whereClause = BuildWhereClause(predicate);
                string columnName = GetColumnFromExpression(field.Body);
                PropertyInfo propertyInfo = GetPropertyInfoFromColumnName(columnName);

                command.CommandText = $"UPDATE {_Sanitizer.SanitizeIdentifier(_TableName)} SET {columnName} = @value WHERE {whereClause};";
                object convertedValue = _DataTypeConverter.ConvertToDatabase(value, typeof(TField), propertyInfo);
                command.Parameters.AddWithValue("@value", convertedValue);
                CaptureSqlFromCommand(command);

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
            object idValue = GetPrimaryKeyValue(entity);
            return DeleteById(idValue, transaction);
        }

        public async Task<bool> DeleteAsync(T entity, ITransaction transaction = null, CancellationToken token = default)
        {
            object idValue = GetPrimaryKeyValue(entity);
            return await DeleteByIdAsync(idValue, transaction, token);
        }

        public bool DeleteById(object id, ITransaction transaction = null)
        {
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                command.CommandText = $"DELETE FROM {_Sanitizer.SanitizeIdentifier(_TableName)} WHERE {_Sanitizer.SanitizeIdentifier(_PrimaryKeyColumn)} = @id;";
                command.Parameters.AddWithValue("@id", id);
                CaptureSqlFromCommand(command);

                return command.ExecuteNonQuery() > 0;
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<bool> DeleteByIdAsync(object id, ITransaction transaction = null, CancellationToken token = default)
        {
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                command.CommandText = $"DELETE FROM {_Sanitizer.SanitizeIdentifier(_TableName)} WHERE {_Sanitizer.SanitizeIdentifier(_PrimaryKeyColumn)} = @id;";
                command.Parameters.AddWithValue("@id", id);
                CaptureSqlFromCommand(command);

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
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                string whereClause = BuildWhereClause(predicate);
                command.CommandText = $"DELETE FROM {_Sanitizer.SanitizeIdentifier(_TableName)} WHERE {whereClause};";
                CaptureSqlFromCommand(command);
                return command.ExecuteNonQuery();
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        public async Task<int> DeleteManyAsync(Expression<Func<T, bool>> predicate, ITransaction transaction = null, CancellationToken token = default)
        {
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                string whereClause = BuildWhereClause(predicate);
                command.CommandText = $"DELETE FROM {_Sanitizer.SanitizeIdentifier(_TableName)} WHERE {whereClause};";
                CaptureSqlFromCommand(command);
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
            (SqliteConnection connection, SqliteCommand command, bool shouldReturnToPool) = GetConnectionAndCommand(transaction);
            try
            {
                command.CommandText = $"DELETE FROM {_Sanitizer.SanitizeIdentifier(_TableName)};";
                CaptureSqlFromCommand(command);
                return command.ExecuteNonQuery();
            }
            finally
            {
                CleanupConnection(connection, command, shouldReturnToPool);
            }
        }

        public async Task<int> DeleteAllAsync(ITransaction transaction = null, CancellationToken token = default)
        {
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                command.CommandText = $"DELETE FROM {_Sanitizer.SanitizeIdentifier(_TableName)};";
                CaptureSqlFromCommand(command);
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
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = GetConnectionAndCommand(transaction);
            try
            {
                List<string> columns = new List<string>();
                List<string> parameters = new List<string>();
                List<string> updatePairs = new List<string>();

                foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
                {
                    string columnName = kvp.Key;
                    PropertyInfo property = kvp.Value;
                    object value = property.GetValue(entity);

                    columns.Add(_Sanitizer.SanitizeIdentifier(columnName));
                    parameters.Add($"@{columnName}");
                    object convertedValue = _DataTypeConverter.ConvertToDatabase(value, property.PropertyType, property);
                    command.Parameters.AddWithValue($"@{columnName}", convertedValue);

                    if (columnName != _PrimaryKeyColumn)
                    {
                        updatePairs.Add($"{_Sanitizer.SanitizeIdentifier(columnName)} = excluded.{_Sanitizer.SanitizeIdentifier(columnName)}");
                    }
                }

                StringBuilder sql = new StringBuilder();
                sql.Append($"INSERT INTO {_Sanitizer.SanitizeIdentifier(_TableName)} ({string.Join(", ", columns)}) ");
                sql.Append($"VALUES ({string.Join(", ", parameters)}) ");
                sql.Append($"ON CONFLICT({_Sanitizer.SanitizeIdentifier(_PrimaryKeyColumn)}) DO UPDATE SET ");
                sql.Append(string.Join(", ", updatePairs));
                sql.Append(";");

                command.CommandText = sql.ToString();
                CaptureSqlFromCommand(command);
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
            (SqliteConnection connection, SqliteCommand command, bool shouldDispose) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                List<string> columns = new List<string>();
                List<string> parameters = new List<string>();
                List<string> updatePairs = new List<string>();

                foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
                {
                    string columnName = kvp.Key;
                    PropertyInfo property = kvp.Value;
                    object value = property.GetValue(entity);

                    columns.Add(_Sanitizer.SanitizeIdentifier(columnName));
                    parameters.Add($"@{columnName}");
                    object convertedValue = _DataTypeConverter.ConvertToDatabase(value, property.PropertyType, property);
                    command.Parameters.AddWithValue($"@{columnName}", convertedValue);

                    if (columnName != _PrimaryKeyColumn)
                    {
                        updatePairs.Add($"{_Sanitizer.SanitizeIdentifier(columnName)} = excluded.{_Sanitizer.SanitizeIdentifier(columnName)}");
                    }
                }

                StringBuilder sql = new StringBuilder();
                sql.Append($"INSERT INTO {_Sanitizer.SanitizeIdentifier(_TableName)} ({string.Join(", ", columns)}) ");
                sql.Append($"VALUES ({string.Join(", ", parameters)}) ");
                sql.Append($"ON CONFLICT({_Sanitizer.SanitizeIdentifier(_PrimaryKeyColumn)}) DO UPDATE SET ");
                sql.Append(string.Join(", ", updatePairs));
                sql.Append(";");

                command.CommandText = sql.ToString();
                CaptureSqlFromCommand(command);
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

                List<T> results = new List<T>();
                foreach (T entity in entities)
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

                List<T> results = new List<T>();
                foreach (T entity in entities)
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
        
        private T CreateCopyOfEntity(T entity)
        {
            if (entity == null)
                return null;
                
            T copy = new T();
            foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
            {
                PropertyInfo property = kvp.Value;
                if (property.CanRead && property.CanWrite)
                {
                    object value = property.GetValue(entity);
                    property.SetValue(copy, value);
                }
            }
            return copy;
        }
        
        private T ReadByIdAtVersion(object id, object version, ITransaction transaction = null)
        {
            (SqliteConnection connection, SqliteCommand command, bool shouldReturnToPool) = GetConnectionAndCommand(transaction);
            try
            {
                string sql = $"SELECT * FROM {_Sanitizer.SanitizeIdentifier(_TableName)} WHERE {_Sanitizer.SanitizeIdentifier(_PrimaryKeyColumn)} = @id";
                if (_VersionColumnInfo != null)
                {
                    sql += $" AND {_Sanitizer.SanitizeIdentifier(_VersionColumnInfo.ColumnName)} = @version";
                }
                sql += ";";
                
                command.CommandText = sql;
                command.Parameters.AddWithValue("@id", id);
                if (_VersionColumnInfo != null)
                {
                    object convertedVersion = _DataTypeConverter.ConvertToDatabase(version, _VersionColumnInfo.PropertyType, _VersionColumnInfo.Property);
                    command.Parameters.AddWithValue("@version", convertedVersion ?? DBNull.Value);
                }
                CaptureSqlFromCommand(command);

                using DbDataReader reader = command.ExecuteReader();
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
        
        private async Task<T> ReadByIdAtVersionAsync(object id, object version, ITransaction transaction = null, CancellationToken token = default)
        {
            (SqliteConnection connection, SqliteCommand command, bool shouldReturnToPool) = await GetConnectionAndCommandAsync(transaction, token);
            try
            {
                string sql = $"SELECT * FROM {_Sanitizer.SanitizeIdentifier(_TableName)} WHERE {_Sanitizer.SanitizeIdentifier(_PrimaryKeyColumn)} = @id";
                if (_VersionColumnInfo != null)
                {
                    sql += $" AND {_Sanitizer.SanitizeIdentifier(_VersionColumnInfo.ColumnName)} = @version";
                }
                sql += ";";
                
                command.CommandText = sql;
                command.Parameters.AddWithValue("@id", id);
                if (_VersionColumnInfo != null)
                {
                    object convertedVersion = _DataTypeConverter.ConvertToDatabase(version, _VersionColumnInfo.PropertyType, _VersionColumnInfo.Property);
                    command.Parameters.AddWithValue("@version", convertedVersion ?? DBNull.Value);
                }
                CaptureSqlFromCommand(command);

                await using DbDataReader reader = await command.ExecuteReaderAsync(token);
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
            // Use provided transaction or check for ambient transaction
            ITransaction effectiveTransaction = transaction ?? TransactionScope.Current?.Transaction;

            if (effectiveTransaction != null)
            {
                SqliteCommand command = new SqliteCommand();
                command.Connection = (SqliteConnection)effectiveTransaction.Connection;
                command.Transaction = (SqliteTransaction)effectiveTransaction.Transaction;
                return ((SqliteConnection)effectiveTransaction.Connection, command, false);
            }
            else
            {
                SqliteConnection connection = GetConnection();
                if (connection.State != ConnectionState.Open)
                {
                    connection.Open();
                }
                SqliteCommand command = new SqliteCommand();
                command.Connection = connection;
                return (connection, command, true);
            }
        }

        internal async Task<(SqliteConnection connection, SqliteCommand command, bool shouldReturnToPool)> GetConnectionAndCommandAsync(ITransaction transaction, CancellationToken token)
        {
            // Use provided transaction or check for ambient transaction
            ITransaction effectiveTransaction = transaction ?? TransactionScope.Current?.Transaction;

            if (effectiveTransaction != null)
            {
                SqliteCommand command = new SqliteCommand();
                command.Connection = (SqliteConnection)effectiveTransaction.Connection;
                command.Transaction = (SqliteTransaction)effectiveTransaction.Transaction;
                return ((SqliteConnection)effectiveTransaction.Connection, command, false);
            }
            else
            {
                SqliteConnection connection = await GetConnectionAsync(token);
                if (connection.State != ConnectionState.Open)
                {
                    await connection.OpenAsync(token);
                }
                SqliteCommand command = new SqliteCommand();
                command.Connection = connection;
                return (connection, command, true);
            }
        }

        protected string GetEntityName()
        {
            EntityAttribute entityAttr = typeof(T).GetCustomAttribute<EntityAttribute>();
            if (entityAttr == null)
                throw new InvalidOperationException($"Type {typeof(T).Name} must have an Entity attribute");
            return entityAttr.Name;
        }

        protected (string columnName, PropertyInfo property) GetPrimaryKeyInfo()
        {
            foreach (PropertyInfo prop in typeof(T).GetProperties())
            {
                PropertyAttribute attr = prop.GetCustomAttribute<PropertyAttribute>();
                if (attr != null && (attr.PropertyFlags & Flags.PrimaryKey) == Flags.PrimaryKey)
                {
                    return (attr.Name, prop);
                }
            }

            throw new InvalidOperationException($"Type {typeof(T).Name} must have a primary key column");
        }

        protected Dictionary<string, PropertyInfo> GetColumnMappings()
        {
            Dictionary<string, PropertyInfo> mappings = new Dictionary<string, PropertyInfo>();

            foreach (PropertyInfo prop in typeof(T).GetProperties())
            {
                PropertyAttribute attr = prop.GetCustomAttribute<PropertyAttribute>();
                if (attr != null)
                {
                    mappings[attr.Name] = prop;
                }
            }

            return mappings;
        }

        protected Dictionary<PropertyInfo, ForeignKeyAttribute> GetForeignKeys()
        {
            Dictionary<PropertyInfo, ForeignKeyAttribute> foreignKeys = new Dictionary<PropertyInfo, ForeignKeyAttribute>();

            foreach (PropertyInfo prop in typeof(T).GetProperties())
            {
                ForeignKeyAttribute fkAttr = prop.GetCustomAttribute<ForeignKeyAttribute>();
                if (fkAttr != null)
                {
                    foreignKeys[prop] = fkAttr;
                }
            }

            return foreignKeys;
        }

        protected Dictionary<PropertyInfo, NavigationPropertyAttribute> GetNavigationProperties()
        {
            Dictionary<PropertyInfo, NavigationPropertyAttribute> navProps = new Dictionary<PropertyInfo, NavigationPropertyAttribute>();

            foreach (PropertyInfo prop in typeof(T).GetProperties())
            {
                NavigationPropertyAttribute navAttr = prop.GetCustomAttribute<NavigationPropertyAttribute>();
                if (navAttr != null)
                {
                    navProps[prop] = navAttr;
                }
            }

            return navProps;
        }

        protected VersionColumnInfo GetVersionColumnInfo()
        {
            foreach (PropertyInfo prop in typeof(T).GetProperties())
            {
                VersionColumnAttribute versionAttr = prop.GetCustomAttribute<VersionColumnAttribute>();
                if (versionAttr != null)
                {
                    PropertyAttribute propAttr = prop.GetCustomAttribute<PropertyAttribute>();
                    if (propAttr == null)
                    {
                        throw new InvalidOperationException(
                            $"Property {prop.Name} has VersionColumnAttribute but no PropertyAttribute for column mapping.");
                    }
                    
                    return new VersionColumnInfo
                    {
                        ColumnName = propAttr.Name,
                        Property = prop,
                        Type = versionAttr.Type,
                        PropertyType = prop.PropertyType
                    };
                }
            }
            
            return null;
        }

        protected object GetPrimaryKeyValue(T entity)
        {
            return _PrimaryKeyProperty.GetValue(entity);
        }

        internal string BuildWhereClause(Expression<Func<T, bool>> predicate)
        {
            ExpressionParser<T> parser = new ExpressionParser<T>(_ColumnMappings, _Sanitizer);
            return parser.ParseExpression(predicate.Body);
        }

        internal string GetColumnFromExpression(Expression expression)
        {
            ExpressionParser<T> parser = new ExpressionParser<T>(_ColumnMappings, _Sanitizer);
            string columnName = parser.GetColumnFromExpression(expression);
            return _Sanitizer.SanitizeIdentifier(columnName);
        }

        internal PropertyInfo GetPropertyInfoFromColumnName(string sanitizedColumnName)
        {
            // The columnName from GetColumnFromExpression is sanitized, but _ColumnMappings uses unsanitized names
            // Find the PropertyInfo by matching the column names
            foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
            {
                if (_Sanitizer.SanitizeIdentifier(kvp.Key) == sanitizedColumnName)
                {
                    return kvp.Value;
                }
            }
            return null;
        }

        internal T MapReaderToEntity(IDataReader reader)
        {
            T entity = new T();

            foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
            {
                string columnName = kvp.Key;
                PropertyInfo property = kvp.Value;

                try
                {
                    int ordinal = reader.GetOrdinal(columnName);
                    if (!reader.IsDBNull(ordinal))
                    {
                        object value = reader.GetValue(ordinal);
                        object convertedValue = _DataTypeConverter.ConvertFromDatabase(value, property.PropertyType, property);
                        property.SetValue(entity, convertedValue);
                    }
                }
                catch (ArgumentOutOfRangeException)
                {
                    // Column not present in result set - skip this property
                    continue;
                }
            }

            return entity;
        }

        protected TResult MapReaderToType<TResult>(IDataReader reader) where TResult : new()
        {
            TResult result = new TResult();
            Type resultType = typeof(TResult);

            for (int i = 0; i < reader.FieldCount; i++)
            {
                string columnName = reader.GetName(i);
                PropertyInfo property = resultType.GetProperty(columnName, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);

                if (property != null && !reader.IsDBNull(i))
                {
                    object value = reader.GetValue(i);
                    property.SetValue(result, Convert.ChangeType(value, property.PropertyType));
                }
            }

            return result;
        }

        private IEnumerable<T> CreateManyOptimized(IList<T> entities, ITransaction transaction)
        {
            (SqliteConnection connection, SqliteCommand _, bool shouldReturnToPool) = GetConnectionAndCommand(transaction);
            List<T> results = new List<T>();
            
            try
            {
                IEnumerable<IList<T>> batches = CreateBatches(entities);
                Dictionary<int, SqliteCommand> preparedCommands = new Dictionary<int, SqliteCommand>();
                
                foreach (IList<T> batch in batches)
                {
                    int batchSize = batch.Count;
                    
                    if (EnablePreparedStatementReuse && preparedCommands.TryGetValue(batchSize, out SqliteCommand preparedCommand))
                    {
                        preparedCommand.Parameters.Clear();
                        AddParametersForBatch(preparedCommand, batch);
                        ExecuteBatchInsert(preparedCommand, batch);
                    }
                    else
                    {
                        using SqliteCommand command = new SqliteCommand();
                        command.Connection = connection;
                        if (transaction != null)
                            command.Transaction = (SqliteTransaction)transaction.Transaction;
                        
                        BuildBatchInsertCommand(command, batch);
                        ExecuteBatchInsert(command, batch);
                        
                        if (EnablePreparedStatementReuse && !preparedCommands.ContainsKey(batchSize))
                        {
                            SqliteCommand newPreparedCommand = new SqliteCommand(command.CommandText, connection);
                            if (transaction != null)
                                newPreparedCommand.Transaction = (SqliteTransaction)transaction.Transaction;
                            preparedCommands[batchSize] = newPreparedCommand;
                        }
                    }
                    
                    results.AddRange(batch);
                }
                
                foreach (SqliteCommand preparedCommand in preparedCommands.Values)
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
            (SqliteConnection connection, SqliteCommand _, bool shouldReturnToPool) = await GetConnectionAndCommandAsync(transaction, token);
            List<T> results = new List<T>();
            
            try
            {
                IEnumerable<IList<T>> batches = CreateBatches(entities);
                Dictionary<int, SqliteCommand> preparedCommands = new Dictionary<int, SqliteCommand>();
                
                foreach (IList<T> batch in batches)
                {
                    int batchSize = batch.Count;
                    
                    if (EnablePreparedStatementReuse && preparedCommands.TryGetValue(batchSize, out SqliteCommand preparedCommand))
                    {
                        preparedCommand.Parameters.Clear();
                        AddParametersForBatch(preparedCommand, batch);
                        await ExecuteBatchInsertAsync(preparedCommand, batch, token);
                    }
                    else
                    {
                        using SqliteCommand command = new SqliteCommand();
                        command.Connection = connection;
                        if (transaction != null)
                            command.Transaction = (SqliteTransaction)transaction.Transaction;
                        
                        BuildBatchInsertCommand(command, batch);
                        await ExecuteBatchInsertAsync(command, batch, token);
                        
                        if (EnablePreparedStatementReuse && !preparedCommands.ContainsKey(batchSize))
                        {
                            SqliteCommand newPreparedCommand = new SqliteCommand(command.CommandText, connection);
                            if (transaction != null)
                                newPreparedCommand.Transaction = (SqliteTransaction)transaction.Transaction;
                            preparedCommands[batchSize] = newPreparedCommand;
                        }
                    }
                    
                    results.AddRange(batch);
                }
                
                foreach (SqliteCommand preparedCommand in preparedCommands.Values)
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
            List<string> nonAutoIncrementColumns = GetNonAutoIncrementColumns();
            int parametersPerEntity = nonAutoIncrementColumns.Count;
            int maxEntitiesPerBatch = Math.Min(
                MaxRowsPerBatch,
                MaxParametersPerStatement / parametersPerEntity);
                
            if (maxEntitiesPerBatch <= 0)
                maxEntitiesPerBatch = 1;
            
            for (int i = 0; i < entities.Count; i += maxEntitiesPerBatch)
            {
                int batchSize = Math.Min(maxEntitiesPerBatch, entities.Count - i);
                List<T> batch = new List<T>(batchSize);
                for (int j = 0; j < batchSize; j++)
                {
                    batch.Add(entities[i + j]);
                }
                yield return batch;
            }
        }
        
        private List<string> GetNonAutoIncrementColumns()
        {
            List<string> columns = new List<string>();
            foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
            {
                string columnName = kvp.Key;
                PropertyInfo property = kvp.Value;
                PropertyAttribute columnAttr = property.GetCustomAttribute<PropertyAttribute>();

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
            List<string> columns = GetNonAutoIncrementColumns();
            List<string> sanitizedColumns = columns.Select(c => _Sanitizer.SanitizeIdentifier(c)).ToList();
            List<string> valuesList = new List<string>();
            
            for (int i = 0; i < entities.Count; i++)
            {
                List<string> parameters = new List<string>();
                foreach (string column in columns)
                {
                    parameters.Add($"@{column}_{i}");
                }
                valuesList.Add($"({string.Join(", ", parameters)})");
            }
            
            command.CommandText = $"INSERT INTO {_Sanitizer.SanitizeIdentifier(_TableName)} ({string.Join(", ", sanitizedColumns)}) VALUES {string.Join(", ", valuesList)};";
            AddParametersForBatch(command, entities);
        }
        
        private void AddParametersForBatch(SqliteCommand command, IList<T> entities)
        {
            List<string> columns = GetNonAutoIncrementColumns();
            
            for (int i = 0; i < entities.Count; i++)
            {
                T entity = entities[i];
                foreach (string column in columns)
                {
                    PropertyInfo property = _ColumnMappings[column];
                    object value = property.GetValue(entity);
                    object convertedValue = _DataTypeConverter.ConvertToDatabase(value, property.PropertyType, property);
                    command.Parameters.AddWithValue($"@{column}_{i}", convertedValue);
                }
            }
        }
        
        private void ExecuteBatchInsert(SqliteCommand command, IList<T> entities)
        {
            CaptureSqlFromCommand(command);
            int rowsAffected = command.ExecuteNonQuery();
            
            if (_PrimaryKeyProperty != null)
            {
                PropertyAttribute columnAttr = _PrimaryKeyProperty.GetCustomAttribute<PropertyAttribute>();
                if (columnAttr != null && (columnAttr.PropertyFlags & Flags.AutoIncrement) == Flags.AutoIncrement)
                {
                    using SqliteCommand idCommand = new SqliteCommand("SELECT last_insert_rowid();", command.Connection, command.Transaction);
                    CaptureSqlFromCommand(idCommand);
                    long lastId = Convert.ToInt64(idCommand.ExecuteScalar());
                    
                    for (int i = entities.Count - 1; i >= 0; i--)
                    {
                        long id = lastId - (entities.Count - 1 - i);
                        _PrimaryKeyProperty.SetValue(entities[i], Convert.ChangeType(id, _PrimaryKeyProperty.PropertyType));
                    }
                }
            }
        }
        
        private async Task ExecuteBatchInsertAsync(SqliteCommand command, IList<T> entities, CancellationToken token)
        {
            CaptureSqlFromCommand(command);
            int rowsAffected = await command.ExecuteNonQueryAsync(token);
            
            if (_PrimaryKeyProperty != null)
            {
                PropertyAttribute columnAttr = _PrimaryKeyProperty.GetCustomAttribute<PropertyAttribute>();
                if (columnAttr != null && (columnAttr.PropertyFlags & Flags.AutoIncrement) == Flags.AutoIncrement)
                {
                    using SqliteCommand idCommand = new SqliteCommand("SELECT last_insert_rowid();", command.Connection, command.Transaction);
                    CaptureSqlFromCommand(idCommand);
                    long lastId = Convert.ToInt64(await idCommand.ExecuteScalarAsync(token));
                    
                    for (int i = entities.Count - 1; i >= 0; i--)
                    {
                        long id = lastId - (entities.Count - 1 - i);
                        _PrimaryKeyProperty.SetValue(entities[i], Convert.ChangeType(id, _PrimaryKeyProperty.PropertyType));
                    }
                }
            }
        }

        internal void CaptureSqlIfEnabled(string sql)
        {
            if (_CaptureSql && !string.IsNullOrEmpty(sql))
            {
                _LastExecutedSql.Value = sql;
            }
        }

        private void CaptureSqlFromCommand(SqliteCommand command)
        {
            if (_CaptureSql && command != null && !string.IsNullOrEmpty(command.CommandText))
            {
                _LastExecutedSql.Value = command.CommandText;
            }
        }

        #endregion
    }
}