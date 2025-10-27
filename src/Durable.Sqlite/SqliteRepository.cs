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

    /// <summary>
    /// SQLite Repository Implementation with Full Transaction Support and Connection Pooling.
    /// Provides comprehensive data access operations for entities with support for optimistic concurrency,
    /// batch operations, SQL capture, and advanced querying capabilities.
    /// </summary>
    /// <typeparam name="T">The entity type that this repository manages. Must be a class with a parameterless constructor.</typeparam>
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
        /// Gets the last SQL statement with parameter values substituted that was executed by this repository instance.
        /// This provides a fully executable SQL statement with actual parameter values for debugging purposes.
        /// Returns null if no SQL has been executed or SQL capture is disabled.
        /// </summary>
        public string LastExecutedSqlWithParameters
        {
            get => _LastExecutedSqlWithParameters.Value;
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

        /// <summary>
        /// Gets the repository settings used to configure the connection
        /// </summary>
        public RepositorySettings Settings { get; }

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
        internal readonly Dictionary<PropertyInfo, (DefaultValueAttribute, IDefaultValueProvider)> _DefaultValueProviders;

        private readonly AsyncLocal<string> _LastExecutedSql = new AsyncLocal<string>();
        private readonly AsyncLocal<string> _LastExecutedSqlWithParameters = new AsyncLocal<string>();
        private bool _CaptureSql;
        private bool _IncludeQueryInResults;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the SqliteRepository with a connection string and optional configuration.
        /// Creates an internal SqliteConnectionFactory for connection management.
        /// </summary>
        /// <param name="connectionString">The SQLite connection string used to connect to the database.</param>
        /// <param name="batchConfig">Optional batch insert configuration settings. Uses default settings if null.</param>
        /// <param name="dataTypeConverter">Optional data type converter for custom type handling. Uses default converter if null.</param>
        /// <param name="conflictResolver">Optional concurrency conflict resolver. Uses default resolver with ThrowException strategy if null.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the entity type T lacks required attributes (Entity, primary key).</exception>
        public SqliteRepository(string connectionString, IBatchInsertConfiguration batchConfig = null, IDataTypeConverter dataTypeConverter = null, IConcurrencyConflictResolver<T> conflictResolver = null)
        {
            ArgumentNullException.ThrowIfNull(connectionString);
            Settings = SqliteRepositorySettings.Parse(connectionString);
            _ConnectionFactory = new SqliteConnectionFactory(connectionString);
            _Sanitizer = new SqliteSanitizer();
            _DataTypeConverter = dataTypeConverter ?? new DataTypeConverter();
            _TableName = GetEntityName();
            PrimaryKeyInfo primaryKeyInfo = GetPrimaryKeyInfo();
            _PrimaryKeyColumn = primaryKeyInfo.ColumnName;
            _PrimaryKeyProperty = primaryKeyInfo.Property;
            _ColumnMappings = GetColumnMappings();
            _ForeignKeys = GetForeignKeys();
            _NavigationProperties = GetNavigationProperties();
            _BatchConfig = batchConfig ?? BatchInsertConfiguration.Default;
            _VersionColumnInfo = GetVersionColumnInfo();
            _ConflictResolver = conflictResolver ?? new DefaultConflictResolver<T>(ConflictResolutionStrategy.ThrowException);
            _ChangeTracker = new SimpleChangeTracker<T>(_ColumnMappings);
            _DefaultValueProviders = GetDefaultValueProviders();
        }

        /// <summary>
        /// Initializes a new instance of the SqliteRepository with repository settings and optional configuration.
        /// Creates an internal SqliteConnectionFactory using the connection string built from settings.
        /// </summary>
        /// <param name="settings">The SQLite repository settings to use for configuration.</param>
        /// <param name="batchConfig">Optional batch insert configuration settings. Uses default settings if null.</param>
        /// <param name="dataTypeConverter">Optional data type converter for custom type handling. Uses default converter if null.</param>
        /// <param name="conflictResolver">Optional concurrency conflict resolver. Uses default resolver with ThrowException strategy if null.</param>
        /// <exception cref="ArgumentNullException">Thrown when settings is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the entity type T lacks required attributes (Entity, primary key), or when settings are invalid.</exception>
        public SqliteRepository(SqliteRepositorySettings settings, IBatchInsertConfiguration batchConfig = null, IDataTypeConverter dataTypeConverter = null, IConcurrencyConflictResolver<T> conflictResolver = null)
        {
            ArgumentNullException.ThrowIfNull(settings);
            Settings = settings;
            string connectionString = settings.BuildConnectionString();
            _ConnectionFactory = new SqliteConnectionFactory(connectionString);
            _Sanitizer = new SqliteSanitizer();
            _DataTypeConverter = dataTypeConverter ?? new DataTypeConverter();
            _TableName = GetEntityName();
            PrimaryKeyInfo primaryKeyInfo = GetPrimaryKeyInfo();
            _PrimaryKeyColumn = primaryKeyInfo.ColumnName;
            _PrimaryKeyProperty = primaryKeyInfo.Property;
            _ColumnMappings = GetColumnMappings();
            _ForeignKeys = GetForeignKeys();
            _NavigationProperties = GetNavigationProperties();
            _BatchConfig = batchConfig ?? BatchInsertConfiguration.Default;
            _VersionColumnInfo = GetVersionColumnInfo();
            _ConflictResolver = conflictResolver ?? new DefaultConflictResolver<T>(ConflictResolutionStrategy.ThrowException);
            _ChangeTracker = new SimpleChangeTracker<T>(_ColumnMappings);
            _DefaultValueProviders = GetDefaultValueProviders();
        }

        /// <summary>
        /// Initializes a new instance of the SqliteRepository with a provided connection factory and optional configuration.
        /// Allows for shared connection pooling and factory management across multiple repository instances.
        /// Note: When using this constructor, the Settings property will be null as no connection string is directly provided.
        /// </summary>
        /// <param name="connectionFactory">The connection factory to use for database connections.</param>
        /// <param name="batchConfig">Optional batch insert configuration settings. Uses default settings if null.</param>
        /// <param name="dataTypeConverter">Optional data type converter for custom type handling. Uses default converter if null.</param>
        /// <param name="conflictResolver">Optional concurrency conflict resolver. Uses default resolver with ThrowException strategy if null.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionFactory is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the entity type T lacks required attributes (Entity, primary key).</exception>
        public SqliteRepository(IConnectionFactory connectionFactory, IBatchInsertConfiguration batchConfig = null, IDataTypeConverter dataTypeConverter = null, IConcurrencyConflictResolver<T> conflictResolver = null)
        {
            _ConnectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
            Settings = null!;
            _Sanitizer = new SqliteSanitizer();
            _DataTypeConverter = dataTypeConverter ?? new DataTypeConverter();
            _TableName = GetEntityName();
            PrimaryKeyInfo primaryKeyInfo = GetPrimaryKeyInfo();
            _PrimaryKeyColumn = primaryKeyInfo.ColumnName;
            _PrimaryKeyProperty = primaryKeyInfo.Property;
            _ColumnMappings = GetColumnMappings();
            _ForeignKeys = GetForeignKeys();
            _NavigationProperties = GetNavigationProperties();
            _BatchConfig = batchConfig ?? BatchInsertConfiguration.Default;
            _VersionColumnInfo = GetVersionColumnInfo();
            _ConflictResolver = conflictResolver ?? new DefaultConflictResolver<T>(ConflictResolutionStrategy.ThrowException);
            _ChangeTracker = new SimpleChangeTracker<T>(_ColumnMappings);
            _DefaultValueProviders = GetDefaultValueProviders();
        }

        #endregion

        #region Public-Methods

        // Read operations
        /// <summary>
        /// Reads the first entity that matches the specified predicate, or the first entity if no predicate is provided.
        /// </summary>
        /// <param name="predicate">Optional filter expression to apply. If null, returns the first entity found.</param>
        /// <param name="transaction">Optional transaction to execute the operation within.</param>
        /// <returns>The first matching entity, or null if no entities match the criteria.</returns>
        public T ReadFirst(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null)
        {
            IQueryBuilder<T> query = Query(transaction);
            if (predicate != null) query = query.Where(predicate);
            return query.Take(1).Execute().FirstOrDefault();
        }

        /// <summary>
        /// Asynchronously reads the first entity that matches the specified predicate, or the first entity if no predicate is provided.
        /// </summary>
        /// <param name="predicate">Optional filter expression to apply. If null, returns the first entity found.</param>
        /// <param name="transaction">Optional transaction to execute the operation within.</param>
        /// <param name="token">Cancellation token to cancel the operation if needed.</param>
        /// <returns>A task representing the asynchronous operation that returns the first matching entity, or null if no entities match the criteria.</returns>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled via the cancellation token.</exception>
        public async Task<T> ReadFirstAsync(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default)
        {
            IQueryBuilder<T> query = Query(transaction);
            if (predicate != null) query.Where(predicate);
            query.Take(1);

            IEnumerable<T> results = await query.ExecuteAsync(token);
            return results.FirstOrDefault();
        }

        /// <summary>
        /// Reads the first entity that matches the specified predicate, or returns default if no match is found.
        /// </summary>
        /// <param name="predicate">Optional predicate to filter entities. If null, returns the first entity.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The first entity that matches the predicate, or default(T) if no match is found.</returns>
        public T ReadFirstOrDefault(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null)
        {
            return ReadFirst(predicate, transaction);
        }

        /// <summary>
        /// Asynchronously reads the first entity that matches the specified predicate, or returns default if no match is found.
        /// </summary>
        /// <param name="predicate">Optional predicate to filter entities. If null, returns the first entity.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing the first entity that matches the predicate, or default(T) if no match is found.</returns>
        public Task<T> ReadFirstOrDefaultAsync(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default)
        {
            return ReadFirstAsync(predicate, transaction, token);
        }

        /// <summary>
        /// Reads a single entity that matches the specified predicate. Throws an exception if zero or more than one entity is found.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The single entity that matches the predicate.</returns>
        /// <exception cref="InvalidOperationException">Thrown when zero or more than one entity matches the predicate.</exception>
        public T ReadSingle(Expression<Func<T, bool>> predicate, ITransaction transaction = null)
        {
            List<T> results = Query(transaction).Where(predicate).Take(2).Execute().ToList();
            if (results.Count != 1)
                throw new InvalidOperationException($"Expected exactly 1 result but found {results.Count}");
            return results[0];
        }

        /// <summary>
        /// Asynchronously reads a single entity that matches the specified predicate. Throws an exception if zero or more than one entity is found.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing the single entity that matches the predicate.</returns>
        /// <exception cref="InvalidOperationException">Thrown when zero or more than one entity matches the predicate.</exception>
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

        /// <summary>
        /// Reads a single entity that matches the specified predicate, or returns default if no match is found. Throws an exception if more than one entity is found.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The single entity that matches the predicate, or default(T) if no match is found.</returns>
        /// <exception cref="InvalidOperationException">Thrown when more than one entity matches the predicate.</exception>
        public T ReadSingleOrDefault(Expression<Func<T, bool>> predicate, ITransaction transaction = null)
        {
            List<T> results = Query(transaction).Where(predicate).Take(2).Execute().ToList();
            if (results.Count > 1)
                throw new InvalidOperationException($"Expected at most 1 result but found {results.Count}");
            return results.FirstOrDefault();
        }

        /// <summary>
        /// Asynchronously reads a single entity that matches the specified predicate, or returns default if no match is found. Throws an exception if more than one entity is found.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing the single entity that matches the predicate, or default(T) if no match is found.</returns>
        /// <exception cref="InvalidOperationException">Thrown when more than one entity matches the predicate.</exception>
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

        /// <summary>
        /// Reads multiple entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">Optional predicate to filter entities. If null, returns all entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>An enumerable collection of entities that match the predicate.</returns>
        public IEnumerable<T> ReadMany(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null)
        {
            IQueryBuilder<T> query = Query(transaction);
            if (predicate != null) query = query.Where(predicate);
            return query.Execute();
        }

        /// <summary>
        /// Asynchronously reads multiple entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">Optional predicate to filter entities. If null, returns all entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>An async enumerable collection of entities that match the predicate.</returns>
        public async IAsyncEnumerable<T> ReadManyAsync(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, [EnumeratorCancellation] CancellationToken token = default)
        {
            IQueryBuilder<T> query = Query(transaction);
            if (predicate != null) query.Where(predicate);

            await foreach (T item in query.ExecuteAsyncEnumerable(token))
            {
                yield return item;
            }
        }

        /// <summary>
        /// Reads all entities from the repository.
        /// </summary>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>An enumerable collection of all entities.</returns>
        public IEnumerable<T> ReadAll(ITransaction transaction = null)
        {
            return ReadMany(null, transaction);
        }

        /// <summary>
        /// Asynchronously reads all entities from the repository.
        /// </summary>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>An async enumerable collection of all entities.</returns>
        public IAsyncEnumerable<T> ReadAllAsync(ITransaction transaction = null, CancellationToken token = default)
        {
            return ReadManyAsync(null, transaction, token);
        }

        /// <summary>
        /// Reads an entity by its primary key value.
        /// </summary>
        /// <param name="id">The primary key value of the entity to retrieve.</param>
        /// <param name="transaction">Optional transaction to execute the operation within.</param>
        /// <returns>The entity with the specified primary key, or null if not found.</returns>
        /// <exception cref="ArgumentNullException">Thrown when id is null.</exception>
        public T ReadById(object id, ITransaction transaction = null)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = GetConnectionAndCommand(transaction);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldReturnToPool = result.ShouldReturnToPool;
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

        /// <summary>
        /// Asynchronously reads an entity by its primary key value.
        /// </summary>
        /// <param name="id">The primary key value of the entity to retrieve.</param>
        /// <param name="transaction">Optional transaction to execute the operation within.</param>
        /// <param name="token">Cancellation token to cancel the operation if needed.</param>
        /// <returns>A task representing the asynchronous operation that returns the entity with the specified primary key, or null if not found.</returns>
        /// <exception cref="ArgumentNullException">Thrown when id is null.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled via the cancellation token.</exception>
        public async Task<T> ReadByIdAsync(object id, ITransaction transaction = null, CancellationToken token = default)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = await GetConnectionAndCommandAsync(transaction, token);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldReturnToPool = result.ShouldReturnToPool;
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
        /// <summary>
        /// Finds the maximum value of the specified property across entities that match the predicate.
        /// </summary>
        /// <typeparam name="TResult">The type of the property to find the maximum of.</typeparam>
        /// <param name="selector">Expression that selects the property to find the maximum of.</param>
        /// <param name="predicate">Optional predicate to filter entities. If null, considers all entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The maximum value of the selected property.</returns>
        public TResult Max<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = GetConnectionAndCommand(transaction);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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

                object scalarResult = command.ExecuteScalar();
                return scalarResult == DBNull.Value || scalarResult == null ? default(TResult) : (TResult)Convert.ChangeType(scalarResult, typeof(TResult));
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        /// <summary>
        /// Asynchronously finds the maximum value of the specified property across entities that match the predicate.
        /// </summary>
        /// <typeparam name="TResult">The type of the property to find the maximum of.</typeparam>
        /// <param name="selector">Expression that selects the property to find the maximum of.</param>
        /// <param name="predicate">Optional predicate to filter entities. If null, considers all entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing the maximum value of the selected property.</returns>
        public async Task<TResult> MaxAsync<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = await GetConnectionAndCommandAsync(transaction, token);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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

                object scalarResult = await command.ExecuteScalarAsync(token);
                return scalarResult == DBNull.Value || scalarResult == null ? default(TResult) : (TResult)Convert.ChangeType(scalarResult, typeof(TResult));
            }
            finally
            {
                await CleanupConnectionAsync(connection, command, shouldDispose);
            }
        }

        /// <summary>
        /// Finds the minimum value of the specified property across entities that match the predicate.
        /// </summary>
        /// <typeparam name="TResult">The type of the property to find the minimum of.</typeparam>
        /// <param name="selector">Expression that selects the property to find the minimum of.</param>
        /// <param name="predicate">Optional predicate to filter entities. If null, considers all entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The minimum value of the selected property.</returns>
        public TResult Min<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = GetConnectionAndCommand(transaction);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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

                object scalarResult = command.ExecuteScalar();
                return scalarResult == DBNull.Value || scalarResult == null ? default(TResult) : (TResult)Convert.ChangeType(scalarResult, typeof(TResult));
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        /// <summary>
        /// Asynchronously finds the minimum value of the specified property across entities that match the predicate.
        /// </summary>
        /// <typeparam name="TResult">The type of the property to find the minimum of.</typeparam>
        /// <param name="selector">Expression that selects the property to find the minimum of.</param>
        /// <param name="predicate">Optional predicate to filter entities. If null, considers all entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing the minimum value of the selected property.</returns>
        public async Task<TResult> MinAsync<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = await GetConnectionAndCommandAsync(transaction, token);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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

                object scalarResult = await command.ExecuteScalarAsync(token);
                return scalarResult == DBNull.Value || scalarResult == null ? default(TResult) : (TResult)Convert.ChangeType(scalarResult, typeof(TResult));
            }
            finally
            {
                await CleanupConnectionAsync(connection, command, shouldDispose);
            }
        }

        /// <summary>
        /// Calculates the average value of the specified decimal property across entities that match the predicate.
        /// </summary>
        /// <param name="selector">Expression that selects the decimal property to calculate the average of.</param>
        /// <param name="predicate">Optional predicate to filter entities. If null, considers all entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The average value of the selected property.</returns>
        public decimal Average(Expression<Func<T, decimal>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = GetConnectionAndCommand(transaction);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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

                object scalarResult = command.ExecuteScalar();
                return scalarResult == DBNull.Value || scalarResult == null ? 0m : Convert.ToDecimal(scalarResult);
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        /// <summary>
        /// Asynchronously calculates the average value of the specified decimal property across entities that match the predicate.
        /// </summary>
        /// <param name="selector">Expression that selects the decimal property to calculate the average of.</param>
        /// <param name="predicate">Optional predicate to filter entities. If null, considers all entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing the average value of the selected property.</returns>
        public async Task<decimal> AverageAsync(Expression<Func<T, decimal>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = await GetConnectionAndCommandAsync(transaction, token);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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

                object scalarResult = await command.ExecuteScalarAsync(token);
                return scalarResult == DBNull.Value || scalarResult == null ? 0m : Convert.ToDecimal(scalarResult);
            }
            finally
            {
                await CleanupConnectionAsync(connection, command, shouldDispose);
            }
        }

        /// <summary>
        /// Calculates the sum of the specified decimal property across entities that match the predicate.
        /// </summary>
        /// <param name="selector">Expression that selects the decimal property to calculate the sum of.</param>
        /// <param name="predicate">Optional predicate to filter entities. If null, considers all entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The sum of the selected property.</returns>
        public decimal Sum(Expression<Func<T, decimal>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = GetConnectionAndCommand(transaction);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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

                object scalarResult = command.ExecuteScalar();
                return Convert.ToDecimal(scalarResult);
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        /// <summary>
        /// Asynchronously calculates the sum of the specified decimal property across entities that match the predicate.
        /// </summary>
        /// <param name="selector">Expression that selects the decimal property to calculate the sum of.</param>
        /// <param name="predicate">Optional predicate to filter entities. If null, considers all entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing the sum of the selected property.</returns>
        public async Task<decimal> SumAsync(Expression<Func<T, decimal>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = await GetConnectionAndCommandAsync(transaction, token);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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

                object scalarResult = await command.ExecuteScalarAsync(token);
                return Convert.ToDecimal(scalarResult);
            }
            finally
            {
                await CleanupConnectionAsync(connection, command, shouldDispose);
            }
        }

        // Batch operations
        /// <summary>
        /// Performs a batch update operation on entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities for updating.</param>
        /// <param name="updateExpression">Expression that defines how to update the entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The number of entities that were updated.</returns>
        public int BatchUpdate(Expression<Func<T, bool>> predicate, Expression<Func<T, T>> updateExpression, ITransaction transaction = null)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = GetConnectionAndCommand(transaction);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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

        /// <summary>
        /// Asynchronously performs a batch update operation on entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities for updating.</param>
        /// <param name="updateExpression">Expression that defines how to update the entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing the number of entities that were updated.</returns>
        public async Task<int> BatchUpdateAsync(Expression<Func<T, bool>> predicate, Expression<Func<T, T>> updateExpression, ITransaction transaction = null, CancellationToken token = default)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = await GetConnectionAndCommandAsync(transaction, token);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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
                await CleanupConnectionAsync(connection, command, shouldDispose);
            }
        }

        /// <summary>
        /// Performs a batch delete operation on entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities for deletion.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The number of entities that were deleted.</returns>
        public int BatchDelete(Expression<Func<T, bool>> predicate, ITransaction transaction = null)
        {
            return DeleteMany(predicate, transaction);
        }

        /// <summary>
        /// Asynchronously performs a batch delete operation on entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities for deletion.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing the number of entities that were deleted.</returns>
        public Task<int> BatchDeleteAsync(Expression<Func<T, bool>> predicate, ITransaction transaction = null, CancellationToken token = default)
        {
            return DeleteManyAsync(predicate, transaction, token);
        }

        // Raw SQL operations
        /// <summary>
        /// Executes a raw SQL query and returns the results as entities of type T.
        /// </summary>
        /// <param name="sql">The raw SQL query to execute.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="parameters">Parameters for the SQL query.</param>
        /// <returns>An enumerable collection of entities returned by the query.</returns>
        public IEnumerable<T> FromSql(string sql, ITransaction transaction = null, params object[] parameters)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = GetConnectionAndCommand(transaction);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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

        /// <summary>
        /// Asynchronously executes a raw SQL query and returns the results as entities of type T.
        /// </summary>
        /// <param name="sql">The raw SQL query to execute.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <param name="parameters">Parameters for the SQL query.</param>
        /// <returns>An async enumerable collection of entities returned by the query.</returns>
        public async IAsyncEnumerable<T> FromSqlAsync(string sql, ITransaction transaction = null, [EnumeratorCancellation] CancellationToken token = default, params object[] parameters)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = await GetConnectionAndCommandAsync(transaction, CancellationToken.None);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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
                await CleanupConnectionAsync(connection, command, shouldDispose);
            }
        }

        /// <summary>
        /// Asynchronously executes a raw SQL query and returns the results as entities of the specified type.
        /// </summary>
        /// <typeparam name="TResult">The type to map the query results to. Must have a parameterless constructor.</typeparam>
        /// <param name="sql">The raw SQL query to execute.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <param name="parameters">Parameters for the SQL query.</param>
        /// <returns>An async enumerable collection of entities of the specified type returned by the query.</returns>
        public async IAsyncEnumerable<TResult> FromSqlAsync<TResult>(string sql, ITransaction transaction = null, [EnumeratorCancellation] CancellationToken token = default, params object[] parameters) where TResult : new()
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = await GetConnectionAndCommandAsync(transaction, CancellationToken.None);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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
                await CleanupConnectionAsync(connection, command, shouldDispose);
            }
        }

        /// <summary>
        /// Executes a raw SQL query and returns the results as entities of the specified type.
        /// </summary>
        /// <typeparam name="TResult">The type to map the query results to. Must have a parameterless constructor.</typeparam>
        /// <param name="sql">The raw SQL query to execute.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="parameters">Parameters for the SQL query.</param>
        /// <returns>An enumerable collection of entities of the specified type returned by the query.</returns>
        public IEnumerable<TResult> FromSql<TResult>(string sql, ITransaction transaction = null, params object[] parameters) where TResult : new()
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = GetConnectionAndCommand(transaction);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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

        /// <summary>
        /// Executes a raw SQL command (INSERT, UPDATE, DELETE) and returns the number of affected rows.
        /// </summary>
        /// <param name="sql">The raw SQL command to execute.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="parameters">Parameters for the SQL command.</param>
        /// <returns>The number of rows affected by the command.</returns>
        public int ExecuteSql(string sql, ITransaction transaction = null, params object[] parameters)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = GetConnectionAndCommand(transaction);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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

        /// <summary>
        /// Asynchronously executes a raw SQL command (INSERT, UPDATE, DELETE) and returns the number of affected rows.
        /// </summary>
        /// <param name="sql">The raw SQL command to execute.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <param name="parameters">Parameters for the SQL command.</param>
        /// <returns>A task that represents the asynchronous operation containing the number of rows affected by the command.</returns>
        public async Task<int> ExecuteSqlAsync(string sql, ITransaction transaction = null, CancellationToken token = default, params object[] parameters)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = await GetConnectionAndCommandAsync(transaction, CancellationToken.None);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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
                await CleanupConnectionAsync(connection, command, shouldDispose);
            }
        }

        // Transaction support
        /// <summary>
        /// Begins a new database transaction for executing multiple operations atomically.
        /// The transaction must be committed or rolled back explicitly.
        /// </summary>
        /// <returns>A transaction object that can be used to execute multiple operations atomically.</returns>
        /// <exception cref="InvalidOperationException">Thrown when unable to create a database connection or transaction.</exception>
        public ITransaction BeginTransaction()
        {
            SqliteConnection connection = GetConnection();
            connection.Open();
            SqliteTransaction transaction = connection.BeginTransaction();
            return new SqliteRepositoryTransaction(connection, transaction, _ConnectionFactory);
        }

        /// <summary>
        /// Asynchronously begins a new database transaction for executing multiple operations atomically.
        /// The transaction must be committed or rolled back explicitly.
        /// </summary>
        /// <param name="token">Cancellation token to cancel the operation if needed.</param>
        /// <returns>A task representing the asynchronous operation that returns a transaction object.</returns>
        /// <exception cref="InvalidOperationException">Thrown when unable to create a database connection or transaction.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled via the cancellation token.</exception>
        public async Task<ITransaction> BeginTransactionAsync(CancellationToken token = default)
        {
            SqliteConnection connection = GetConnection();
            await connection.OpenAsync(token);
            SqliteTransaction transaction = (SqliteTransaction)await connection.BeginTransactionAsync(token);
            return new SqliteRepositoryTransaction(connection, transaction, _ConnectionFactory);
        }

        // Existence checks
        /// <summary>
        /// Determines whether any entity exists that matches the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to test entities against.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>True if any entity matches the predicate; otherwise, false.</returns>
        public bool Exists(Expression<Func<T, bool>> predicate, ITransaction transaction = null)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = GetConnectionAndCommand(transaction);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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

        /// <summary>
        /// Asynchronously determines whether any entity exists that matches the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to test entities against.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing true if any entity matches the predicate; otherwise, false.</returns>
        public async Task<bool> ExistsAsync(Expression<Func<T, bool>> predicate, ITransaction transaction = null, CancellationToken token = default)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = await GetConnectionAndCommandAsync(transaction, token);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
            try
            {
                string whereClause = BuildWhereClause(predicate);
                command.CommandText = $"SELECT EXISTS(SELECT 1 FROM {_Sanitizer.SanitizeIdentifier(_TableName)} WHERE {whereClause} LIMIT 1);";
                CaptureSqlFromCommand(command);
                object scalarResult = await command.ExecuteScalarAsync(token);
                return Convert.ToBoolean(scalarResult);
            }
            finally
            {
                await CleanupConnectionAsync(connection, command, shouldDispose);
            }
        }

        /// <summary>
        /// Determines whether an entity with the specified primary key exists.
        /// </summary>
        /// <param name="id">The primary key value to search for.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>True if an entity with the specified primary key exists; otherwise, false.</returns>
        public bool ExistsById(object id, ITransaction transaction = null)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = GetConnectionAndCommand(transaction);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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

        /// <summary>
        /// Asynchronously determines whether an entity with the specified primary key exists.
        /// </summary>
        /// <param name="id">The primary key value to search for.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing true if an entity with the specified primary key exists; otherwise, false.</returns>
        public async Task<bool> ExistsByIdAsync(object id, ITransaction transaction = null, CancellationToken token = default)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = await GetConnectionAndCommandAsync(transaction, token);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
            try
            {
                command.CommandText = $"SELECT EXISTS(SELECT 1 FROM {_Sanitizer.SanitizeIdentifier(_TableName)} WHERE {_Sanitizer.SanitizeIdentifier(_PrimaryKeyColumn)} = @id LIMIT 1);";
                command.Parameters.AddWithValue("@id", id);
                CaptureSqlFromCommand(command);
                object scalarResult = await command.ExecuteScalarAsync(token);
                return Convert.ToBoolean(scalarResult);
            }
            finally
            {
                await CleanupConnectionAsync(connection, command, shouldDispose);
            }
        }

        // Count operations
        /// <summary>
        /// Counts the number of entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">Optional predicate to filter entities. If null, counts all entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The number of entities that match the predicate.</returns>
        public int Count(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = GetConnectionAndCommand(transaction);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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

        /// <summary>
        /// Asynchronously counts the number of entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">Optional predicate to filter entities. If null, counts all entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing the number of entities that match the predicate.</returns>
        public async Task<int> CountAsync(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = await GetConnectionAndCommandAsync(transaction, token);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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
                object scalarResult = await command.ExecuteScalarAsync(token);
                return Convert.ToInt32(scalarResult);
            }
            finally
            {
                await CleanupConnectionAsync(connection, command, shouldDispose);
            }
        }

        // Create operations
        /// <summary>
        /// Creates a new entity in the database. Auto-increment primary keys will be set on the entity after creation.
        /// Version columns will be initialized with default values if not already set.
        /// </summary>
        /// <param name="entity">The entity to create in the database.</param>
        /// <param name="transaction">Optional transaction to execute the operation within.</param>
        /// <returns>The created entity with any auto-generated values (like primary keys) populated.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the entity violates database constraints.</exception>
        public T Create(T entity, ITransaction transaction = null)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = GetConnectionAndCommand(transaction);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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

                    // Apply default value providers if configured
                    if (_DefaultValueProviders.TryGetValue(property, out (DefaultValueAttribute, IDefaultValueProvider) providerInfo))
                    {
                        DefaultValueAttribute attr = providerInfo.Item1;
                        IDefaultValueProvider provider = providerInfo.Item2;
                        if (provider.ShouldApply(value, property.PropertyType))
                        {
                            value = provider.GetDefaultValue(property, entity);
                            property.SetValue(entity, value);
                        }
                    }

                    object convertedValue = _DataTypeConverter.ConvertToDatabase(value, property.PropertyType, property);
                    command.Parameters.AddWithValue($"@{columnName}", convertedValue);
                }

                command.CommandText = $"INSERT INTO {_Sanitizer.SanitizeIdentifier(_TableName)} ({string.Join(", ", columns)}) VALUES ({string.Join(", ", parameters)}) RETURNING *;";
                CaptureSqlFromCommand(command);

                using SqliteDataReader reader = command.ExecuteReader();
                if (reader.Read())
                {
                    return MapReaderToEntity(reader);
                }

                return entity;
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        /// <summary>
        /// Asynchronously creates a new entity in the database. Auto-increment primary keys will be set on the entity after creation.
        /// Version columns will be initialized with default values if not already set.
        /// </summary>
        /// <param name="entity">The entity to create in the database.</param>
        /// <param name="transaction">Optional transaction to execute the operation within.</param>
        /// <param name="token">Cancellation token to cancel the operation if needed.</param>
        /// <returns>A task representing the asynchronous operation that returns the created entity with any auto-generated values populated.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the entity violates database constraints.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled via the cancellation token.</exception>
        public async Task<T> CreateAsync(T entity, ITransaction transaction = null, CancellationToken token = default)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = await GetConnectionAndCommandAsync(transaction, token);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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

                    // Apply default value providers if configured
                    if (_DefaultValueProviders.TryGetValue(property, out (DefaultValueAttribute, IDefaultValueProvider) providerInfo))
                    {
                        DefaultValueAttribute attr = providerInfo.Item1;
                        IDefaultValueProvider provider = providerInfo.Item2;
                        if (provider.ShouldApply(value, property.PropertyType))
                        {
                            value = provider.GetDefaultValue(property, entity);
                            property.SetValue(entity, value);
                        }
                    }

                    object convertedValue = _DataTypeConverter.ConvertToDatabase(value, property.PropertyType, property);
                    command.Parameters.AddWithValue($"@{columnName}", convertedValue);
                }

                command.CommandText = $"INSERT INTO {_Sanitizer.SanitizeIdentifier(_TableName)} ({string.Join(", ", columns)}) VALUES ({string.Join(", ", parameters)}) RETURNING *;";
                CaptureSqlFromCommand(command);

                using SqliteDataReader reader = await command.ExecuteReaderAsync(token);
                if (await reader.ReadAsync(token))
                {
                    return MapReaderToEntity(reader);
                }

                return entity;
            }
            finally
            {
                await CleanupConnectionAsync(connection, command, shouldDispose);
            }
        }

        /// <summary>
        /// Creates multiple entities in the repository using batch insert operations for optimal performance.
        /// </summary>
        /// <param name="entities">The collection of entities to create.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The created entities with any generated values (like auto-increment IDs) populated.</returns>
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
                    transaction = new SqliteRepositoryTransaction(connection, localTransaction, _ConnectionFactory);
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

        /// <summary>
        /// Asynchronously creates multiple entities in the repository using batch insert operations for optimal performance.
        /// </summary>
        /// <param name="entities">The collection of entities to create.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing the created entities with any generated values (like auto-increment IDs) populated.</returns>
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
                    transaction = new SqliteRepositoryTransaction(connection, localTransaction, _ConnectionFactory);
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
        /// <summary>
        /// Updates an existing entity in the database. Supports optimistic concurrency control through version columns.
        /// If a concurrency conflict occurs, the configured conflict resolver will attempt to resolve it automatically.
        /// </summary>
        /// <param name="entity">The entity to update in the database.</param>
        /// <param name="transaction">Optional transaction to execute the operation within.</param>
        /// <returns>The updated entity with incremented version column if applicable.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when no rows are affected by the update operation.</exception>
        /// <exception cref="OptimisticConcurrencyException">Thrown when a concurrency conflict cannot be resolved automatically.</exception>
        public T Update(T entity, ITransaction transaction = null)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = GetConnectionAndCommand(transaction);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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

                command.CommandText = $"UPDATE {_Sanitizer.SanitizeIdentifier(_TableName)} SET {string.Join(", ", setPairs)} WHERE {whereClause} RETURNING *;";
                CaptureSqlFromCommand(command);

                using SqliteDataReader reader = command.ExecuteReader();
                if (reader.Read())
                {
                    return MapReaderToEntity(reader);
                }
                else
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
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        /// <summary>
        /// Asynchronously updates an existing entity in the repository.
        /// </summary>
        /// <param name="entity">The entity to update with new values.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing the updated entity.</returns>
        public async Task<T> UpdateAsync(T entity, ITransaction transaction = null, CancellationToken token = default)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = await GetConnectionAndCommandAsync(transaction, token);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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

                command.CommandText = $"UPDATE {_Sanitizer.SanitizeIdentifier(_TableName)} SET {string.Join(", ", setPairs)} WHERE {whereClause} RETURNING *;";
                CaptureSqlFromCommand(command);

                using SqliteDataReader reader = await command.ExecuteReaderAsync(token);
                if (await reader.ReadAsync(token))
                {
                    return MapReaderToEntity(reader);
                }
                else
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
                        TryResolveConflictResult<T> conflictResult = await _ConflictResolver.TryResolveConflictAsync(currentDbEntity, entity, originalEntity, strategy);

                        if (conflictResult.Success && conflictResult.ResolvedEntity != null)
                        {
                            // Copy the current version from the database to the resolved entity
                            _VersionColumnInfo.SetValue(conflictResult.ResolvedEntity, actualVersion);
                            // Retry the update with the resolved entity
                            return await UpdateAsync(conflictResult.ResolvedEntity, transaction, token);
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
            }
            finally
            {
                await CleanupConnectionAsync(connection, command, shouldDispose);
            }
        }

        /// <summary>
        /// Updates multiple entities that match the specified predicate by applying an update action to each entity.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities for updating.</param>
        /// <param name="updateAction">The action to apply to each matching entity for updating.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The number of entities that were updated.</returns>
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
                    transaction = new SqliteRepositoryTransaction(connection, localTransaction, _ConnectionFactory);
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

        /// <summary>
        /// Asynchronously updates multiple entities that match the specified predicate by applying an async update action to each entity.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities for updating.</param>
        /// <param name="updateAction">The async action to apply to each matching entity for updating.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing the number of entities that were updated.</returns>
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
                    transaction = new SqliteRepositoryTransaction(connection, localTransaction, _ConnectionFactory);
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
                    if (connection != null) await _ConnectionFactory.ReturnConnectionAsync(connection);
                }
            }
        }

        /// <summary>
        /// Updates a specific field for all entities that match the specified predicate.
        /// </summary>
        /// <typeparam name="TField">The type of the field being updated.</typeparam>
        /// <param name="predicate">The predicate to filter entities for updating.</param>
        /// <param name="field">Expression that selects the field to update.</param>
        /// <param name="value">The new value for the field.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The number of entities that were updated.</returns>
        public int UpdateField<TField>(Expression<Func<T, bool>> predicate, Expression<Func<T, TField>> field, TField value, ITransaction transaction = null)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = GetConnectionAndCommand(transaction);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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

        /// <summary>
        /// Asynchronously updates a specific field for all entities that match the specified predicate.
        /// </summary>
        /// <typeparam name="TField">The type of the field being updated.</typeparam>
        /// <param name="predicate">The predicate to filter entities for updating.</param>
        /// <param name="field">Expression that selects the field to update.</param>
        /// <param name="value">The new value for the field.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing the number of entities that were updated.</returns>
        public async Task<int> UpdateFieldAsync<TField>(Expression<Func<T, bool>> predicate, Expression<Func<T, TField>> field, TField value, ITransaction transaction = null, CancellationToken token = default)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = await GetConnectionAndCommandAsync(transaction, token);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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
                await CleanupConnectionAsync(connection, command, shouldDispose);
            }
        }

        // Delete operations
        /// <summary>
        /// Deletes an entity from the database by its primary key value.
        /// </summary>
        /// <param name="entity">The entity to delete from the database.</param>
        /// <param name="transaction">Optional transaction to execute the operation within.</param>
        /// <returns>True if the entity was deleted successfully, false if the entity was not found.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null.</exception>
        public bool Delete(T entity, ITransaction transaction = null)
        {
            object idValue = GetPrimaryKeyValue(entity);
            return DeleteById(idValue, transaction);
        }

        /// <summary>
        /// Asynchronously deletes an entity from the repository.
        /// </summary>
        /// <param name="entity">The entity to delete.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing true if the entity was deleted; otherwise, false.</returns>
        public async Task<bool> DeleteAsync(T entity, ITransaction transaction = null, CancellationToken token = default)
        {
            object idValue = GetPrimaryKeyValue(entity);
            return await DeleteByIdAsync(idValue, transaction, token);
        }

        /// <summary>
        /// Deletes an entity by its primary key.
        /// </summary>
        /// <param name="id">The primary key value of the entity to delete.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>True if the entity was deleted; otherwise, false.</returns>
        public bool DeleteById(object id, ITransaction transaction = null)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = GetConnectionAndCommand(transaction);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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

        /// <summary>
        /// Asynchronously deletes an entity by its primary key.
        /// </summary>
        /// <param name="id">The primary key value of the entity to delete.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing true if the entity was deleted; otherwise, false.</returns>
        public async Task<bool> DeleteByIdAsync(object id, ITransaction transaction = null, CancellationToken token = default)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = await GetConnectionAndCommandAsync(transaction, token);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
            try
            {
                command.CommandText = $"DELETE FROM {_Sanitizer.SanitizeIdentifier(_TableName)} WHERE {_Sanitizer.SanitizeIdentifier(_PrimaryKeyColumn)} = @id;";
                command.Parameters.AddWithValue("@id", id);
                CaptureSqlFromCommand(command);

                return await command.ExecuteNonQueryAsync(token) > 0;
            }
            finally
            {
                await CleanupConnectionAsync(connection, command, shouldDispose);
            }
        }

        /// <summary>
        /// Deletes multiple entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities for deletion.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The number of entities that were deleted.</returns>
        public int DeleteMany(Expression<Func<T, bool>> predicate, ITransaction transaction = null)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = GetConnectionAndCommand(transaction);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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

        /// <summary>
        /// Asynchronously deletes multiple entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities for deletion.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing the number of entities that were deleted.</returns>
        public async Task<int> DeleteManyAsync(Expression<Func<T, bool>> predicate, ITransaction transaction = null, CancellationToken token = default)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = await GetConnectionAndCommandAsync(transaction, token);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
            try
            {
                string whereClause = BuildWhereClause(predicate);
                command.CommandText = $"DELETE FROM {_Sanitizer.SanitizeIdentifier(_TableName)} WHERE {whereClause};";
                CaptureSqlFromCommand(command);
                return await command.ExecuteNonQueryAsync(token);
            }
            finally
            {
                await CleanupConnectionAsync(connection, command, shouldDispose);
            }
        }

        /// <summary>
        /// Deletes all entities from the repository.
        /// </summary>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The number of entities that were deleted.</returns>
        public int DeleteAll(ITransaction transaction = null)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = GetConnectionAndCommand(transaction);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldReturnToPool = result.ShouldReturnToPool;
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

        /// <summary>
        /// Asynchronously deletes all entities from the repository.
        /// </summary>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing the number of entities that were deleted.</returns>
        public async Task<int> DeleteAllAsync(ITransaction transaction = null, CancellationToken token = default)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = await GetConnectionAndCommandAsync(transaction, token);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
            try
            {
                command.CommandText = $"DELETE FROM {_Sanitizer.SanitizeIdentifier(_TableName)};";
                CaptureSqlFromCommand(command);
                return await command.ExecuteNonQueryAsync(token);
            }
            finally
            {
                await CleanupConnectionAsync(connection, command, shouldDispose);
            }
        }

        // Upsert operations
        /// <summary>
        /// Inserts or updates an entity depending on whether it already exists in the repository.
        /// </summary>
        /// <param name="entity">The entity to insert or update.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The entity after the upsert operation, with any generated values populated.</returns>
        public T Upsert(T entity, ITransaction transaction = null)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = GetConnectionAndCommand(transaction);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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
                sql.Append(" RETURNING *;");

                command.CommandText = sql.ToString();
                CaptureSqlFromCommand(command);

                using SqliteDataReader reader = command.ExecuteReader();
                if (reader.Read())
                {
                    return MapReaderToEntity(reader);
                }

                return entity;
            }
            finally
            {
                CleanupConnection(connection, command, shouldDispose);
            }
        }

        /// <summary>
        /// Asynchronously inserts or updates an entity depending on whether it already exists in the repository.
        /// </summary>
        /// <param name="entity">The entity to insert or update.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing the entity after the upsert operation, with any generated values populated.</returns>
        public async Task<T> UpsertAsync(T entity, ITransaction transaction = null, CancellationToken token = default)
        {
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = await GetConnectionAndCommandAsync(transaction, token);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldDispose = result.ShouldReturnToPool;
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
                sql.Append(" RETURNING *;");

                command.CommandText = sql.ToString();
                CaptureSqlFromCommand(command);

                using SqliteDataReader reader = await command.ExecuteReaderAsync(token);
                if (await reader.ReadAsync(token))
                {
                    return MapReaderToEntity(reader);
                }

                return entity;
            }
            finally
            {
                await CleanupConnectionAsync(connection, command, shouldDispose);
            }
        }

        /// <summary>
        /// Inserts or updates multiple entities depending on whether they already exist in the repository.
        /// </summary>
        /// <param name="entities">The collection of entities to insert or update.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The entities after the upsert operation, with any generated values populated.</returns>
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
                    transaction = new SqliteRepositoryTransaction(connection, localTransaction, _ConnectionFactory);
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

        /// <summary>
        /// Asynchronously inserts or updates multiple entities depending on whether they already exist in the repository.
        /// </summary>
        /// <param name="entities">The collection of entities to insert or update.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing the entities after the upsert operation, with any generated values populated.</returns>
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
                    transaction = new SqliteRepositoryTransaction(connection, localTransaction, _ConnectionFactory);
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
                    if (connection != null) await _ConnectionFactory.ReturnConnectionAsync(connection);
                }
            }
        }

        // Query builder
        /// <summary>
        /// Creates a new query builder for constructing complex queries against the entity table.
        /// Provides a fluent interface for building SELECT, WHERE, ORDER BY, and other SQL clauses.
        /// </summary>
        /// <param name="transaction">Optional transaction to execute the query within.</param>
        /// <returns>A query builder instance for constructing and executing complex queries.</returns>
        public IQueryBuilder<T> Query(ITransaction transaction = null)
        {
            return new SqliteQueryBuilder<T>(this, transaction);
        }

        // IBatchInsertConfiguration implementation
        /// <summary>
        /// Gets the maximum number of rows to include in a single batch operation.
        /// </summary>
        /// <value>The maximum number of rows per batch. Default is typically 1000.</value>
        public int MaxRowsPerBatch => _BatchConfig.MaxRowsPerBatch;
        /// <summary>
        /// Gets the maximum number of parameters allowed per SQL statement.
        /// </summary>
        /// <value>The maximum number of parameters per statement. Default is typically 999 for SQLite.</value>
        public int MaxParametersPerStatement => _BatchConfig.MaxParametersPerStatement;
        /// <summary>
        /// Gets a value indicating whether prepared statement reuse is enabled for improved performance.
        /// </summary>
        /// <value>True if prepared statement reuse is enabled; otherwise, false.</value>
        public bool EnablePreparedStatementReuse => _BatchConfig.EnablePreparedStatementReuse;
        /// <summary>
        /// Gets a value indicating whether multi-row insert statements are enabled for batch operations.
        /// </summary>
        /// <value>True if multi-row insert is enabled; otherwise, false.</value>
        public bool EnableMultiRowInsert => _BatchConfig.EnableMultiRowInsert;

        /// <summary>
        /// Disposes of the repository and releases all managed resources including the connection factory.
        /// After disposal, the repository instance should not be used for any operations.
        /// </summary>
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
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = GetConnectionAndCommand(transaction);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldReturnToPool = result.ShouldReturnToPool;
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
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = await GetConnectionAndCommandAsync(transaction, token);
            SqliteConnection connection = result.Connection;
            SqliteCommand command = result.Command;
            bool shouldReturnToPool = result.ShouldReturnToPool;
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

        /// <summary>
        /// Gets a connection to the SQLite database from the connection factory.
        /// </summary>
        /// <returns>A SQLite database connection.</returns>
        public SqliteConnection GetConnection()
        {
            return (SqliteConnection)_ConnectionFactory.GetConnection();
        }

        /// <summary>
        /// Asynchronously gets a connection to the SQLite database from the connection factory.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing a SQLite database connection.</returns>
        public async Task<SqliteConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
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

        internal ConnectionCommandResult<SqliteConnection, SqliteCommand> GetConnectionAndCommand(ITransaction transaction)
        {
            // Use provided transaction or check for ambient transaction
            ITransaction effectiveTransaction = transaction ?? TransactionScope.Current?.Transaction;

            if (effectiveTransaction != null)
            {
                SqliteCommand command = new SqliteCommand();
                command.Connection = (SqliteConnection)effectiveTransaction.Connection;
                command.Transaction = (SqliteTransaction)effectiveTransaction.Transaction;
                return new ConnectionCommandResult<SqliteConnection, SqliteCommand>((SqliteConnection)effectiveTransaction.Connection, command, false);
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
                return new ConnectionCommandResult<SqliteConnection, SqliteCommand>(connection, command, true);
            }
        }

        internal async Task<ConnectionCommandResult<SqliteConnection, SqliteCommand>> GetConnectionAndCommandAsync(ITransaction transaction, CancellationToken token)
        {
            // Use provided transaction or check for ambient transaction
            ITransaction effectiveTransaction = transaction ?? TransactionScope.Current?.Transaction;

            if (effectiveTransaction != null)
            {
                SqliteCommand command = new SqliteCommand();
                command.Connection = (SqliteConnection)effectiveTransaction.Connection;
                command.Transaction = (SqliteTransaction)effectiveTransaction.Transaction;
                return new ConnectionCommandResult<SqliteConnection, SqliteCommand>((SqliteConnection)effectiveTransaction.Connection, command, false);
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
                return new ConnectionCommandResult<SqliteConnection, SqliteCommand>(connection, command, true);
            }
        }

        /// <summary>
        /// Gets the entity name (table name) for the current entity type from the Entity attribute.
        /// </summary>
        /// <returns>The name of the database table for this entity type.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the entity type does not have an Entity attribute.</exception>
        public string GetEntityName()
        {
            EntityAttribute entityAttr = typeof(T).GetCustomAttribute<EntityAttribute>();
            if (entityAttr == null)
                throw new InvalidOperationException($"Type {typeof(T).Name} must have an Entity attribute");
            return entityAttr.Name;
        }

        /// <summary>
        /// Gets the primary key column information for the current entity type.
        /// </summary>
        /// <returns>A PrimaryKeyInfo object containing the column name and PropertyInfo for the primary key.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the entity type does not have a primary key column.</exception>
        public PrimaryKeyInfo GetPrimaryKeyInfo()
        {
            foreach (PropertyInfo prop in typeof(T).GetProperties())
            {
                PropertyAttribute attr = prop.GetCustomAttribute<PropertyAttribute>();
                if (attr != null && (attr.PropertyFlags & Flags.PrimaryKey) == Flags.PrimaryKey)
                {
                    return new PrimaryKeyInfo(attr.Name, prop);
                }
            }

            throw new InvalidOperationException($"Type {typeof(T).Name} must have a primary key column");
        }

        /// <summary>
        /// Gets the column mappings for the current entity type, mapping database column names to entity properties.
        /// </summary>
        /// <returns>A dictionary mapping column names to their corresponding PropertyInfo objects.</returns>
        public Dictionary<string, PropertyInfo> GetColumnMappings()
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

        /// <summary>
        /// Gets the foreign key relationships for the current entity type.
        /// </summary>
        /// <returns>A dictionary mapping properties to their foreign key attributes.</returns>
        public Dictionary<PropertyInfo, ForeignKeyAttribute> GetForeignKeys()
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

        /// <summary>
        /// Gets the navigation properties for the current entity type.
        /// </summary>
        /// <returns>A dictionary mapping properties to their navigation property attributes.</returns>
        public Dictionary<PropertyInfo, NavigationPropertyAttribute> GetNavigationProperties()
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

        /// <summary>
        /// Gets the version column information for optimistic concurrency control.
        /// </summary>
        /// <returns>Version column information, or null if no version column is defined.</returns>
        public VersionColumnInfo GetVersionColumnInfo()
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

        /// <summary>
        /// Gets the default value providers for properties with DefaultValueAttribute
        /// </summary>
        /// <returns>A dictionary mapping properties to their default value providers and attributes</returns>
        public Dictionary<PropertyInfo, (DefaultValueAttribute, IDefaultValueProvider)> GetDefaultValueProviders()
        {
            Dictionary<PropertyInfo, (DefaultValueAttribute, IDefaultValueProvider)> providers =
                new Dictionary<PropertyInfo, (DefaultValueAttribute, IDefaultValueProvider)>();

            foreach (PropertyInfo prop in typeof(T).GetProperties())
            {
                DefaultValueAttribute? attr = prop.GetCustomAttribute<DefaultValueAttribute>();
                if (attr == null)
                    continue;

                IDefaultValueProvider? provider = null;

                // Create provider based on attribute configuration
                switch (attr.ValueType)
                {
                    case DefaultValueType.CurrentDateTimeUtc:
                        provider = new DefaultValueProviders.CurrentDateTimeUtcProvider();
                        break;

                    case DefaultValueType.CurrentDateTimeLocal:
                        provider = new DefaultValueProviders.DelegateValueProvider(() => DateTime.Now, attr.OnlyIfNull);
                        break;

                    case DefaultValueType.NewGuid:
                        provider = new DefaultValueProviders.NewGuidProvider();
                        break;

                    case DefaultValueType.SequentialGuid:
                        provider = new DefaultValueProviders.SequentialGuidProvider();
                        break;

                    case DefaultValueType.EmptyGuid:
                        provider = new DefaultValueProviders.StaticValueProvider(Guid.Empty, attr.OnlyIfNull);
                        break;

                    case DefaultValueType.EmptyString:
                        provider = new DefaultValueProviders.StaticValueProvider(string.Empty, attr.OnlyIfNull);
                        break;

                    case DefaultValueType.Zero:
                        provider = new DefaultValueProviders.StaticValueProvider(0, attr.OnlyIfNull);
                        break;

                    case DefaultValueType.CurrentDateUtc:
                        provider = new DefaultValueProviders.DelegateValueProvider(() => DateTime.UtcNow.Date, attr.OnlyIfNull);
                        break;

                    case DefaultValueType.CurrentDateLocal:
                        provider = new DefaultValueProviders.DelegateValueProvider(() => DateTime.Now.Date, attr.OnlyIfNull);
                        break;

                    case DefaultValueType.True:
                        provider = new DefaultValueProviders.StaticValueProvider(true, attr.OnlyIfNull);
                        break;

                    case DefaultValueType.False:
                        provider = new DefaultValueProviders.StaticValueProvider(false, attr.OnlyIfNull);
                        break;

                    case DefaultValueType.StaticValue:
                        if (attr.StaticValue != null)
                            provider = new DefaultValueProviders.StaticValueProvider(attr.StaticValue, attr.OnlyIfNull);
                        break;

                    case DefaultValueType.CustomProvider:
                        if (attr.ProviderType != null)
                        {
                            provider = (IDefaultValueProvider?)Activator.CreateInstance(attr.ProviderType);
                        }
                        break;
                }

                if (provider != null)
                {
                    providers[prop] = (attr, provider);
                }
            }

            return providers;
        }

        /// <summary>
        /// Gets the primary key value from the specified entity.
        /// </summary>
        /// <param name="entity">The entity to get the primary key value from.</param>
        /// <returns>The primary key value of the entity.</returns>
        public object GetPrimaryKeyValue(T entity)
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

        /// <summary>
        /// Maps an IDataReader to an instance of the specified result type.
        /// </summary>
        /// <typeparam name="TResult">The type to map the reader data to. Must have a parameterless constructor.</typeparam>
        /// <param name="reader">The data reader containing the data to map.</param>
        /// <returns>An instance of TResult with properties populated from the reader data.</returns>
        public TResult MapReaderToType<TResult>(IDataReader reader) where TResult : new()
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
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = GetConnectionAndCommand(transaction);
            SqliteConnection connection = result.Connection;
            bool shouldReturnToPool = result.ShouldReturnToPool;
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
            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = await GetConnectionAndCommandAsync(transaction, token);
            SqliteConnection connection = result.Connection;
            bool shouldReturnToPool = result.ShouldReturnToPool;
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
            
            command.CommandText = $"INSERT INTO {_Sanitizer.SanitizeIdentifier(_TableName)} ({string.Join(", ", sanitizedColumns)}) VALUES {string.Join(", ", valuesList)} RETURNING *;";
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
            using SqliteDataReader reader = command.ExecuteReader();

            int index = 0;
            while (reader.Read() && index < entities.Count)
            {
                T updatedEntity = MapReaderToEntity(reader);

                // Copy all properties from the returned entity to the original entity
                foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
                {
                    PropertyInfo property = kvp.Value;
                    object value = property.GetValue(updatedEntity);
                    property.SetValue(entities[index], value);
                }

                index++;
            }
        }
        
        private async Task ExecuteBatchInsertAsync(SqliteCommand command, IList<T> entities, CancellationToken token)
        {
            CaptureSqlFromCommand(command);
            using SqliteDataReader reader = await command.ExecuteReaderAsync(token);

            int index = 0;
            while (await reader.ReadAsync(token) && index < entities.Count)
            {
                T updatedEntity = MapReaderToEntity(reader);

                // Copy all properties from the returned entity to the original entity
                foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
                {
                    PropertyInfo property = kvp.Value;
                    object value = property.GetValue(updatedEntity);
                    property.SetValue(entities[index], value);
                }

                index++;
            }
        }

        internal void CaptureSqlIfEnabled(string sql)
        {
            if (_CaptureSql && !string.IsNullOrEmpty(sql))
            {
                _LastExecutedSql.Value = sql;
                _LastExecutedSqlWithParameters.Value = sql;
            }
        }

        private void CaptureSqlFromCommand(SqliteCommand command)
        {
            if (_CaptureSql && command != null && !string.IsNullOrEmpty(command.CommandText))
            {
                _LastExecutedSql.Value = command.CommandText;
                _LastExecutedSqlWithParameters.Value = BuildSqlWithParameters(command);
            }
        }

        private string BuildSqlWithParameters(SqliteCommand command)
        {
            if (command?.Parameters == null || command.Parameters.Count == 0)
            {
                return command?.CommandText;
            }

            string sql = command.CommandText;
            foreach (SqliteParameter parameter in command.Parameters)
            {
                string parameterValue = FormatParameterValue(parameter.Value);
                sql = sql.Replace(parameter.ParameterName, parameterValue);
            }
            return sql;
        }

        private string FormatParameterValue(object value)
        {
            if (value == null || value == DBNull.Value)
            {
                return "NULL";
            }

            if (value is string stringValue)
            {
                return $"'{stringValue.Replace("'", "''")}'";
            }

            if (value is DateTime dateTime)
            {
                return $"'{dateTime:yyyy-MM-dd HH:mm:ss}'";
            }

            if (value is bool boolValue)
            {
                return boolValue ? "1" : "0";
            }

            if (value is byte[] bytes)
            {
                return $"X'{Convert.ToHexString(bytes)}'";
            }

            return value.ToString();
        }

        #endregion

        #region Initialization

        /// <inheritdoc/>
        public void InitializeTable(Type entityType, ITransaction transaction = null)
        {
            if (entityType == null)
                throw new ArgumentNullException(nameof(entityType));

            List<string> errors;
            List<string> warnings;
            if (!ValidateTable(entityType, out errors, out warnings))
            {
                string errorMessage = "Table validation failed:\n" + string.Join("\n", errors);
                throw new InvalidOperationException(errorMessage);
            }

            // Log warnings if any
            foreach (string warning in warnings)
            {
                // Could add logging here if ILogger is available
                System.Diagnostics.Debug.WriteLine($"Warning: {warning}");
            }

            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = GetConnectionAndCommand(transaction);

            try
            {
                // Get table name
                EntityAttribute? entityAttr = entityType.GetCustomAttribute<EntityAttribute>();
                string tableName = entityAttr!.Name; // Already validated in ValidateTable

                // Check if table exists
                bool tableExists = SqliteSchemaBuilder.TableExists(tableName, result.Connection);

                if (!tableExists)
                {
                    // Create the table
                    SqliteSchemaBuilder schemaBuilder = new SqliteSchemaBuilder(_Sanitizer, _DataTypeConverter);
                    string createTableSql = schemaBuilder.BuildCreateTableSql(entityType);

                    result.Command.CommandText = createTableSql;
                    result.Command.ExecuteNonQuery();
                }
                else
                {
                    // Table exists - validate schema matches
                    List<ColumnInfo> existingColumns = SqliteSchemaBuilder.GetTableColumns(tableName, result.Connection);
                    List<string> existingColumnNames = existingColumns.Select(c => c.Name).ToList();

                    // Get expected columns from entity
                    List<string> expectedColumnNames = new List<string>();
                    foreach (PropertyInfo prop in entityType.GetProperties())
                    {
                        PropertyAttribute? propAttr = prop.GetCustomAttribute<PropertyAttribute>();
                        if (propAttr != null)
                        {
                            expectedColumnNames.Add(propAttr.Name);
                        }
                    }

                    // Check for missing columns in database
                    foreach (string expectedColumn in expectedColumnNames)
                    {
                        if (!existingColumnNames.Contains(expectedColumn, StringComparer.OrdinalIgnoreCase))
                        {
                            throw new InvalidOperationException(
                                $"Table '{tableName}' exists but column '{expectedColumn}' is missing. " +
                                "Schema migration is not supported - please update the database schema manually or drop and recreate the table.");
                        }
                    }

                    // Check for extra columns (warning only, not an error)
                    foreach (string existingColumn in existingColumnNames)
                    {
                        if (!expectedColumnNames.Contains(existingColumn, StringComparer.OrdinalIgnoreCase))
                        {
                            System.Diagnostics.Debug.WriteLine(
                                $"Warning: Table '{tableName}' contains column '{existingColumn}' which is not defined in the entity type '{entityType.Name}'");
                        }
                    }
                }
            }
            finally
            {
                CleanupConnection(result.Connection, result.Command, result.ShouldReturnToPool);
            }
        }

        /// <inheritdoc/>
        public async Task InitializeTableAsync(Type entityType, ITransaction transaction = null, CancellationToken cancellationToken = default)
        {
            if (entityType == null)
                throw new ArgumentNullException(nameof(entityType));

            List<string> errors;
            List<string> warnings;
            if (!ValidateTable(entityType, out errors, out warnings))
            {
                string errorMessage = "Table validation failed:\n" + string.Join("\n", errors);
                throw new InvalidOperationException(errorMessage);
            }

            // Log warnings if any
            foreach (string warning in warnings)
            {
                System.Diagnostics.Debug.WriteLine($"Warning: {warning}");
            }

            ConnectionCommandResult<SqliteConnection, SqliteCommand> result = await GetConnectionAndCommandAsync(transaction, cancellationToken);

            try
            {
                // Get table name
                EntityAttribute? entityAttr = entityType.GetCustomAttribute<EntityAttribute>();
                string tableName = entityAttr!.Name;

                // Check if table exists
                bool tableExists = SqliteSchemaBuilder.TableExists(tableName, result.Connection);

                if (!tableExists)
                {
                    // Create the table
                    SqliteSchemaBuilder schemaBuilder = new SqliteSchemaBuilder(_Sanitizer, _DataTypeConverter);
                    string createTableSql = schemaBuilder.BuildCreateTableSql(entityType);

                    result.Command.CommandText = createTableSql;
                    await result.Command.ExecuteNonQueryAsync(cancellationToken);
                }
                else
                {
                    // Table exists - validate schema matches
                    List<ColumnInfo> existingColumns = SqliteSchemaBuilder.GetTableColumns(tableName, result.Connection);
                    List<string> existingColumnNames = existingColumns.Select(c => c.Name).ToList();

                    // Get expected columns from entity
                    List<string> expectedColumnNames = new List<string>();
                    foreach (PropertyInfo prop in entityType.GetProperties())
                    {
                        PropertyAttribute? propAttr = prop.GetCustomAttribute<PropertyAttribute>();
                        if (propAttr != null)
                        {
                        expectedColumnNames.Add(propAttr.Name);
                        }
                    }

                    // Check for missing columns in database
                    foreach (string expectedColumn in expectedColumnNames)
                    {
                        if (!existingColumnNames.Contains(expectedColumn, StringComparer.OrdinalIgnoreCase))
                        {
                            throw new InvalidOperationException(
                                $"Table '{tableName}' exists but column '{expectedColumn}' is missing. " +
                                "Schema migration is not supported - please update the database schema manually or drop and recreate the table.");
                        }
                    }

                    // Check for extra columns (warning only)
                    foreach (string existingColumn in existingColumnNames)
                    {
                        if (!expectedColumnNames.Contains(existingColumn, StringComparer.OrdinalIgnoreCase))
                        {
                            System.Diagnostics.Debug.WriteLine(
                                $"Warning: Table '{tableName}' contains column '{existingColumn}' which is not defined in the entity type '{entityType.Name}'");
                        }
                    }
                }
            }
            finally
            {
                await CleanupConnectionAsync(result.Connection, result.Command, result.ShouldReturnToPool);
            }
        }

        /// <inheritdoc/>
        public void InitializeTables(IEnumerable<Type> entityTypes, ITransaction transaction = null)
        {
            if (entityTypes == null)
                throw new ArgumentNullException(nameof(entityTypes));

            ITransaction localTransaction = transaction;
            bool ownTransaction = false;

            try
            {
                // Create transaction if not provided
                if (localTransaction == null)
                {
                    localTransaction = BeginTransaction();
                    ownTransaction = true;
                }

                foreach (Type entityType in entityTypes)
                {
                    InitializeTable(entityType, localTransaction);
                }

                // Commit if we own the transaction
                if (ownTransaction)
                {
                    localTransaction.Commit();
                }
            }
            catch
            {
                // Rollback if we own the transaction
                if (ownTransaction && localTransaction != null)
                {
                    try { localTransaction.Rollback(); } catch { }
                }
                throw;
            }
            finally
            {
                if (ownTransaction && localTransaction != null)
                {
                    localTransaction.Dispose();
                }
            }
        }

        /// <inheritdoc/>
        public async Task InitializeTablesAsync(IEnumerable<Type> entityTypes, ITransaction transaction = null, CancellationToken cancellationToken = default)
        {
            if (entityTypes == null)
                throw new ArgumentNullException(nameof(entityTypes));

            ITransaction localTransaction = transaction;
            bool ownTransaction = false;

            try
            {
                // Create transaction if not provided
                if (localTransaction == null)
                {
                    localTransaction = await BeginTransactionAsync(cancellationToken);
                    ownTransaction = true;
                }

                foreach (Type entityType in entityTypes)
                {
                    await InitializeTableAsync(entityType, localTransaction, cancellationToken);
                }

                // Commit if we own the transaction
                if (ownTransaction)
                {
                    await localTransaction.CommitAsync(cancellationToken);
                }
            }
            catch
            {
                // Rollback if we own the transaction
                if (ownTransaction && localTransaction != null)
                {
                    try { await localTransaction.RollbackAsync(cancellationToken); } catch { }
                }
                throw;
            }
            finally
            {
                if (ownTransaction && localTransaction != null)
                {
                    localTransaction.Dispose();
                }
            }
        }

        /// <inheritdoc/>
        public bool ValidateTable(Type entityType, out List<string> errors, out List<string> warnings)
        {
            if (entityType == null)
                throw new ArgumentNullException(nameof(entityType));

            errors = new List<string>();
            warnings = new List<string>();
            bool isValid = true;

            // Check for Entity attribute
            EntityAttribute? entityAttr = entityType.GetCustomAttribute<EntityAttribute>();
            if (entityAttr == null)
            {
                errors.Add($"Type '{entityType.Name}' must have an Entity attribute");
                isValid = false;
                return isValid; // Can't continue without entity attribute
            }

            string tableName = entityAttr.Name;
            PropertyInfo? primaryKeyProperty = null;
            List<PropertyInfo> columnProperties = new List<PropertyInfo>();

            // Scan properties
            foreach (PropertyInfo prop in entityType.GetProperties())
            {
                PropertyAttribute? propAttr = prop.GetCustomAttribute<PropertyAttribute>();
                if (propAttr == null)
                    continue;

                columnProperties.Add(prop);

                // Check for primary key
                if ((propAttr.PropertyFlags & Flags.PrimaryKey) == Flags.PrimaryKey)
                {
                    if (primaryKeyProperty != null)
                    {
                        errors.Add($"Type '{entityType.Name}' has multiple primary key columns: '{primaryKeyProperty.Name}' and '{prop.Name}'");
                        isValid = false;
                    }
                    primaryKeyProperty = prop;
                }
            }

            // Validate primary key exists
            if (primaryKeyProperty == null)
            {
                errors.Add($"Type '{entityType.Name}' must have a primary key column");
                isValid = false;
            }

            // Validate at least one column exists
            if (columnProperties.Count == 0)
            {
                errors.Add($"Type '{entityType.Name}' must have at least one property with a Property attribute");
                isValid = false;
            }

            // If table exists, check schema compatibility
            try
            {
                using (SqliteConnection connection = (SqliteConnection)_ConnectionFactory.GetConnection())
                {
                    if (SqliteSchemaBuilder.TableExists(tableName, connection))
                    {
                        List<ColumnInfo> existingColumns = SqliteSchemaBuilder.GetTableColumns(tableName, connection);
                        List<string> existingColumnNames = existingColumns.Select(c => c.Name).ToList();

                        // Check if entity columns exist in database
                        foreach (PropertyInfo prop in columnProperties)
                        {
                            PropertyAttribute? propAttr = prop.GetCustomAttribute<PropertyAttribute>();
                            if (propAttr != null)
                            {
                                if (!existingColumnNames.Contains(propAttr.Name, StringComparer.OrdinalIgnoreCase))
                                {
                                    errors.Add($"Table '{tableName}' exists but column '{propAttr.Name}' (for property '{prop.Name}') does not exist");
                                    isValid = false;
                                }
                            }
                        }

                        // Check for extra columns in database
                        List<string> entityColumnNames = columnProperties
                            .Select(p => p.GetCustomAttribute<PropertyAttribute>()?.Name)
                            .Where(name => name != null)
                            .ToList()!;

                        foreach (string existingColumn in existingColumnNames)
                        {
                            if (!entityColumnNames.Contains(existingColumn, StringComparer.OrdinalIgnoreCase))
                            {
                                warnings.Add($"Table '{tableName}' contains column '{existingColumn}' which is not defined in entity type '{entityType.Name}'");
                            }
                        }
                    }
                    else
                    {
                        warnings.Add($"Table '{tableName}' does not exist and will be created during initialization");
                    }

                    _ConnectionFactory.ReturnConnection(connection);
                }
            }
            catch (Exception ex)
            {
                warnings.Add($"Could not check if table '{tableName}' exists: {ex.Message}");
            }

            return isValid;
        }

        /// <inheritdoc/>
        public bool ValidateTables(IEnumerable<Type> entityTypes, out List<string> errors, out List<string> warnings)
        {
            if (entityTypes == null)
                throw new ArgumentNullException(nameof(entityTypes));

            errors = new List<string>();
            warnings = new List<string>();
            bool allValid = true;

            foreach (Type entityType in entityTypes)
            {
                List<string> typeErrors;
                List<string> typeWarnings;

                bool isValid = ValidateTable(entityType, out typeErrors, out typeWarnings);
                if (!isValid)
                {
                    allValid = false;
                }

                errors.AddRange(typeErrors);
                warnings.AddRange(typeWarnings);
            }

            return allValid;
        }

        /// <inheritdoc/>
        public void CreateIndexes(Type entityType, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(entityType);

            SqliteSchemaBuilder schemaBuilder = new SqliteSchemaBuilder(_Sanitizer, _DataTypeConverter);
            List<string> indexSqlStatements = schemaBuilder.BuildCreateIndexSql(entityType);

            if (indexSqlStatements.Count == 0)
            {
                return; // No indexes to create
            }

            if (transaction != null)
            {
                SqliteConnection connection = (SqliteConnection)transaction.Connection;
                SqliteTransaction sqliteTransaction = (SqliteTransaction)transaction.Transaction;

                foreach (string sql in indexSqlStatements)
                {
                    using (SqliteCommand command = connection.CreateCommand())
                    {
                        command.Transaction = sqliteTransaction;
                        command.CommandText = sql;
                        command.ExecuteNonQuery();
                    }
                }
            }
            else
            {
                using (SqliteConnection connection = (SqliteConnection)_ConnectionFactory.GetConnection())
                {
                    foreach (string sql in indexSqlStatements)
                    {
                        using (SqliteCommand command = connection.CreateCommand())
                        {
                            command.CommandText = sql;
                            command.ExecuteNonQuery();
                        }
                    }

                    _ConnectionFactory.ReturnConnection(connection);
                }
            }
        }

        /// <inheritdoc/>
        public async Task CreateIndexesAsync(Type entityType, ITransaction? transaction = null, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            cancellationToken.ThrowIfCancellationRequested();

            SqliteSchemaBuilder schemaBuilder = new SqliteSchemaBuilder(_Sanitizer, _DataTypeConverter);
            List<string> indexSqlStatements = schemaBuilder.BuildCreateIndexSql(entityType);

            if (indexSqlStatements.Count == 0)
            {
                return; // No indexes to create
            }

            if (transaction != null)
            {
                SqliteConnection connection = (SqliteConnection)transaction.Connection;
                SqliteTransaction sqliteTransaction = (SqliteTransaction)transaction.Transaction;

                foreach (string sql in indexSqlStatements)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    using (SqliteCommand command = connection.CreateCommand())
                    {
                        command.Transaction = sqliteTransaction;
                        command.CommandText = sql;
                        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            else
            {
                using (SqliteConnection connection = (SqliteConnection)_ConnectionFactory.GetConnection())
                {
                    foreach (string sql in indexSqlStatements)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        using (SqliteCommand command = connection.CreateCommand())
                        {
                            command.CommandText = sql;
                            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                        }
                    }

                    _ConnectionFactory.ReturnConnection(connection);
                }
            }
        }

        /// <inheritdoc/>
        public void DropIndex(string indexName, ITransaction? transaction = null)
        {
            if (string.IsNullOrWhiteSpace(indexName))
                throw new ArgumentNullException(nameof(indexName), "Index name cannot be null or empty");

            string sql = $"DROP INDEX IF EXISTS {_Sanitizer.SanitizeIdentifier(indexName)}";

            if (transaction != null)
            {
                SqliteConnection connection = (SqliteConnection)transaction.Connection;
                SqliteTransaction sqliteTransaction = (SqliteTransaction)transaction.Transaction;

                using (SqliteCommand command = connection.CreateCommand())
                {
                    command.Transaction = sqliteTransaction;
                    command.CommandText = sql;
                    command.ExecuteNonQuery();
                }
            }
            else
            {
                using (SqliteConnection connection = (SqliteConnection)_ConnectionFactory.GetConnection())
                {
                    using (SqliteCommand command = connection.CreateCommand())
                    {
                        command.CommandText = sql;
                        command.ExecuteNonQuery();
                    }

                    _ConnectionFactory.ReturnConnection(connection);
                }
            }
        }

        /// <inheritdoc/>
        public async Task DropIndexAsync(string indexName, ITransaction? transaction = null, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(indexName))
                throw new ArgumentNullException(nameof(indexName), "Index name cannot be null or empty");

            cancellationToken.ThrowIfCancellationRequested();

            string sql = $"DROP INDEX IF EXISTS {_Sanitizer.SanitizeIdentifier(indexName)}";

            if (transaction != null)
            {
                SqliteConnection connection = (SqliteConnection)transaction.Connection;
                SqliteTransaction sqliteTransaction = (SqliteTransaction)transaction.Transaction;

                using (SqliteCommand command = connection.CreateCommand())
                {
                    command.Transaction = sqliteTransaction;
                    command.CommandText = sql;
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            else
            {
                using (SqliteConnection connection = (SqliteConnection)_ConnectionFactory.GetConnection())
                {
                    using (SqliteCommand command = connection.CreateCommand())
                    {
                        command.CommandText = sql;
                        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }

                    _ConnectionFactory.ReturnConnection(connection);
                }
            }
        }

        /// <inheritdoc/>
        public List<string> GetIndexes(Type entityType)
        {
            ArgumentNullException.ThrowIfNull(entityType);

            EntityAttribute? entityAttr = entityType.GetCustomAttribute<EntityAttribute>();
            if (entityAttr == null)
                throw new InvalidOperationException($"Type '{entityType.Name}' must have an Entity attribute");

            string tableName = entityAttr.Name;

            using (SqliteConnection connection = (SqliteConnection)_ConnectionFactory.GetConnection())
            {
                List<IndexInfo> indexes = SqliteSchemaBuilder.GetExistingIndexes(tableName, connection);
                List<string> indexNames = indexes.Select(i => i.Name).ToList();

                _ConnectionFactory.ReturnConnection(connection);

                return indexNames;
            }
        }

        /// <inheritdoc/>
        public async Task<List<string>> GetIndexesAsync(Type entityType, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            cancellationToken.ThrowIfCancellationRequested();

            EntityAttribute? entityAttr = entityType.GetCustomAttribute<EntityAttribute>();
            if (entityAttr == null)
                throw new InvalidOperationException($"Type '{entityType.Name}' must have an Entity attribute");

            string tableName = entityAttr.Name;

            using (SqliteConnection connection = (SqliteConnection)_ConnectionFactory.GetConnection())
            {
                await Task.Run(() =>
                {
                    // SQLite operations are synchronous
                }, cancellationToken).ConfigureAwait(false);

                List<IndexInfo> indexes = SqliteSchemaBuilder.GetExistingIndexes(tableName, connection);
                List<string> indexNames = indexes.Select(i => i.Name).ToList();

                _ConnectionFactory.ReturnConnection(connection);

                return indexNames;
            }
        }

        /// <inheritdoc/>
        public void CreateDatabaseIfNotExists()
        {
            // For SQLite, opening a connection creates the database file if it doesn't exist
            // This is handled automatically by SqliteConnection when Mode is ReadWriteCreate (default)

            // Just verify we can connect
            using (SqliteConnection connection = (SqliteConnection)_ConnectionFactory.GetConnection())
            {
                // Connection created successfully
                _ConnectionFactory.ReturnConnection(connection);
            }
        }

        /// <inheritdoc/>
        public async Task CreateDatabaseIfNotExistsAsync(CancellationToken cancellationToken = default)
        {
            // For SQLite, opening a connection creates the database file if it doesn't exist
            // This is handled automatically by SqliteConnection when Mode is ReadWriteCreate (default)

            // Just verify we can connect
            using (SqliteConnection connection = (SqliteConnection)await _ConnectionFactory.GetConnectionAsync(cancellationToken))
            {
                // Connection created successfully
                await _ConnectionFactory.ReturnConnectionAsync(connection);
            }
        }

        #endregion
    }
}