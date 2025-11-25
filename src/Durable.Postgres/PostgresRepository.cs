namespace Durable.Postgres
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
    using Durable;
    using Durable.ConcurrencyConflictResolvers;
    using Durable.DefaultValueProviders;
    using Npgsql;

    /// <summary>
    /// PostgreSQL Repository Implementation with Full Transaction Support and Connection Pooling.
    /// Provides comprehensive data access operations for entities with support for optimistic concurrency,
    /// batch operations, SQL capture, and advanced querying capabilities.
    /// </summary>
    /// <typeparam name="T">The entity type that this repository manages. Must be a class with a parameterless constructor.</typeparam>
    public class PostgresRepository<T> : IRepository<T>, IBatchInsertConfiguration, ISqlCapture, ISqlTrackingConfiguration, IDisposable where T : class, new()
    {
#pragma warning disable CS8600

        #region Public-Members

        /// <summary>
        /// Gets the last SQL statement that was executed by this repository instance.
        /// Returns null if no SQL has been executed or SQL capture is disabled.
        /// </summary>
        public string? LastExecutedSql
        {
            get => _LastExecutedSql;
        }

        /// <summary>
        /// Gets the last SQL statement with parameter values substituted that was executed by this repository instance.
        /// This provides a fully executable SQL statement with actual parameter values for debugging purposes.
        /// Returns null if no SQL has been executed or SQL capture is disabled.
        /// </summary>
        public string? LastExecutedSqlWithParameters
        {
            get => _LastExecutedSqlWithParameters;
        }

        /// <summary>
        /// Gets or sets whether SQL statements should be captured and stored.
        /// Default value is false for performance reasons.
        /// This property is thread-safe and can be safely accessed from multiple threads.
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
        /// This property is thread-safe and can be safely accessed from multiple threads.
        /// </summary>
        public bool IncludeQueryInResults
        {
            get => _IncludeQueryInResults;
            set => _IncludeQueryInResults = value;
        }

        /// <summary>
        /// Gets the maximum number of rows to include in a single multi-row INSERT statement.
        /// PostgreSQL can handle large batches efficiently, default is 1000 rows.
        /// </summary>
        public int MaxRowsPerBatch => _BatchConfig.MaxRowsPerBatch;

        /// <summary>
        /// Gets the maximum number of parameters per INSERT statement.
        /// PostgreSQL has a high limit for parameters, default is 65535.
        /// </summary>
        public int MaxParametersPerStatement => _BatchConfig.MaxParametersPerStatement;

        /// <summary>
        /// Gets whether to use prepared statement reuse for batch operations.
        /// PostgreSQL benefits from prepared statement reuse.
        /// </summary>
        public bool EnablePreparedStatementReuse => _BatchConfig.EnablePreparedStatementReuse;

        /// <summary>
        /// Gets whether to use multi-row INSERT syntax when possible.
        /// PostgreSQL has excellent support for multi-row INSERT statements.
        /// </summary>
        public bool EnableMultiRowInsert => _BatchConfig.EnableMultiRowInsert;

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
        internal readonly VersionColumnInfo? _VersionColumnInfo;
        internal readonly IConcurrencyConflictResolver<T> _ConflictResolver;
        internal readonly IChangeTracker<T> _ChangeTracker;
        internal readonly Dictionary<PropertyInfo, (DefaultValueAttribute, IDefaultValueProvider)> _DefaultValueProviders;

        private volatile string? _LastExecutedSql;
        private volatile string? _LastExecutedSqlWithParameters;
        private readonly bool _OwnsConnectionFactory;
        private volatile bool _CaptureSql;
        private volatile bool _IncludeQueryInResults;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the PostgresRepository with a connection string and optional configuration.
        /// Creates an internal PostgresConnectionFactory for connection management.
        /// </summary>
        /// <param name="connectionString">The PostgreSQL connection string used to connect to the database.</param>
        /// <param name="batchConfig">Optional batch insert configuration settings. Uses default settings if null.</param>
        /// <param name="dataTypeConverter">Optional data type converter for custom type handling. Uses default converter if null.</param>
        /// <param name="conflictResolver">Optional concurrency conflict resolver. Uses default resolver with ThrowException strategy if null.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the entity type T lacks required attributes (Entity, primary key).</exception>
        public PostgresRepository(string connectionString, IBatchInsertConfiguration? batchConfig = null, IDataTypeConverter? dataTypeConverter = null, IConcurrencyConflictResolver<T>? conflictResolver = null)
        {
            ArgumentNullException.ThrowIfNull(connectionString);
            Settings = PostgresRepositorySettings.Parse(connectionString);
            _ConnectionFactory = new PostgresConnectionFactory(connectionString);
            _OwnsConnectionFactory = true; // We created this factory, so we own it
            _Sanitizer = new PostgresSanitizer();
            _DataTypeConverter = dataTypeConverter ?? new PostgresDataTypeConverter();
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
        /// Initializes a new instance of the PostgresRepository with repository settings and optional configuration.
        /// Creates an internal PostgresConnectionFactory using the connection string built from settings.
        /// </summary>
        /// <param name="settings">The PostgreSQL repository settings to use for configuration.</param>
        /// <param name="batchConfig">Optional batch insert configuration settings. Uses default settings if null.</param>
        /// <param name="dataTypeConverter">Optional data type converter for custom type handling. Uses default converter if null.</param>
        /// <param name="conflictResolver">Optional concurrency conflict resolver. Uses default resolver with ThrowException strategy if null.</param>
        /// <exception cref="ArgumentNullException">Thrown when settings is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the entity type T lacks required attributes (Entity, primary key), or when settings are invalid.</exception>
        public PostgresRepository(PostgresRepositorySettings settings, IBatchInsertConfiguration? batchConfig = null, IDataTypeConverter? dataTypeConverter = null, IConcurrencyConflictResolver<T>? conflictResolver = null)
        {
            ArgumentNullException.ThrowIfNull(settings);
            Settings = settings;
            string connectionString = settings.BuildConnectionString();
            _ConnectionFactory = new PostgresConnectionFactory(connectionString);
            _OwnsConnectionFactory = true; // We created this factory, so we own it
            _Sanitizer = new PostgresSanitizer();
            _DataTypeConverter = dataTypeConverter ?? new PostgresDataTypeConverter();
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
        /// Initializes a new instance of the PostgresRepository with a provided connection factory and optional configuration.
        /// Allows for shared connection pooling and factory management across multiple repository instances.
        /// Note: When using this constructor, the Settings property will be null as no connection string is directly provided.
        /// </summary>
        /// <param name="connectionFactory">The connection factory to use for database connections.</param>
        /// <param name="batchConfig">Optional batch insert configuration settings. Uses default settings if null.</param>
        /// <param name="dataTypeConverter">Optional data type converter for custom type handling. Uses default converter if null.</param>
        /// <param name="conflictResolver">Optional concurrency conflict resolver. Uses default resolver with ThrowException strategy if null.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionFactory is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the entity type T lacks required attributes (Entity, primary key).</exception>
        public PostgresRepository(IConnectionFactory connectionFactory, IBatchInsertConfiguration? batchConfig = null, IDataTypeConverter? dataTypeConverter = null, IConcurrencyConflictResolver<T>? conflictResolver = null)
        {
            _ConnectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
            _OwnsConnectionFactory = false; // External factory, we don't own it
            Settings = null!;
            _Sanitizer = new PostgresSanitizer();
            _DataTypeConverter = dataTypeConverter ?? new PostgresDataTypeConverter();
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

        // For now, we'll create stub implementations that throw NotImplementedException
        // This allows the code to compile and build while we work on the full implementation

        /// <summary>
        /// Returns the first entity that matches the specified predicate, or throws an exception if no entity is found.
        /// </summary>
        /// <param name="predicate">Optional expression to filter entities. If null, returns the first entity in the table.</param>
        /// <param name="transaction">Optional transaction context for the operation.</param>
        /// <returns>The first entity that matches the predicate.</returns>
        /// <exception cref="InvalidOperationException">Thrown when no entities match the predicate.</exception>
        public T ReadFirst(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            IQueryBuilder<T> query = Query(transaction);
            if (predicate != null) query = query.Where(predicate);
            return query.Take(1).Execute().FirstOrDefault() ?? throw new InvalidOperationException("Sequence contains no elements");
        }

        /// <summary>
        /// Returns the first entity that matches the specified predicate, or default if no entity is found.
        /// </summary>
        /// <param name="predicate">Optional expression to filter entities. If null, returns the first entity in the table.</param>
        /// <param name="transaction">Optional transaction context for the operation.</param>
        /// <returns>The first entity that matches the predicate, or default(T) if no entity is found.</returns>
        public T? ReadFirstOrDefault(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            IQueryBuilder<T> query = Query(transaction);
            if (predicate != null) query = query.Where(predicate);
            return query.Take(1).Execute().FirstOrDefault();
        }

        /// <summary>
        /// Returns the only entity that matches the specified predicate, and throws an exception if there is not exactly one entity.
        /// </summary>
        /// <param name="predicate">Expression to filter entities. Cannot be null.</param>
        /// <param name="transaction">Optional transaction context for the operation.</param>
        /// <returns>The single entity that matches the predicate.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when zero or more than one entity matches the predicate.</exception>
        public T ReadSingle(Expression<Func<T, bool>> predicate, ITransaction? transaction = null)
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));

            List<T> results = Query(transaction).Where(predicate).Take(2).Execute().ToList();
            if (results.Count != 1)
                throw new InvalidOperationException($"Expected exactly 1 result but found {results.Count}");
            return results[0];
        }

        /// <summary>
        /// Returns the only entity that matches the specified predicate, or default if no such entity exists; this method throws an exception if more than one entity matches the predicate.
        /// </summary>
        /// <param name="predicate">Expression to filter entities. Cannot be null.</param>
        /// <param name="transaction">Optional transaction context for the operation.</param>
        /// <returns>The single entity that matches the predicate, or default(T) if no entity is found.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when more than one entity matches the predicate.</exception>
        public T? ReadSingleOrDefault(Expression<Func<T, bool>> predicate, ITransaction? transaction = null)
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));

            List<T> results = Query(transaction).Where(predicate).Take(2).Execute().ToList();
            if (results.Count > 1)
                throw new InvalidOperationException($"Expected 0 or 1 result but found {results.Count}");
            return results.FirstOrDefault();
        }

        /// <summary>
        /// Reads multiple entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities. If null, returns all entities.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>A collection of entities that match the predicate.</returns>
        public IEnumerable<T> ReadMany(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            IQueryBuilder<T> query = Query(transaction);
            if (predicate != null)
                query = query.Where(predicate);
            return query.Execute();
        }

        /// <summary>
        /// Reads all entities from the database.
        /// </summary>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>A collection of all entities.</returns>
        public IEnumerable<T> ReadAll(ITransaction? transaction = null)
        {
            return Query(transaction).Execute();
        }

        /// <summary>
        /// Reads an entity by its identifier.
        /// </summary>
        /// <param name="id">The identifier of the entity to read.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>The entity with the specified identifier.</returns>
        public T? ReadById(object id, ITransaction? transaction = null)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            // Use the async version with synchronous execution
            return ReadByIdAsync(id, transaction, CancellationToken.None).GetAwaiter().GetResult();
        }

        // Async read methods
        /// <summary>
        /// Asynchronously returns the first entity that matches the specified predicate, or throws an exception if no entity is found.
        /// </summary>
        /// <param name="predicate">Optional expression to filter entities. If null, returns the first entity in the table.</param>
        /// <param name="transaction">Optional transaction context for the operation.</param>
        /// <param name="token">Cancellation token to support operation cancellation.</param>
        /// <returns>A task that represents the asynchronous operation containing the first entity that matches the predicate.</returns>
        /// <exception cref="InvalidOperationException">Thrown when no entities match the predicate.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled via the cancellation token.</exception>
        public async Task<T?> ReadFirstAsync(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            IQueryBuilder<T> query = Query(transaction);
            if (predicate != null) query = query.Where(predicate);

            IAsyncEnumerable<T> results = query.Take(1).ExecuteAsyncEnumerable(token);
            await foreach (T result in results.WithCancellation(token).ConfigureAwait(false))
            {
                return result;
            }

            return default(T);
        }

        /// <summary>
        /// Asynchronously returns the first entity that matches the specified predicate, or default if no entity is found.
        /// </summary>
        /// <param name="predicate">Optional expression to filter entities. If null, returns the first entity in the table.</param>
        /// <param name="transaction">Optional transaction context for the operation.</param>
        /// <param name="token">Cancellation token to support operation cancellation.</param>
        /// <returns>A task that represents the asynchronous operation containing the first entity that matches the predicate, or default(T) if no entity is found.</returns>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled via the cancellation token.</exception>
        public async Task<T?> ReadFirstOrDefaultAsync(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            IQueryBuilder<T> query = Query(transaction);
            if (predicate != null) query = query.Where(predicate);

            IAsyncEnumerable<T> results = query.Take(1).ExecuteAsyncEnumerable(token);
            await foreach (T result in results.WithCancellation(token).ConfigureAwait(false))
            {
                return result;
            }

            return default(T);
        }

        /// <summary>
        /// Asynchronously returns the only entity that matches the specified predicate, and throws an exception if there is not exactly one entity.
        /// </summary>
        /// <param name="predicate">Expression to filter entities. Cannot be null.</param>
        /// <param name="transaction">Optional transaction context for the operation.</param>
        /// <param name="token">Cancellation token to support operation cancellation.</param>
        /// <returns>A task that represents the asynchronous operation containing the single entity that matches the predicate.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when zero or more than one entity matches the predicate.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled via the cancellation token.</exception>
        public async Task<T> ReadSingleAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));

            token.ThrowIfCancellationRequested();

            List<T> results = new List<T>();
            IAsyncEnumerable<T> queryResults = Query(transaction).Where(predicate).Take(2).ExecuteAsyncEnumerable(token);

            await foreach (T result in queryResults.WithCancellation(token).ConfigureAwait(false))
            {
                results.Add(result);
            }

            if (results.Count != 1)
                throw new InvalidOperationException($"Expected exactly 1 result but found {results.Count}");
            return results[0];
        }

        /// <summary>
        /// Asynchronously returns the only entity that matches the specified predicate, or default if no such entity exists; this method throws an exception if more than one entity matches the predicate.
        /// </summary>
        /// <param name="predicate">Expression to filter entities. Cannot be null.</param>
        /// <param name="transaction">Optional transaction context for the operation.</param>
        /// <param name="token">Cancellation token to support operation cancellation.</param>
        /// <returns>A task that represents the asynchronous operation containing the single entity that matches the predicate, or default(T) if no entity is found.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when more than one entity matches the predicate.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled via the cancellation token.</exception>
        public async Task<T?> ReadSingleOrDefaultAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));

            token.ThrowIfCancellationRequested();

            List<T> results = new List<T>();
            IAsyncEnumerable<T> queryResults = Query(transaction).Where(predicate).Take(2).ExecuteAsyncEnumerable(token);

            await foreach (T result in queryResults.WithCancellation(token).ConfigureAwait(false))
            {
                results.Add(result);
            }

            if (results.Count > 1)
                throw new InvalidOperationException($"Expected 0 or 1 result but found {results.Count}");
            return results.FirstOrDefault();
        }

        /// <summary>
        /// Asynchronously reads multiple entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">Optional predicate to filter entities. If null, returns all entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token for the async operation.</param>
        /// <returns>An async enumerable of entities that match the predicate.</returns>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
        public async IAsyncEnumerable<T> ReadManyAsync(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            IQueryBuilder<T> query = Query(transaction);
            if (predicate != null)
                query = query.Where(predicate);

            IEnumerable<T> results = await query.ExecuteAsync(token).ConfigureAwait(false);

            foreach (T entity in results)
            {
                token.ThrowIfCancellationRequested();
                yield return entity;
            }
        }

        /// <summary>
        /// Asynchronously reads all entities from the database.
        /// </summary>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token for the async operation.</param>
        /// <returns>An async enumerable of all entities.</returns>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
        public IAsyncEnumerable<T> ReadAllAsync(ITransaction? transaction = null, CancellationToken token = default)
        {
            return ReadManyAsync(null, transaction, token);
        }

        /// <summary>
        /// Asynchronously reads an entity by its identifier.
        /// </summary>
        /// <param name="id">The primary key value of the entity to read.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token for the async operation.</param>
        /// <returns>The entity with the specified identifier, or null if not found.</returns>
        /// <exception cref="ArgumentNullException">Thrown when id is null.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
        public async Task<T?> ReadByIdAsync(object id, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            token.ThrowIfCancellationRequested();

            string sql = $"SELECT * FROM {_Sanitizer.SanitizeIdentifier(_TableName)} WHERE {_Sanitizer.SanitizeIdentifier(_PrimaryKeyColumn)} = @id LIMIT 1";

            await foreach (T result in FromSqlAsync(sql, transaction, token, ("@id", id)))
            {
                return result;
            }

            return null;
        }

        // Existence checks
        /// <summary>
        /// Determines whether any entity matches the specified predicate.
        /// </summary>
        /// <param name="predicate">Expression to filter entities. Cannot be null.</param>
        /// <param name="transaction">Optional transaction context for the operation.</param>
        /// <returns>True if any entity matches the predicate; otherwise, false.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null.</exception>
        public bool Exists(Expression<Func<T, bool>> predicate, ITransaction? transaction = null)
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));

            return ExistsAsync(predicate, transaction, CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Determines whether an entity with the specified primary key exists.
        /// </summary>
        /// <param name="id">The primary key value to search for. Cannot be null.</param>
        /// <param name="transaction">Optional transaction context for the operation.</param>
        /// <returns>True if an entity with the specified primary key exists; otherwise, false.</returns>
        /// <exception cref="ArgumentNullException">Thrown when id is null.</exception>
        public bool ExistsById(object id, ITransaction? transaction = null)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            return ExistsByIdAsync(id, transaction, CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Asynchronously determines whether any entity matches the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to test entities against.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token for the async operation.</param>
        /// <returns>True if any entity matches the predicate, false otherwise.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
        public async Task<bool> ExistsAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));

            token.ThrowIfCancellationRequested();

            PostgresExpressionParser<T> expressionParser = new PostgresExpressionParser<T>(_ColumnMappings, _Sanitizer);
            string whereClause = expressionParser.ParseExpressionWithParameters(predicate.Body);
            string sql = $"SELECT EXISTS(SELECT 1 FROM {_Sanitizer.SanitizeIdentifier(_TableName)} WHERE {whereClause})";
            object[] parameters = expressionParser.GetParameters().Cast<object>().ToArray();

            if (transaction != null)
            {
                object? result = await ExecuteScalarWithConnectionAsync<object>(transaction.Connection, sql, transaction.Transaction, token, parameters.Cast<(string, object?)>().ToArray()).ConfigureAwait(false);
                return Convert.ToBoolean(result);
            }
            else
            {
                DbConnection? connection = null;
                try
                {
                    connection = await _ConnectionFactory.GetConnectionAsync().ConfigureAwait(false);
                    object? result = await ExecuteScalarWithConnectionAsync<object>(connection, sql, null, token, parameters.Cast<(string, object?)>().ToArray()).ConfigureAwait(false);
                    return Convert.ToBoolean(result);
                }
                finally
                {
                    if (connection != null)
                        await _ConnectionFactory.ReturnConnectionAsync(connection).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Asynchronously determines whether an entity with the specified primary key exists.
        /// </summary>
        /// <param name="id">The primary key value to search for. Cannot be null.</param>
        /// <param name="transaction">Optional transaction context for the operation.</param>
        /// <param name="token">Cancellation token to support operation cancellation.</param>
        /// <returns>A task that represents the asynchronous operation containing true if an entity with the specified primary key exists; otherwise, false.</returns>
        /// <exception cref="ArgumentNullException">Thrown when id is null.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled via the cancellation token.</exception>
        public async Task<bool> ExistsByIdAsync(object id, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            token.ThrowIfCancellationRequested();

            string sql = $"SELECT EXISTS(SELECT 1 FROM {_Sanitizer.SanitizeIdentifier(_TableName)} WHERE {_Sanitizer.SanitizeIdentifier(_PrimaryKeyColumn)} = @id)";
            (string, object?)[] parameters = { ("@id", id) };

            if (transaction != null)
            {
                object? result = await ExecuteScalarWithConnectionAsync<object>(transaction.Connection, sql, transaction.Transaction, token, parameters).ConfigureAwait(false);
                return Convert.ToBoolean(result);
            }
            else
            {
                DbConnection? connection = null;
                try
                {
                    connection = await _ConnectionFactory.GetConnectionAsync().ConfigureAwait(false);
                    object? result = await ExecuteScalarWithConnectionAsync<object>(connection, sql, null, token, parameters).ConfigureAwait(false);
                    return Convert.ToBoolean(result);
                }
                finally
                {
                    if (connection != null)
                        await _ConnectionFactory.ReturnConnectionAsync(connection).ConfigureAwait(false);
                }
            }
        }

        // Count operations
        /// <summary>
        /// Counts the number of entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">Optional predicate to filter entities. If null, counts all entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The number of entities that match the predicate.</returns>
        public int Count(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            string sql;
            object[] parameters = Array.Empty<object>();

            if (predicate == null)
            {
                sql = $"SELECT COUNT(*) FROM {_Sanitizer.SanitizeIdentifier(_TableName)}";
            }
            else
            {
                PostgresExpressionParser<T> expressionParser = new PostgresExpressionParser<T>(_ColumnMappings, _Sanitizer);
                string whereClause = expressionParser.ParseExpressionWithParameters(predicate.Body);
                sql = $"SELECT COUNT(*) FROM {_Sanitizer.SanitizeIdentifier(_TableName)} WHERE {whereClause}";
                parameters = expressionParser.GetParameters().Cast<object>().ToArray();
            }

            if (transaction != null)
            {
                object? result = ExecuteScalarWithConnection<object>(transaction.Connection, sql, transaction.Transaction, parameters.Cast<(string, object?)>().ToArray());
                return Convert.ToInt32(result);
            }
            else
            {
                DbConnection? connection = null;
                try
                {
                    connection = (DbConnection)_ConnectionFactory.GetConnection();
                    object? result = ExecuteScalarWithConnection<object>(connection, sql, null, parameters.Cast<(string, object?)>().ToArray());
                    return Convert.ToInt32(result);
                }
                finally
                {
                    if (connection != null)
                        _ConnectionFactory.ReturnConnection(connection);
                }
            }
        }

        /// <summary>
        /// Asynchronously counts the number of entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">Optional predicate to filter entities. If null, counts all entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token for the async operation.</param>
        /// <returns>The number of entities that match the predicate.</returns>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
        public async Task<int> CountAsync(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            string sql;
            object[] parameters = Array.Empty<object>();

            if (predicate == null)
            {
                sql = $"SELECT COUNT(*) FROM {_Sanitizer.SanitizeIdentifier(_TableName)}";
            }
            else
            {
                PostgresExpressionParser<T> expressionParser = new PostgresExpressionParser<T>(_ColumnMappings, _Sanitizer);
                string whereClause = expressionParser.ParseExpressionWithParameters(predicate.Body);
                sql = $"SELECT COUNT(*) FROM {_Sanitizer.SanitizeIdentifier(_TableName)} WHERE {whereClause}";
                parameters = expressionParser.GetParameters().Cast<object>().ToArray();
            }

            if (transaction != null)
            {
                object? result = await ExecuteScalarWithConnectionAsync<object>(transaction.Connection, sql, transaction.Transaction, token, parameters.Cast<(string, object?)>().ToArray()).ConfigureAwait(false);
                return Convert.ToInt32(result);
            }
            else
            {
                DbConnection? connection = null;
                try
                {
                    connection = await _ConnectionFactory.GetConnectionAsync().ConfigureAwait(false);
                    object? result = await ExecuteScalarWithConnectionAsync<object>(connection, sql, null, token, parameters.Cast<(string, object?)>().ToArray()).ConfigureAwait(false);
                    return Convert.ToInt32(result);
                }
                finally
                {
                    if (connection != null)
                        await _ConnectionFactory.ReturnConnectionAsync(connection).ConfigureAwait(false);
                }
            }
        }

        // Aggregation operations
        /// <summary>
        /// Finds the maximum value of the specified property among entities that match the predicate.
        /// </summary>
        /// <typeparam name="TResult">The type of the property to find the maximum value for.</typeparam>
        /// <param name="selector">Expression selecting the property to find the maximum value for.</param>
        /// <param name="predicate">Optional predicate to filter entities. If null, considers all entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The maximum value of the selected property.</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when SQL execution fails.</exception>
        public TResult Max<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            PostgresExpressionParser<T> parser = new PostgresExpressionParser<T>(_ColumnMappings, _Sanitizer);
            string column = parser.GetColumnFromExpression(selector.Body);

            StringBuilder sql = new StringBuilder($"SELECT MAX({column}) FROM {_Sanitizer.SanitizeIdentifier(_TableName)}");
            List<(string name, object? value)> parameters = new List<(string name, object?)>();

            if (predicate != null)
            {
                string whereClause = parser.ParseExpressionWithParameters(predicate.Body);
                sql.Append($" WHERE {whereClause}");
                parameters.AddRange(parser.GetParameters());
            }

            if (transaction != null)
            {
                object? result = ExecuteScalarWithConnection<object>(transaction.Connection, sql.ToString(), transaction.Transaction, parameters.ToArray());
                return SafeConvertDatabaseResult<TResult>(result);
            }
            else
            {
                DbConnection? connection = null;
                try
                {
                    connection = _ConnectionFactory.GetConnection();
                    object? result = ExecuteScalarWithConnection<object>(connection, sql.ToString(), null, parameters.ToArray());
                    return SafeConvertDatabaseResult<TResult>(result);
                }
                finally
                {
                    if (connection != null)
                        _ConnectionFactory.ReturnConnection(connection);
                }
            }
        }

        /// <summary>
        /// Finds the minimum value of the specified property among entities that match the predicate.
        /// </summary>
        /// <typeparam name="TResult">The type of the property to find the minimum value for.</typeparam>
        /// <param name="selector">Expression selecting the property to find the minimum value for.</param>
        /// <param name="predicate">Optional predicate to filter entities. If null, considers all entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The minimum value of the selected property.</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when SQL execution fails.</exception>
        public TResult Min<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            PostgresExpressionParser<T> parser = new PostgresExpressionParser<T>(_ColumnMappings, _Sanitizer);
            string column = parser.GetColumnFromExpression(selector.Body);

            StringBuilder sql = new StringBuilder($"SELECT MIN({column}) FROM {_Sanitizer.SanitizeIdentifier(_TableName)}");
            List<(string name, object? value)> parameters = new List<(string name, object?)>();

            if (predicate != null)
            {
                string whereClause = parser.ParseExpressionWithParameters(predicate.Body);
                sql.Append($" WHERE {whereClause}");
                parameters.AddRange(parser.GetParameters());
            }

            if (transaction != null)
            {
                object? result = ExecuteScalarWithConnection<object>(transaction.Connection, sql.ToString(), transaction.Transaction, parameters.ToArray());
                return SafeConvertDatabaseResult<TResult>(result);
            }
            else
            {
                DbConnection? connection = null;
                try
                {
                    connection = _ConnectionFactory.GetConnection();
                    object? result = ExecuteScalarWithConnection<object>(connection, sql.ToString(), null, parameters.ToArray());
                    return SafeConvertDatabaseResult<TResult>(result);
                }
                finally
                {
                    if (connection != null)
                        _ConnectionFactory.ReturnConnection(connection);
                }
            }
        }

        /// <summary>
        /// Calculates the average value of the specified decimal property among entities that match the predicate.
        /// </summary>
        /// <param name="selector">Expression selecting the decimal property to calculate the average for.</param>
        /// <param name="predicate">Optional predicate to filter entities. If null, considers all entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The average value of the selected property.</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when SQL execution fails.</exception>
        public decimal Average(Expression<Func<T, decimal>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            PostgresExpressionParser<T> parser = new PostgresExpressionParser<T>(_ColumnMappings, _Sanitizer);
            string column = parser.GetColumnFromExpression(selector.Body);

            StringBuilder sql = new StringBuilder($"SELECT COALESCE(AVG({column}), 0) FROM {_Sanitizer.SanitizeIdentifier(_TableName)}");
            List<(string name, object? value)> parameters = new List<(string name, object?)>();

            if (predicate != null)
            {
                string whereClause = parser.ParseExpressionWithParameters(predicate.Body);
                sql.Append($" WHERE {whereClause}");
                parameters.AddRange(parser.GetParameters());
            }

            if (transaction != null)
            {
                object? result = ExecuteScalarWithConnection<object>(transaction.Connection, sql.ToString(), transaction.Transaction, parameters.ToArray());
                return result == DBNull.Value || result == null ? 0m : Convert.ToDecimal(result);
            }
            else
            {
                DbConnection? connection = null;
                try
                {
                    connection = _ConnectionFactory.GetConnection();
                    object? result = ExecuteScalarWithConnection<object>(connection, sql.ToString(), null, parameters.ToArray());
                    return result == DBNull.Value || result == null ? 0m : Convert.ToDecimal(result);
                }
                finally
                {
                    if (connection != null)
                        _ConnectionFactory.ReturnConnection(connection);
                }
            }
        }

        /// <summary>
        /// Calculates the sum of the specified decimal property among entities that match the predicate.
        /// </summary>
        /// <param name="selector">Expression selecting the decimal property to calculate the sum for.</param>
        /// <param name="predicate">Optional predicate to filter entities. If null, considers all entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The sum of the selected property.</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when SQL execution fails.</exception>
        public decimal Sum(Expression<Func<T, decimal>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            PostgresExpressionParser<T> parser = new PostgresExpressionParser<T>(_ColumnMappings, _Sanitizer);
            string column = parser.GetColumnFromExpression(selector.Body);

            StringBuilder sql = new StringBuilder($"SELECT COALESCE(SUM({column}), 0) FROM {_Sanitizer.SanitizeIdentifier(_TableName)}");
            List<(string name, object? value)> parameters = new List<(string name, object?)>();

            if (predicate != null)
            {
                string whereClause = parser.ParseExpressionWithParameters(predicate.Body);
                sql.Append($" WHERE {whereClause}");
                parameters.AddRange(parser.GetParameters());
            }

            if (transaction != null)
            {
                object? result = ExecuteScalarWithConnection<object>(transaction.Connection, sql.ToString(), transaction.Transaction, parameters.ToArray());
                return result == DBNull.Value || result == null ? 0m : Convert.ToDecimal(result);
            }
            else
            {
                DbConnection? connection = null;
                try
                {
                    connection = _ConnectionFactory.GetConnection();
                    object? result = ExecuteScalarWithConnection<object>(connection, sql.ToString(), null, parameters.ToArray());
                    return result == DBNull.Value || result == null ? 0m : Convert.ToDecimal(result);
                }
                finally
                {
                    if (connection != null)
                        _ConnectionFactory.ReturnConnection(connection);
                }
            }
        }

        // Async aggregation operations
        /// <summary>
        /// Asynchronously finds the maximum value of the specified property among entities that match the predicate.
        /// </summary>
        /// <typeparam name="TResult">The type of the property to find the maximum value for.</typeparam>
        /// <param name="selector">Expression selecting the property to find the maximum value for.</param>
        /// <param name="predicate">Optional predicate to filter entities. If null, considers all entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token for the async operation.</param>
        /// <returns>The maximum value of the selected property.</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
        /// <exception cref="InvalidOperationException">Thrown when SQL execution fails.</exception>
        public async Task<TResult> MaxAsync<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            token.ThrowIfCancellationRequested();

            PostgresExpressionParser<T> parser = new PostgresExpressionParser<T>(_ColumnMappings, _Sanitizer);
            string column = parser.GetColumnFromExpression(selector.Body);

            StringBuilder sql = new StringBuilder($"SELECT MAX({column}) FROM {_Sanitizer.SanitizeIdentifier(_TableName)}");
            List<(string name, object? value)> parameters = new List<(string name, object?)>();

            if (predicate != null)
            {
                string whereClause = parser.ParseExpressionWithParameters(predicate.Body);
                sql.Append($" WHERE {whereClause}");
                parameters.AddRange(parser.GetParameters());
            }

            if (transaction != null)
            {
                object? result = await ExecuteScalarWithConnectionAsync<object>(transaction.Connection, sql.ToString(), transaction.Transaction, token, parameters.ToArray()).ConfigureAwait(false);
                return SafeConvertDatabaseResult<TResult>(result);
            }
            else
            {
                DbConnection? connection = null;
                try
                {
                    connection = await _ConnectionFactory.GetConnectionAsync().ConfigureAwait(false);
                    object? result = await ExecuteScalarWithConnectionAsync<object>(connection, sql.ToString(), null, token, parameters.ToArray()).ConfigureAwait(false);
                    return SafeConvertDatabaseResult<TResult>(result);
                }
                finally
                {
                    if (connection != null)
                        await _ConnectionFactory.ReturnConnectionAsync(connection).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Asynchronously finds the minimum value of the specified property among entities that match the predicate.
        /// </summary>
        /// <typeparam name="TResult">The type of the property to find the minimum value for.</typeparam>
        /// <param name="selector">Expression selecting the property to find the minimum value for.</param>
        /// <param name="predicate">Optional predicate to filter entities. If null, considers all entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token for the async operation.</param>
        /// <returns>The minimum value of the selected property.</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
        /// <exception cref="InvalidOperationException">Thrown when SQL execution fails.</exception>
        public async Task<TResult> MinAsync<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            token.ThrowIfCancellationRequested();

            PostgresExpressionParser<T> parser = new PostgresExpressionParser<T>(_ColumnMappings, _Sanitizer);
            string column = parser.GetColumnFromExpression(selector.Body);

            StringBuilder sql = new StringBuilder($"SELECT MIN({column}) FROM {_Sanitizer.SanitizeIdentifier(_TableName)}");
            List<(string name, object? value)> parameters = new List<(string name, object?)>();

            if (predicate != null)
            {
                string whereClause = parser.ParseExpressionWithParameters(predicate.Body);
                sql.Append($" WHERE {whereClause}");
                parameters.AddRange(parser.GetParameters());
            }

            if (transaction != null)
            {
                object? result = await ExecuteScalarWithConnectionAsync<object>(transaction.Connection, sql.ToString(), transaction.Transaction, token, parameters.ToArray()).ConfigureAwait(false);
                return SafeConvertDatabaseResult<TResult>(result);
            }
            else
            {
                DbConnection? connection = null;
                try
                {
                    connection = await _ConnectionFactory.GetConnectionAsync().ConfigureAwait(false);
                    object? result = await ExecuteScalarWithConnectionAsync<object>(connection, sql.ToString(), null, token, parameters.ToArray()).ConfigureAwait(false);
                    return SafeConvertDatabaseResult<TResult>(result);
                }
                finally
                {
                    if (connection != null)
                        await _ConnectionFactory.ReturnConnectionAsync(connection).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Asynchronously calculates the average value of the specified decimal property among entities that match the predicate.
        /// </summary>
        /// <param name="selector">Expression selecting the decimal property to calculate the average for.</param>
        /// <param name="predicate">Optional predicate to filter entities. If null, considers all entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token for the async operation.</param>
        /// <returns>The average value of the selected property.</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
        /// <exception cref="InvalidOperationException">Thrown when SQL execution fails.</exception>
        public async Task<decimal> AverageAsync(Expression<Func<T, decimal>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            token.ThrowIfCancellationRequested();

            PostgresExpressionParser<T> parser = new PostgresExpressionParser<T>(_ColumnMappings, _Sanitizer);
            string column = parser.GetColumnFromExpression(selector.Body);

            StringBuilder sql = new StringBuilder($"SELECT COALESCE(AVG({column}), 0) FROM {_Sanitizer.SanitizeIdentifier(_TableName)}");
            List<(string name, object? value)> parameters = new List<(string name, object?)>();

            if (predicate != null)
            {
                string whereClause = parser.ParseExpressionWithParameters(predicate.Body);
                sql.Append($" WHERE {whereClause}");
                parameters.AddRange(parser.GetParameters());
            }

            if (transaction != null)
            {
                object? result = await ExecuteScalarWithConnectionAsync<object>(transaction.Connection, sql.ToString(), transaction.Transaction, token, parameters.ToArray()).ConfigureAwait(false);
                return result == DBNull.Value || result == null ? 0m : Convert.ToDecimal(result);
            }
            else
            {
                DbConnection? connection = null;
                try
                {
                    connection = await _ConnectionFactory.GetConnectionAsync().ConfigureAwait(false);
                    object? result = await ExecuteScalarWithConnectionAsync<object>(connection, sql.ToString(), null, token, parameters.ToArray()).ConfigureAwait(false);
                    return result == DBNull.Value || result == null ? 0m : Convert.ToDecimal(result);
                }
                finally
                {
                    if (connection != null)
                        await _ConnectionFactory.ReturnConnectionAsync(connection).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Asynchronously calculates the sum of the specified decimal property among entities that match the predicate.
        /// </summary>
        /// <param name="selector">Expression selecting the decimal property to calculate the sum for.</param>
        /// <param name="predicate">Optional predicate to filter entities. If null, considers all entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token for the async operation.</param>
        /// <returns>The sum of the selected property.</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
        /// <exception cref="InvalidOperationException">Thrown when SQL execution fails.</exception>
        public async Task<decimal> SumAsync(Expression<Func<T, decimal>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            token.ThrowIfCancellationRequested();

            PostgresExpressionParser<T> parser = new PostgresExpressionParser<T>(_ColumnMappings, _Sanitizer);
            string column = parser.GetColumnFromExpression(selector.Body);

            StringBuilder sql = new StringBuilder($"SELECT COALESCE(SUM({column}), 0) FROM {_Sanitizer.SanitizeIdentifier(_TableName)}");
            List<(string name, object? value)> parameters = new List<(string name, object?)>();

            if (predicate != null)
            {
                string whereClause = parser.ParseExpressionWithParameters(predicate.Body);
                sql.Append($" WHERE {whereClause}");
                parameters.AddRange(parser.GetParameters());
            }

            if (transaction != null)
            {
                object? result = await ExecuteScalarWithConnectionAsync<object>(transaction.Connection, sql.ToString(), transaction.Transaction, token, parameters.ToArray()).ConfigureAwait(false);
                return result == DBNull.Value || result == null ? 0m : Convert.ToDecimal(result);
            }
            else
            {
                DbConnection? connection = null;
                try
                {
                    connection = await _ConnectionFactory.GetConnectionAsync().ConfigureAwait(false);
                    object? result = await ExecuteScalarWithConnectionAsync<object>(connection, sql.ToString(), null, token, parameters.ToArray()).ConfigureAwait(false);
                    return result == DBNull.Value || result == null ? 0m : Convert.ToDecimal(result);
                }
                finally
                {
                    if (connection != null)
                        await _ConnectionFactory.ReturnConnectionAsync(connection).ConfigureAwait(false);
                }
            }
        }

        // Create operations
        /// <summary>
        /// Creates a new entity in the database.
        /// </summary>
        /// <param name="entity">The entity to create.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The created entity with any auto-generated values populated.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null.</exception>
        public T Create(T entity, ITransaction? transaction = null)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));

            // Use the async version with synchronous execution
            return CreateAsync(entity, transaction, CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Creates multiple entities in the database.
        /// </summary>
        /// <param name="entities">The collection of entities to create.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The created entities with any auto-generated values populated.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entities is null.</exception>
        public IEnumerable<T> CreateMany(IEnumerable<T> entities, ITransaction? transaction = null)
        {
            if (entities == null)
                throw new ArgumentNullException(nameof(entities));

            IList<T> entitiesList = entities.ToList();
            if (!entitiesList.Any())
                return Enumerable.Empty<T>();

            List<T> results = new List<T>();

            if (_BatchConfig.EnableMultiRowInsert && entitiesList.Count > 1)
            {
                results.AddRange(CreateManyOptimized(entitiesList, transaction));
            }
            else
            {
                // Fall back to individual inserts if batch operations are disabled
                foreach (T entity in entitiesList)
                {
                    T createdEntity = Create(entity, transaction);
                    results.Add(createdEntity);
                }
            }

            return results;
        }

        /// <summary>
        /// Asynchronously creates a new entity in the database.
        /// </summary>
        /// <param name="entity">The entity to create.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token for the async operation.</param>
        /// <returns>The created entity with any auto-generated values populated.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
        public async Task<T> CreateAsync(T entity, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));

            token.ThrowIfCancellationRequested();

            // Apply default values from providers before insert
            foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
            {
                PropertyInfo property = kvp.Value;

                // Check if this property has a default value provider
                if (_DefaultValueProviders.TryGetValue(property, out (DefaultValueAttribute, IDefaultValueProvider) providerInfo))
                {
                    DefaultValueAttribute attr = providerInfo.Item1;
                    IDefaultValueProvider provider = providerInfo.Item2;
                    object? value = property.GetValue(entity);

                    // Apply default value if provider says we should
                    if (provider.ShouldApply(value, property.PropertyType))
                    {
                        value = provider.GetDefaultValue(property, entity);
                        property.SetValue(entity, value);
                    }
                }
            }

            List<string> columns = new List<string>();
            List<(string name, object? value)> parameters = new List<(string, object?)>();

            foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
            {
                string columnName = kvp.Key;
                PropertyInfo property = kvp.Value;

                PropertyAttribute? attr = property.GetCustomAttribute<PropertyAttribute>();
                if (attr?.PropertyFlags.HasFlag(Flags.AutoIncrement) == true)
                    continue;

                object? value = property.GetValue(entity);
                columns.Add(_Sanitizer.SanitizeIdentifier(columnName));
                parameters.Add(($"@{columnName}", _DataTypeConverter.ConvertToDatabase(value!, property.PropertyType, property)));
            }

            string insertSql = $"INSERT INTO {_Sanitizer.SanitizeIdentifier(_TableName)} ({string.Join(", ", columns)}) VALUES ({string.Join(", ", parameters.Select(p => p.name))})";

            PropertyAttribute? pkAttr = _PrimaryKeyProperty.GetCustomAttribute<PropertyAttribute>();
            bool hasAutoIncrement = pkAttr?.PropertyFlags.HasFlag(Flags.AutoIncrement) == true;

            if (hasAutoIncrement)
            {
                insertSql += $" RETURNING {_Sanitizer.SanitizeIdentifier(_PrimaryKeyColumn)}";

                object? insertedId;
                if (transaction != null)
                {
                    insertedId = await ExecuteScalarWithConnectionAsync<object>(transaction.Connection, insertSql, transaction.Transaction, token, parameters.ToArray()).ConfigureAwait(false);
                }
                else
                {
                    DbConnection? connection = null;
                    try
                    {
                        connection = await _ConnectionFactory.GetConnectionAsync().ConfigureAwait(false);
                        insertedId = await ExecuteScalarWithConnectionAsync<object>(connection, insertSql, null, token, parameters.ToArray()).ConfigureAwait(false);
                    }
                    finally
                    {
                        if (connection != null)
                            await _ConnectionFactory.ReturnConnectionAsync(connection).ConfigureAwait(false);
                    }
                }

                if (insertedId != null && insertedId != DBNull.Value)
                {
                    object? convertedId = _DataTypeConverter.ConvertFromDatabase(insertedId, _PrimaryKeyProperty.PropertyType, _PrimaryKeyProperty);
                    _PrimaryKeyProperty.SetValue(entity, convertedId);
                }
            }
            else
            {
                if (transaction != null)
                {
                    await ExecuteNonQueryWithConnectionAsync(transaction.Connection, insertSql, transaction.Transaction, token, parameters.ToArray()).ConfigureAwait(false);
                }
                else
                {
                    DbConnection? connection = null;
                    try
                    {
                        connection = await _ConnectionFactory.GetConnectionAsync().ConfigureAwait(false);
                        await ExecuteNonQueryWithConnectionAsync(connection, insertSql, null, token, parameters.ToArray()).ConfigureAwait(false);
                    }
                    finally
                    {
                        if (connection != null)
                            await _ConnectionFactory.ReturnConnectionAsync(connection).ConfigureAwait(false);
                    }
                }
            }

            return entity;
        }

        /// <summary>
        /// Asynchronously creates multiple entities in the database.
        /// </summary>
        /// <param name="entities">The collection of entities to create.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token for the async operation.</param>
        /// <returns>The created entities with any auto-generated values populated.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entities is null.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
        public async Task<IEnumerable<T>> CreateManyAsync(IEnumerable<T> entities, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (entities == null)
                throw new ArgumentNullException(nameof(entities));

            token.ThrowIfCancellationRequested();

            IList<T> entitiesList = entities.ToList();
            if (!entitiesList.Any())
                return Enumerable.Empty<T>();

            List<T> results = new List<T>();

            if (_BatchConfig.EnableMultiRowInsert && entitiesList.Count > 1)
            {
                results.AddRange(await CreateManyOptimizedAsync(entitiesList, transaction, token).ConfigureAwait(false));
            }
            else
            {
                // Fall back to individual inserts if batch operations are disabled
                foreach (T entity in entitiesList)
                {
                    token.ThrowIfCancellationRequested();
                    T createdEntity = await CreateAsync(entity, transaction, token).ConfigureAwait(false);
                    results.Add(createdEntity);
                }
            }

            return results;
        }

        // Update operations
        /// <summary>
        /// Updates an existing entity in the database.
        /// </summary>
        /// <param name="entity">The entity to update.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The updated entity.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null.</exception>
        public T Update(T entity, ITransaction? transaction = null)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));

            // Use the async version with synchronous execution
            return UpdateAsync(entity, transaction, CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Updates multiple entities that match the specified predicate by applying an update action to each.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities to update.</param>
        /// <param name="updateAction">The action to apply to each entity before updating.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The number of entities that were updated.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate or updateAction is null.</exception>
        public int UpdateMany(Expression<Func<T, bool>> predicate, Action<T> updateAction, ITransaction? transaction = null)
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));
            if (updateAction == null)
                throw new ArgumentNullException(nameof(updateAction));

            // Read entities that match the predicate
            List<T> entities = new List<T>();
            foreach (T entity in ReadMany(predicate, transaction))
            {
                entities.Add(entity);
            }

            if (entities.Count == 0)
                return 0;

            int updatedCount = 0;
            foreach (T entity in entities)
            {
                // Apply the update action to the entity
                updateAction(entity);

                // Update the modified entity
                Update(entity, transaction);
                updatedCount++;
            }

            return updatedCount;
        }

        /// <summary>
        /// Updates a specific field for all entities that match the specified predicate.
        /// </summary>
        /// <typeparam name="TField">The type of the field to update.</typeparam>
        /// <param name="predicate">The predicate to filter entities to update.</param>
        /// <param name="field">Expression selecting the field to update.</param>
        /// <param name="value">The new value to set for the field.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The number of entities that were updated.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate or field is null.</exception>
        public int UpdateField<TField>(Expression<Func<T, bool>> predicate, Expression<Func<T, TField>> field, TField value, ITransaction? transaction = null)
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));
            if (field == null)
                throw new ArgumentNullException(nameof(field));

            PostgresExpressionParser<T> parser = new PostgresExpressionParser<T>(_ColumnMappings, _Sanitizer);
            string whereClause = parser.ParseExpressionWithParameters(predicate.Body, true);
            List<(string name, object? value)> parameters = parser.GetParameters().ToList();

            string columnName = parser.GetColumnFromExpression(field.Body);
            PropertyInfo? fieldProperty = GetPropertyFromExpression(field.Body);
            object? convertedValue = _DataTypeConverter.ConvertToDatabase(value!, typeof(TField), fieldProperty);

            parameters.Add(("@value", convertedValue));

            string sql = $"UPDATE {_Sanitizer.SanitizeIdentifier(_TableName)} SET {columnName} = @value WHERE {whereClause}";

            int rowsAffected;
            if (transaction != null)
            {
                rowsAffected = ExecuteNonQueryWithConnection(transaction.Connection, sql, transaction.Transaction, parameters.ToArray());
            }
            else
            {
                DbConnection? connection = null;
                try
                {
                    connection = _ConnectionFactory.GetConnection();
                    rowsAffected = ExecuteNonQueryWithConnection(connection, sql, null, parameters.ToArray());
                }
                finally
                {
                    if (connection != null)
                        _ConnectionFactory.ReturnConnection(connection);
                }
            }

            return rowsAffected;
        }

        /// <summary>
        /// Asynchronously updates an existing entity in the database.
        /// </summary>
        /// <param name="entity">The entity to update.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token for the async operation.</param>
        /// <returns>The updated entity.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when entity has null primary key or no rows were affected.</exception>
        /// <exception cref="OptimisticConcurrencyException">Thrown when version-based concurrency conflict occurs.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
        public async Task<T> UpdateAsync(T entity, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));

            token.ThrowIfCancellationRequested();

            List<string> setPairs = new List<string>();
            List<(string name, object? value)> parameters = new List<(string, object?)>();
            object? idValue = null;
            object? currentVersion = null;

            foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
            {
                string columnName = kvp.Key;
                PropertyInfo property = kvp.Value;
                object? value = property.GetValue(entity);

                if (columnName == _PrimaryKeyColumn)
                {
                    idValue = value;
                }
                else if (_VersionColumnInfo != null && columnName == _VersionColumnInfo.ColumnName)
                {
                    currentVersion = value;
                    object? newVersion = _VersionColumnInfo.IncrementVersion(currentVersion!);
                    setPairs.Add($"{_Sanitizer.SanitizeIdentifier(columnName)} = @new_version");
                    object? convertedNewVersion = _DataTypeConverter.ConvertToDatabase(newVersion!, _VersionColumnInfo.PropertyType, property);
                    parameters.Add(("@new_version", convertedNewVersion));
                    _VersionColumnInfo.SetValue(entity, newVersion);
                }
                else
                {
                    setPairs.Add($"{_Sanitizer.SanitizeIdentifier(columnName)} = @{columnName}");
                    object? convertedValue = _DataTypeConverter.ConvertToDatabase(value!, property.PropertyType, property);
                    parameters.Add(($"@{columnName}", convertedValue));
                }
            }

            if (idValue == null)
                throw new InvalidOperationException("Cannot update entity with null primary key");

            parameters.Add(("@id", idValue));

            string sql;
            if (_VersionColumnInfo != null)
            {
                sql = $"UPDATE {_Sanitizer.SanitizeIdentifier(_TableName)} SET {string.Join(", ", setPairs)} WHERE {_Sanitizer.SanitizeIdentifier(_PrimaryKeyColumn)} = @id AND {_Sanitizer.SanitizeIdentifier(_VersionColumnInfo.ColumnName)} = @current_version";
                parameters.Add(("@current_version", currentVersion));
            }
            else
            {
                sql = $"UPDATE {_Sanitizer.SanitizeIdentifier(_TableName)} SET {string.Join(", ", setPairs)} WHERE {_Sanitizer.SanitizeIdentifier(_PrimaryKeyColumn)} = @id";
            }

            int rowsAffected;
            if (transaction != null)
            {
                rowsAffected = await ExecuteNonQueryWithConnectionAsync(transaction.Connection, sql, transaction.Transaction, token, parameters.ToArray()).ConfigureAwait(false);
            }
            else
            {
                DbConnection? connection = null;
                try
                {
                    connection = await _ConnectionFactory.GetConnectionAsync().ConfigureAwait(false);
                    rowsAffected = await ExecuteNonQueryWithConnectionAsync(connection, sql, null, token, parameters.ToArray()).ConfigureAwait(false);
                }
                finally
                {
                    if (connection != null)
                        await _ConnectionFactory.ReturnConnectionAsync(connection).ConfigureAwait(false);
                }
            }

            if (rowsAffected == 0)
            {
                if (_VersionColumnInfo != null)
                {
                    throw new OptimisticConcurrencyException($"Optimistic concurrency conflict detected for entity with ID {idValue}. The entity was modified or deleted by another process.");
                }
                throw new InvalidOperationException($"No rows were affected during update for entity with ID {idValue}");
            }

            return entity;
        }

        /// <summary>
        /// Asynchronously updates multiple entities that match the specified predicate by applying an async update action to each.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities to update.</param>
        /// <param name="updateAction">The async action to apply to each entity before updating.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token for the async operation.</param>
        /// <returns>The number of entities that were updated.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate or updateAction is null.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
        public async Task<int> UpdateManyAsync(Expression<Func<T, bool>> predicate, Func<T, Task> updateAction, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));
            if (updateAction == null)
                throw new ArgumentNullException(nameof(updateAction));

            token.ThrowIfCancellationRequested();

            // Read entities that match the predicate
            List<T> entities = new List<T>();
            await foreach (T entity in ReadManyAsync(predicate, transaction, token).ConfigureAwait(false))
            {
                token.ThrowIfCancellationRequested();
                entities.Add(entity);
            }

            if (entities.Count == 0)
                return 0;

            int updatedCount = 0;
            foreach (T entity in entities)
            {
                token.ThrowIfCancellationRequested();

                // Apply the update action to the entity
                await updateAction(entity).ConfigureAwait(false);

                // Update the modified entity
                await UpdateAsync(entity, transaction, token).ConfigureAwait(false);
                updatedCount++;
            }

            return updatedCount;
        }

        /// <summary>
        /// Asynchronously updates a specific field for all entities that match the specified predicate.
        /// </summary>
        /// <typeparam name="TField">The type of the field to update.</typeparam>
        /// <param name="predicate">The predicate to filter entities to update.</param>
        /// <param name="field">Expression selecting the field to update.</param>
        /// <param name="value">The new value to set for the field.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token for the async operation.</param>
        /// <returns>The number of entities that were updated.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate or field is null.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
        public async Task<int> UpdateFieldAsync<TField>(Expression<Func<T, bool>> predicate, Expression<Func<T, TField>> field, TField value, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));
            if (field == null)
                throw new ArgumentNullException(nameof(field));

            token.ThrowIfCancellationRequested();

            PostgresExpressionParser<T> parser = new PostgresExpressionParser<T>(_ColumnMappings, _Sanitizer);
            string whereClause = parser.ParseExpressionWithParameters(predicate.Body, true);
            List<(string name, object? value)> parameters = parser.GetParameters().ToList();

            string columnName = parser.GetColumnFromExpression(field.Body);
            PropertyInfo? fieldProperty = GetPropertyFromExpression(field.Body);
            object? convertedValue = _DataTypeConverter.ConvertToDatabase(value!, typeof(TField), fieldProperty);

            parameters.Add(("@value", convertedValue));

            string sql = $"UPDATE {_Sanitizer.SanitizeIdentifier(_TableName)} SET {columnName} = @value WHERE {whereClause}";

            int rowsAffected;
            if (transaction != null)
            {
                rowsAffected = await ExecuteNonQueryWithConnectionAsync(transaction.Connection, sql, transaction.Transaction, token, parameters.ToArray()).ConfigureAwait(false);
            }
            else
            {
                DbConnection? connection = null;
                try
                {
                    connection = await _ConnectionFactory.GetConnectionAsync().ConfigureAwait(false);
                    rowsAffected = await ExecuteNonQueryWithConnectionAsync(connection, sql, null, token, parameters.ToArray()).ConfigureAwait(false);
                }
                finally
                {
                    if (connection != null)
                        await _ConnectionFactory.ReturnConnectionAsync(connection).ConfigureAwait(false);
                }
            }

            return rowsAffected;
        }

        // Batch operations
        /// <summary>
        /// Performs a batch update operation using an expression to define the update logic.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities to update.</param>
        /// <param name="updateExpression">Expression defining how to update the entity (e.g., x => new Entity { Name = "NewName", Status = x.Status }).</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The number of entities that were updated.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate or updateExpression is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when updateExpression format is not supported.</exception>
        public int BatchUpdate(Expression<Func<T, bool>> predicate, Expression<Func<T, T>> updateExpression, ITransaction? transaction = null)
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));
            if (updateExpression == null)
                throw new ArgumentNullException(nameof(updateExpression));

            PostgresExpressionParser<T> expressionParser = new PostgresExpressionParser<T>(_ColumnMappings, _Sanitizer);

            // Initialize parameterized mode (but don't parse anything yet, just set the flag)
            expressionParser.ParseExpressionWithParameters(Expression.Constant(true));  // Dummy expression to set the flag

            // Parse the SET clause from the update expression (will use parameterized mode)
            string setClause = expressionParser.ParseUpdateExpression(updateExpression);

            // Parse the WHERE clause (will add to existing parameters)
            string whereClause = expressionParser.ParseExpression(predicate.Body);

            // Get all parameters (from both SET and WHERE clauses)
            List<(string name, object? value)> parameters = expressionParser.GetParameters();

            // Build UPDATE SQL
            string sql = $"UPDATE {_Sanitizer.SanitizeIdentifier(_TableName)} SET {setClause} WHERE {whereClause}";

            int rowsAffected;
            if (transaction != null)
            {
                rowsAffected = ExecuteNonQueryWithConnection((DbConnection)transaction.Connection, sql, (DbTransaction?)transaction.Transaction, parameters.ToArray());
            }
            else
            {
                DbConnection? connection = null;
                try
                {
                    connection = (DbConnection)_ConnectionFactory.GetConnection();
                    rowsAffected = ExecuteNonQueryWithConnection(connection, sql, null, parameters.ToArray());
                }
                finally
                {
                    if (connection != null)
                        _ConnectionFactory.ReturnConnection(connection);
                }
            }

            return rowsAffected;
        }

        /// <summary>
        /// Performs a batch delete operation for entities matching the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities to delete.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The number of entities that were deleted.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null.</exception>
        public int BatchDelete(Expression<Func<T, bool>> predicate, ITransaction? transaction = null)
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));

            PostgresExpressionParser<T> expressionParser = new PostgresExpressionParser<T>(_ColumnMappings, _Sanitizer);
            string whereClause = expressionParser.ParseExpressionWithParameters(predicate.Body);
            string sql = $"DELETE FROM {_Sanitizer.SanitizeIdentifier(_TableName)} WHERE {whereClause}";
            object[] parameters = expressionParser.GetParameters().Cast<object>().ToArray();

            int rowsAffected;
            if (transaction != null)
            {
                rowsAffected = ExecuteNonQueryWithConnection((DbConnection)transaction.Connection, sql, (DbTransaction?)transaction.Transaction, parameters.Cast<(string, object?)>().ToArray());
            }
            else
            {
                DbConnection? connection = null;
                try
                {
                    connection = (DbConnection)_ConnectionFactory.GetConnection();
                    rowsAffected = ExecuteNonQueryWithConnection(connection, sql, null, parameters.Cast<(string, object?)>().ToArray());
                }
                finally
                {
                    if (connection != null)
                        _ConnectionFactory.ReturnConnection(connection);
                }
            }

            return rowsAffected;
        }

        /// <summary>
        /// Asynchronously performs a batch update operation using an expression to define the update logic.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities to update.</param>
        /// <param name="updateExpression">Expression defining how to update the entity (e.g., x => new Entity { Name = "NewName", Status = x.Status }).</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token for the async operation.</param>
        /// <returns>The number of entities that were updated.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate or updateExpression is null.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
        /// <exception cref="InvalidOperationException">Thrown when updateExpression format is not supported.</exception>
        public async Task<int> BatchUpdateAsync(Expression<Func<T, bool>> predicate, Expression<Func<T, T>> updateExpression, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));
            if (updateExpression == null)
                throw new ArgumentNullException(nameof(updateExpression));

            token.ThrowIfCancellationRequested();

            PostgresExpressionParser<T> expressionParser = new PostgresExpressionParser<T>(_ColumnMappings, _Sanitizer);

            // Initialize parameterized mode (but don't parse anything yet, just set the flag)
            expressionParser.ParseExpressionWithParameters(Expression.Constant(true));  // Dummy expression to set the flag

            // Parse the SET clause from the update expression (will use parameterized mode)
            string setClause = expressionParser.ParseUpdateExpression(updateExpression);

            // Parse the WHERE clause (will add to existing parameters)
            string whereClause = expressionParser.ParseExpression(predicate.Body);

            // Get all parameters (from both SET and WHERE clauses)
            List<(string name, object? value)> parameters = expressionParser.GetParameters();

            // Build UPDATE SQL
            string sql = $"UPDATE {_Sanitizer.SanitizeIdentifier(_TableName)} SET {setClause} WHERE {whereClause}";

            int rowsAffected;
            if (transaction != null)
            {
                rowsAffected = await ExecuteNonQueryWithConnectionAsync(transaction.Connection, sql, transaction.Transaction, token, parameters.ToArray()).ConfigureAwait(false);
            }
            else
            {
                DbConnection? connection = null;
                try
                {
                    connection = await _ConnectionFactory.GetConnectionAsync(token).ConfigureAwait(false);
                    rowsAffected = await ExecuteNonQueryWithConnectionAsync(connection, sql, null, token, parameters.ToArray()).ConfigureAwait(false);
                }
                finally
                {
                    if (connection != null)
                        await _ConnectionFactory.ReturnConnectionAsync(connection).ConfigureAwait(false);
                }
            }

            return rowsAffected;
        }

        /// <summary>
        /// Asynchronously performs a batch delete operation for entities matching the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities to delete.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token for the async operation.</param>
        /// <returns>The number of entities that were deleted.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
        public async Task<int> BatchDeleteAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));

            token.ThrowIfCancellationRequested();

            PostgresExpressionParser<T> expressionParser = new PostgresExpressionParser<T>(_ColumnMappings, _Sanitizer);
            string whereClause = expressionParser.ParseExpressionWithParameters(predicate.Body);
            string sql = $"DELETE FROM {_Sanitizer.SanitizeIdentifier(_TableName)} WHERE {whereClause}";
            object[] parameters = expressionParser.GetParameters().Cast<object>().ToArray();

            int rowsAffected;
            if (transaction != null)
            {
                rowsAffected = await ExecuteNonQueryWithConnectionAsync(transaction.Connection, sql, transaction.Transaction, token, parameters.Cast<(string, object?)>().ToArray()).ConfigureAwait(false);
            }
            else
            {
                DbConnection? connection = null;
                try
                {
                    connection = await _ConnectionFactory.GetConnectionAsync().ConfigureAwait(false);
                    rowsAffected = await ExecuteNonQueryWithConnectionAsync(connection, sql, null, token, parameters.Cast<(string, object?)>().ToArray()).ConfigureAwait(false);
                }
                finally
                {
                    if (connection != null)
                        await _ConnectionFactory.ReturnConnectionAsync(connection).ConfigureAwait(false);
                }
            }

            return rowsAffected;
        }

        // Delete operations
        /// <summary>
        /// Deletes an entity from the database.
        /// </summary>
        /// <param name="entity">The entity to delete.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>True if the entity was deleted, false otherwise.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the entity has a null primary key.</exception>
        public bool Delete(T entity, ITransaction? transaction = null)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));

            // Use the async version with synchronous execution
            return DeleteAsync(entity, transaction, CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Deletes an entity by its identifier.
        /// </summary>
        /// <param name="id">The identifier of the entity to delete.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>True if the entity was deleted, false otherwise.</returns>
        /// <exception cref="ArgumentNullException">Thrown when id is null.</exception>
        public bool DeleteById(object id, ITransaction? transaction = null)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            string sql = $"DELETE FROM {_Sanitizer.SanitizeIdentifier(_TableName)} WHERE {_Sanitizer.SanitizeIdentifier(_PrimaryKeyColumn)} = @id";

            int rowsAffected;
            if (transaction != null)
            {
                rowsAffected = ExecuteNonQueryWithConnection((DbConnection)transaction.Connection, sql, (DbTransaction?)transaction.Transaction, ("@id", id));
            }
            else
            {
                DbConnection? connection = null;
                try
                {
                    connection = (DbConnection)_ConnectionFactory.GetConnection();
                    rowsAffected = ExecuteNonQueryWithConnection(connection, sql, null, ("@id", id));
                }
                finally
                {
                    if (connection != null)
                        _ConnectionFactory.ReturnConnection(connection);
                }
            }

            return rowsAffected > 0;
        }

        /// <summary>
        /// Deletes multiple entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities to delete.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The number of entities deleted.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null.</exception>
        public int DeleteMany(Expression<Func<T, bool>> predicate, ITransaction? transaction = null)
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));

            // For now, use a simple implementation that gets matching records and deletes them
            // In a full implementation, this would generate a DELETE WHERE SQL statement
            IEnumerable<T> entitiesToDelete = Query(transaction).Where(predicate).Execute();
            int count = 0;

            foreach (T entity in entitiesToDelete)
            {
                if (Delete(entity, transaction))
                {
                    count++;
                }
            }

            return count;
        }

        /// <summary>
        /// Deletes all entities from the database.
        /// </summary>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The number of entities deleted.</returns>
        public int DeleteAll(ITransaction? transaction = null)
        {
            string sql = $"DELETE FROM {_Sanitizer.SanitizeIdentifier(_TableName)}";

            int rowsAffected;
            if (transaction != null)
            {
                rowsAffected = ExecuteNonQueryWithConnection((DbConnection)transaction.Connection, sql, (DbTransaction?)transaction.Transaction);
            }
            else
            {
                DbConnection? connection = null;
                try
                {
                    connection = (DbConnection)_ConnectionFactory.GetConnection();
                    rowsAffected = ExecuteNonQueryWithConnection(connection, sql, null);
                }
                finally
                {
                    if (connection != null)
                        _ConnectionFactory.ReturnConnection(connection);
                }
            }

            return rowsAffected;
        }

        /// <summary>
        /// Asynchronously deletes an entity from the database.
        /// </summary>
        /// <param name="entity">The entity to delete.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token for the async operation.</param>
        /// <returns>True if the entity was deleted, false if no rows were affected.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when entity has null primary key.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
        public async Task<bool> DeleteAsync(T entity, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));

            token.ThrowIfCancellationRequested();

            object? id = _PrimaryKeyProperty.GetValue(entity);
            if (id == null)
                throw new InvalidOperationException("Cannot delete entity with null primary key");

            return await DeleteByIdAsync(id, transaction, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Asynchronously deletes an entity by its identifier.
        /// </summary>
        /// <param name="id">The primary key value of the entity to delete.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token for the async operation.</param>
        /// <returns>True if the entity was deleted, false if no rows were affected.</returns>
        /// <exception cref="ArgumentNullException">Thrown when id is null.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
        public async Task<bool> DeleteByIdAsync(object id, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            token.ThrowIfCancellationRequested();

            string sql = $"DELETE FROM {_Sanitizer.SanitizeIdentifier(_TableName)} WHERE {_Sanitizer.SanitizeIdentifier(_PrimaryKeyColumn)} = @id";

            int rowsAffected;
            if (transaction != null)
            {
                rowsAffected = await ExecuteNonQueryWithConnectionAsync(transaction.Connection, sql, transaction.Transaction, token, ("@id", id)).ConfigureAwait(false);
            }
            else
            {
                DbConnection? connection = null;
                try
                {
                    connection = await _ConnectionFactory.GetConnectionAsync().ConfigureAwait(false);
                    rowsAffected = await ExecuteNonQueryWithConnectionAsync(connection, sql, null, token, ("@id", id)).ConfigureAwait(false);
                }
                finally
                {
                    if (connection != null)
                        await _ConnectionFactory.ReturnConnectionAsync(connection).ConfigureAwait(false);
                }
            }

            return rowsAffected > 0;
        }

        /// <summary>
        /// Asynchronously deletes multiple entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities to delete.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token for the async operation.</param>
        /// <returns>The number of entities that were deleted.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
        public async Task<int> DeleteManyAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));

            token.ThrowIfCancellationRequested();

            // Get entities that match the predicate and delete them individually
            // This approach ensures proper entity lifecycle and validation
            List<T> entitiesToDelete = new List<T>();
            await foreach (T entity in ReadManyAsync(predicate, transaction, token).ConfigureAwait(false))
            {
                token.ThrowIfCancellationRequested();
                entitiesToDelete.Add(entity);
            }

            int count = 0;
            foreach (T entity in entitiesToDelete)
            {
                token.ThrowIfCancellationRequested();
                if (await DeleteAsync(entity, transaction, token).ConfigureAwait(false))
                {
                    count++;
                }
            }

            return count;
        }

        /// <summary>
        /// Asynchronously deletes all entities from the database.
        /// </summary>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token for the async operation.</param>
        /// <returns>The number of entities that were deleted.</returns>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
        public async Task<int> DeleteAllAsync(ITransaction? transaction = null, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            string sql = $"DELETE FROM {_Sanitizer.SanitizeIdentifier(_TableName)}";

            int rowsAffected;
            if (transaction != null)
            {
                rowsAffected = await ExecuteNonQueryWithConnectionAsync(transaction.Connection, sql, transaction.Transaction, token).ConfigureAwait(false);
            }
            else
            {
                DbConnection? connection = null;
                try
                {
                    connection = await _ConnectionFactory.GetConnectionAsync().ConfigureAwait(false);
                    rowsAffected = await ExecuteNonQueryWithConnectionAsync(connection, sql, null, token).ConfigureAwait(false);
                }
                finally
                {
                    if (connection != null)
                        await _ConnectionFactory.ReturnConnectionAsync(connection).ConfigureAwait(false);
                }
            }

            return rowsAffected;
        }

        // Upsert operations
        /// <summary>
        /// Inserts a new entity or updates an existing entity if it already exists in the repository.
        /// Uses PostgreSQL's INSERT ... ON CONFLICT DO UPDATE syntax for atomic upsert operations.
        /// </summary>
        /// <param name="entity">The entity to insert or update. Cannot be null.</param>
        /// <param name="transaction">Optional transaction context for the operation. If null, the operation runs in its own transaction.</param>
        /// <returns>The entity after the upsert operation, with any generated values populated</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when the upsert operation fails</exception>
        public T Upsert(T entity, ITransaction? transaction = null)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));

            List<string> columns = new List<string>();
            List<string> values = new List<string>();
            List<string> updatePairs = new List<string>();
            List<(string name, object? value)> parameters = new List<(string, object?)>();

            foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
            {
                string columnName = kvp.Key;
                PropertyInfo property = kvp.Value;
                object? value = property.GetValue(entity);

                PropertyAttribute? attr = property.GetCustomAttribute<PropertyAttribute>();

                // Skip primary key in INSERT if it's auto-increment and has default value (0 for int)
                if (columnName == _PrimaryKeyColumn && attr?.PropertyFlags.HasFlag(Flags.AutoIncrement) == true)
                {
                    if (value == null || (value is int intValue && intValue == 0) || (value is long longValue && longValue == 0))
                    {
                        continue; // Skip including this column in the INSERT
                    }
                }

                columns.Add(_Sanitizer.SanitizeIdentifier(columnName));
                values.Add($"@{columnName}");
                parameters.Add(($"@{columnName}", _DataTypeConverter.ConvertToDatabase(value!, property.PropertyType, property)));

                // Don't update primary key or auto-increment columns in the UPDATE part
                if (columnName != _PrimaryKeyColumn && !attr?.PropertyFlags.HasFlag(Flags.AutoIncrement) == true)
                {
                    updatePairs.Add($"{_Sanitizer.SanitizeIdentifier(columnName)} = EXCLUDED.{_Sanitizer.SanitizeIdentifier(columnName)}");
                }
            }

            // PostgreSQL UPSERT using ON CONFLICT DO UPDATE with RETURNING clause to get the ID
            string sql = $"INSERT INTO {_Sanitizer.SanitizeIdentifier(_TableName)} ({string.Join(", ", columns)}) VALUES ({string.Join(", ", values)}) ON CONFLICT ({_Sanitizer.SanitizeIdentifier(_PrimaryKeyColumn)}) DO UPDATE SET {string.Join(", ", updatePairs)} RETURNING {_Sanitizer.SanitizeIdentifier(_PrimaryKeyColumn)}";

            object? returnedId;
            if (transaction != null)
            {
                returnedId = ExecuteScalarWithConnection<object>(transaction.Connection, sql, transaction.Transaction, parameters.ToArray());
            }
            else
            {
                DbConnection? connection = null;
                try
                {
                    connection = _ConnectionFactory.GetConnection();
                    returnedId = ExecuteScalarWithConnection<object>(connection, sql, null, parameters.ToArray());
                }
                finally
                {
                    if (connection != null)
                        _ConnectionFactory.ReturnConnection(connection);
                }
            }

            // Set the primary key if it was returned and the entity doesn't have one
            if (_PrimaryKeyProperty != null && returnedId != null && returnedId != DBNull.Value)
            {
                object? convertedId = _DataTypeConverter.ConvertFromDatabase(returnedId, _PrimaryKeyProperty.PropertyType, _PrimaryKeyProperty);
                _PrimaryKeyProperty.SetValue(entity, convertedId);
            }

            return entity;
        }

        /// <summary>
        /// Inserts or updates multiple entities depending on whether they already exist in the repository.
        /// Uses PostgreSQL's INSERT ... ON CONFLICT DO UPDATE syntax within a transaction for consistency.
        /// </summary>
        /// <param name="entities">The collection of entities to insert or update. Cannot be null.</param>
        /// <param name="transaction">Optional transaction context for the operation. If null, a new transaction is created for the operation.</param>
        /// <returns>The entities after the upsert operation, with any generated values populated</returns>
        /// <exception cref="ArgumentNullException">Thrown when entities is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when the upsert operation fails</exception>
        public IEnumerable<T> UpsertMany(IEnumerable<T> entities, ITransaction? transaction = null)
        {
            if (entities == null)
                throw new ArgumentNullException(nameof(entities));

            List<T> entitiesList = entities.ToList();
            if (entitiesList.Count == 0)
                return entitiesList;

            List<T> results = new List<T>();

            if (transaction != null)
            {
                // Use provided transaction
                foreach (T entity in entitiesList)
                {
                    results.Add(Upsert(entity, transaction));
                }
            }
            else
            {
                // Create our own transaction
                using ITransaction localTransaction = BeginTransaction();
                try
                {
                    foreach (T entity in entitiesList)
                    {
                        results.Add(Upsert(entity, localTransaction));
                    }
                    localTransaction.Commit();
                }
                catch
                {
                    localTransaction.Rollback();
                    throw;
                }
            }

            return results;
        }

        /// <summary>
        /// Asynchronously inserts a new entity or updates an existing entity if it already exists in the repository.
        /// Uses PostgreSQL's INSERT ... ON CONFLICT DO UPDATE syntax for atomic upsert operations.
        /// </summary>
        /// <param name="entity">The entity to insert or update. Cannot be null.</param>
        /// <param name="transaction">Optional transaction context for the operation. If null, the operation runs in its own transaction.</param>
        /// <param name="token">Cancellation token to support operation cancellation.</param>
        /// <returns>A task that represents the asynchronous operation containing the entity after the upsert operation, with any generated values populated</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when the upsert operation fails</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled via the cancellation token</exception>
        public async Task<T> UpsertAsync(T entity, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));

            token.ThrowIfCancellationRequested();

            List<string> columns = new List<string>();
            List<string> values = new List<string>();
            List<string> updatePairs = new List<string>();
            List<(string name, object? value)> parameters = new List<(string, object?)>();

            foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
            {
                string columnName = kvp.Key;
                PropertyInfo property = kvp.Value;
                object? value = property.GetValue(entity);

                PropertyAttribute? attr = property.GetCustomAttribute<PropertyAttribute>();

                // Skip primary key in INSERT if it's auto-increment and has default value (0 for int)
                if (columnName == _PrimaryKeyColumn && attr?.PropertyFlags.HasFlag(Flags.AutoIncrement) == true)
                {
                    if (value == null || (value is int intValue && intValue == 0) || (value is long longValue && longValue == 0))
                    {
                        continue; // Skip including this column in the INSERT
                    }
                }

                columns.Add(_Sanitizer.SanitizeIdentifier(columnName));
                values.Add($"@{columnName}");
                parameters.Add(($"@{columnName}", _DataTypeConverter.ConvertToDatabase(value!, property.PropertyType, property)));

                // Don't update primary key or auto-increment columns in the UPDATE part
                if (columnName != _PrimaryKeyColumn && !attr?.PropertyFlags.HasFlag(Flags.AutoIncrement) == true)
                {
                    updatePairs.Add($"{_Sanitizer.SanitizeIdentifier(columnName)} = EXCLUDED.{_Sanitizer.SanitizeIdentifier(columnName)}");
                }
            }

            // PostgreSQL UPSERT using ON CONFLICT DO UPDATE with RETURNING clause to get the ID
            string sql = $"INSERT INTO {_Sanitizer.SanitizeIdentifier(_TableName)} ({string.Join(", ", columns)}) VALUES ({string.Join(", ", values)}) ON CONFLICT ({_Sanitizer.SanitizeIdentifier(_PrimaryKeyColumn)}) DO UPDATE SET {string.Join(", ", updatePairs)} RETURNING {_Sanitizer.SanitizeIdentifier(_PrimaryKeyColumn)}";

            object? returnedId;
            if (transaction != null)
            {
                returnedId = await ExecuteScalarWithConnectionAsync<object>(transaction.Connection, sql, transaction.Transaction, token, parameters.ToArray()).ConfigureAwait(false);
            }
            else
            {
                DbConnection? connection = null;
                try
                {
                    connection = await _ConnectionFactory.GetConnectionAsync().ConfigureAwait(false);
                    returnedId = await ExecuteScalarWithConnectionAsync<object>(connection, sql, null, token, parameters.ToArray()).ConfigureAwait(false);
                }
                finally
                {
                    if (connection != null)
                        await _ConnectionFactory.ReturnConnectionAsync(connection).ConfigureAwait(false);
                }
            }

            // Set the primary key if it was returned and the entity doesn't have one
            if (_PrimaryKeyProperty != null && returnedId != null && returnedId != DBNull.Value)
            {
                object? convertedId = _DataTypeConverter.ConvertFromDatabase(returnedId, _PrimaryKeyProperty.PropertyType, _PrimaryKeyProperty);
                _PrimaryKeyProperty.SetValue(entity, convertedId);
            }

            return entity;
        }

        /// <summary>
        /// Asynchronously inserts or updates multiple entities depending on whether they already exist in the repository.
        /// Uses PostgreSQL's INSERT ... ON CONFLICT DO UPDATE syntax within a transaction for consistency.
        /// </summary>
        /// <param name="entities">The collection of entities to insert or update. Cannot be null.</param>
        /// <param name="transaction">Optional transaction context for the operation. If null, a new transaction is created for the operation.</param>
        /// <param name="token">Cancellation token to support operation cancellation.</param>
        /// <returns>A task that represents the asynchronous operation containing the entities after the upsert operation, with any generated values populated</returns>
        /// <exception cref="ArgumentNullException">Thrown when entities is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when the upsert operation fails</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled via the cancellation token</exception>
        public async Task<IEnumerable<T>> UpsertManyAsync(IEnumerable<T> entities, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (entities == null)
                throw new ArgumentNullException(nameof(entities));

            token.ThrowIfCancellationRequested();

            List<T> entitiesList = entities.ToList();
            if (entitiesList.Count == 0)
                return entitiesList;

            List<T> results = new List<T>();

            if (transaction != null)
            {
                // Use provided transaction
                foreach (T entity in entitiesList)
                {
                    token.ThrowIfCancellationRequested();
                    results.Add(await UpsertAsync(entity, transaction, token).ConfigureAwait(false));
                }
            }
            else
            {
                // Create our own transaction
                using ITransaction localTransaction = await BeginTransactionAsync(token).ConfigureAwait(false);
                try
                {
                    foreach (T entity in entitiesList)
                    {
                        token.ThrowIfCancellationRequested();
                        results.Add(await UpsertAsync(entity, localTransaction, token).ConfigureAwait(false));
                    }
                    await localTransaction.CommitAsync().ConfigureAwait(false);
                }
                catch
                {
                    await localTransaction.RollbackAsync().ConfigureAwait(false);
                    throw;
                }
            }

            return results;
        }

        // Raw SQL operations
        /// <summary>
        /// Executes a raw SQL query and returns the results as entities of type T.
        /// </summary>
        /// <param name="sql">The raw SQL query to execute.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="parameters">Parameters for the SQL query.</param>
        /// <returns>An enumerable collection of entities returned by the query.</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null or empty.</exception>
        /// <exception cref="InvalidOperationException">Thrown when query execution fails.</exception>
        public IEnumerable<T> FromSql(string sql, ITransaction? transaction = null, params object[] parameters)
        {
            if (string.IsNullOrWhiteSpace(sql))
                throw new ArgumentException("SQL cannot be null or empty", nameof(sql));

            if (transaction != null)
            {
                return FromSqlWithConnection(sql, transaction.Connection, transaction.Transaction, parameters);
            }
            else
            {
                using (DbConnection conn = _ConnectionFactory.GetConnection())
                {
                    return FromSqlWithConnection(sql, conn, null, parameters);
                }
            }
        }

        private List<T> FromSqlWithConnection(string sql, DbConnection connection, DbTransaction? sqlTransaction, params object[] parameters)
        {
            List<T> results = new List<T>();

            if (connection.State != System.Data.ConnectionState.Open)
            {
                connection.Open();
            }

            NpgsqlConnection npgsqlConn = (NpgsqlConnection)PooledConnectionHandle.Unwrap(connection);

            using NpgsqlCommand command = new NpgsqlCommand(sql, npgsqlConn, (NpgsqlTransaction?)sqlTransaction);
            AddParametersToCommand(command, parameters);

            using NpgsqlDataReader reader = command.ExecuteReader();
            PostgresEntityMapper<T> mapper = new PostgresEntityMapper<T>(_DataTypeConverter, _ColumnMappings, _Sanitizer);

            while (reader.Read())
            {
                results.Add(mapper.MapEntity(reader));
            }

            return results;
        }

        /// <summary>
        /// Executes a raw SQL query and returns the results as entities of the specified type.
        /// </summary>
        /// <typeparam name="TResult">The type to map the query results to. Must have a parameterless constructor.</typeparam>
        /// <param name="sql">The raw SQL query to execute.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="parameters">Parameters for the SQL query.</param>
        /// <returns>An enumerable collection of entities of the specified type returned by the query.</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null or empty.</exception>
        /// <exception cref="InvalidOperationException">Thrown when query execution fails.</exception>
        public IEnumerable<TResult> FromSql<TResult>(string sql, ITransaction? transaction = null, params object[] parameters) where TResult : new()
        {
            if (string.IsNullOrWhiteSpace(sql))
                throw new ArgumentException("SQL cannot be null or empty", nameof(sql));

            if (transaction != null)
            {
                return FromSqlGenericWithConnection<TResult>(sql, transaction.Connection, transaction.Transaction, parameters);
            }
            else
            {
                using (DbConnection conn = _ConnectionFactory.GetConnection())
                {
                    return FromSqlGenericWithConnection<TResult>(sql, conn, null, parameters);
                }
            }
        }

        private List<TResult> FromSqlGenericWithConnection<TResult>(string sql, DbConnection connection, DbTransaction? sqlTransaction, params object[] parameters) where TResult : new()
        {
            List<TResult> results = new List<TResult>();

            if (connection.State != System.Data.ConnectionState.Open)
            {
                connection.Open();
            }

            NpgsqlConnection npgsqlConn = (NpgsqlConnection)PooledConnectionHandle.Unwrap(connection);

            using NpgsqlCommand command = new NpgsqlCommand(sql, npgsqlConn, (NpgsqlTransaction?)sqlTransaction);
            AddParametersToCommand(command, parameters);

            // Capture SQL if enabled
            if (_CaptureSql)
            {
                _LastExecutedSql = command.CommandText;
                _LastExecutedSqlWithParameters = BuildSqlWithParameters(command);
            }

            using NpgsqlDataReader reader = command.ExecuteReader();

            while (reader.Read())
            {
                results.Add(MapReaderToType<TResult>(reader));
            }

            return results;
        }

        /// <summary>
        /// Executes a raw SQL command and returns the number of rows affected.
        /// </summary>
        /// <param name="sql">The raw SQL command to execute.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="parameters">Parameters for the SQL command.</param>
        /// <returns>The number of rows affected by the command.</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null or empty.</exception>
        /// <exception cref="InvalidOperationException">Thrown when command execution fails.</exception>
        public int ExecuteSql(string sql, ITransaction? transaction = null, params object[] parameters)
        {
            if (string.IsNullOrWhiteSpace(sql))
                throw new ArgumentException("SQL cannot be null or empty", nameof(sql));

            if (transaction != null)
            {
                return ExecuteNonQueryWithConnection((DbConnection)transaction.Connection, sql, (DbTransaction?)transaction.Transaction, parameters.Cast<(string, object?)>().ToArray());
            }
            else
            {
                DbConnection? connection = null;
                try
                {
                    connection = (DbConnection)_ConnectionFactory.GetConnection();
                    return ExecuteNonQueryWithConnection(connection, sql, null, parameters.Cast<(string, object?)>().ToArray());
                }
                finally
                {
                    if (connection != null)
                        _ConnectionFactory.ReturnConnection(connection);
                }
            }
        }

        /// <summary>
        /// Asynchronously executes a raw SQL query and returns the results as an async enumerable of entities of type T.
        /// </summary>
        /// <param name="sql">The raw SQL query to execute.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <param name="parameters">Parameters for the SQL query.</param>
        /// <returns>An async enumerable collection of entities returned by the query.</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null or empty.</exception>
        /// <exception cref="InvalidOperationException">Thrown when query execution fails.</exception>
        public async IAsyncEnumerable<T> FromSqlAsync(string sql, ITransaction? transaction = null, [EnumeratorCancellation] CancellationToken token = default, params object[] parameters)
        {
            if (string.IsNullOrWhiteSpace(sql))
                throw new ArgumentException("SQL cannot be null or empty", nameof(sql));

            token.ThrowIfCancellationRequested();

            DbConnection? dbConnection = null;
            NpgsqlConnection connection;
            NpgsqlTransaction? sqlTransaction = null;
            bool shouldDisposeConnection = false;

            if (transaction != null)
            {
                connection = (NpgsqlConnection)PooledConnectionHandle.Unwrap(transaction.Connection);
                sqlTransaction = (NpgsqlTransaction)transaction.Transaction;
            }
            else
            {
                dbConnection = await _ConnectionFactory.GetConnectionAsync().ConfigureAwait(false);
                connection = (NpgsqlConnection)PooledConnectionHandle.Unwrap(dbConnection);
                shouldDisposeConnection = true;
            }

            try
            {
                await EnsureConnectionOpenAsync(connection, token).ConfigureAwait(false);

                using NpgsqlCommand command = new NpgsqlCommand(sql, connection, sqlTransaction);
                AddParametersToCommand(command, parameters);

                using NpgsqlDataReader reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
                PostgresEntityMapper<T> mapper = new PostgresEntityMapper<T>(_DataTypeConverter, _ColumnMappings, _Sanitizer);

                while (await reader.ReadAsync(token).ConfigureAwait(false))
                {
                    token.ThrowIfCancellationRequested();
                    yield return mapper.MapEntity(reader);
                }
            }
            finally
            {
                if (shouldDisposeConnection && dbConnection != null)
                {
                    await _ConnectionFactory.ReturnConnectionAsync(dbConnection).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Asynchronously executes a raw SQL query and returns the results as an async enumerable of entities of the specified type.
        /// </summary>
        /// <typeparam name="TResult">The type to map the query results to. Must have a parameterless constructor.</typeparam>
        /// <param name="sql">The raw SQL query to execute.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <param name="parameters">Parameters for the SQL query.</param>
        /// <returns>An async enumerable collection of entities of the specified type returned by the query.</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null or empty.</exception>
        /// <exception cref="InvalidOperationException">Thrown when query execution fails.</exception>
        public async IAsyncEnumerable<TResult> FromSqlAsync<TResult>(string sql, ITransaction? transaction = null, [EnumeratorCancellation] CancellationToken token = default, params object[] parameters) where TResult : new()
        {
            if (string.IsNullOrWhiteSpace(sql))
                throw new ArgumentException("SQL cannot be null or empty", nameof(sql));

            token.ThrowIfCancellationRequested();

            DbConnection? dbConnection = null;
            NpgsqlConnection connection;
            NpgsqlTransaction? sqlTransaction = null;
            bool shouldDisposeConnection = false;

            if (transaction != null)
            {
                connection = (NpgsqlConnection)PooledConnectionHandle.Unwrap(transaction.Connection);
                sqlTransaction = (NpgsqlTransaction)transaction.Transaction;
            }
            else
            {
                dbConnection = await _ConnectionFactory.GetConnectionAsync().ConfigureAwait(false);
                connection = (NpgsqlConnection)PooledConnectionHandle.Unwrap(dbConnection);
                shouldDisposeConnection = true;
            }

            try
            {
                await EnsureConnectionOpenAsync(connection, token).ConfigureAwait(false);

                using NpgsqlCommand command = new NpgsqlCommand(sql, connection, sqlTransaction);
                AddParametersToCommand(command, parameters);

                // Capture SQL if enabled
                if (_CaptureSql)
                {
                    _LastExecutedSql = command.CommandText;
                    _LastExecutedSqlWithParameters = BuildSqlWithParameters(command);
                }

                using NpgsqlDataReader reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);

                while (await reader.ReadAsync(token).ConfigureAwait(false))
                {
                    token.ThrowIfCancellationRequested();
                    yield return MapReaderToType<TResult>(reader);
                }
            }
            finally
            {
                if (shouldDisposeConnection && dbConnection != null)
                {
                    await _ConnectionFactory.ReturnConnectionAsync(dbConnection).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Asynchronously executes a raw SQL command and returns the number of rows affected.
        /// </summary>
        /// <param name="sql">The raw SQL command to execute.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <param name="parameters">Parameters for the SQL command.</param>
        /// <returns>A task that represents the asynchronous operation containing the number of rows affected by the command.</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null or empty.</exception>
        /// <exception cref="InvalidOperationException">Thrown when command execution fails.</exception>
        public async Task<int> ExecuteSqlAsync(string sql, ITransaction? transaction = null, CancellationToken token = default, params object[] parameters)
        {
            if (string.IsNullOrWhiteSpace(sql))
                throw new ArgumentException("SQL cannot be null or empty", nameof(sql));

            token.ThrowIfCancellationRequested();

            if (transaction != null)
            {
                return await ExecuteNonQueryWithConnectionAsync(transaction.Connection, sql, transaction.Transaction, token, parameters.Select(p => p is (string, object) ? ((string, object?))p : ($"@p{Array.IndexOf(parameters, p)}", p)).ToArray()).ConfigureAwait(false);
            }
            else
            {
                DbConnection? connection = null;
                try
                {
                    connection = await _ConnectionFactory.GetConnectionAsync().ConfigureAwait(false);
                    return await ExecuteNonQueryWithConnectionAsync(connection, sql, null, token, parameters.Select(p => p is (string, object) ? ((string, object?))p : ($"@p{Array.IndexOf(parameters, p)}", p)).ToArray()).ConfigureAwait(false);
                }
                finally
                {
                    if (connection != null)
                        await _ConnectionFactory.ReturnConnectionAsync(connection).ConfigureAwait(false);
                }
            }
        }

        // Query builder
        /// <summary>
        /// Creates a query builder for building and executing complex queries.
        /// </summary>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>A query builder instance for the entity type.</returns>
        public IQueryBuilder<T> Query(ITransaction? transaction = null)
        {
            return new PostgresQueryBuilder<T>(this, transaction);
        }

        // Transaction operations
        /// <summary>
        /// Begins a new database transaction.
        /// </summary>
        /// <returns>A new transaction instance.</returns>
        public ITransaction BeginTransaction()
        {
            NpgsqlConnection? connection = null;
            try
            {
                connection = (NpgsqlConnection)PooledConnectionHandle.Unwrap(_ConnectionFactory.GetConnection());

                // Ensure connection is open (GetConnection might return an already open connection)
                if (connection.State != ConnectionState.Open)
                {
                    connection.Open();
                }

                NpgsqlTransaction transaction = connection.BeginTransaction();
                PostgresRepositoryTransaction result = new PostgresRepositoryTransaction(connection, transaction, _ConnectionFactory);
                connection = null; // Transaction now owns the connection
                return result;
            }
            finally
            {
                if (connection != null)
                {
                    _ConnectionFactory.ReturnConnection(connection);
                }
            }
        }

        /// <summary>
        /// Asynchronously begins a new database transaction.
        /// </summary>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with a new transaction instance.</returns>
        public async Task<ITransaction> BeginTransactionAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            NpgsqlConnection? connection = null;
            try
            {
                connection = (NpgsqlConnection)PooledConnectionHandle.Unwrap(await _ConnectionFactory.GetConnectionAsync().ConfigureAwait(false));

                // Ensure connection is open (GetConnectionAsync might return an already open connection)
                if (connection.State != ConnectionState.Open)
                {
                    await connection.OpenAsync(token).ConfigureAwait(false);
                }

                NpgsqlTransaction transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);
                PostgresRepositoryTransaction result = new PostgresRepositoryTransaction(connection, transaction, _ConnectionFactory);
                connection = null; // Transaction now owns the connection
                return result;
            }
            finally
            {
                if (connection != null)
                {
                    await _ConnectionFactory.ReturnConnectionAsync(connection).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Disposes the repository and its resources. If this instance owns the connection factory, it will also be disposed.
        /// </summary>
        public void Dispose()
        {
            if (_OwnsConnectionFactory)
            {
                _ConnectionFactory?.Dispose();
            }
        }

        private PropertyInfo? GetPropertyFromExpression(Expression expression)
        {
            if (expression is MemberExpression memberExpr)
            {
                return memberExpr.Member as PropertyInfo;
            }

            if (expression is UnaryExpression unaryExpr && unaryExpr.NodeType == ExpressionType.Convert)
            {
                return GetPropertyFromExpression(unaryExpr.Operand);
            }

            throw new ArgumentException($"Expression must be a property accessor, but was {expression.NodeType}");
        }

        /// <summary>
        /// Gets a connection from the connection factory.
        /// Note: The caller is responsible for disposing the connection.
        /// </summary>
        /// <returns>A database connection</returns>
        public DbConnection GetConnection()
        {
            return _ConnectionFactory.GetConnection();
        }

        /// <summary>
        /// Asynchronously gets a connection from the connection factory.
        /// Note: The caller is responsible for disposing the connection.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token for the async operation</param>
        /// <returns>A database connection</returns>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        public async Task<DbConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return await _ConnectionFactory.GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Gets the primary key value from an entity instance.
        /// </summary>
        /// <param name="entity">The entity to extract the primary key from</param>
        /// <returns>The primary key value</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null</exception>
        public object? GetPrimaryKeyValue(T entity)
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            return _PrimaryKeyProperty.GetValue(entity);
        }

        #endregion

        #region Private-Methods

        // These are placeholder methods that would contain the actual reflection logic
        // from the MySQL implementation
        /// <summary>
        /// Gets the name of the entity/table for this repository.
        /// </summary>
        /// <returns>The entity name or table name</returns>
        public string GetEntityName()
        {
            EntityAttribute? entityAttr = typeof(T).GetCustomAttribute<EntityAttribute>();
            return entityAttr?.Name ?? typeof(T).Name.ToLowerInvariant();
        }

        private PrimaryKeyInfo GetPrimaryKeyInfo()
        {
            PropertyInfo[] properties = typeof(T).GetProperties();
            foreach (PropertyInfo prop in properties)
            {
                if (prop.Name.Equals("Id", StringComparison.OrdinalIgnoreCase))
                {
                    PropertyAttribute? propAttr = prop.GetCustomAttribute<PropertyAttribute>();
                    return new PrimaryKeyInfo(propAttr?.Name ?? prop.Name.ToLowerInvariant(), prop);
                }
            }
            throw new InvalidOperationException($"No primary key property found for entity type {typeof(T).Name}");
        }

        /// <summary>
        /// Gets the column-to-property mappings for this entity type.
        /// </summary>
        /// <returns>A dictionary mapping column names to PropertyInfo objects</returns>
        public Dictionary<string, PropertyInfo> GetColumnMappings()
        {
            Dictionary<string, PropertyInfo> mappings = new Dictionary<string, PropertyInfo>();
            PropertyInfo[] properties = typeof(T).GetProperties();

            foreach (PropertyInfo prop in properties)
            {
                PropertyAttribute? propAttr = prop.GetCustomAttribute<PropertyAttribute>();
                if (propAttr != null)
                {
                    mappings[propAttr.Name] = prop;
                }
            }

            return mappings;
        }

        private void EnsureConnectionOpen(DbConnection connection)
        {
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }
        }

        private async Task EnsureConnectionOpenAsync(DbConnection connection, CancellationToken token)
        {
            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync(token).ConfigureAwait(false);
            }
        }

        private void AddParametersToCommand(NpgsqlCommand command, params object[] parameters)
        {
            if (parameters != null)
            {
                for (int i = 0; i < parameters.Length; i++)
                {
                    if (parameters[i] is (string name, object value))
                    {
                        command.Parameters.AddWithValue(name, value ?? DBNull.Value);
                    }
                    else
                    {
                        command.Parameters.AddWithValue($"@p{i}", parameters[i] ?? DBNull.Value);
                    }
                }
            }
        }

        private int ExecuteNonQueryWithConnection(DbConnection connection, string sql, DbTransaction? transaction, params (string name, object? value)[] parameters)
        {
            NpgsqlConnection npgsqlConn = (NpgsqlConnection)PooledConnectionHandle.Unwrap(connection);
            if (npgsqlConn.State != ConnectionState.Open)
                npgsqlConn.Open();

            using NpgsqlCommand command = new NpgsqlCommand(sql, npgsqlConn, (NpgsqlTransaction?)transaction);
            foreach ((string name, object? value) param in parameters)
            {
                command.Parameters.AddWithValue(param.name, param.value ?? DBNull.Value);
            }

            // Capture SQL if enabled
            if (_CaptureSql)
            {
                _LastExecutedSql = command.CommandText;
                _LastExecutedSqlWithParameters = BuildSqlWithParameters(command);
            }

            return command.ExecuteNonQuery();
        }

        private async Task<int> ExecuteNonQueryWithConnectionAsync(DbConnection connection, string sql, DbTransaction? transaction, CancellationToken token, params (string name, object? value)[] parameters)
        {
            NpgsqlConnection npgsqlConn = (NpgsqlConnection)PooledConnectionHandle.Unwrap(connection);
            await EnsureConnectionOpenAsync(npgsqlConn, token).ConfigureAwait(false);

            using NpgsqlCommand command = new NpgsqlCommand(sql, npgsqlConn, (NpgsqlTransaction?)transaction);
            foreach ((string name, object? value) param in parameters)
            {
                command.Parameters.AddWithValue(param.name, param.value ?? DBNull.Value);
            }

            // Capture SQL if enabled
            if (_CaptureSql)
            {
                _LastExecutedSql = command.CommandText;
                _LastExecutedSqlWithParameters = BuildSqlWithParameters(command);
            }

            return await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        private TResult? ExecuteScalarWithConnection<TResult>(DbConnection connection, string sql, DbTransaction? transaction, params (string name, object? value)[] parameters)
        {
            NpgsqlConnection npgsqlConn = (NpgsqlConnection)PooledConnectionHandle.Unwrap(connection);
            if (npgsqlConn.State != ConnectionState.Open)
                npgsqlConn.Open();

            using NpgsqlCommand command = new NpgsqlCommand(sql, npgsqlConn, (NpgsqlTransaction?)transaction);
            foreach ((string name, object? value) param in parameters)
            {
                command.Parameters.AddWithValue(param.name, param.value ?? DBNull.Value);
            }

            // Capture SQL if enabled
            if (_CaptureSql)
            {
                _LastExecutedSql = command.CommandText;
                _LastExecutedSqlWithParameters = BuildSqlWithParameters(command);
            }

            object? result = command.ExecuteScalar();
            if (result == null || result == DBNull.Value)
                return default(TResult);

            return (TResult)result;
        }

        private async Task<TResult?> ExecuteScalarWithConnectionAsync<TResult>(DbConnection connection, string sql, DbTransaction? transaction, CancellationToken token, params (string name, object? value)[] parameters)
        {
            NpgsqlConnection npgsqlConn = (NpgsqlConnection)PooledConnectionHandle.Unwrap(connection);
            await EnsureConnectionOpenAsync(npgsqlConn, token).ConfigureAwait(false);

            using NpgsqlCommand command = new NpgsqlCommand(sql, npgsqlConn, (NpgsqlTransaction?)transaction);
            foreach ((string name, object? value) param in parameters)
            {
                command.Parameters.AddWithValue(param.name, param.value ?? DBNull.Value);
            }

            // Capture SQL if enabled
            if (_CaptureSql)
            {
                _LastExecutedSql = command.CommandText;
                _LastExecutedSqlWithParameters = BuildSqlWithParameters(command);
            }

            object? result = await command.ExecuteScalarAsync(token).ConfigureAwait(false);
            if (result == null || result == DBNull.Value)
                return default(TResult);

            return (TResult)result;
        }

        private TResult SafeConvertDatabaseResult<TResult>(object? result)
        {
            if (result == DBNull.Value || result == null)
                return default(TResult)!;

            try
            {
                return (TResult)_DataTypeConverter.ConvertFromDatabase(result, typeof(TResult))!;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to convert database result to type {typeof(TResult).Name}", ex);
            }
        }

        private async Task<IEnumerable<T>> CreateManyOptimizedAsync(IList<T> entities, ITransaction? transaction, CancellationToken token)
        {
            List<T> results = new List<T>();
            List<string> columns = new List<string>();
            PropertyInfo? autoIncrementProperty = null;

            // Get column mappings and identify auto-increment column
            foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
            {
                string columnName = kvp.Key;
                PropertyInfo property = kvp.Value;

                PropertyAttribute? attr = property.GetCustomAttribute<PropertyAttribute>();
                if (attr?.PropertyFlags.HasFlag(Flags.AutoIncrement) == true)
                {
                    autoIncrementProperty = property;
                    continue;
                }

                columns.Add(_Sanitizer.SanitizeIdentifier(columnName));
            }

            int batchSize = Math.Min(_BatchConfig.MaxRowsPerBatch, entities.Count);
            int processed = 0;

            while (processed < entities.Count)
            {
                int currentBatchSize = Math.Min(batchSize, entities.Count - processed);
                List<T> batch = entities.Skip(processed).Take(currentBatchSize).ToList();

                // Build multi-row INSERT statement
                List<string> valueRows = new List<string>();
                List<(string name, object? value)> parameters = new List<(string, object?)>();
                int paramCounter = 0;

                foreach (T entity in batch)
                {
                    List<string> rowValues = new List<string>();

                    foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
                    {
                        string columnName = kvp.Key;
                        PropertyInfo property = kvp.Value;

                        PropertyAttribute? attr = property.GetCustomAttribute<PropertyAttribute>();
                        if (attr?.PropertyFlags.HasFlag(Flags.AutoIncrement) == true)
                            continue;

                        object? value = property.GetValue(entity);
                        string paramName = $"@p{paramCounter}";
                        rowValues.Add(paramName);
                        parameters.Add((paramName, _DataTypeConverter.ConvertToDatabase(value!, property.PropertyType, property)));
                        paramCounter++;
                    }

                    valueRows.Add($"({string.Join(", ", rowValues)})");
                }

                string sql;
                if (autoIncrementProperty != null)
                {
                    sql = $"INSERT INTO {_Sanitizer.SanitizeIdentifier(_TableName)} ({string.Join(", ", columns)}) VALUES {string.Join(", ", valueRows)} RETURNING {_Sanitizer.SanitizeIdentifier(_PrimaryKeyColumn)}";
                }
                else
                {
                    sql = $"INSERT INTO {_Sanitizer.SanitizeIdentifier(_TableName)} ({string.Join(", ", columns)}) VALUES {string.Join(", ", valueRows)}";
                }

                // Execute batch insert
                if (autoIncrementProperty != null)
                {
                    // Handle auto-increment IDs
                    List<object> insertedIds = new List<object>();

                    if (transaction != null)
                    {
                        NpgsqlConnection txnConn = (NpgsqlConnection)PooledConnectionHandle.Unwrap(transaction.Connection);
                        using NpgsqlCommand command = new NpgsqlCommand(sql, txnConn, (NpgsqlTransaction)transaction.Transaction);
                        foreach ((string name, object value) in parameters)
                        {
                            command.Parameters.AddWithValue(name, value ?? DBNull.Value);
                        }

                        using NpgsqlDataReader reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
                        while (await reader.ReadAsync(token).ConfigureAwait(false))
                        {
                            insertedIds.Add(reader.GetValue(0));
                        }
                    }
                    else
                    {
                        DbConnection? dbConnection = null;
                        try
                        {
                            dbConnection = await _ConnectionFactory.GetConnectionAsync().ConfigureAwait(false);
                            NpgsqlConnection connection = (NpgsqlConnection)PooledConnectionHandle.Unwrap(dbConnection);
                            await EnsureConnectionOpenAsync(connection, token).ConfigureAwait(false);

                            using NpgsqlCommand command = new NpgsqlCommand(sql, connection);
#pragma warning disable CS8600
                            foreach ((string name, object value) in parameters)
#pragma warning restore CS8600
                            {
                                command.Parameters.AddWithValue(name, value ?? DBNull.Value);
                            }

                            using NpgsqlDataReader reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
                            while (await reader.ReadAsync(token).ConfigureAwait(false))
                            {
                                insertedIds.Add(reader.GetValue(0));
                            }
                        }
                        finally
                        {
                            if (dbConnection != null)
                                await _ConnectionFactory.ReturnConnectionAsync(dbConnection).ConfigureAwait(false);
                        }
                    }

                    // Set the auto-increment IDs on the entities
                    for (int i = 0; i < batch.Count && i < insertedIds.Count; i++)
                    {
                        object? convertedId = _DataTypeConverter.ConvertFromDatabase(insertedIds[i], autoIncrementProperty.PropertyType, autoIncrementProperty);
                        autoIncrementProperty.SetValue(batch[i], convertedId);
                    }
                }
                else
                {
                    // No auto-increment, just execute the batch insert
                    if (transaction != null)
                    {
                        await ExecuteNonQueryWithConnectionAsync(transaction.Connection, sql, transaction.Transaction, token, parameters.ToArray()).ConfigureAwait(false);
                    }
                    else
                    {
                        DbConnection? connection = null;
                        try
                        {
                            connection = await _ConnectionFactory.GetConnectionAsync().ConfigureAwait(false);
                            await ExecuteNonQueryWithConnectionAsync(connection, sql, null, token, parameters.ToArray()).ConfigureAwait(false);
                        }
                        finally
                        {
                            if (connection != null)
                                await _ConnectionFactory.ReturnConnectionAsync(connection).ConfigureAwait(false);
                        }
                    }
                }

                results.AddRange(batch);
                processed += currentBatchSize;
            }

            return results;
        }

        private IEnumerable<T> CreateManyOptimized(IList<T> entities, ITransaction? transaction)
        {
            // Use the async version with synchronous execution
            return CreateManyOptimizedAsync(entities, transaction, CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Gets the foreign key relationships for this entity type.
        /// </summary>
        /// <returns>A dictionary mapping properties to their foreign key attributes</returns>
        public Dictionary<PropertyInfo, ForeignKeyAttribute> GetForeignKeys()
        {
            return new Dictionary<PropertyInfo, ForeignKeyAttribute>();
        }

        /// <summary>
        /// Gets the navigation properties for this entity type.
        /// </summary>
        /// <returns>A dictionary mapping properties to their navigation property attributes</returns>
        public Dictionary<PropertyInfo, NavigationPropertyAttribute> GetNavigationProperties()
        {
            return new Dictionary<PropertyInfo, NavigationPropertyAttribute>();
        }

        /// <summary>
        /// Gets the version column information for optimistic concurrency control.
        /// </summary>
        /// <returns>Version column information or null if not available</returns>
        public VersionColumnInfo? GetVersionColumnInfo()
        {
            PropertyInfo[] properties = typeof(T).GetProperties();
            foreach (PropertyInfo property in properties)
            {
                VersionColumnAttribute? attr = property.GetCustomAttribute<VersionColumnAttribute>();
                if (attr != null)
                {
                    PropertyAttribute? propAttr = property.GetCustomAttribute<PropertyAttribute>();
                    if (propAttr == null)
                        throw new InvalidOperationException($"Version column property {property.Name} must have [Property] attribute");

                    return new VersionColumnInfo
                    {
                        Property = property,
                        ColumnName = propAttr.Name,
                        Type = attr.Type,
                        PropertyType = property.PropertyType
                    };
                }
            }

            return null; // No version column
        }

        /// <summary>
        /// Gets the default value providers for properties with DefaultValueAttribute.
        /// </summary>
        /// <returns>A dictionary mapping PropertyInfo to (DefaultValueAttribute, IDefaultValueProvider)</returns>
        private Dictionary<PropertyInfo, (DefaultValueAttribute, IDefaultValueProvider)> GetDefaultValueProviders()
        {
            Dictionary<PropertyInfo, (DefaultValueAttribute, IDefaultValueProvider)> providers = new Dictionary<PropertyInfo, (DefaultValueAttribute, IDefaultValueProvider)>();
            PropertyInfo[] properties = typeof(T).GetProperties();

            foreach (PropertyInfo property in properties)
            {
                DefaultValueAttribute? attr = property.GetCustomAttribute<DefaultValueAttribute>();
                if (attr != null)
                {
                    IDefaultValueProvider provider;

                    // Create provider based on attribute configuration
                    if (attr.ProviderType != null)
                    {
                        // Custom provider type specified
                        provider = (IDefaultValueProvider)Activator.CreateInstance(attr.ProviderType)!;
                    }
                    else
                    {
                        // Use built-in provider based on DefaultValueType
                        provider = attr.ValueType switch
                        {
                            DefaultValueType.CurrentDateTimeUtc => new CurrentDateTimeUtcProvider(),
                            DefaultValueType.NewGuid => new NewGuidProvider(),
                            DefaultValueType.SequentialGuid => new SequentialGuidProvider(),
                            DefaultValueType.StaticValue => new StaticValueProvider(attr.StaticValue),
                            _ => throw new InvalidOperationException($"Unknown DefaultValueType: {attr.ValueType}")
                        };
                    }

                    providers[property] = (attr, provider);
                }
            }

            return providers;
        }

        /// <summary>
        /// Maps a database reader to a specific result type.
        /// </summary>
        /// <typeparam name="TResult">The type to map to</typeparam>
        /// <param name="reader">The database reader containing the data</param>
        /// <returns>A mapped instance of the result type</returns>
        public TResult MapReaderToType<TResult>(IDataReader reader) where TResult : new()
        {
            TResult result = new TResult();
            Type resultType = typeof(TResult);
            for (int i = 0; i < reader.FieldCount; i++)
            {
                string columnName = reader.GetName(i);
                PropertyInfo? property = resultType.GetProperty(columnName, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
                if (property != null && !reader.IsDBNull(i))
                {
                    object value = reader.GetValue(i);
                    try
                    {
                        object? convertedValue = _DataTypeConverter.ConvertFromDatabase(value, property.PropertyType, property);
                        property.SetValue(result, convertedValue);
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException($"Error mapping column '{columnName}' to property '{property.Name}': {ex.Message}", ex);
                    }
                }
            }
            return result;
        }

        private string BuildSqlWithParameters(NpgsqlCommand command)
        {
            if (command?.Parameters == null || command.Parameters.Count == 0)
            {
                return command?.CommandText ?? string.Empty;
            }

            string sql = command.CommandText;
            foreach (NpgsqlParameter parameter in command.Parameters)
            {
                string parameterValue = FormatParameterValue(parameter.Value);
                sql = sql.Replace(parameter.ParameterName, parameterValue);
            }
            return sql;
        }

        private string FormatParameterValue(object? value)
        {
            if (value == null || value == DBNull.Value)
            {
                return "NULL";
            }

            if (value is string stringValue)
            {
                return $"'{stringValue.Replace("'", "''")}'";
            }

            if (value is DateTime dateTimeValue)
            {
                return $"'{dateTimeValue:yyyy-MM-dd HH:mm:ss}'";
            }

            if (value is DateTimeOffset dateTimeOffsetValue)
            {
                return $"'{dateTimeOffsetValue:yyyy-MM-dd HH:mm:ss zzz}'";
            }

            if (value is bool boolValue)
            {
                return boolValue ? "true" : "false";
            }

            if (value is Guid guidValue)
            {
                return $"'{guidValue}'";
            }

            return value.ToString() ?? "NULL";
        }

        internal string GetColumnFromExpression(Expression expression)
        {
            PostgresExpressionParser<T> parser = new PostgresExpressionParser<T>(_ColumnMappings, _Sanitizer);
            // The parser's GetColumnFromExpression already returns sanitized column names with double quotes
            return parser.GetColumnFromExpression(expression);
        }

        internal void SetLastExecutedSql(string sql)
        {
            if (_CaptureSql)
            {
                _LastExecutedSql = sql;
                // BuildSqlWithParameters requires a command, so we'll keep it simple for now
                _LastExecutedSqlWithParameters = sql;
            }
        }

        #endregion

        #region Initialization

        /// <inheritdoc/>
        public void InitializeTable(Type entityType, ITransaction? transaction = null)
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

            // Get table name
            EntityAttribute? entityAttr = entityType.GetCustomAttribute<EntityAttribute>();
            string tableName = entityAttr!.Name; // Already validated in ValidateTable

            // Get schema name from settings (default to "public")
            string schemaName = Settings?.Database ?? "public";

            // Check if table exists
            bool tableExists;
            if (transaction != null)
            {
                EnsureConnectionOpen(transaction.Connection);
                tableExists = PostgresSchemaBuilder.TableExists(tableName, schemaName, transaction.Connection);
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                EnsureConnectionOpen(connection);
                tableExists = PostgresSchemaBuilder.TableExists(tableName, schemaName, connection);
            }

            if (!tableExists)
            {
                // Create the table
                PostgresSchemaBuilder schemaBuilder = new PostgresSchemaBuilder(_Sanitizer, _DataTypeConverter);
                string createTableSql = schemaBuilder.BuildCreateTableSql(entityType);

                if (transaction != null)
                {
                    ExecuteNonQueryWithConnection(transaction.Connection, createTableSql, transaction.Transaction);
                }
                else
                {
                    using DbConnection connection = _ConnectionFactory.GetConnection();
                    ExecuteNonQueryWithConnection(connection, createTableSql, null);
                }
            }
            else
            {
                // Table exists - could add schema migration logic here in the future
                // For now, we just validate that it exists
            }
        }

        /// <inheritdoc/>
        public async Task InitializeTableAsync(Type entityType, ITransaction? transaction = null, CancellationToken cancellationToken = default)
        {
            if (entityType == null)
                throw new ArgumentNullException(nameof(entityType));

            cancellationToken.ThrowIfCancellationRequested();

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

            // Get table name
            EntityAttribute? entityAttr = entityType.GetCustomAttribute<EntityAttribute>();
            string tableName = entityAttr!.Name; // Already validated in ValidateTable

            // Get schema name from settings (default to "public")
            string schemaName = Settings?.Database ?? "public";

            // Check if table exists
            bool tableExists;
            if (transaction != null)
            {
                await EnsureConnectionOpenAsync(transaction.Connection, cancellationToken).ConfigureAwait(false);
                tableExists = PostgresSchemaBuilder.TableExists(tableName, schemaName, transaction.Connection);
            }
            else
            {
                using DbConnection connection = await _ConnectionFactory.GetConnectionAsync(cancellationToken).ConfigureAwait(false);
                await EnsureConnectionOpenAsync(connection, cancellationToken).ConfigureAwait(false);
                tableExists = PostgresSchemaBuilder.TableExists(tableName, schemaName, connection);
            }

            if (!tableExists)
            {
                // Create the table
                PostgresSchemaBuilder schemaBuilder = new PostgresSchemaBuilder(_Sanitizer, _DataTypeConverter);
                string createTableSql = schemaBuilder.BuildCreateTableSql(entityType);

                if (transaction != null)
                {
                    await ExecuteNonQueryWithConnectionAsync(transaction.Connection, createTableSql, transaction.Transaction, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    using DbConnection connection = await _ConnectionFactory.GetConnectionAsync(cancellationToken).ConfigureAwait(false);
                    await ExecuteNonQueryWithConnectionAsync(connection, createTableSql, null, cancellationToken).ConfigureAwait(false);
                }
            }
            else
            {
                // Table exists - could add schema migration logic here in the future
                // For now, we just validate that it exists
            }
        }

        /// <inheritdoc/>
        public void InitializeTables(IEnumerable<Type> entityTypes, ITransaction? transaction = null)
        {
            if (entityTypes == null)
                throw new ArgumentNullException(nameof(entityTypes));

            ITransaction? localTransaction = transaction;
            bool createdTransaction = false;

            try
            {
                // Create a transaction if one wasn't provided
                if (localTransaction == null)
                {
                    localTransaction = BeginTransaction();
                    createdTransaction = true;
                }

                // Initialize each table within the transaction
                foreach (Type entityType in entityTypes)
                {
                    InitializeTable(entityType, localTransaction);
                }

                // Commit if we created the transaction
                if (createdTransaction)
                {
                    localTransaction.Commit();
                }
            }
            catch
            {
                // Rollback if we created the transaction and something failed
                if (createdTransaction && localTransaction != null)
                {
                    localTransaction.Rollback();
                }
                throw;
            }
            finally
            {
                // Dispose if we created the transaction
                if (createdTransaction && localTransaction != null)
                {
                    localTransaction.Dispose();
                }
            }
        }

        /// <inheritdoc/>
        public async Task InitializeTablesAsync(IEnumerable<Type> entityTypes, ITransaction? transaction = null, CancellationToken cancellationToken = default)
        {
            if (entityTypes == null)
                throw new ArgumentNullException(nameof(entityTypes));

            cancellationToken.ThrowIfCancellationRequested();

            ITransaction? localTransaction = transaction;
            bool createdTransaction = false;

            try
            {
                // Create a transaction if one wasn't provided
                if (localTransaction == null)
                {
                    localTransaction = await BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
                    createdTransaction = true;
                }

                // Initialize each table within the transaction
                foreach (Type entityType in entityTypes)
                {
                    await InitializeTableAsync(entityType, localTransaction, cancellationToken).ConfigureAwait(false);
                }

                // Commit if we created the transaction
                if (createdTransaction)
                {
                    localTransaction.Commit();
                }
            }
            catch
            {
                // Rollback if we created the transaction and something failed
                if (createdTransaction && localTransaction != null)
                {
                    localTransaction.Rollback();
                }
                throw;
            }
            finally
            {
                // Dispose if we created the transaction
                if (createdTransaction && localTransaction != null)
                {
                    localTransaction.Dispose();
                }
            }
        }

        /// <inheritdoc/>
        public bool ValidateTable(Type entityType, out List<string> errors, out List<string> warnings)
        {
            errors = new List<string>();
            warnings = new List<string>();

            if (entityType == null)
            {
                errors.Add("Entity type cannot be null");
                return false;
            }

            // Check for Entity attribute
            EntityAttribute? entityAttr = entityType.GetCustomAttribute<EntityAttribute>();
            if (entityAttr == null)
            {
                errors.Add($"Type '{entityType.Name}' must have an [Entity] attribute");
                return false;
            }

            // Check for at least one property with Property attribute
            PropertyInfo[] properties = entityType.GetProperties();
            List<PropertyInfo> mappedProperties = properties
                .Where(p => p.GetCustomAttribute<PropertyAttribute>() != null)
                .ToList();

            if (mappedProperties.Count == 0)
            {
                errors.Add($"Type '{entityType.Name}' must have at least one property with [Property] attribute");
                return false;
            }

            // Check for primary key
            PropertyInfo? primaryKeyProperty = mappedProperties
                .FirstOrDefault(p => p.GetCustomAttribute<PropertyAttribute>()?.PropertyFlags.HasFlag(Flags.PrimaryKey) == true);

            if (primaryKeyProperty == null)
            {
                errors.Add($"Type '{entityType.Name}' must have a property with [Property] attribute and PrimaryKey flag");
                return false;
            }

            // Validate foreign keys
            foreach (PropertyInfo property in mappedProperties)
            {
                ForeignKeyAttribute? fkAttr = property.GetCustomAttribute<ForeignKeyAttribute>();
                if (fkAttr != null)
                {
                    // Check that referenced type has Entity attribute
                    EntityAttribute? refEntityAttr = fkAttr.ReferencedType.GetCustomAttribute<EntityAttribute>();
                    if (refEntityAttr == null)
                    {
                        errors.Add($"Foreign key on property '{property.Name}' references type '{fkAttr.ReferencedType.Name}' which does not have an [Entity] attribute");
                    }

                    // Check that referenced property exists
                    PropertyInfo? refProperty = fkAttr.ReferencedType.GetProperty(fkAttr.ReferencedProperty);
                    if (refProperty == null)
                    {
                        errors.Add($"Foreign key on property '{property.Name}' references non-existent property '{fkAttr.ReferencedProperty}' on type '{fkAttr.ReferencedType.Name}'");
                    }
                    else
                    {
                        // Check that referenced property has Property attribute
                        PropertyAttribute? refPropAttr = refProperty.GetCustomAttribute<PropertyAttribute>();
                        if (refPropAttr == null)
                        {
                            errors.Add($"Foreign key on property '{property.Name}' references property '{fkAttr.ReferencedProperty}' on type '{fkAttr.ReferencedType.Name}' which does not have a [Property] attribute");
                        }
                    }
                }
            }

            // If table exists, check schema compatibility
            string tableName = entityAttr.Name;
            string schemaName = Settings?.Database ?? "public";
            try
            {
                using (DbConnection conn = _ConnectionFactory.GetConnection())
                {
                    NpgsqlConnection connection = (NpgsqlConnection)PooledConnectionHandle.Unwrap(conn);

                    if (PostgresSchemaBuilder.TableExists(tableName, schemaName, connection))
                    {
                        List<ColumnInfo> existingColumns = PostgresSchemaBuilder.GetTableColumns(tableName, schemaName, connection);
                        List<string> existingColumnNames = existingColumns.Select(c => c.Name).ToList();

                        // Check if entity columns exist in database
                        foreach (PropertyInfo prop in mappedProperties)
                        {
                            PropertyAttribute? propAttr = prop.GetCustomAttribute<PropertyAttribute>();
                            if (propAttr != null)
                            {
                                if (!existingColumnNames.Contains(propAttr.Name, StringComparer.OrdinalIgnoreCase))
                                {
                                    errors.Add($"Table '{tableName}' exists but column '{propAttr.Name}' (for property '{prop.Name}') does not exist in the database");
                                }
                            }
                        }

                        // Check for extra columns in database (warning only)
                        foreach (ColumnInfo dbColumn in existingColumns)
                        {
                            bool foundInEntity = false;
                            foreach (PropertyInfo prop in mappedProperties)
                            {
                                PropertyAttribute? propAttr = prop.GetCustomAttribute<PropertyAttribute>();
                                if (propAttr != null && propAttr.Name.Equals(dbColumn.Name, StringComparison.OrdinalIgnoreCase))
                                {
                                    foundInEntity = true;
                                    break;
                                }
                            }

                            if (!foundInEntity)
                            {
                                warnings.Add($"Table '{tableName}' has column '{dbColumn.Name}' which is not mapped to any property in type '{entityType.Name}'");
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                warnings.Add($"Could not validate table schema against database: {ex.Message}");
            }

            return errors.Count == 0;
        }

        /// <inheritdoc/>
        public bool ValidateTables(IEnumerable<Type> entityTypes, out List<string> errors, out List<string> warnings)
        {
            errors = new List<string>();
            warnings = new List<string>();

            if (entityTypes == null)
            {
                errors.Add("Entity types collection cannot be null");
                return false;
            }

            bool allValid = true;
            foreach (Type entityType in entityTypes)
            {
                List<string> typeErrors;
                List<string> typeWarnings;

                if (!ValidateTable(entityType, out typeErrors, out typeWarnings))
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

            PostgresSchemaBuilder schemaBuilder = new PostgresSchemaBuilder(_Sanitizer, _DataTypeConverter);
            List<string> indexSqlStatements = schemaBuilder.BuildCreateIndexSql(entityType);

            if (indexSqlStatements.Count == 0)
            {
                return; // No indexes to create
            }

            if (transaction != null)
            {
                DbConnection connection = transaction.Connection;
                DbTransaction dbTransaction = transaction.Transaction;

                foreach (string sql in indexSqlStatements)
                {
                    using (DbCommand command = connection.CreateCommand())
                    {
                        command.Transaction = dbTransaction;
                        command.CommandText = sql;
                        command.ExecuteNonQuery();
                    }
                }
            }
            else
            {
                using (DbConnection connection = _ConnectionFactory.GetConnection())
                {
                    foreach (string sql in indexSqlStatements)
                    {
                        using (DbCommand command = connection.CreateCommand())
                        {
                            command.CommandText = sql;
                            command.ExecuteNonQuery();
                        }
                    }
                }
            }
        }

        /// <inheritdoc/>
        public async Task CreateIndexesAsync(Type entityType, ITransaction? transaction = null, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            cancellationToken.ThrowIfCancellationRequested();

            PostgresSchemaBuilder schemaBuilder = new PostgresSchemaBuilder(_Sanitizer, _DataTypeConverter);
            List<string> indexSqlStatements = schemaBuilder.BuildCreateIndexSql(entityType);

            if (indexSqlStatements.Count == 0)
            {
                return; // No indexes to create
            }

            if (transaction != null)
            {
                DbConnection connection = transaction.Connection;
                DbTransaction dbTransaction = transaction.Transaction;

                foreach (string sql in indexSqlStatements)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    using (DbCommand command = connection.CreateCommand())
                    {
                        command.Transaction = dbTransaction;
                        command.CommandText = sql;
                        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            else
            {
                using (DbConnection connection = _ConnectionFactory.GetConnection())
                {
                    foreach (string sql in indexSqlStatements)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        using (DbCommand command = connection.CreateCommand())
                        {
                            command.CommandText = sql;
                            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                        }
                    }
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
                DbConnection connection = transaction.Connection;
                DbTransaction dbTransaction = transaction.Transaction;

                using (DbCommand command = connection.CreateCommand())
                {
                    command.Transaction = dbTransaction;
                    command.CommandText = sql;
                    command.ExecuteNonQuery();
                }
            }
            else
            {
                using (DbConnection connection = _ConnectionFactory.GetConnection())
                {
                    using (DbCommand command = connection.CreateCommand())
                    {
                        command.CommandText = sql;
                        command.ExecuteNonQuery();
                    }
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
                DbConnection connection = transaction.Connection;
                DbTransaction dbTransaction = transaction.Transaction;

                using (DbCommand command = connection.CreateCommand())
                {
                    command.Transaction = dbTransaction;
                    command.CommandText = sql;
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            else
            {
                using (DbConnection connection = _ConnectionFactory.GetConnection())
                {
                    using (DbCommand command = connection.CreateCommand())
                    {
                        command.CommandText = sql;
                        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }
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
            string schemaName = "public"; // PostgreSQL default schema

            using (DbConnection conn = _ConnectionFactory.GetConnection())
            {
                NpgsqlConnection connection = (NpgsqlConnection)PooledConnectionHandle.Unwrap(conn);

                List<IndexInfo> indexes = PostgresSchemaBuilder.GetExistingIndexes(tableName, schemaName, connection);
                List<string> indexNames = indexes.Select(i => i.Name).ToList();

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
            string schemaName = "public"; // PostgreSQL default schema

            using (DbConnection conn = _ConnectionFactory.GetConnection())
            {
                NpgsqlConnection connection = (NpgsqlConnection)PooledConnectionHandle.Unwrap(conn);

                await Task.Run(() =>
                {
                    // Npgsql operations
                }, cancellationToken).ConfigureAwait(false);

                List<IndexInfo> indexes = PostgresSchemaBuilder.GetExistingIndexes(tableName, schemaName, connection);
                List<string> indexNames = indexes.Select(i => i.Name).ToList();

                return indexNames;
            }
        }

        /// <inheritdoc/>
        public void CreateDatabaseIfNotExists()
        {
            if (Settings == null)
            {
                throw new InvalidOperationException("Cannot create database when Settings is null. Use a constructor that provides connection settings.");
            }

            string? databaseName = Settings.Database;
            if (string.IsNullOrWhiteSpace(databaseName))
            {
                throw new InvalidOperationException("Database name cannot be null or empty");
            }

            // Create a connection string without database specified (connect to postgres database)
            PostgresRepositorySettings tempSettings = new PostgresRepositorySettings
            {
                Hostname = Settings.Hostname,
                Port = Settings.Port,
                Username = Settings.Username,
                Password = Settings.Password,
                Database = "postgres" // Connect to default postgres database
            };

            string connectionString = tempSettings.BuildConnectionString();

            using (NpgsqlConnection connection = new NpgsqlConnection(connectionString))
            {
                connection.Open();

                // Check if database exists
                bool exists = PostgresSchemaBuilder.DatabaseExists(databaseName, connection);

                if (!exists)
                {
                    // Create the database
                    using (NpgsqlCommand command = connection.CreateCommand())
                    {
                        // PostgreSQL identifiers are case-sensitive, use quotes for exact naming
                        command.CommandText = $"CREATE DATABASE \"{databaseName}\" ENCODING 'UTF8'";
                        command.ExecuteNonQuery();
                    }
                }
            }
        }

        /// <inheritdoc/>
        public async Task CreateDatabaseIfNotExistsAsync(CancellationToken cancellationToken = default)
        {
            if (Settings == null)
            {
                throw new InvalidOperationException("Cannot create database when Settings is null. Use a constructor that provides connection settings.");
            }

            cancellationToken.ThrowIfCancellationRequested();

            string? databaseName = Settings.Database;
            if (string.IsNullOrWhiteSpace(databaseName))
            {
                throw new InvalidOperationException("Database name cannot be null or empty");
            }

            // Create a connection string without database specified (connect to postgres database)
            PostgresRepositorySettings tempSettings = new PostgresRepositorySettings
            {
                Hostname = Settings.Hostname,
                Port = Settings.Port,
                Username = Settings.Username,
                Password = Settings.Password,
                Database = "postgres" // Connect to default postgres database
            };

            string connectionString = tempSettings.BuildConnectionString();

            using (NpgsqlConnection connection = new NpgsqlConnection(connectionString))
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                // Check if database exists
                bool exists = PostgresSchemaBuilder.DatabaseExists(databaseName, connection);

                if (!exists)
                {
                    // Create the database
                    using (NpgsqlCommand command = connection.CreateCommand())
                    {
                        // PostgreSQL identifiers are case-sensitive, use quotes for exact naming
                        command.CommandText = $"CREATE DATABASE \"{databaseName}\" ENCODING 'UTF8'";
                        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }
                }
            }
        }

        #endregion

#pragma warning restore CS8600
    }
}