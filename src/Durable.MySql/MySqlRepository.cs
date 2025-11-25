namespace Durable.MySql
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
    using MySqlConnector;
    using Durable.ConcurrencyConflictResolvers;
    using Durable.DefaultValueProviders;

    /// <summary>
    /// MySQL Repository Implementation with Full Transaction Support and Connection Pooling.
    /// Provides comprehensive data access operations for entities with support for optimistic concurrency,
    /// batch operations, SQL capture, and advanced querying capabilities.
    /// </summary>
    /// <typeparam name="T">The entity type that this repository manages. Must be a class with a parameterless constructor.</typeparam>
    public class MySqlRepository<T> : IRepository<T>, IBatchInsertConfiguration, ISqlCapture, ISqlTrackingConfiguration, IDisposable where T : class, new()
    {
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
        /// MySQL can handle large batches efficiently, default is 1000 rows.
        /// </summary>
        public int MaxRowsPerBatch => _BatchConfig.MaxRowsPerBatch;

        /// <summary>
        /// Gets the maximum number of parameters per INSERT statement.
        /// MySQL has a high limit for parameters, default is 65535.
        /// </summary>
        public int MaxParametersPerStatement => _BatchConfig.MaxParametersPerStatement;

        /// <summary>
        /// Gets whether to use prepared statement reuse for batch operations.
        /// MySQL benefits from prepared statement reuse.
        /// </summary>
        public bool EnablePreparedStatementReuse => _BatchConfig.EnablePreparedStatementReuse;

        /// <summary>
        /// Gets whether to use multi-row INSERT syntax when possible.
        /// MySQL has excellent support for multi-row INSERT statements.
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
        internal readonly Dictionary<PropertyInfo, DefaultValueProviderInfo> _DefaultValueProviders;

        private volatile string? _LastExecutedSql;
        private volatile string? _LastExecutedSqlWithParameters;
        private readonly bool _OwnsConnectionFactory;
        private volatile bool _CaptureSql;
        private volatile bool _IncludeQueryInResults;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the MySqlRepository with a connection string and optional configuration.
        /// Creates an internal MySqlConnectionFactory for connection management.
        /// </summary>
        /// <param name="connectionString">The MySQL connection string used to connect to the database.</param>
        /// <param name="batchConfig">Optional batch insert configuration settings. Uses default settings if null.</param>
        /// <param name="dataTypeConverter">Optional data type converter for custom type handling. Uses default converter if null.</param>
        /// <param name="conflictResolver">Optional concurrency conflict resolver. Uses default resolver with ThrowException strategy if null.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the entity type T lacks required attributes (Entity, primary key).</exception>
        public MySqlRepository(string connectionString, IBatchInsertConfiguration? batchConfig = null, IDataTypeConverter? dataTypeConverter = null, IConcurrencyConflictResolver<T>? conflictResolver = null)
        {
            ArgumentNullException.ThrowIfNull(connectionString);
            Settings = MySqlRepositorySettings.Parse(connectionString);
            _ConnectionFactory = new MySqlConnectionFactory(connectionString);
            _OwnsConnectionFactory = true; // We created this factory, so we own it
            _Sanitizer = new MySqlSanitizer();
            _DataTypeConverter = dataTypeConverter ?? new MySqlDataTypeConverter();
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
        /// Initializes a new instance of the MySqlRepository with repository settings and optional configuration.
        /// Creates an internal MySqlConnectionFactory using the connection string built from settings.
        /// </summary>
        /// <param name="settings">The MySQL repository settings to use for configuration.</param>
        /// <param name="batchConfig">Optional batch insert configuration settings. Uses default settings if null.</param>
        /// <param name="dataTypeConverter">Optional data type converter for custom type handling. Uses default converter if null.</param>
        /// <param name="conflictResolver">Optional concurrency conflict resolver. Uses default resolver with ThrowException strategy if null.</param>
        /// <exception cref="ArgumentNullException">Thrown when settings is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the entity type T lacks required attributes (Entity, primary key), or when settings are invalid.</exception>
        public MySqlRepository(MySqlRepositorySettings settings, IBatchInsertConfiguration? batchConfig = null, IDataTypeConverter? dataTypeConverter = null, IConcurrencyConflictResolver<T>? conflictResolver = null)
        {
            ArgumentNullException.ThrowIfNull(settings);
            Settings = settings;
            string connectionString = settings.BuildConnectionString();
            _ConnectionFactory = new MySqlConnectionFactory(connectionString);
            _OwnsConnectionFactory = true; // We created this factory, so we own it
            _Sanitizer = new MySqlSanitizer();
            _DataTypeConverter = dataTypeConverter ?? new MySqlDataTypeConverter();
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
        /// Initializes a new instance of the MySqlRepository with a provided connection factory and optional configuration.
        /// Allows for shared connection pooling and factory management across multiple repository instances.
        /// Note: When using this constructor, the Settings property will be null as no connection string is directly provided.
        /// </summary>
        /// <param name="connectionFactory">The connection factory to use for database connections.</param>
        /// <param name="batchConfig">Optional batch insert configuration settings. Uses default settings if null.</param>
        /// <param name="dataTypeConverter">Optional data type converter for custom type handling. Uses default converter if null.</param>
        /// <param name="conflictResolver">Optional concurrency conflict resolver. Uses default resolver with ThrowException strategy if null.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionFactory is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the entity type T lacks required attributes (Entity, primary key).</exception>
        public MySqlRepository(IConnectionFactory connectionFactory, IBatchInsertConfiguration? batchConfig = null, IDataTypeConverter? dataTypeConverter = null, IConcurrencyConflictResolver<T>? conflictResolver = null)
        {
            _ConnectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
            _OwnsConnectionFactory = false; // External factory, we don't own it
            Settings = null!;
            _Sanitizer = new MySqlSanitizer();
            _DataTypeConverter = dataTypeConverter ?? new MySqlDataTypeConverter();
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
        public T? ReadFirst(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            IQueryBuilder<T> query = Query(transaction);
            if (predicate != null) query = query.Where(predicate);
            return query.Take(1).Execute().FirstOrDefault();
        }

        /// <summary>
        /// Reads the first entity that matches the specified predicate, or returns default if no match is found.
        /// </summary>
        /// <param name="predicate">Optional predicate to filter entities. If null, returns the first entity.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The first entity that matches the predicate, or default(T) if no match is found.</returns>
        public T? ReadFirstOrDefault(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            return ReadFirst(predicate, transaction);
        }

        /// <summary>
        /// Reads a single entity that matches the specified predicate. Throws an exception if zero or more than one entity is found.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The single entity that matches the predicate.</returns>
        /// <exception cref="InvalidOperationException">Thrown when zero or more than one entity matches the predicate.</exception>
        public T ReadSingle(Expression<Func<T, bool>> predicate, ITransaction? transaction = null)
        {
            List<T> results = Query(transaction).Where(predicate).Take(2).Execute().ToList();
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
        public T? ReadSingleOrDefault(Expression<Func<T, bool>> predicate, ITransaction? transaction = null)
        {
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
            if (predicate != null) query = query.Where(predicate);
            return query.Execute();
        }

        /// <summary>
        /// Reads all entities from the repository.
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
            if (id == null) throw new ArgumentNullException(nameof(id));
            return Query(transaction).Where(BuildIdPredicate(id)).Execute().FirstOrDefault();
        }

        /// <summary>
        /// Creates an advanced query builder for constructing complex queries.
        /// </summary>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>A query builder instance for the entity type.</returns>
        public IQueryBuilder<T> Query(ITransaction? transaction = null)
        {
            return new MySqlQueryBuilder<T>(this, transaction);
        }

        /// <summary>
        /// Clears the SQL generation cache and resets all cache statistics for this entity type.
        /// </summary>
        public void ClearSqlCache()
        {
            MySqlQueryBuilder<T>.ClearSqlCache();
        }

        /// <summary>
        /// Gets the number of SQL cache hits for this entity type.
        /// </summary>
        /// <returns>The total number of cache hits</returns>
        public long GetSqlCacheHitCount()
        {
            return MySqlQueryBuilder<T>.GetSqlCacheHitCount();
        }

        /// <summary>
        /// Gets the number of SQL cache misses for this entity type.
        /// </summary>
        /// <returns>The total number of cache misses</returns>
        public long GetSqlCacheMissCount()
        {
            return MySqlQueryBuilder<T>.GetSqlCacheMissCount();
        }

        /// <summary>
        /// Gets the total number of entries currently in the SQL cache for this entity type.
        /// </summary>
        /// <returns>The number of cached SQL strings</returns>
        public int GetSqlCacheEntryCount()
        {
            return MySqlQueryBuilder<T>.GetSqlCacheEntryCount();
        }

        /// <summary>
        /// Gets the SQL cache hit rate as a percentage for this entity type.
        /// </summary>
        /// <returns>The cache hit rate percentage (0.0 to 100.0)</returns>
        public double GetSqlCacheHitRate()
        {
            return MySqlQueryBuilder<T>.GetSqlCacheHitRate();
        }

        /// <summary>
        /// Begins a new database transaction.
        /// </summary>
        /// <returns>A new transaction instance.</returns>
        public ITransaction BeginTransaction()
        {
            MySqlConnection connection = (MySqlConnection)PooledConnectionHandle.Unwrap(_ConnectionFactory.GetConnection());
            EnsureConnectionOpen(connection);
            MySqlTransaction transaction = connection.BeginTransaction();
            return new MySqlRepositoryTransaction(connection, transaction, _ConnectionFactory);
        }

        /// <summary>
        /// Asynchronously begins a new database transaction.
        /// </summary>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with a new transaction instance.</returns>
        public async Task<ITransaction> BeginTransactionAsync(CancellationToken token = default)
        {
            MySqlConnection connection = (MySqlConnection)PooledConnectionHandle.Unwrap(await _ConnectionFactory.GetConnectionAsync(token).ConfigureAwait(false));
            await EnsureConnectionOpenAsync(connection, token).ConfigureAwait(false);
            MySqlTransaction transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);
            return new MySqlRepositoryTransaction(connection, transaction, _ConnectionFactory);
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Ensures the connection is open in a thread-safe manner.
        /// Handles race conditions where connection state might change between check and open operations.
        /// </summary>
        /// <param name="connection">The database connection to ensure is open</param>
        private static void EnsureConnectionOpen(System.Data.Common.DbConnection connection)
        {
            if (connection.State == ConnectionState.Open)
                return;

            try
            {
                connection.Open();
            }
            catch (InvalidOperationException)
            {
                // Connection might already be open due to race condition, verify state
                if (connection.State != ConnectionState.Open)
                    throw;
            }
        }

        /// <summary>
        /// Asynchronously ensures the connection is open in a thread-safe manner.
        /// Handles race conditions where connection state might change between check and open operations.
        /// </summary>
        /// <param name="connection">The database connection to ensure is open</param>
        /// <param name="token">Cancellation token for the async operation</param>
        private static async Task EnsureConnectionOpenAsync(System.Data.Common.DbConnection connection, CancellationToken token = default)
        {
            if (connection.State == ConnectionState.Open)
                return;

            try
            {
                await connection.OpenAsync(token).ConfigureAwait(false);
            }
            catch (InvalidOperationException)
            {
                // Connection might already be open due to race condition, verify state
                if (connection.State != ConnectionState.Open)
                    throw;
            }
        }

        /// <summary>
        /// Creates an approximation of the original entity state for MergeChangesResolver.
        /// This is needed because the repository doesn't track original entity state.
        /// </summary>
        /// <param name="currentEntity">The current entity state from the database</param>
        /// <param name="incomingEntity">The incoming entity from the client</param>
        /// <returns>An approximated original entity for merge operations</returns>
        private T CreateOriginalEntityApproximation(T currentEntity, T incomingEntity)
        {
            try
            {
                // Create a new instance for the original entity approximation
                T originalEntity = (T)Activator.CreateInstance(typeof(T))!;
                PropertyInfo[] properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);

                foreach (PropertyInfo property in properties)
                {
                    if (property.CanRead && property.CanWrite)
                    {
                        // For version column, use the incoming entity's version (what client originally had)
                        if (property.Name == "Version" ||
                            (_VersionColumnInfo != null && property.Name == _VersionColumnInfo.Property.Name))
                        {
                            property.SetValue(originalEntity, property.GetValue(incomingEntity));
                        }
                        else
                        {
                            // For merge scenarios, we create a hybrid approach:
                            // - If current and incoming values are different, use current (assuming server won)
                            // - If they're the same, use that value
                            object? currentValue = property.GetValue(currentEntity);
                            object? incomingValue = property.GetValue(incomingEntity);

                            // Use current entity's value as baseline - this assumes current represents
                            // a more recent state that we want to preserve during merge operations
                            property.SetValue(originalEntity, currentValue);
                        }
                    }
                }

                return originalEntity;
            }
            catch (Exception)
            {
                // If we can't create the approximation, fall back to using the incoming entity
                return incomingEntity;
            }
        }

        /// <summary>
        /// Safely converts a database result to the specified type using the data type converter.
        /// Handles type conversion failures gracefully with detailed error messages.
        /// </summary>
        /// <typeparam name="TResult">The target type to convert to</typeparam>
        /// <param name="result">The database result to convert</param>
        /// <returns>The converted result</returns>
        /// <exception cref="InvalidCastException">Thrown when the conversion fails with detailed type information</exception>
        private TResult SafeConvertDatabaseResult<TResult>(object? result)
        {
            if (result == DBNull.Value || result == null)
                return default(TResult)!;

            try
            {
                object? converted = _DataTypeConverter.ConvertFromDatabase(result, typeof(TResult));

                if (converted == null && !typeof(TResult).IsClass && Nullable.GetUnderlyingType(typeof(TResult)) == null)
                {
                    throw new InvalidCastException($"Cannot convert null to non-nullable value type '{typeof(TResult).Name}'");
                }

                if (converted != null && !typeof(TResult).IsAssignableFrom(converted.GetType()))
                {
                    throw new InvalidCastException($"Cannot cast '{converted.GetType().Name}' to '{typeof(TResult).Name}'. DataTypeConverter returned incompatible type.");
                }

                return (TResult)converted!;
            }
            catch (InvalidCastException)
            {
                throw; // Re-throw our own InvalidCastExceptions
            }
            catch (Exception ex)
            {
                throw new InvalidCastException($"Failed to convert database result of type '{result?.GetType().Name ?? "null"}' to '{typeof(TResult).Name}'. Original error: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Gets the entity name (table name) for this repository.
        /// </summary>
        /// <returns>The table name</returns>
        public string GetEntityName()
        {
            EntityAttribute? entityAttr = typeof(T).GetCustomAttribute<EntityAttribute>();
            if (entityAttr == null)
                throw new InvalidOperationException($"Type {typeof(T).Name} must be decorated with [Entity] attribute");
            return entityAttr.Name;
        }

        private PrimaryKeyInfo GetPrimaryKeyInfo()
        {
            PropertyInfo[] properties = typeof(T).GetProperties();
            PropertyInfo? pkProperty = properties.FirstOrDefault(p =>
                p.GetCustomAttribute<PropertyAttribute>()?.PropertyFlags.HasFlag(Flags.PrimaryKey) == true);

            if (pkProperty == null)
                throw new InvalidOperationException($"Type {typeof(T).Name} must have a property with [Property] attribute and PrimaryKey flag");

            PropertyAttribute? attr = pkProperty.GetCustomAttribute<PropertyAttribute>();
            return new PrimaryKeyInfo(attr!.Name, pkProperty);
        }

        /// <summary>
        /// Gets the column mappings dictionary for this repository.
        /// </summary>
        /// <returns>A dictionary mapping column names to PropertyInfo objects</returns>
        public Dictionary<string, PropertyInfo> GetColumnMappings()
        {
            Dictionary<string, PropertyInfo> mappings = new Dictionary<string, PropertyInfo>();
            PropertyInfo[] properties = typeof(T).GetProperties();

            foreach (PropertyInfo property in properties)
            {
                PropertyAttribute? attr = property.GetCustomAttribute<PropertyAttribute>();
                if (attr != null)
                {
                    mappings[attr.Name] = property;
                }
            }

            return new Dictionary<string, PropertyInfo>(mappings);
        }

        /// <summary>
        /// Gets all foreign key properties for this entity type.
        /// </summary>
        /// <returns>A dictionary mapping PropertyInfo to ForeignKeyAttribute</returns>
        public Dictionary<PropertyInfo, ForeignKeyAttribute> GetForeignKeys()
        {
            Dictionary<PropertyInfo, ForeignKeyAttribute> foreignKeys = new Dictionary<PropertyInfo, ForeignKeyAttribute>();
            PropertyInfo[] properties = typeof(T).GetProperties();

            foreach (PropertyInfo property in properties)
            {
                ForeignKeyAttribute? attr = property.GetCustomAttribute<ForeignKeyAttribute>();
                if (attr != null)
                {
                    foreignKeys[property] = attr;
                }
            }

            return foreignKeys;
        }

        /// <summary>
        /// Gets all navigation properties for this entity type.
        /// </summary>
        /// <returns>A dictionary mapping PropertyInfo to NavigationPropertyAttribute</returns>
        public Dictionary<PropertyInfo, NavigationPropertyAttribute> GetNavigationProperties()
        {
            Dictionary<PropertyInfo, NavigationPropertyAttribute> navigationProps = new Dictionary<PropertyInfo, NavigationPropertyAttribute>();
            PropertyInfo[] properties = typeof(T).GetProperties();

            foreach (PropertyInfo property in properties)
            {
                NavigationPropertyAttribute? attr = property.GetCustomAttribute<NavigationPropertyAttribute>();
                if (attr != null)
                {
                    navigationProps[property] = attr;
                }
            }

            return navigationProps;
        }

        /// <summary>
        /// Gets the version column information for this entity type.
        /// </summary>
        /// <returns>VersionColumnInfo if entity has versioning, null otherwise</returns>
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
        /// <returns>A dictionary mapping PropertyInfo to DefaultValueProviderInfo</returns>
        private Dictionary<PropertyInfo, DefaultValueProviderInfo> GetDefaultValueProviders()
        {
            Dictionary<PropertyInfo, DefaultValueProviderInfo> providers = new Dictionary<PropertyInfo, DefaultValueProviderInfo>();
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

                    providers[property] = new DefaultValueProviderInfo(attr, provider);
                }
            }

            return providers;
        }

        private Expression<Func<T, bool>> BuildIdPredicate(object id)
        {
            ParameterExpression parameter = Expression.Parameter(typeof(T), "x");
            MemberExpression property = Expression.Property(parameter, _PrimaryKeyProperty);
            ConstantExpression constant = Expression.Constant(id);
            BinaryExpression equal = Expression.Equal(property, constant);
            return Expression.Lambda<Func<T, bool>>(equal, parameter);
        }

        private string BuildInsertSql(T entity)
        {
            List<string> columns = new List<string>();
            List<string> values = new List<string>();

            foreach (KeyValuePair<string, PropertyInfo> mapping in _ColumnMappings)
            {
                string columnName = mapping.Key;
                PropertyInfo property = mapping.Value;

                // Skip auto-increment primary keys
                PropertyAttribute? attr = property.GetCustomAttribute<PropertyAttribute>();
                if (attr?.PropertyFlags.HasFlag(Flags.AutoIncrement) == true)
                    continue;

                object? value = property.GetValue(entity);
                columns.Add(_Sanitizer.SanitizeIdentifier(columnName));
                values.Add(_Sanitizer.FormatValue(value!));
            }

            string sql = $"INSERT INTO `{_TableName}` ({string.Join(", ", columns)}) VALUES ({string.Join(", ", values)})";
            return sql;
        }

        private int ExecuteSqlNonQuery(string sql, ITransaction? transaction = null)
        {
            if (transaction != null)
            {
                return ExecuteNonQueryWithConnection(transaction.Connection, sql, transaction.Transaction);
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                return ExecuteNonQueryWithConnection(connection, sql, null);
            }
        }

        private int ExecuteNonQueryWithConnection(System.Data.Common.DbConnection connection, string sql, System.Data.Common.DbTransaction? transaction)
        {
            return ExecuteNonQueryWithConnection(connection, sql, transaction, Array.Empty<SqlParameter>());
        }

        private int ExecuteNonQueryWithConnection(System.Data.Common.DbConnection connection, string sql, System.Data.Common.DbTransaction? transaction, params SqlParameter[] parameters)
        {
            EnsureConnectionOpen(connection);

            using MySqlCommand command = (MySqlCommand)connection.CreateCommand();
            command.CommandText = sql;

            if (transaction != null)
            {
                command.Transaction = (MySqlTransaction)transaction;
            }

            // Add parameters
            foreach (SqlParameter param in parameters)
            {
                MySqlParameter parameter = command.CreateParameter();
                parameter.ParameterName = param.Name;
                parameter.Value = param.Value ?? DBNull.Value;
                command.Parameters.Add(parameter);
            }

            // Capture SQL if enabled
            SetLastExecutedSql(sql);

            try
            {
                return command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error executing SQL: {sql}", ex);
            }
        }

        private TResult ExecuteScalarWithConnection<TResult>(System.Data.Common.DbConnection connection, string sql, System.Data.Common.DbTransaction? transaction, params SqlParameter[] parameters)
        {
            EnsureConnectionOpen(connection);

            using MySqlCommand command = (MySqlCommand)connection.CreateCommand();
            command.CommandText = sql;

            if (transaction != null)
            {
                command.Transaction = (MySqlTransaction)transaction;
            }

            // Add parameters
            foreach (SqlParameter param in parameters)
            {
                MySqlParameter parameter = command.CreateParameter();
                parameter.ParameterName = param.Name;
                parameter.Value = param.Value ?? DBNull.Value;
                command.Parameters.Add(parameter);
            }

            // Capture SQL if enabled
            SetLastExecutedSql(sql);

            try
            {
                object? result = command.ExecuteScalar();
                if (result == null || result == DBNull.Value)
                {
                    return default(TResult)!;
                }
                return (TResult)Convert.ChangeType(result, typeof(TResult));
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error executing SQL: {sql}", ex);
            }
        }

        /// <summary>
        /// Asynchronously executes a scalar SQL command and returns the result.
        /// </summary>
        /// <typeparam name="TResult">The type of the result to return</typeparam>
        /// <param name="connection">The database connection to use</param>
        /// <param name="sql">The SQL command to execute</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <param name="parameters">Parameters for the SQL command</param>
        /// <returns>The scalar result of the command execution</returns>
        /// <exception cref="InvalidOperationException">Thrown when SQL execution fails</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        private async Task<TResult> ExecuteScalarWithConnectionAsync<TResult>(System.Data.Common.DbConnection connection, string sql, System.Data.Common.DbTransaction? transaction, CancellationToken token, params SqlParameter[] parameters)
        {
            token.ThrowIfCancellationRequested();
            await EnsureConnectionOpenAsync(connection, token).ConfigureAwait(false);

            using MySqlCommand command = (MySqlCommand)connection.CreateCommand();
            command.CommandText = sql;

            if (transaction != null)
            {
                command.Transaction = (MySqlTransaction)transaction;
            }

            // Add parameters
            foreach (SqlParameter param in parameters)
            {
                MySqlParameter parameter = command.CreateParameter();
                parameter.ParameterName = param.Name;
                parameter.Value = param.Value ?? DBNull.Value;
                command.Parameters.Add(parameter);
            }

            // Capture SQL if enabled
            SetLastExecutedSql(sql);

            try
            {
                object? result = await command.ExecuteScalarAsync(token).ConfigureAwait(false);
                if (result == null || result == DBNull.Value)
                {
                    return default(TResult)!;
                }
                return (TResult)Convert.ChangeType(result, typeof(TResult));
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error executing SQL: {sql}", ex);
            }
        }

        /// <summary>
        /// Asynchronously executes a non-query SQL command and returns the number of affected rows.
        /// </summary>
        /// <param name="connection">The database connection to use</param>
        /// <param name="sql">The SQL command to execute</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <param name="parameters">Parameters for the SQL command</param>
        /// <returns>The number of rows affected by the command</returns>
        /// <exception cref="InvalidOperationException">Thrown when SQL execution fails</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        private async Task<int> ExecuteNonQueryWithConnectionAsync(System.Data.Common.DbConnection connection, string sql, System.Data.Common.DbTransaction? transaction, CancellationToken token, params SqlParameter[] parameters)
        {
            token.ThrowIfCancellationRequested();
            await EnsureConnectionOpenAsync(connection, token).ConfigureAwait(false);

            using MySqlCommand command = (MySqlCommand)connection.CreateCommand();
            command.CommandText = sql;

            if (transaction != null)
            {
                command.Transaction = (MySqlTransaction)transaction;
            }

            // Add parameters
            foreach (SqlParameter param in parameters)
            {
                MySqlParameter parameter = command.CreateParameter();
                parameter.ParameterName = param.Name;
                parameter.Value = param.Value ?? DBNull.Value;
                command.Parameters.Add(parameter);
            }

            // Capture SQL if enabled
            SetLastExecutedSql(sql);

            try
            {
                return await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error executing SQL: {sql}", ex);
            }
        }

        /// <summary>
        /// Sets the last executed SQL for SQL capture functionality.
        /// </summary>
        /// <param name="sql">The SQL statement that was executed</param>
        internal void SetLastExecutedSql(string sql)
        {
            if (CaptureSql)
            {
                _LastExecutedSql = sql;
                // For now, just set the same value for both properties
                // In a full implementation, this would substitute parameter values
                _LastExecutedSqlWithParameters = sql;
            }
        }

        /// <summary>
        /// Disposes the repository and releases associated resources.
        /// Only disposes the connection factory if this repository created it internally.
        /// </summary>
        public void Dispose()
        {
            if (_OwnsConnectionFactory)
            {
                _ConnectionFactory?.Dispose();
            }
        }

        #endregion

        #region Not-Yet-Implemented

        // The following methods need to be implemented to complete the IRepository<T> interface
        // They will be added incrementally based on the SQLite patterns

        /// <summary>
        /// Asynchronously reads the first entity that matches the optional predicate.
        /// </summary>
        /// <param name="predicate">Optional predicate to filter entities. If null, returns the first entity</param>
        /// <param name="transaction">Optional transaction to execute the operation within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>The first matching entity, or null if no entities match the criteria</returns>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        public async Task<T?> ReadFirstAsync(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            IQueryBuilder<T> query = Query(transaction);
            if (predicate != null)
                query = query.Where(predicate);

            IEnumerable<T> results = await query.Take(1).ExecuteAsync(token).ConfigureAwait(false);
            return results.FirstOrDefault();
        }

        /// <summary>
        /// Asynchronously reads the first entity that matches the specified predicate, or returns default if no match is found.
        /// </summary>
        /// <param name="predicate">Optional predicate to filter entities. If null, returns the first entity</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>The first entity that matches the predicate, or default(T) if no match is found</returns>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        public async Task<T?> ReadFirstOrDefaultAsync(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            return await ReadFirstAsync(predicate, transaction, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Asynchronously reads a single entity that matches the specified predicate. Throws an exception if zero or more than one entity is found.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>The single entity that matches the predicate</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when zero or more than one entity matches the predicate</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        public async Task<T> ReadSingleAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));

            token.ThrowIfCancellationRequested();

            IEnumerable<T> results = await Query(transaction).Where(predicate).Take(2).ExecuteAsync(token).ConfigureAwait(false);
            List<T> resultsList = results.ToList();

            if (resultsList.Count != 1)
                throw new InvalidOperationException($"Expected exactly 1 result but found {resultsList.Count}");

            return resultsList[0];
        }

        /// <summary>
        /// Asynchronously reads a single entity that matches the specified predicate, or returns default if no match is found. Throws an exception if more than one entity is found.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>The single entity that matches the predicate, or default(T) if no match is found</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when more than one entity matches the predicate</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        public async Task<T?> ReadSingleOrDefaultAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));

            token.ThrowIfCancellationRequested();

            IEnumerable<T> results = await Query(transaction).Where(predicate).Take(2).ExecuteAsync(token).ConfigureAwait(false);
            List<T> resultsList = results.ToList();

            if (resultsList.Count > 1)
                throw new InvalidOperationException($"Expected 0 or 1 result but found {resultsList.Count}");

            return resultsList.FirstOrDefault();
        }

        /// <summary>
        /// Asynchronously reads multiple entities that match the optional predicate as an async enumerable.
        /// </summary>
        /// <param name="predicate">Optional predicate to filter entities. If null, returns all entities</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>An async enumerable of entities that match the predicate</returns>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
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
        /// Asynchronously reads all entities as an async enumerable.
        /// </summary>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>An async enumerable of all entities</returns>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        public IAsyncEnumerable<T> ReadAllAsync(ITransaction? transaction = null, CancellationToken token = default)
        {
            return ReadManyAsync(null, transaction, token);
        }

        /// <summary>
        /// Asynchronously reads an entity by its identifier.
        /// </summary>
        /// <param name="id">The identifier of the entity to read</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>The entity with the specified identifier, or null if not found</returns>
        /// <exception cref="ArgumentNullException">Thrown when id is null</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        public async Task<T?> ReadByIdAsync(object id, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            token.ThrowIfCancellationRequested();

            IEnumerable<T> results = await Query(transaction).Where(BuildIdPredicate(id)).ExecuteAsync(token).ConfigureAwait(false);
            return results.FirstOrDefault();
        }

        /// <summary>
        /// Checks if any entity exists that matches the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>True if any entity matches the predicate, false otherwise.</returns>
        public bool Exists(Expression<Func<T, bool>> predicate, ITransaction? transaction = null)
        {
            return Query(transaction).Where(predicate).Take(1).Execute().Any();
        }

        /// <summary>
        /// Checks if an entity exists with the specified identifier.
        /// </summary>
        /// <param name="id">The identifier to check for.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>True if an entity with the specified id exists, false otherwise.</returns>
        /// <exception cref="ArgumentNullException">Thrown when id is null.</exception>
        public bool ExistsById(object id, ITransaction? transaction = null)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            return Query(transaction).Where(BuildIdPredicate(id)).Take(1).Execute().Any();
        }

        /// <summary>
        /// Asynchronously checks if any entity exists that matches the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>True if any entity matches the predicate, false otherwise</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        public async Task<bool> ExistsAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));

            token.ThrowIfCancellationRequested();

            IEnumerable<T> results = await Query(transaction).Where(predicate).Take(1).ExecuteAsync(token).ConfigureAwait(false);
            return results.Any();
        }

        /// <summary>
        /// Asynchronously checks if an entity exists with the specified identifier.
        /// </summary>
        /// <param name="id">The identifier to check for</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>True if an entity with the specified id exists, false otherwise</returns>
        /// <exception cref="ArgumentNullException">Thrown when id is null</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        public async Task<bool> ExistsByIdAsync(object id, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            return await ExistsAsync(BuildIdPredicate(id), transaction, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Counts the number of entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">Optional predicate to filter entities. If null, counts all entities.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The number of entities that match the predicate.</returns>
        public int Count(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            // Use ambient transaction if no explicit transaction provided
            transaction ??= TransactionScope.Current?.Transaction;

            string sql = $"SELECT COUNT(*) FROM `{_TableName}`";
            List<SqlParameter> parameters = new List<SqlParameter>();

            if (predicate != null)
            {
                MySqlExpressionParser<T> parser = new MySqlExpressionParser<T>(_ColumnMappings, _Sanitizer);
                string whereClause = parser.ParseExpressionWithParameters(predicate.Body);
                sql += $" WHERE {whereClause}";
                foreach (var p in parser.GetParameters()) parameters.Add(new SqlParameter(p.name, p.value));
            }

            if (transaction != null)
            {
                return ExecuteScalarWithConnection<int>(transaction.Connection, sql, transaction.Transaction, parameters.ToArray());
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                return ExecuteScalarWithConnection<int>(connection, sql, null, parameters.ToArray());
            }
        }

        /// <summary>
        /// Asynchronously counts the number of entities that match the optional predicate.
        /// </summary>
        /// <param name="predicate">Optional predicate to filter entities. If null, counts all entities</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>The number of entities that match the predicate</returns>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        public async Task<int> CountAsync(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            // Use ambient transaction if no explicit transaction provided
            transaction ??= TransactionScope.Current?.Transaction;

            string sql = $"SELECT COUNT(*) FROM `{_TableName}`";
            List<SqlParameter> parameters = new List<SqlParameter>();

            if (predicate != null)
            {
                MySqlExpressionParser<T> parser = new MySqlExpressionParser<T>(_ColumnMappings, _Sanitizer);
                string whereClause = parser.ParseExpressionWithParameters(predicate.Body);
                sql += $" WHERE {whereClause}";
                foreach (var p in parser.GetParameters()) parameters.Add(new SqlParameter(p.name, p.value));
            }

            if (transaction != null)
            {
                return await ExecuteScalarWithConnectionAsync<int>(transaction.Connection, sql, transaction.Transaction, token, parameters.ToArray()).ConfigureAwait(false);
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                return await ExecuteScalarWithConnectionAsync<int>(connection, sql, null, token, parameters.ToArray()).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Finds the maximum value of a property for entities that match the optional predicate.
        /// </summary>
        /// <typeparam name="TResult">The type of the property being compared</typeparam>
        /// <param name="selector">Expression that selects the property to find the maximum of</param>
        /// <param name="predicate">Optional predicate to filter entities. If null, considers all entities</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <returns>The maximum value of the selected property</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when SQL execution fails</exception>
        public TResult Max<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            MySqlExpressionParser<T> parser = new MySqlExpressionParser<T>(_ColumnMappings, _Sanitizer);
            string column = parser.GetColumnFromExpression(selector.Body);

            StringBuilder sql = new StringBuilder($"SELECT MAX({column}) FROM `{_TableName}`");

            List<SqlParameter> parameters = new List<SqlParameter>();

            if (predicate != null)
            {
                string whereClause = parser.ParseExpressionWithParameters(predicate.Body);
                sql.Append($" WHERE {whereClause}");
                foreach (var p in parser.GetParameters()) parameters.Add(new SqlParameter(p.name, p.value));
            }

            if (transaction != null)
            {
                object? result = ExecuteScalarWithConnection<object>(transaction.Connection, sql.ToString(), transaction.Transaction, parameters.ToArray());
                return SafeConvertDatabaseResult<TResult>(result);
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                object? result = ExecuteScalarWithConnection<object>(connection, sql.ToString(), null, parameters.ToArray());
                return SafeConvertDatabaseResult<TResult>(result);
            }
        }

        /// <summary>
        /// Finds the minimum value of a property for entities that match the optional predicate.
        /// </summary>
        /// <typeparam name="TResult">The type of the property being compared</typeparam>
        /// <param name="selector">Expression that selects the property to find the minimum of</param>
        /// <param name="predicate">Optional predicate to filter entities. If null, considers all entities</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <returns>The minimum value of the selected property</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when SQL execution fails</exception>
        public TResult Min<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            MySqlExpressionParser<T> parser = new MySqlExpressionParser<T>(_ColumnMappings, _Sanitizer);
            string column = parser.GetColumnFromExpression(selector.Body);

            StringBuilder sql = new StringBuilder($"SELECT MIN({column}) FROM `{_TableName}`");

            List<SqlParameter> parameters = new List<SqlParameter>();

            if (predicate != null)
            {
                string whereClause = parser.ParseExpressionWithParameters(predicate.Body);
                sql.Append($" WHERE {whereClause}");
                foreach (var p in parser.GetParameters()) parameters.Add(new SqlParameter(p.name, p.value));
            }

            if (transaction != null)
            {
                object? result = ExecuteScalarWithConnection<object>(transaction.Connection, sql.ToString(), transaction.Transaction, parameters.ToArray());
                return SafeConvertDatabaseResult<TResult>(result);
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                object? result = ExecuteScalarWithConnection<object>(connection, sql.ToString(), null, parameters.ToArray());
                return SafeConvertDatabaseResult<TResult>(result);
            }
        }

        /// <summary>
        /// Calculates the average value of a decimal property for entities that match the optional predicate.
        /// </summary>
        /// <param name="selector">Expression that selects the decimal property to average</param>
        /// <param name="predicate">Optional predicate to filter entities. If null, considers all entities</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <returns>The average value of the selected property</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when SQL execution fails</exception>
        public decimal Average(Expression<Func<T, decimal>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            MySqlExpressionParser<T> parser = new MySqlExpressionParser<T>(_ColumnMappings, _Sanitizer);
            string column = parser.GetColumnFromExpression(selector.Body);

            // MySQL doesn't need explicit casting like SQLite, AVG function works with numeric types
            StringBuilder sql = new StringBuilder($"SELECT COALESCE(AVG({column}), 0) FROM `{_TableName}`");

            List<SqlParameter> parameters = new List<SqlParameter>();

            if (predicate != null)
            {
                string whereClause = parser.ParseExpressionWithParameters(predicate.Body);
                sql.Append($" WHERE {whereClause}");
                foreach (var p in parser.GetParameters()) parameters.Add(new SqlParameter(p.name, p.value));
            }

            if (transaction != null)
            {
                object? result = ExecuteScalarWithConnection<object>(transaction.Connection, sql.ToString(), transaction.Transaction, parameters.ToArray());
                return result == DBNull.Value || result == null ? 0m : (decimal)_DataTypeConverter.ConvertFromDatabase(result, typeof(decimal))!;
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                object? result = ExecuteScalarWithConnection<object>(connection, sql.ToString(), null, parameters.ToArray());
                return result == DBNull.Value || result == null ? 0m : (decimal)_DataTypeConverter.ConvertFromDatabase(result, typeof(decimal))!;
            }
        }

        /// <summary>
        /// Calculates the sum of a decimal property for entities that match the optional predicate.
        /// </summary>
        /// <param name="selector">Expression that selects the decimal property to sum</param>
        /// <param name="predicate">Optional predicate to filter entities. If null, considers all entities</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <returns>The sum of the selected property</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when SQL execution fails</exception>
        public decimal Sum(Expression<Func<T, decimal>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            MySqlExpressionParser<T> parser = new MySqlExpressionParser<T>(_ColumnMappings, _Sanitizer);
            string column = parser.GetColumnFromExpression(selector.Body);

            StringBuilder sql = new StringBuilder($"SELECT COALESCE(SUM({column}), 0) FROM `{_TableName}`");

            List<SqlParameter> parameters = new List<SqlParameter>();

            if (predicate != null)
            {
                string whereClause = parser.ParseExpressionWithParameters(predicate.Body);
                sql.Append($" WHERE {whereClause}");
                foreach (var p in parser.GetParameters()) parameters.Add(new SqlParameter(p.name, p.value));
            }

            if (transaction != null)
            {
                object? result = ExecuteScalarWithConnection<object>(transaction.Connection, sql.ToString(), transaction.Transaction, parameters.ToArray());
                return result == DBNull.Value || result == null ? 0m : (decimal)_DataTypeConverter.ConvertFromDatabase(result, typeof(decimal))!;
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                object? result = ExecuteScalarWithConnection<object>(connection, sql.ToString(), null, parameters.ToArray());
                return result == DBNull.Value || result == null ? 0m : (decimal)_DataTypeConverter.ConvertFromDatabase(result, typeof(decimal))!;
            }
        }

        /// <summary>
        /// Asynchronously finds the maximum value of a property for entities that match the optional predicate.
        /// </summary>
        /// <typeparam name="TResult">The type of the property being compared</typeparam>
        /// <param name="selector">Expression that selects the property to find the maximum of</param>
        /// <param name="predicate">Optional predicate to filter entities. If null, considers all entities</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>The maximum value of the selected property</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        /// <exception cref="InvalidOperationException">Thrown when SQL execution fails</exception>
        public async Task<TResult> MaxAsync<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            token.ThrowIfCancellationRequested();

            MySqlExpressionParser<T> parser = new MySqlExpressionParser<T>(_ColumnMappings, _Sanitizer);
            string column = parser.GetColumnFromExpression(selector.Body);

            StringBuilder sql = new StringBuilder($"SELECT MAX({column}) FROM `{_TableName}`");

            List<SqlParameter> parameters = new List<SqlParameter>();

            if (predicate != null)
            {
                string whereClause = parser.ParseExpressionWithParameters(predicate.Body);
                sql.Append($" WHERE {whereClause}");
                foreach (var p in parser.GetParameters()) parameters.Add(new SqlParameter(p.name, p.value));
            }

            if (transaction != null)
            {
                object? result = await ExecuteScalarWithConnectionAsync<object>(transaction.Connection, sql.ToString(), transaction.Transaction, token, parameters.ToArray()).ConfigureAwait(false);
                return SafeConvertDatabaseResult<TResult>(result);
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                object? result = await ExecuteScalarWithConnectionAsync<object>(connection, sql.ToString(), null, token, parameters.ToArray()).ConfigureAwait(false);
                return SafeConvertDatabaseResult<TResult>(result);
            }
        }

        /// <summary>
        /// Asynchronously finds the minimum value of a property for entities that match the optional predicate.
        /// </summary>
        /// <typeparam name="TResult">The type of the property being compared</typeparam>
        /// <param name="selector">Expression that selects the property to find the minimum of</param>
        /// <param name="predicate">Optional predicate to filter entities. If null, considers all entities</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>The minimum value of the selected property</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        /// <exception cref="InvalidOperationException">Thrown when SQL execution fails</exception>
        public async Task<TResult> MinAsync<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            token.ThrowIfCancellationRequested();

            MySqlExpressionParser<T> parser = new MySqlExpressionParser<T>(_ColumnMappings, _Sanitizer);
            string column = parser.GetColumnFromExpression(selector.Body);

            StringBuilder sql = new StringBuilder($"SELECT MIN({column}) FROM `{_TableName}`");

            List<SqlParameter> parameters = new List<SqlParameter>();

            if (predicate != null)
            {
                string whereClause = parser.ParseExpressionWithParameters(predicate.Body);
                sql.Append($" WHERE {whereClause}");
                foreach (var p in parser.GetParameters()) parameters.Add(new SqlParameter(p.name, p.value));
            }

            if (transaction != null)
            {
                object? result = await ExecuteScalarWithConnectionAsync<object>(transaction.Connection, sql.ToString(), transaction.Transaction, token, parameters.ToArray()).ConfigureAwait(false);
                return SafeConvertDatabaseResult<TResult>(result);
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                object? result = await ExecuteScalarWithConnectionAsync<object>(connection, sql.ToString(), null, token, parameters.ToArray()).ConfigureAwait(false);
                return SafeConvertDatabaseResult<TResult>(result);
            }
        }

        /// <summary>
        /// Asynchronously calculates the average value of a decimal property for entities that match the optional predicate.
        /// </summary>
        /// <param name="selector">Expression that selects the decimal property to average</param>
        /// <param name="predicate">Optional predicate to filter entities. If null, considers all entities</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>The average value of the selected property</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        /// <exception cref="InvalidOperationException">Thrown when SQL execution fails</exception>
        public async Task<decimal> AverageAsync(Expression<Func<T, decimal>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            token.ThrowIfCancellationRequested();

            MySqlExpressionParser<T> parser = new MySqlExpressionParser<T>(_ColumnMappings, _Sanitizer);
            string column = parser.GetColumnFromExpression(selector.Body);

            StringBuilder sql = new StringBuilder($"SELECT COALESCE(AVG({column}), 0) FROM `{_TableName}`");

            List<SqlParameter> parameters = new List<SqlParameter>();

            if (predicate != null)
            {
                string whereClause = parser.ParseExpressionWithParameters(predicate.Body);
                sql.Append($" WHERE {whereClause}");
                foreach (var p in parser.GetParameters()) parameters.Add(new SqlParameter(p.name, p.value));
            }

            if (transaction != null)
            {
                object? result = await ExecuteScalarWithConnectionAsync<object>(transaction.Connection, sql.ToString(), transaction.Transaction, token, parameters.ToArray()).ConfigureAwait(false);
                return result == DBNull.Value || result == null ? 0m : (decimal)_DataTypeConverter.ConvertFromDatabase(result, typeof(decimal))!;
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                object? result = await ExecuteScalarWithConnectionAsync<object>(connection, sql.ToString(), null, token, parameters.ToArray()).ConfigureAwait(false);
                return result == DBNull.Value || result == null ? 0m : (decimal)_DataTypeConverter.ConvertFromDatabase(result, typeof(decimal))!;
            }
        }

        /// <summary>
        /// Asynchronously calculates the sum of a decimal property for entities that match the optional predicate.
        /// </summary>
        /// <param name="selector">Expression that selects the decimal property to sum</param>
        /// <param name="predicate">Optional predicate to filter entities. If null, considers all entities</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>The sum of the selected property</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        /// <exception cref="InvalidOperationException">Thrown when SQL execution fails</exception>
        public async Task<decimal> SumAsync(Expression<Func<T, decimal>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            token.ThrowIfCancellationRequested();

            MySqlExpressionParser<T> parser = new MySqlExpressionParser<T>(_ColumnMappings, _Sanitizer);
            string column = parser.GetColumnFromExpression(selector.Body);

            StringBuilder sql = new StringBuilder($"SELECT COALESCE(SUM({column}), 0) FROM `{_TableName}`");

            List<SqlParameter> parameters = new List<SqlParameter>();

            if (predicate != null)
            {
                string whereClause = parser.ParseExpressionWithParameters(predicate.Body);
                sql.Append($" WHERE {whereClause}");
                foreach (var p in parser.GetParameters()) parameters.Add(new SqlParameter(p.name, p.value));
            }

            if (transaction != null)
            {
                object? result = await ExecuteScalarWithConnectionAsync<object>(transaction.Connection, sql.ToString(), transaction.Transaction, token, parameters.ToArray()).ConfigureAwait(false);
                return result == DBNull.Value || result == null ? 0m : (decimal)_DataTypeConverter.ConvertFromDatabase(result, typeof(decimal))!;
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                object? result = await ExecuteScalarWithConnectionAsync<object>(connection, sql.ToString(), null, token, parameters.ToArray()).ConfigureAwait(false);
                return result == DBNull.Value || result == null ? 0m : (decimal)_DataTypeConverter.ConvertFromDatabase(result, typeof(decimal))!;
            }
        }

        /// <summary>
        /// Creates a new entity in the repository.
        /// </summary>
        /// <param name="entity">The entity to create.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The created entity with any auto-generated values populated.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null.</exception>
        public T Create(T entity, ITransaction? transaction = null)
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));

            // Use ambient transaction if no explicit transaction provided
            transaction ??= TransactionScope.Current?.Transaction;

            // Apply default values from providers before insert
            foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
            {
                PropertyInfo property = kvp.Value;

                // Check if this property has a default value provider
                if (_DefaultValueProviders.TryGetValue(property, out DefaultValueProviderInfo? providerInfo))
                {
                    DefaultValueAttribute attr = providerInfo.Attribute;
                    IDefaultValueProvider provider = providerInfo.Provider;
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
            List<SqlParameter> parameters = new List<SqlParameter>();

            foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
            {
                string columnName = kvp.Key;
                PropertyInfo property = kvp.Value;

                // Skip auto-increment primary keys
                PropertyAttribute? attr = property.GetCustomAttribute<PropertyAttribute>();
                if (attr?.PropertyFlags.HasFlag(Flags.AutoIncrement) == true)
                    continue;

                object? value = property.GetValue(entity);
                columns.Add($"`{columnName}`");
                parameters.Add(new SqlParameter($"@{columnName}", _DataTypeConverter.ConvertToDatabase(value!, property.PropertyType, property)));
            }

            string insertSql = $"INSERT INTO `{_TableName}` ({string.Join(", ", columns)}) VALUES ({string.Join(", ", parameters.Select(p => p.Name))})";

            // Check if we have an auto-increment primary key
            PropertyAttribute? pkAttr = _PrimaryKeyProperty.GetCustomAttribute<PropertyAttribute>();
            bool hasAutoIncrement = pkAttr?.PropertyFlags.HasFlag(Flags.AutoIncrement) == true;

            if (hasAutoIncrement)
            {
                // Get the last inserted ID
                object? insertedId;
                if (transaction != null)
                {
                    ExecuteNonQueryWithConnection(transaction.Connection, insertSql, transaction.Transaction, parameters.ToArray());
                    insertedId = ExecuteScalarWithConnection<object>(transaction.Connection, "SELECT LAST_INSERT_ID()", transaction.Transaction);
                }
                else
                {
                    using DbConnection connection = _ConnectionFactory.GetConnection();
                    ExecuteNonQueryWithConnection(connection, insertSql, null, parameters.ToArray());
                    insertedId = ExecuteScalarWithConnection<object>(connection, "SELECT LAST_INSERT_ID()", null);
                }

                // Set the ID on the entity
                if (insertedId != null && insertedId != DBNull.Value)
                {
                    object? convertedId = _DataTypeConverter.ConvertFromDatabase(insertedId, _PrimaryKeyProperty.PropertyType, _PrimaryKeyProperty);
                    _PrimaryKeyProperty.SetValue(entity, convertedId);
                }
            }
            else
            {
                // No auto-increment, just execute the insert
                if (transaction != null)
                {
                    ExecuteNonQueryWithConnection(transaction.Connection, insertSql, transaction.Transaction, parameters.ToArray());
                }
                else
                {
                    using DbConnection connection = _ConnectionFactory.GetConnection();
                    ExecuteNonQueryWithConnection(connection, insertSql, null, parameters.ToArray());
                }
            }

            return entity;
        }

        /// <summary>
        /// Creates multiple entities in the repository using optimized batch insert operations.
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
        /// <param name="entity">The entity to create</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>The created entity with any auto-generated values populated</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        public async Task<T> CreateAsync(T entity, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));

            token.ThrowIfCancellationRequested();

            // Use ambient transaction if no explicit transaction provided
            transaction ??= TransactionScope.Current?.Transaction;

            // Apply default values from providers before insert
            foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
            {
                PropertyInfo property = kvp.Value;

                // Check if this property has a default value provider
                if (_DefaultValueProviders.TryGetValue(property, out DefaultValueProviderInfo? providerInfo))
                {
                    DefaultValueAttribute attr = providerInfo.Attribute;
                    IDefaultValueProvider provider = providerInfo.Provider;
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
            List<SqlParameter> parameters = new List<SqlParameter>();

            foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
            {
                string columnName = kvp.Key;
                PropertyInfo property = kvp.Value;

                // Skip auto-increment primary keys
                PropertyAttribute? attr = property.GetCustomAttribute<PropertyAttribute>();
                if (attr?.PropertyFlags.HasFlag(Flags.AutoIncrement) == true)
                    continue;

                object? value = property.GetValue(entity);
                columns.Add($"`{columnName}`");
                parameters.Add(new SqlParameter($"@{columnName}", _DataTypeConverter.ConvertToDatabase(value!, property.PropertyType, property)));
            }

            string insertSql = $"INSERT INTO `{_TableName}` ({string.Join(", ", columns)}) VALUES ({string.Join(", ", parameters.Select(p => p.Name))})";

            // Check if we have an auto-increment primary key
            PropertyAttribute? pkAttr = _PrimaryKeyProperty.GetCustomAttribute<PropertyAttribute>();
            bool hasAutoIncrement = pkAttr?.PropertyFlags.HasFlag(Flags.AutoIncrement) == true;

            if (hasAutoIncrement)
            {
                // Get the last inserted ID
                object? insertedId;
                if (transaction != null)
                {
                    await ExecuteNonQueryWithConnectionAsync(transaction.Connection, insertSql, transaction.Transaction, token, parameters.ToArray()).ConfigureAwait(false);
                    insertedId = await ExecuteScalarWithConnectionAsync<object>(transaction.Connection, "SELECT LAST_INSERT_ID()", transaction.Transaction, token).ConfigureAwait(false);
                }
                else
                {
                    using DbConnection connection = _ConnectionFactory.GetConnection();
                    await ExecuteNonQueryWithConnectionAsync(connection, insertSql, null, token, parameters.ToArray()).ConfigureAwait(false);
                    insertedId = await ExecuteScalarWithConnectionAsync<object>(connection, "SELECT LAST_INSERT_ID()", null, token).ConfigureAwait(false);
                }

                // Set the ID on the entity
                if (insertedId != null && insertedId != DBNull.Value)
                {
                    object? convertedId = _DataTypeConverter.ConvertFromDatabase(insertedId, _PrimaryKeyProperty.PropertyType, _PrimaryKeyProperty);
                    _PrimaryKeyProperty.SetValue(entity, convertedId);
                }
            }
            else
            {
                // No auto-increment, just execute the insert
                if (transaction != null)
                {
                    await ExecuteNonQueryWithConnectionAsync(transaction.Connection, insertSql, transaction.Transaction, token, parameters.ToArray()).ConfigureAwait(false);
                }
                else
                {
                    using DbConnection connection = _ConnectionFactory.GetConnection();
                    await ExecuteNonQueryWithConnectionAsync(connection, insertSql, null, token, parameters.ToArray()).ConfigureAwait(false);
                }
            }

            return entity;
        }

        /// <summary>
        /// Asynchronously creates multiple entities in the database.
        /// </summary>
        /// <param name="entities">The entities to create</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>The created entities with any auto-generated values populated</returns>
        /// <exception cref="ArgumentNullException">Thrown when entities is null</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
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

        /// <summary>
        /// Updates an existing entity in the repository.
        /// </summary>
        /// <param name="entity">The entity to update.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The updated entity.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null.</exception>
        public T Update(T entity, ITransaction? transaction = null)
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));

            List<string> setPairs = new List<string>();
            List<SqlParameter> parameters = new List<SqlParameter>();
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
                    setPairs.Add($"`{columnName}` = @new_version");
                    object? convertedNewVersion = _DataTypeConverter.ConvertToDatabase(newVersion!, _VersionColumnInfo.PropertyType, property);
                    parameters.Add(new SqlParameter($"@new_version", convertedNewVersion));
                    _VersionColumnInfo.SetValue(entity, newVersion);
                }
                else
                {
                    setPairs.Add($"`{columnName}` = @{columnName}");
                    object? convertedValue = _DataTypeConverter.ConvertToDatabase(value!, property.PropertyType, property);
                    parameters.Add(new SqlParameter($"@{columnName}", convertedValue));
                }
            }

            if (idValue == null)
                throw new InvalidOperationException("Cannot update entity with null primary key");

            parameters.Add(new SqlParameter($"@id", idValue));

            string sql;
            if (_VersionColumnInfo != null)
            {
                sql = $"UPDATE `{_TableName}` SET {string.Join(", ", setPairs)} WHERE `{_PrimaryKeyColumn}` = @id AND `{_VersionColumnInfo.ColumnName}` = @current_version";
                parameters.Add(new SqlParameter($"@current_version", currentVersion));
            }
            else
            {
                sql = $"UPDATE `{_TableName}` SET {string.Join(", ", setPairs)} WHERE `{_PrimaryKeyColumn}` = @id";
            }

            int rowsAffected;
            if (transaction != null)
            {
                rowsAffected = ExecuteNonQueryWithConnection(transaction.Connection, sql, transaction.Transaction, parameters.ToArray());
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                rowsAffected = ExecuteNonQueryWithConnection(connection, sql, null, parameters.ToArray());
            }

            if (rowsAffected == 0)
            {
                if (_VersionColumnInfo != null)
                {
                    // Optimistic concurrency conflict detected - attempt resolution
                    try
                    {
                        // Read the current entity from database to get its current state
                        T? currentEntity = ReadById(idValue, transaction);

                        if (currentEntity == null)
                        {
                            // Entity was deleted - throw appropriate exception
                            throw new OptimisticConcurrencyException($"Entity with ID {idValue} was deleted by another process");
                        }

                        // Attempt to resolve the conflict using the configured resolver
                        // For MergeChangesResolver, we create an original entity by taking the current entity's values
                        // but with the incoming entity's version - this approximates the original state the client had
                        T originalEntity = entity;

                        if (_ConflictResolver is MergeChangesResolver<T> merger)
                        {
                            // Create a better approximation of the original entity for merge operations
                            // We'll assume the current entity represents the state before the concurrent modification
                            // but with the incoming entity's version to match what the client originally fetched
                            originalEntity = CreateOriginalEntityApproximation(currentEntity, entity);
                        }

                        if (_ConflictResolver.TryResolveConflict(currentEntity, entity, originalEntity, _ConflictResolver.DefaultStrategy, out T resolvedEntity))
                        {
                            // Conflict resolved successfully - retry the update with resolved entity
                            return Update(resolvedEntity, transaction);
                        }
                        else
                        {
                            // Resolver couldn't resolve the conflict
                            throw new OptimisticConcurrencyException($"Optimistic concurrency conflict detected for entity with ID {idValue} and could not be resolved");
                        }
                    }
                    catch (OptimisticConcurrencyException)
                    {
                        // Re-throw concurrency exceptions as-is
                        throw;
                    }
                    catch (Exception ex)
                    {
                        // Wrap other exceptions in concurrency exception
                        throw new OptimisticConcurrencyException($"Error during conflict resolution for entity with ID {idValue}: {ex.Message}", ex);
                    }
                }
                else
                {
                    throw new InvalidOperationException($"No rows were affected during update for entity with ID {idValue}");
                }
            }

            return entity;
        }

        /// <summary>
        /// Updates multiple entities that match the specified predicate by applying an update action to each entity.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities to update</param>
        /// <param name="updateAction">The action to apply to each entity before updating</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <returns>The number of entities that were updated</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate or updateAction is null</exception>
        public int UpdateMany(Expression<Func<T, bool>> predicate, Action<T> updateAction, ITransaction? transaction = null)
        {
            if (predicate == null) throw new ArgumentNullException(nameof(predicate));
            if (updateAction == null) throw new ArgumentNullException(nameof(updateAction));

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
        /// Updates a specific field for multiple entities that match the specified predicate.
        /// </summary>
        /// <typeparam name="TField">The type of the field being updated</typeparam>
        /// <param name="predicate">The predicate to filter entities to update</param>
        /// <param name="field">Expression selecting the field to update</param>
        /// <param name="value">The new value to set for the field</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <returns>The number of entities that were updated</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate or field is null</exception>
        public int UpdateField<TField>(Expression<Func<T, bool>> predicate, Expression<Func<T, TField>> field, TField value, ITransaction? transaction = null)
        {
            if (predicate == null) throw new ArgumentNullException(nameof(predicate));
            if (field == null) throw new ArgumentNullException(nameof(field));

            MySqlExpressionParser<T> parser = new MySqlExpressionParser<T>(_ColumnMappings, _Sanitizer);

            // Build WHERE clause with parameters
            string whereClause = parser.ParseExpressionWithParameters(predicate.Body, true);
            List<SqlParameter> parameters = new List<SqlParameter>();
            foreach (var p in parser.GetParameters()) parameters.Add(new SqlParameter(p.name, p.value));

            // Get column name from field expression
            string columnName = parser.GetColumnFromExpression(field.Body);

            // Build UPDATE SQL
            string sql = $"UPDATE `{_TableName}` SET {columnName} = @value WHERE {whereClause}";

            // Convert value to database format
            PropertyInfo? fieldProperty = GetPropertyFromExpression(field.Body);
            object? convertedValue = _DataTypeConverter.ConvertToDatabase(value!, typeof(TField), fieldProperty);
            parameters.Add(new SqlParameter($"@value", convertedValue));

            int rowsAffected;
            if (transaction != null)
            {
                rowsAffected = ExecuteNonQueryWithConnection(transaction.Connection, sql, transaction.Transaction, parameters.ToArray());
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                rowsAffected = ExecuteNonQueryWithConnection(connection, sql, null, parameters.ToArray());
            }

            return rowsAffected;
        }

        /// <summary>
        /// Asynchronously updates an entity in the database.
        /// </summary>
        /// <param name="entity">The entity to update</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>The updated entity</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when entity has null primary key or no rows were affected</exception>
        /// <exception cref="OptimisticConcurrencyException">Thrown when version-based concurrency conflict occurs</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        public async Task<T> UpdateAsync(T entity, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            token.ThrowIfCancellationRequested();

            List<string> setPairs = new List<string>();
            List<SqlParameter> parameters = new List<SqlParameter>();
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
                    setPairs.Add($"`{columnName}` = @new_version");
                    object? convertedNewVersion = _DataTypeConverter.ConvertToDatabase(newVersion!, _VersionColumnInfo.PropertyType, property);
                    parameters.Add(new SqlParameter($"@new_version", convertedNewVersion));
                    _VersionColumnInfo.SetValue(entity, newVersion);
                }
                else
                {
                    setPairs.Add($"`{columnName}` = @{columnName}");
                    object? convertedValue = _DataTypeConverter.ConvertToDatabase(value!, property.PropertyType, property);
                    parameters.Add(new SqlParameter($"@{columnName}", convertedValue));
                }
            }

            if (idValue == null)
                throw new InvalidOperationException("Cannot update entity with null primary key");

            parameters.Add(new SqlParameter($"@id", idValue));

            string sql;
            if (_VersionColumnInfo != null)
            {
                sql = $"UPDATE `{_TableName}` SET {string.Join(", ", setPairs)} WHERE `{_PrimaryKeyColumn}` = @id AND `{_VersionColumnInfo.ColumnName}` = @current_version";
                parameters.Add(new SqlParameter($"@current_version", currentVersion));
            }
            else
            {
                sql = $"UPDATE `{_TableName}` SET {string.Join(", ", setPairs)} WHERE `{_PrimaryKeyColumn}` = @id";
            }

            int rowsAffected;
            if (transaction != null)
            {
                rowsAffected = await ExecuteNonQueryWithConnectionAsync(transaction.Connection, sql, transaction.Transaction, token, parameters.ToArray()).ConfigureAwait(false);
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                rowsAffected = await ExecuteNonQueryWithConnectionAsync(connection, sql, null, token, parameters.ToArray()).ConfigureAwait(false);
            }

            if (rowsAffected == 0)
            {
                if (_VersionColumnInfo != null)
                {
                    // Optimistic concurrency conflict detected - attempt resolution
                    try
                    {
                        // Read the current entity from database to get its current state
                        T? currentEntity = await ReadByIdAsync(idValue, transaction, token).ConfigureAwait(false);

                        if (currentEntity == null)
                        {
                            // Entity was deleted - throw appropriate exception
                            throw new OptimisticConcurrencyException($"Entity with ID {idValue} was deleted by another process");
                        }

                        // Attempt to resolve the conflict using the configured resolver
                        // For MergeChangesResolver, we create an original entity by taking the current entity's values
                        // but with the incoming entity's version - this approximates the original state the client had
                        T originalEntity = entity;

                        if (_ConflictResolver is MergeChangesResolver<T> merger)
                        {
                            // Create a better approximation of the original entity for merge operations
                            originalEntity = CreateOriginalEntityApproximation(currentEntity, entity);
                        }

                        TryResolveConflictResult<T> resolveResult = await _ConflictResolver.TryResolveConflictAsync(currentEntity, entity, originalEntity, _ConflictResolver.DefaultStrategy).ConfigureAwait(false);

                        if (resolveResult.Success && resolveResult.ResolvedEntity != null)
                        {
                            // Conflict resolved successfully - retry the update with resolved entity
                            return await UpdateAsync(resolveResult.ResolvedEntity, transaction, token).ConfigureAwait(false);
                        }
                        else
                        {
                            // Resolver couldn't resolve the conflict
                            throw new OptimisticConcurrencyException($"Optimistic concurrency conflict detected for entity with ID {idValue} and could not be resolved");
                        }
                    }
                    catch (OptimisticConcurrencyException)
                    {
                        // Re-throw concurrency exceptions as-is
                        throw;
                    }
                    catch (Exception ex)
                    {
                        // Wrap other exceptions in concurrency exception
                        throw new OptimisticConcurrencyException($"Error during conflict resolution for entity with ID {idValue}: {ex.Message}", ex);
                    }
                }
                else
                {
                    throw new InvalidOperationException($"No rows were affected during update for entity with ID {idValue}");
                }
            }

            return entity;
        }

        /// <summary>
        /// Asynchronously updates multiple entities that match the specified predicate by applying an update action to each entity.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities to update</param>
        /// <param name="updateAction">The async action to apply to each entity before updating</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>The number of entities that were updated</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate or updateAction is null</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        public async Task<int> UpdateManyAsync(Expression<Func<T, bool>> predicate, Func<T, Task> updateAction, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (predicate == null) throw new ArgumentNullException(nameof(predicate));
            if (updateAction == null) throw new ArgumentNullException(nameof(updateAction));
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
        /// Asynchronously updates a specific field for multiple entities that match the specified predicate.
        /// </summary>
        /// <typeparam name="TField">The type of the field being updated</typeparam>
        /// <param name="predicate">The predicate to filter entities to update</param>
        /// <param name="field">Expression selecting the field to update</param>
        /// <param name="value">The new value to set for the field</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>The number of entities that were updated</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate or field is null</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        public async Task<int> UpdateFieldAsync<TField>(Expression<Func<T, bool>> predicate, Expression<Func<T, TField>> field, TField value, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (predicate == null) throw new ArgumentNullException(nameof(predicate));
            if (field == null) throw new ArgumentNullException(nameof(field));
            token.ThrowIfCancellationRequested();

            MySqlExpressionParser<T> parser = new MySqlExpressionParser<T>(_ColumnMappings, _Sanitizer);

            // Build WHERE clause with parameters
            string whereClause = parser.ParseExpressionWithParameters(predicate.Body, true);
            List<SqlParameter> parameters = new List<SqlParameter>();
            foreach (var p in parser.GetParameters()) parameters.Add(new SqlParameter(p.name, p.value));

            // Get column name from field expression
            string columnName = parser.GetColumnFromExpression(field.Body);

            // Build UPDATE SQL
            string sql = $"UPDATE `{_TableName}` SET {columnName} = @value WHERE {whereClause}";

            // Convert value to database format
            PropertyInfo? fieldProperty = GetPropertyFromExpression(field.Body);
            object? convertedValue = _DataTypeConverter.ConvertToDatabase(value!, typeof(TField), fieldProperty);
            parameters.Add(new SqlParameter($"@value", convertedValue));

            int rowsAffected;
            if (transaction != null)
            {
                rowsAffected = await ExecuteNonQueryWithConnectionAsync(transaction.Connection, sql, transaction.Transaction, token, parameters.ToArray()).ConfigureAwait(false);
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                rowsAffected = await ExecuteNonQueryWithConnectionAsync(connection, sql, null, token, parameters.ToArray()).ConfigureAwait(false);
            }

            return rowsAffected;
        }

        /// <summary>
        /// Updates multiple entities that match the specified predicate using an expression-based update.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities to update</param>
        /// <param name="updateExpression">Expression defining how to update the entity (e.g., x => new Entity { Name = "NewName", Status = x.Status })</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <returns>The number of entities that were updated</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate or updateExpression is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when updateExpression format is not supported</exception>
        public int BatchUpdate(Expression<Func<T, bool>> predicate, Expression<Func<T, T>> updateExpression, ITransaction? transaction = null)
        {
            if (predicate == null) throw new ArgumentNullException(nameof(predicate));
            if (updateExpression == null) throw new ArgumentNullException(nameof(updateExpression));

            // For now, fall back to entity-by-entity updates since expression parsing for object initialization is complex
            // A full implementation would parse MemberInitExpression or NewExpression to generate SET clauses directly
            return UpdateMany(predicate, entity =>
            {
                // Compile and execute the update expression
                Func<T, T> compiledUpdate = updateExpression.Compile();
                T updatedEntity = compiledUpdate(entity);

                // Copy updated values back to the original entity
                foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
                {
                    PropertyInfo property = kvp.Value;
                    if (property.CanWrite)
                    {
                        object? newValue = property.GetValue(updatedEntity);
                        property.SetValue(entity, newValue);
                    }
                }
            }, transaction);
        }

        /// <summary>
        /// Deletes multiple entities that match the specified predicate using a single SQL statement.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities to delete</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <returns>The number of entities that were deleted</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null</exception>
        public int BatchDelete(Expression<Func<T, bool>> predicate, ITransaction? transaction = null)
        {
            if (predicate == null) throw new ArgumentNullException(nameof(predicate));

            MySqlExpressionParser<T> parser = new MySqlExpressionParser<T>(_ColumnMappings, _Sanitizer);

            // Build WHERE clause with parameters
            string whereClause = parser.ParseExpressionWithParameters(predicate.Body, true);
            List<SqlParameter> parameters = parser.GetParameters().Select(p => new SqlParameter(p.name, p.value)).ToList();

            // Build DELETE SQL
            string sql = $"DELETE FROM `{_TableName}` WHERE {whereClause}";

            int rowsAffected;
            if (transaction != null)
            {
                rowsAffected = ExecuteNonQueryWithConnection(transaction.Connection, sql, transaction.Transaction, parameters.ToArray());
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                rowsAffected = ExecuteNonQueryWithConnection(connection, sql, null, parameters.ToArray());
            }

            return rowsAffected;
        }

        /// <summary>
        /// Asynchronously updates multiple entities that match the specified predicate using an expression-based update.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities to update</param>
        /// <param name="updateExpression">Expression defining how to update the entity (e.g., x => new Entity { Name = "NewName", Status = x.Status })</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>The number of entities that were updated</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate or updateExpression is null</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        /// <exception cref="InvalidOperationException">Thrown when updateExpression format is not supported</exception>
        public async Task<int> BatchUpdateAsync(Expression<Func<T, bool>> predicate, Expression<Func<T, T>> updateExpression, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (predicate == null) throw new ArgumentNullException(nameof(predicate));
            if (updateExpression == null) throw new ArgumentNullException(nameof(updateExpression));
            token.ThrowIfCancellationRequested();

            // For now, fall back to entity-by-entity updates since expression parsing for object initialization is complex
            // A full implementation would parse MemberInitExpression or NewExpression to generate SET clauses directly
            return await UpdateManyAsync(predicate, entity =>
            {
                token.ThrowIfCancellationRequested();

                // Compile and execute the update expression
                Func<T, T> compiledUpdate = updateExpression.Compile();
                T updatedEntity = compiledUpdate(entity);

                // Copy updated values back to the original entity
                foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
                {
                    PropertyInfo property = kvp.Value;
                    if (property.CanWrite)
                    {
                        object? newValue = property.GetValue(updatedEntity);
                        property.SetValue(entity, newValue);
                    }
                }
                return Task.CompletedTask;
            }, transaction, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Asynchronously deletes multiple entities that match the specified predicate using a single SQL statement.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities to delete</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>The number of entities that were deleted</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        public async Task<int> BatchDeleteAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (predicate == null) throw new ArgumentNullException(nameof(predicate));
            token.ThrowIfCancellationRequested();

            MySqlExpressionParser<T> parser = new MySqlExpressionParser<T>(_ColumnMappings, _Sanitizer);

            // Build WHERE clause with parameters
            string whereClause = parser.ParseExpressionWithParameters(predicate.Body, true);
            List<SqlParameter> parameters = parser.GetParameters().Select(p => new SqlParameter(p.name, p.value)).ToList();

            // Build DELETE SQL
            string sql = $"DELETE FROM `{_TableName}` WHERE {whereClause}";

            int rowsAffected;
            if (transaction != null)
            {
                rowsAffected = await ExecuteNonQueryWithConnectionAsync(transaction.Connection, sql, transaction.Transaction, token, parameters.ToArray()).ConfigureAwait(false);
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                rowsAffected = await ExecuteNonQueryWithConnectionAsync(connection, sql, null, token, parameters.ToArray()).ConfigureAwait(false);
            }

            return rowsAffected;
        }

        /// <summary>
        /// Deletes an entity from the repository.
        /// </summary>
        /// <param name="entity">The entity to delete.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>True if the entity was deleted, false otherwise.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the entity has a null primary key.</exception>
        public bool Delete(T entity, ITransaction? transaction = null)
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));

            object? id = _PrimaryKeyProperty.GetValue(entity);
            if (id == null) throw new InvalidOperationException("Cannot delete entity with null primary key");

            // If the entity has a version column, use optimistic concurrency control
            if (_VersionColumnInfo != null)
            {
                object? version = _VersionColumnInfo.GetValue(entity);
                string sql = $"DELETE FROM `{_TableName}` WHERE `{_PrimaryKeyColumn}` = @id AND `{_VersionColumnInfo.ColumnName}` = @version";

                int rowsAffected;
                if (transaction != null)
                {
                    rowsAffected = ExecuteNonQueryWithConnection(transaction.Connection, sql, transaction.Transaction, new SqlParameter("@id", id), new SqlParameter("@version", version));
                }
                else
                {
                    using DbConnection connection = _ConnectionFactory.GetConnection();
                    rowsAffected = ExecuteNonQueryWithConnection(connection, sql, null, new SqlParameter("@id", id), new SqlParameter("@version", version));
                }

                if (rowsAffected == 0)
                {
                    // Check if entity still exists
                    T? currentEntity = ReadById(id, transaction);
                    if (currentEntity == null)
                    {
                        // Entity was already deleted by another process
                        throw new OptimisticConcurrencyException($"Entity with ID {id} was deleted by another process");
                    }
                    else
                    {
                        // Entity exists but version doesn't match
                        throw new OptimisticConcurrencyException($"Optimistic concurrency conflict detected during delete for entity with ID {id}");
                    }
                }

                return rowsAffected > 0;
            }
            else
            {
                // No version column, use simple delete by ID
                return DeleteById(id, transaction);
            }
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
            if (id == null) throw new ArgumentNullException(nameof(id));

            string sql = $"DELETE FROM `{_TableName}` WHERE `{_PrimaryKeyColumn}` = @id";

            if (transaction != null)
            {
                return ExecuteNonQueryWithConnection(transaction.Connection, sql, transaction.Transaction, new SqlParameter("@id", id)) > 0;
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                return ExecuteNonQueryWithConnection(connection, sql, null, new SqlParameter("@id", id)) > 0;
            }
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
            if (predicate == null) throw new ArgumentNullException(nameof(predicate));

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
        /// Deletes all entities from the repository.
        /// </summary>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <returns>The number of entities deleted.</returns>
        public int DeleteAll(ITransaction? transaction = null)
        {
            string sql = $"DELETE FROM `{_TableName}`";

            if (transaction != null)
            {
                return ExecuteNonQueryWithConnection(transaction.Connection, sql, transaction.Transaction);
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                return ExecuteNonQueryWithConnection(connection, sql, null);
            }
        }

        /// <summary>
        /// Asynchronously deletes an entity from the database.
        /// </summary>
        /// <param name="entity">The entity to delete</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>True if the entity was deleted, false if no rows were affected</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when entity has null primary key</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        public async Task<bool> DeleteAsync(T entity, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            token.ThrowIfCancellationRequested();

            object? id = _PrimaryKeyProperty.GetValue(entity);
            if (id == null) throw new InvalidOperationException("Cannot delete entity with null primary key");

            // If the entity has a version column, use optimistic concurrency control
            if (_VersionColumnInfo != null)
            {
                object? version = _VersionColumnInfo.GetValue(entity);
                string sql = $"DELETE FROM `{_TableName}` WHERE `{_PrimaryKeyColumn}` = @id AND `{_VersionColumnInfo.ColumnName}` = @version";

                int rowsAffected;
                if (transaction != null)
                {
                    rowsAffected = await ExecuteNonQueryWithConnectionAsync(transaction.Connection, sql, transaction.Transaction, token, new SqlParameter("@id", id), new SqlParameter("@version", version)).ConfigureAwait(false);
                }
                else
                {
                    using DbConnection connection = _ConnectionFactory.GetConnection();
                    rowsAffected = await ExecuteNonQueryWithConnectionAsync(connection, sql, null, token, new SqlParameter("@id", id), new SqlParameter("@version", version)).ConfigureAwait(false);
                }

                if (rowsAffected == 0)
                {
                    // Check if entity still exists
                    T? currentEntity = await ReadByIdAsync(id, transaction, token).ConfigureAwait(false);
                    if (currentEntity == null)
                    {
                        // Entity was already deleted by another process
                        throw new OptimisticConcurrencyException($"Entity with ID {id} was deleted by another process");
                    }
                    else
                    {
                        // Entity exists but version doesn't match
                        throw new OptimisticConcurrencyException($"Optimistic concurrency conflict detected during delete for entity with ID {id}");
                    }
                }

                return rowsAffected > 0;
            }
            else
            {
                // No version column, use simple delete by ID
                return await DeleteByIdAsync(id, transaction, token).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Asynchronously deletes an entity by its primary key.
        /// </summary>
        /// <param name="id">The primary key value of the entity to delete</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>True if the entity was deleted, false if no rows were affected</returns>
        /// <exception cref="ArgumentNullException">Thrown when id is null</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        public async Task<bool> DeleteByIdAsync(object id, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            token.ThrowIfCancellationRequested();

            string sql = $"DELETE FROM `{_TableName}` WHERE `{_PrimaryKeyColumn}` = @id";

            int rowsAffected;
            if (transaction != null)
            {
                rowsAffected = await ExecuteNonQueryWithConnectionAsync(transaction.Connection, sql, transaction.Transaction, token, new SqlParameter("@id", id)).ConfigureAwait(false);
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                rowsAffected = await ExecuteNonQueryWithConnectionAsync(connection, sql, null, token, new SqlParameter("@id", id)).ConfigureAwait(false);
            }

            return rowsAffected > 0;
        }

        /// <summary>
        /// Asynchronously deletes multiple entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities to delete</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>The number of entities that were deleted</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        public async Task<int> DeleteManyAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (predicate == null) throw new ArgumentNullException(nameof(predicate));
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
        /// Asynchronously deletes all entities from the table.
        /// </summary>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>The number of entities that were deleted</returns>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        public async Task<int> DeleteAllAsync(ITransaction? transaction = null, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            string sql = $"DELETE FROM `{_TableName}`";

            int rowsAffected;
            if (transaction != null)
            {
                rowsAffected = await ExecuteNonQueryWithConnectionAsync(transaction.Connection, sql, transaction.Transaction, token).ConfigureAwait(false);
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                rowsAffected = await ExecuteNonQueryWithConnectionAsync(connection, sql, null, token).ConfigureAwait(false);
            }

            return rowsAffected;
        }

        /// <summary>
        /// Inserts or updates an entity depending on whether it already exists in the repository.
        /// Uses MySQL's INSERT ... ON DUPLICATE KEY UPDATE syntax.
        /// </summary>
        /// <param name="entity">The entity to insert or update</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <returns>The entity after the upsert operation, with any generated values populated</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when the upsert operation fails</exception>
        public T Upsert(T entity, ITransaction? transaction = null)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));

            if (transaction != null)
            {
                return UpsertWithConnection(transaction.Connection, entity, transaction.Transaction);
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                return UpsertWithConnection(connection, entity, null);
            }
        }

        private T UpsertWithConnection(System.Data.Common.DbConnection connection, T entity, System.Data.Common.DbTransaction? transaction)
        {
            EnsureConnectionOpen(connection);

            using MySqlCommand command = (MySqlCommand)connection.CreateCommand();
            command.Transaction = (MySqlTransaction?)transaction;

            List<string> columns = new List<string>();
            List<string> parameters = new List<string>();
            List<string> updatePairs = new List<string>();
            List<SqlParameter> parameterValues = new List<SqlParameter>();

            foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
            {
                string columnName = kvp.Key;
                PropertyInfo property = kvp.Value;
                object? value = property.GetValue(entity);

                columns.Add($"`{columnName}`");
                parameters.Add($"@{columnName}");

                object? convertedValue = _DataTypeConverter.ConvertToDatabase(value!, property.PropertyType, property);
                parameterValues.Add(new SqlParameter($"@{columnName}", convertedValue));

                // For UPDATE part - exclude primary key from updates
                if (columnName != _PrimaryKeyColumn)
                {
                    updatePairs.Add($"`{columnName}` = VALUES(`{columnName}`)");
                }
            }

            StringBuilder sql = new StringBuilder();
            sql.Append($"INSERT INTO `{_TableName}` ({string.Join(", ", columns)}) ");
            sql.Append($"VALUES ({string.Join(", ", parameters)}) ");
            sql.Append("ON DUPLICATE KEY UPDATE ");
            sql.Append(string.Join(", ", updatePairs));

            command.CommandText = sql.ToString();

            // Add parameters
            foreach (SqlParameter param in parameterValues)
            {
                MySqlConnector.MySqlParameter parameter = new MySqlConnector.MySqlParameter(param.Name, param.Value ?? DBNull.Value);
                command.Parameters.Add(parameter);
            }

            // Capture SQL if enabled
            if (_CaptureSql && command != null && !string.IsNullOrEmpty(command.CommandText))
            {
                _LastExecutedSql = command.CommandText;
                _LastExecutedSqlWithParameters = BuildSqlWithParameters((MySqlConnector.MySqlCommand)command);
            }

            try
            {
                int rowsAffected = command!.ExecuteNonQuery();

                // Handle auto-generated primary key for new insertions
                if (_PrimaryKeyProperty != null && _PrimaryKeyProperty.GetValue(entity) == null)
                {
                    object? insertedId = ExecuteScalarWithConnection<object>(connection, "SELECT LAST_INSERT_ID()", transaction);
                    if (insertedId != null && insertedId != DBNull.Value)
                    {
                        object? convertedId = _DataTypeConverter.ConvertFromDatabase(insertedId, _PrimaryKeyProperty.PropertyType, _PrimaryKeyProperty);
                        _PrimaryKeyProperty.SetValue(entity, convertedId);
                    }
                }

                return entity;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error executing upsert operation: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Inserts or updates multiple entities depending on whether they already exist in the repository.
        /// Uses MySQL's INSERT ... ON DUPLICATE KEY UPDATE syntax within a transaction for consistency.
        /// </summary>
        /// <param name="entities">The entities to insert or update</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <returns>The entities after the upsert operation, with any generated values populated</returns>
        /// <exception cref="ArgumentNullException">Thrown when entities is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when the upsert operation fails</exception>
        public IEnumerable<T> UpsertMany(IEnumerable<T> entities, ITransaction? transaction = null)
        {
            if (entities == null)
                throw new ArgumentNullException(nameof(entities));

            IList<T> entitiesList = entities.ToList();
            if (!entitiesList.Any())
                return Enumerable.Empty<T>();

            bool ownTransaction = transaction == null;
            MySqlConnection? connection = null;
            MySqlTransaction? localTransaction = null;

            try
            {
                if (ownTransaction)
                {
                    connection = (MySqlConnection)PooledConnectionHandle.Unwrap(_ConnectionFactory.GetConnection());
                    EnsureConnectionOpen(connection);
                    localTransaction = connection.BeginTransaction();
                    transaction = new MySqlRepositoryTransaction(connection, localTransaction, _ConnectionFactory);
                }

                List<T> results = new List<T>();
                foreach (T entity in entitiesList)
                {
                    results.Add(Upsert(entity, transaction));
                }

                if (ownTransaction)
                {
                    localTransaction?.Commit();
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
                    if (connection != null)
                    {
                        _ConnectionFactory.ReturnConnection(connection);
                    }
                }
            }
        }

        /// <summary>
        /// Asynchronously inserts or updates an entity depending on whether it already exists in the repository.
        /// Uses MySQL's INSERT ... ON DUPLICATE KEY UPDATE syntax.
        /// </summary>
        /// <param name="entity">The entity to insert or update</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token to cancel the operation</param>
        /// <returns>A task that represents the asynchronous operation containing the entity after the upsert operation, with any generated values populated</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when the upsert operation fails</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        public async Task<T> UpsertAsync(T entity, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));

            token.ThrowIfCancellationRequested();

            if (transaction != null)
            {
                return await UpsertAsyncWithConnection(transaction.Connection, entity, transaction.Transaction, token).ConfigureAwait(false);
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                return await UpsertAsyncWithConnection(connection, entity, null, token).ConfigureAwait(false);
            }
        }

        private async Task<T> UpsertAsyncWithConnection(System.Data.Common.DbConnection connection, T entity, System.Data.Common.DbTransaction? transaction, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();

            EnsureConnectionOpen(connection);

            using MySqlCommand command = (MySqlCommand)connection.CreateCommand();
            command.Transaction = (MySqlTransaction?)transaction;

            List<string> columns = new List<string>();
            List<string> parameters = new List<string>();
            List<string> updatePairs = new List<string>();
            List<SqlParameter> parameterValues = new List<SqlParameter>();

            foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
            {
                string columnName = kvp.Key;
                PropertyInfo property = kvp.Value;
                object? value = property.GetValue(entity);

                columns.Add($"`{columnName}`");
                parameters.Add($"@{columnName}");

                object? convertedValue = _DataTypeConverter.ConvertToDatabase(value!, property.PropertyType, property);
                parameterValues.Add(new SqlParameter($"@{columnName}", convertedValue));

                // For UPDATE part - exclude primary key from updates
                if (columnName != _PrimaryKeyColumn)
                {
                    updatePairs.Add($"`{columnName}` = VALUES(`{columnName}`)");
                }
            }

            StringBuilder sql = new StringBuilder();
            sql.Append($"INSERT INTO `{_TableName}` ({string.Join(", ", columns)}) ");
            sql.Append($"VALUES ({string.Join(", ", parameters)}) ");
            sql.Append("ON DUPLICATE KEY UPDATE ");
            sql.Append(string.Join(", ", updatePairs));

            command.CommandText = sql.ToString();

            // Add parameters
            foreach (SqlParameter param in parameterValues)
            {
                MySqlConnector.MySqlParameter parameter = new MySqlConnector.MySqlParameter(param.Name, param.Value ?? DBNull.Value);
                command.Parameters.Add(parameter);
            }

            // Capture SQL if enabled
            if (_CaptureSql && command != null && !string.IsNullOrEmpty(command.CommandText))
            {
                _LastExecutedSql = command.CommandText;
                _LastExecutedSqlWithParameters = BuildSqlWithParameters((MySqlConnector.MySqlCommand)command);
            }

            try
            {
                int rowsAffected = await command!.ExecuteNonQueryAsync(token).ConfigureAwait(false);

                // Handle auto-generated primary key for new insertions
                if (_PrimaryKeyProperty != null && _PrimaryKeyProperty.GetValue(entity) == null)
                {
                    object? insertedId = await ExecuteScalarWithConnectionAsync<object>(connection, "SELECT LAST_INSERT_ID()", transaction, token).ConfigureAwait(false);
                    if (insertedId != null && insertedId != DBNull.Value)
                    {
                        object? convertedId = _DataTypeConverter.ConvertFromDatabase(insertedId, _PrimaryKeyProperty.PropertyType, _PrimaryKeyProperty);
                        _PrimaryKeyProperty.SetValue(entity, convertedId);
                    }
                }

                return entity;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error executing upsert operation: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Asynchronously inserts or updates multiple entities depending on whether they already exist in the repository.
        /// Uses MySQL's INSERT ... ON DUPLICATE KEY UPDATE syntax within a transaction for consistency.
        /// </summary>
        /// <param name="entities">The entities to insert or update</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token to cancel the operation</param>
        /// <returns>A task that represents the asynchronous operation containing the entities after the upsert operation, with any generated values populated</returns>
        /// <exception cref="ArgumentNullException">Thrown when entities is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when the upsert operation fails</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        public async Task<IEnumerable<T>> UpsertManyAsync(IEnumerable<T> entities, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (entities == null)
                throw new ArgumentNullException(nameof(entities));

            token.ThrowIfCancellationRequested();

            IList<T> entitiesList = entities.ToList();
            if (!entitiesList.Any())
                return Enumerable.Empty<T>();

            bool ownTransaction = transaction == null;
            MySqlConnection? connection = null;
            MySqlTransaction? localTransaction = null;

            try
            {
                if (ownTransaction)
                {
                    connection = (MySqlConnection)PooledConnectionHandle.Unwrap(await _ConnectionFactory.GetConnectionAsync(token).ConfigureAwait(false));
                    await EnsureConnectionOpenAsync(connection, token).ConfigureAwait(false);
                    localTransaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);
                    transaction = new MySqlRepositoryTransaction(connection, localTransaction, _ConnectionFactory);
                }

                List<T> results = new List<T>();
                foreach (T entity in entitiesList)
                {
                    token.ThrowIfCancellationRequested();
                    results.Add(await UpsertAsync(entity, transaction, token).ConfigureAwait(false));
                }

                if (ownTransaction)
                {
                    await localTransaction!.CommitAsync(token).ConfigureAwait(false);
                }

                return results;
            }
            catch
            {
                if (ownTransaction)
                {
                    if (localTransaction != null)
                        await localTransaction.RollbackAsync(token).ConfigureAwait(false);
                }
                throw;
            }
            finally
            {
                if (ownTransaction)
                {
                    if (localTransaction != null)
                        await localTransaction.DisposeAsync().ConfigureAwait(false);
                    if (connection != null)
                        await _ConnectionFactory.ReturnConnectionAsync(connection).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Executes a raw SQL query and returns the results as entities of type T.
        /// </summary>
        /// <param name="sql">The raw SQL query to execute</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="parameters">Parameters for the SQL query</param>
        /// <returns>An enumerable collection of entities returned by the query</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null or empty</exception>
        /// <exception cref="InvalidOperationException">Thrown when query execution fails</exception>
        public IEnumerable<T> FromSql(string sql, ITransaction? transaction = null, params object[] parameters)
        {
            if (string.IsNullOrWhiteSpace(sql))
                throw new ArgumentNullException(nameof(sql));

            if (transaction != null)
            {
                return ExecuteFromSqlWithConnection(transaction.Connection, sql, transaction.Transaction, parameters);
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                return ExecuteFromSqlWithConnection(connection, sql, null, parameters);
            }
        }

        private List<T> ExecuteFromSqlWithConnection(System.Data.Common.DbConnection connection, string sql, System.Data.Common.DbTransaction? transaction, params object[] parameters)
        {
            EnsureConnectionOpen(connection);

            using MySqlCommand command = (MySqlCommand)connection.CreateCommand();
            command.CommandText = sql;
            if (transaction != null)
                command.Transaction = (MySqlTransaction)transaction;

            // Add parameters
            for (int i = 0; i < parameters.Length; i++)
            {
                MySqlConnector.MySqlParameter parameter = new MySqlConnector.MySqlParameter($"@p{i}", parameters[i] ?? DBNull.Value);
                command.Parameters.Add(parameter);
            }

            // Capture SQL if enabled
            if (_CaptureSql && command != null && !string.IsNullOrEmpty(command.CommandText))
            {
                _LastExecutedSql = command.CommandText;
                _LastExecutedSqlWithParameters = BuildSqlWithParameters((MySqlConnector.MySqlCommand)command);
            }

            try
            {
                using MySqlConnector.MySqlDataReader reader = (MySqlConnector.MySqlDataReader)command!.ExecuteReader();
                List<T> results = new List<T>();
                while (reader.Read())
                {
                    results.Add(MapReaderToEntity(reader));
                }
                return results;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error executing raw SQL query: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Executes a raw SQL query and returns the results as entities of the specified type.
        /// </summary>
        /// <typeparam name="TResult">The type to map the query results to. Must have a parameterless constructor</typeparam>
        /// <param name="sql">The raw SQL query to execute</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="parameters">Parameters for the SQL query</param>
        /// <returns>An enumerable collection of entities of the specified type returned by the query</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null or empty</exception>
        /// <exception cref="InvalidOperationException">Thrown when query execution fails</exception>
        public IEnumerable<TResult> FromSql<TResult>(string sql, ITransaction? transaction = null, params object[] parameters) where TResult : new()
        {
            if (string.IsNullOrWhiteSpace(sql))
                throw new ArgumentNullException(nameof(sql));

            if (transaction != null)
            {
                return ExecuteFromSqlWithConnection<TResult>(transaction.Connection, sql, transaction.Transaction, parameters);
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                return ExecuteFromSqlWithConnection<TResult>(connection, sql, null, parameters);
            }
        }

        private List<TResult> ExecuteFromSqlWithConnection<TResult>(System.Data.Common.DbConnection connection, string sql, System.Data.Common.DbTransaction? transaction, params object[] parameters) where TResult : new()
        {
            EnsureConnectionOpen(connection);

            using MySqlCommand command = (MySqlCommand)connection.CreateCommand();
            command.CommandText = sql;
            if (transaction != null)
                command.Transaction = (MySqlTransaction)transaction;

            // Add parameters
            for (int i = 0; i < parameters.Length; i++)
            {
                MySqlConnector.MySqlParameter parameter = new MySqlConnector.MySqlParameter($"@p{i}", parameters[i] ?? DBNull.Value);
                command.Parameters.Add(parameter);
            }

            // Capture SQL if enabled
            if (_CaptureSql && command != null && !string.IsNullOrEmpty(command.CommandText))
            {
                _LastExecutedSql = command.CommandText;
                _LastExecutedSqlWithParameters = BuildSqlWithParameters((MySqlConnector.MySqlCommand)command);
            }

            try
            {
                using MySqlConnector.MySqlDataReader reader = (MySqlConnector.MySqlDataReader)command!.ExecuteReader();
                List<TResult> results = new List<TResult>();
                while (reader.Read())
                {
                    results.Add(MapReaderToType<TResult>(reader));
                }
                return results;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error executing raw SQL query: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Executes a raw SQL command (INSERT, UPDATE, DELETE) and returns the number of affected rows.
        /// </summary>
        /// <param name="sql">The raw SQL command to execute</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="parameters">Parameters for the SQL command</param>
        /// <returns>The number of rows affected by the command</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null or empty</exception>
        /// <exception cref="InvalidOperationException">Thrown when command execution fails</exception>
        public int ExecuteSql(string sql, ITransaction? transaction = null, params object[] parameters)
        {
            if (string.IsNullOrWhiteSpace(sql))
                throw new ArgumentNullException(nameof(sql));

            if (transaction != null)
            {
                return ExecuteSqlWithConnection(transaction.Connection, sql, transaction.Transaction, parameters);
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                return ExecuteSqlWithConnection(connection, sql, null, parameters);
            }
        }

        private int ExecuteSqlWithConnection(System.Data.Common.DbConnection connection, string sql, System.Data.Common.DbTransaction? transaction, params object[] parameters)
        {
            EnsureConnectionOpen(connection);

            using MySqlCommand command = (MySqlCommand)connection.CreateCommand();
            command.CommandText = sql;
            if (transaction != null)
                command.Transaction = (MySqlTransaction)transaction;

            // Add parameters
            for (int i = 0; i < parameters.Length; i++)
            {
                MySqlConnector.MySqlParameter parameter = new MySqlConnector.MySqlParameter($"@p{i}", parameters[i] ?? DBNull.Value);
                command.Parameters.Add(parameter);
            }

            // Capture SQL if enabled
            if (_CaptureSql && command != null && !string.IsNullOrEmpty(command.CommandText))
            {
                _LastExecutedSql = command.CommandText;
                _LastExecutedSqlWithParameters = BuildSqlWithParameters((MySqlConnector.MySqlCommand)command);
            }

            try
            {
                return command!.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error executing raw SQL command: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Asynchronously executes a raw SQL query and returns the results as entities of type T.
        /// </summary>
        /// <param name="sql">The raw SQL query to execute</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token to cancel the operation</param>
        /// <param name="parameters">Parameters for the SQL query</param>
        /// <returns>An async enumerable collection of entities returned by the query</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null or empty</exception>
        /// <exception cref="InvalidOperationException">Thrown when query execution fails</exception>
        public async IAsyncEnumerable<T> FromSqlAsync(string sql, ITransaction? transaction = null, [EnumeratorCancellation] CancellationToken token = default, params object[] parameters)
        {
            if (string.IsNullOrWhiteSpace(sql))
                throw new ArgumentNullException(nameof(sql));

            if (transaction != null)
            {
                await foreach (T item in ExecuteFromSqlAsyncWithConnection(transaction.Connection, sql, transaction.Transaction, token, parameters).ConfigureAwait(false))
                {
                    yield return item;
                }
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                await foreach (T item in ExecuteFromSqlAsyncWithConnection(connection, sql, null, token, parameters).ConfigureAwait(false))
                {
                    yield return item;
                }
            }
        }

        private async IAsyncEnumerable<T> ExecuteFromSqlAsyncWithConnection(System.Data.Common.DbConnection connection, string sql, System.Data.Common.DbTransaction? transaction, [EnumeratorCancellation] CancellationToken token, params object[] parameters)
        {
            token.ThrowIfCancellationRequested();

            EnsureConnectionOpen(connection);

            using MySqlCommand command = (MySqlCommand)connection.CreateCommand();
            command.CommandText = sql;
            if (transaction != null)
                command.Transaction = (MySqlTransaction)transaction;

            // Add parameters
            for (int i = 0; i < parameters.Length; i++)
            {
                MySqlConnector.MySqlParameter parameter = new MySqlConnector.MySqlParameter($"@p{i}", parameters[i] ?? DBNull.Value);
                command.Parameters.Add(parameter);
            }

            // Capture SQL if enabled
            if (_CaptureSql && command != null && !string.IsNullOrEmpty(command.CommandText))
            {
                _LastExecutedSql = command.CommandText;
                _LastExecutedSqlWithParameters = BuildSqlWithParameters((MySqlConnector.MySqlCommand)command);
            }

            using MySqlConnector.MySqlDataReader reader = (MySqlConnector.MySqlDataReader)await command!.ExecuteReaderAsync(token).ConfigureAwait(false);
            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                token.ThrowIfCancellationRequested();
                yield return MapReaderToEntity(reader);
            }
        }

        /// <summary>
        /// Asynchronously executes a raw SQL query and returns the results as entities of the specified type.
        /// </summary>
        /// <typeparam name="TResult">The type to map the query results to. Must have a parameterless constructor</typeparam>
        /// <param name="sql">The raw SQL query to execute</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token to cancel the operation</param>
        /// <param name="parameters">Parameters for the SQL query</param>
        /// <returns>An async enumerable collection of entities of the specified type returned by the query</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null or empty</exception>
        /// <exception cref="InvalidOperationException">Thrown when query execution fails</exception>
        public async IAsyncEnumerable<TResult> FromSqlAsync<TResult>(string sql, ITransaction? transaction = null, [EnumeratorCancellation] CancellationToken token = default, params object[] parameters) where TResult : new()
        {
            if (string.IsNullOrWhiteSpace(sql))
                throw new ArgumentNullException(nameof(sql));

            if (transaction != null)
            {
                await foreach (TResult item in ExecuteFromSqlAsyncWithConnection<TResult>(transaction.Connection, sql, transaction.Transaction, token, parameters).ConfigureAwait(false))
                {
                    yield return item;
                }
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                await foreach (TResult item in ExecuteFromSqlAsyncWithConnection<TResult>(connection, sql, null, token, parameters).ConfigureAwait(false))
                {
                    yield return item;
                }
            }
        }

        private async IAsyncEnumerable<TResult> ExecuteFromSqlAsyncWithConnection<TResult>(System.Data.Common.DbConnection connection, string sql, System.Data.Common.DbTransaction? transaction, [EnumeratorCancellation] CancellationToken token, params object[] parameters) where TResult : new()
        {
            token.ThrowIfCancellationRequested();

            EnsureConnectionOpen(connection);

            using MySqlCommand command = (MySqlCommand)connection.CreateCommand();
            command.CommandText = sql;
            if (transaction != null)
                command.Transaction = (MySqlTransaction)transaction;

            // Add parameters
            for (int i = 0; i < parameters.Length; i++)
            {
                MySqlConnector.MySqlParameter parameter = new MySqlConnector.MySqlParameter($"@p{i}", parameters[i] ?? DBNull.Value);
                command.Parameters.Add(parameter);
            }

            // Capture SQL if enabled
            if (_CaptureSql && command != null && !string.IsNullOrEmpty(command.CommandText))
            {
                _LastExecutedSql = command.CommandText;
                _LastExecutedSqlWithParameters = BuildSqlWithParameters((MySqlConnector.MySqlCommand)command);
            }

            using MySqlConnector.MySqlDataReader reader = (MySqlConnector.MySqlDataReader)await command!.ExecuteReaderAsync(token).ConfigureAwait(false);
            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                token.ThrowIfCancellationRequested();
                yield return MapReaderToType<TResult>(reader);
            }
        }

        /// <summary>
        /// Asynchronously executes a raw SQL command (INSERT, UPDATE, DELETE) and returns the number of affected rows.
        /// </summary>
        /// <param name="sql">The raw SQL command to execute</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token to cancel the operation</param>
        /// <param name="parameters">Parameters for the SQL command</param>
        /// <returns>A task that represents the asynchronous operation containing the number of rows affected by the command</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null or empty</exception>
        /// <exception cref="InvalidOperationException">Thrown when command execution fails</exception>
        public async Task<int> ExecuteSqlAsync(string sql, ITransaction? transaction = null, CancellationToken token = default, params object[] parameters)
        {
            if (string.IsNullOrWhiteSpace(sql))
                throw new ArgumentNullException(nameof(sql));

            if (transaction != null)
            {
                return await ExecuteSqlAsyncWithConnection(transaction.Connection, sql, transaction.Transaction, token, parameters).ConfigureAwait(false);
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                return await ExecuteSqlAsyncWithConnection(connection, sql, null, token, parameters).ConfigureAwait(false);
            }
        }

        private async Task<int> ExecuteSqlAsyncWithConnection(System.Data.Common.DbConnection connection, string sql, System.Data.Common.DbTransaction? transaction, CancellationToken token, params object[] parameters)
        {
            token.ThrowIfCancellationRequested();

            EnsureConnectionOpen(connection);

            using MySqlCommand command = (MySqlCommand)connection.CreateCommand();
            command.CommandText = sql;
            if (transaction != null)
                command.Transaction = (MySqlTransaction)transaction;

            // Add parameters
            for (int i = 0; i < parameters.Length; i++)
            {
                MySqlConnector.MySqlParameter parameter = new MySqlConnector.MySqlParameter($"@p{i}", parameters[i] ?? DBNull.Value);
                command.Parameters.Add(parameter);
            }

            // Capture SQL if enabled
            if (_CaptureSql && command != null && !string.IsNullOrEmpty(command.CommandText))
            {
                _LastExecutedSql = command.CommandText;
                _LastExecutedSqlWithParameters = BuildSqlWithParameters((MySqlConnector.MySqlCommand)command);
            }

            try
            {
                return await command!.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error executing raw SQL command: {ex.Message}", ex);
            }
        }

        #endregion

        #region Batch-Operations

        /// <summary>
        /// Optimized batch insert implementation for MySQL using multi-row INSERT syntax.
        /// </summary>
        /// <param name="entities">The entities to insert</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <returns>The created entities with any auto-generated values populated</returns>
        private IEnumerable<T> CreateManyOptimized(IList<T> entities, ITransaction? transaction)
        {
            DbConnection dbConnection = transaction?.Connection ?? _ConnectionFactory.GetConnection();
            MySqlConnection connection = (MySqlConnection)PooledConnectionHandle.Unwrap(dbConnection);
            List<T> results = new List<T>();

            try
            {
                IEnumerable<IList<T>> batches = CreateBatches(entities);
                Dictionary<int, MySqlConnector.MySqlCommand> preparedCommands = new Dictionary<int, MySqlConnector.MySqlCommand>();

                foreach (IList<T> batch in batches)
                {
                    int batchSize = batch.Count;

                    if (EnablePreparedStatementReuse && preparedCommands.TryGetValue(batchSize, out MySqlConnector.MySqlCommand? preparedCommand))
                    {
                        preparedCommand.Parameters.Clear();
                        AddParametersForBatch(preparedCommand, batch);
                        ExecuteBatchInsert(preparedCommand, batch);
                    }
                    else
                    {
                        using MySqlConnector.MySqlCommand command = new MySqlConnector.MySqlCommand();
                        command.Connection = connection;
                        if (transaction != null)
                            command.Transaction = (MySqlConnector.MySqlTransaction)transaction.Transaction;

                        BuildBatchInsertCommand(command, batch);
                        ExecuteBatchInsert(command, batch);

                        if (EnablePreparedStatementReuse && !preparedCommands.ContainsKey(batchSize))
                        {
                            MySqlConnector.MySqlCommand newPreparedCommand = new MySqlConnector.MySqlCommand(command.CommandText, connection);
                            if (transaction != null)
                                newPreparedCommand.Transaction = (MySqlConnector.MySqlTransaction)transaction.Transaction;
                            preparedCommands[batchSize] = newPreparedCommand;
                        }
                    }

                    results.AddRange(batch);
                }

                foreach (MySqlConnector.MySqlCommand preparedCommand in preparedCommands.Values)
                {
                    preparedCommand?.Dispose();
                }

                return results;
            }
            finally
            {
                if (transaction == null)
                {
                    _ConnectionFactory.ReturnConnection(dbConnection);
                }
            }
        }

        /// <summary>
        /// Asynchronous optimized batch insert implementation for MySQL using multi-row INSERT syntax.
        /// </summary>
        /// <param name="entities">The entities to insert</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="token">Cancellation token for the async operation</param>
        /// <returns>The created entities with any auto-generated values populated</returns>
        private async Task<IEnumerable<T>> CreateManyOptimizedAsync(IList<T> entities, ITransaction? transaction, CancellationToken token)
        {
            DbConnection dbConnection = transaction?.Connection ?? _ConnectionFactory.GetConnection();
            MySqlConnection connection = (MySqlConnection)PooledConnectionHandle.Unwrap(dbConnection);
            bool shouldDisposeConnection = transaction == null;
            List<T> results = new List<T>();

            try
            {
                IEnumerable<IList<T>> batches = CreateBatches(entities);
                Dictionary<int, MySqlConnector.MySqlCommand> preparedCommands = new Dictionary<int, MySqlConnector.MySqlCommand>();

                foreach (IList<T> batch in batches)
                {
                    token.ThrowIfCancellationRequested();

                    int batchSize = batch.Count;

                    if (EnablePreparedStatementReuse && preparedCommands.TryGetValue(batchSize, out MySqlConnector.MySqlCommand? preparedCommand))
                    {
                        preparedCommand.Parameters.Clear();
                        AddParametersForBatch(preparedCommand, batch);
                        await ExecuteBatchInsertAsync(preparedCommand, batch, token).ConfigureAwait(false);
                    }
                    else
                    {
                        using MySqlConnector.MySqlCommand command = new MySqlConnector.MySqlCommand();
                        command.Connection = (MySqlConnector.MySqlConnection)connection;
                        if (transaction != null)
                            command.Transaction = (MySqlConnector.MySqlTransaction)transaction.Transaction;

                        BuildBatchInsertCommand(command, batch);
                        await ExecuteBatchInsertAsync(command, batch, token).ConfigureAwait(false);

                        if (EnablePreparedStatementReuse && !preparedCommands.ContainsKey(batchSize))
                        {
                            MySqlConnector.MySqlCommand newPreparedCommand = new MySqlConnector.MySqlCommand(command.CommandText, (MySqlConnector.MySqlConnection)connection);
                            if (transaction != null)
                                newPreparedCommand.Transaction = (MySqlConnector.MySqlTransaction)transaction.Transaction;
                            preparedCommands[batchSize] = newPreparedCommand;
                        }
                    }

                    results.AddRange(batch);
                }

                foreach (MySqlConnector.MySqlCommand preparedCommand in preparedCommands.Values)
                {
                    preparedCommand?.Dispose();
                }

                return results;
            }
            finally
            {
                if (transaction == null)
                {
                    await _ConnectionFactory.ReturnConnectionAsync(dbConnection).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Creates batches of entities based on configured limits for parameters and rows per batch.
        /// </summary>
        /// <param name="entities">The entities to batch</param>
        /// <returns>Batches of entities optimized for MySQL insert performance</returns>
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

        /// <summary>
        /// Gets a list of column names excluding auto-increment primary keys.
        /// </summary>
        /// <returns>List of column names for batch insert operations</returns>
        private List<string> GetNonAutoIncrementColumns()
        {
            List<string> columns = new List<string>();
            foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
            {
                string columnName = kvp.Key;
                PropertyInfo property = kvp.Value;
                PropertyAttribute? columnAttr = property.GetCustomAttribute<PropertyAttribute>();

                if (columnAttr != null &&
                    (columnAttr.PropertyFlags & Flags.PrimaryKey) == Flags.PrimaryKey &&
                    (columnAttr.PropertyFlags & Flags.AutoIncrement) == Flags.AutoIncrement)
                {
                    continue; // Skip auto-increment primary key columns
                }

                columns.Add(columnName);
            }
            return columns;
        }

        /// <summary>
        /// Gets the PropertyInfo from a member expression.
        /// </summary>
        /// <param name="expression">The expression to extract PropertyInfo from</param>
        /// <returns>The PropertyInfo or null if not found</returns>
        private PropertyInfo? GetPropertyFromExpression(Expression expression)
        {
            if (expression is MemberExpression memberExpr && memberExpr.Member is PropertyInfo prop)
            {
                return prop;
            }
            return null;
        }

        /// <summary>
        /// Builds a MySQL multi-row INSERT command for a batch of entities.
        /// </summary>
        /// <param name="command">The command to configure</param>
        /// <param name="entities">The entities to include in the batch</param>
        private void BuildBatchInsertCommand(MySqlConnector.MySqlCommand command, IList<T> entities)
        {
            List<string> columns = GetNonAutoIncrementColumns();
            List<string> sanitizedColumns = columns.Select(c => $"`{c}`").ToList();
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

            command.CommandText = $"INSERT INTO `{_TableName}` ({string.Join(", ", sanitizedColumns)}) VALUES {string.Join(", ", valuesList)}";
            AddParametersForBatch(command, entities);
        }

        /// <summary>
        /// Adds parameters to a MySQL command for a batch of entities.
        /// </summary>
        /// <param name="command">The command to add parameters to</param>
        /// <param name="entities">The entities to parameterize</param>
        private void AddParametersForBatch(MySqlConnector.MySqlCommand command, IList<T> entities)
        {
            List<string> columns = GetNonAutoIncrementColumns();

            for (int i = 0; i < entities.Count; i++)
            {
                T entity = entities[i];
                foreach (string column in columns)
                {
                    PropertyInfo property = _ColumnMappings[column];
                    object? value = property.GetValue(entity);
                    object convertedValue = _DataTypeConverter.ConvertToDatabase(value!, property.PropertyType, property);
                    command.Parameters.AddWithValue($"@{column}_{i}", convertedValue);
                }
            }
        }

        /// <summary>
        /// Executes a MySQL batch insert command synchronously.
        /// </summary>
        /// <param name="command">The command to execute</param>
        /// <param name="entities">The entities being inserted</param>
        private void ExecuteBatchInsert(MySqlConnector.MySqlCommand command, IList<T> entities)
        {
            // Capture SQL for tracking
            if (_CaptureSql && command != null && !string.IsNullOrEmpty(command.CommandText))
            {
                _LastExecutedSql = command.CommandText;
                _LastExecutedSqlWithParameters = BuildSqlWithParameters(command);
            }

            try
            {
                int rowsAffected = command!.ExecuteNonQuery();
                if (rowsAffected != entities.Count)
                {
                    throw new InvalidOperationException($"Expected to insert {entities.Count} rows, but {rowsAffected} were affected");
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error executing batch insert: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Executes a MySQL batch insert command asynchronously.
        /// </summary>
        /// <param name="command">The command to execute</param>
        /// <param name="entities">The entities being inserted</param>
        /// <param name="token">Cancellation token for the async operation</param>
        private async Task ExecuteBatchInsertAsync(MySqlConnector.MySqlCommand command, IList<T> entities, CancellationToken token)
        {
            // Capture SQL for tracking
            if (_CaptureSql && command != null && !string.IsNullOrEmpty(command.CommandText))
            {
                _LastExecutedSql = command.CommandText;
                _LastExecutedSqlWithParameters = BuildSqlWithParameters(command);
            }

            try
            {
                int rowsAffected = await command!.ExecuteNonQueryAsync(token).ConfigureAwait(false);
                if (rowsAffected != entities.Count)
                {
                    throw new InvalidOperationException($"Expected to insert {entities.Count} rows, but {rowsAffected} were affected");
                }

                // If primary key is auto-increment, populate the IDs
                if (_PrimaryKeyProperty != null)
                {
                    PropertyAttribute? pkAttr = _PrimaryKeyProperty.GetCustomAttribute<PropertyAttribute>();
                    bool hasAutoIncrement = pkAttr?.PropertyFlags.HasFlag(Flags.AutoIncrement) == true;

                    if (hasAutoIncrement)
                    {
                        // Get the first auto-generated ID
                        using MySqlConnector.MySqlCommand idCommand = new MySqlConnector.MySqlCommand("SELECT LAST_INSERT_ID()", (MySqlConnector.MySqlConnection)command.Connection!, command.Transaction);
                        object? firstIdResult = await idCommand.ExecuteScalarAsync(token).ConfigureAwait(false);

                        if (firstIdResult != null && firstIdResult != DBNull.Value)
                        {
                            long firstId = Convert.ToInt64(firstIdResult);

                            // Assign consecutive IDs to each entity
                            for (int i = 0; i < entities.Count; i++)
                            {
                                long entityId = firstId + i;
                                object? convertedId = _DataTypeConverter.ConvertFromDatabase(entityId, _PrimaryKeyProperty.PropertyType, _PrimaryKeyProperty);
                                _PrimaryKeyProperty.SetValue(entities[i], convertedId);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error executing batch insert: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Builds a SQL string with parameter values substituted for debugging purposes.
        /// </summary>
        /// <param name="command">The MySQL command to process</param>
        /// <returns>SQL string with parameter values substituted</returns>
        private string BuildSqlWithParameters(MySqlConnector.MySqlCommand command)
        {
            if (command?.Parameters == null || command.Parameters.Count == 0)
            {
                return command?.CommandText ?? string.Empty;
            }

            string sql = command.CommandText;
            foreach (MySqlConnector.MySqlParameter parameter in command.Parameters)
            {
                string parameterValue = FormatParameterValue(parameter.Value);
                sql = sql.Replace(parameter.ParameterName, parameterValue);
            }
            return sql;
        }

        /// <summary>
        /// Formats a parameter value for SQL string substitution.
        /// </summary>
        /// <param name="value">The parameter value to format</param>
        /// <returns>Formatted parameter value as string</returns>
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

            if (value is bool boolValue)
            {
                return boolValue ? "1" : "0";
            }

            return value.ToString() ?? "NULL";
        }

        /// <summary>
        /// Maps a data reader to an entity of type T.
        /// </summary>
        /// <param name="reader">The data reader containing the data to map</param>
        /// <returns>An instance of T with properties populated from the reader data</returns>
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
                        object convertedValue = _DataTypeConverter.ConvertFromDatabase(value, property.PropertyType, property)!;
                        property.SetValue(entity, convertedValue);
                    }
                }
                catch (IndexOutOfRangeException)
                {
                    // Column doesn't exist in the result set, which is fine for Raw SQL scenarios
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Error mapping column '{columnName}' to property '{property.Name}': {ex.Message}", ex);
                }
            }
            return entity;
        }

        /// <summary>
        /// Maps a data reader to an instance of the specified result type.
        /// </summary>
        /// <typeparam name="TResult">The type to map the reader data to</typeparam>
        /// <param name="reader">The data reader containing the data to map</param>
        /// <returns>An instance of TResult with properties populated from the reader data</returns>
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
                        object convertedValue = _DataTypeConverter.ConvertFromDatabase(value, property.PropertyType, property)!;
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

        /// <summary>
        /// Gets the column name corresponding to the specified expression.
        /// </summary>
        /// <param name="expression">The expression to analyze.</param>
        /// <returns>The sanitized column name for the expression.</returns>
        internal string GetColumnFromExpression(Expression expression)
        {
            MySqlExpressionParser<T> parser = new MySqlExpressionParser<T>(_ColumnMappings, _Sanitizer);
            // The parser's GetColumnFromExpression already returns sanitized column names with backticks
            return parser.GetColumnFromExpression(expression);
        }

        #endregion

        #region Public-Utility-Methods

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

        /// <summary>
        /// Gets a connection from the connection factory.
        /// The returned connection is a PooledConnectionHandle that wraps the actual MySQL connection.
        /// Note: The caller is responsible for disposing the connection, which will return it to the pool.
        /// </summary>
        /// <returns>A database connection wrapping the MySQL connection</returns>
        public DbConnection GetConnection()
        {
            return _ConnectionFactory.GetConnection();
        }

        /// <summary>
        /// Asynchronously gets a connection from the connection factory.
        /// The returned connection is a PooledConnectionHandle that wraps the actual MySQL connection.
        /// Note: The caller is responsible for disposing the connection, which will return it to the pool.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token for the async operation</param>
        /// <returns>A database connection wrapping the MySQL connection</returns>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
        public async Task<DbConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return await _ConnectionFactory.GetConnectionAsync(cancellationToken).ConfigureAwait(false);
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

            // Check if table exists
            bool tableExists;
            if (transaction != null)
            {
                EnsureConnectionOpen(transaction.Connection);
                string databaseName = Settings?.Database ?? transaction.Connection.Database;
                tableExists = MySqlSchemaBuilder.TableExists(tableName, databaseName, transaction.Connection, transaction.Transaction);
            }
            else
            {
                using DbConnection connection = _ConnectionFactory.GetConnection();
                EnsureConnectionOpen(connection);
                string databaseName = Settings?.Database ?? connection.Database;
                tableExists = MySqlSchemaBuilder.TableExists(tableName, databaseName, connection);
            }

            if (!tableExists)
            {
                // Create the table
                MySqlSchemaBuilder schemaBuilder = new MySqlSchemaBuilder(_Sanitizer, _DataTypeConverter);
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

            // Check if table exists
            bool tableExists;
            if (transaction != null)
            {
                await EnsureConnectionOpenAsync(transaction.Connection, cancellationToken).ConfigureAwait(false);
                string databaseName = Settings?.Database ?? transaction.Connection.Database;
                tableExists = MySqlSchemaBuilder.TableExists(tableName, databaseName, transaction.Connection, transaction.Transaction);
            }
            else
            {
                using DbConnection connection = await _ConnectionFactory.GetConnectionAsync(cancellationToken).ConfigureAwait(false);
                await EnsureConnectionOpenAsync(connection, cancellationToken).ConfigureAwait(false);
                string databaseName = Settings?.Database ?? connection.Database;
                tableExists = MySqlSchemaBuilder.TableExists(tableName, databaseName, connection);
            }

            if (!tableExists)
            {
                // Create the table
                MySqlSchemaBuilder schemaBuilder = new MySqlSchemaBuilder(_Sanitizer, _DataTypeConverter);
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
            try
            {
                using (DbConnection conn = _ConnectionFactory.GetConnection())
                {
                    MySqlConnection connection = (MySqlConnection)PooledConnectionHandle.Unwrap(conn);

                    if (MySqlSchemaBuilder.TableExists(tableName, Settings.Database, connection))
                    {
                        List<ColumnInfo> existingColumns = MySqlSchemaBuilder.GetTableColumns(tableName, connection);
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

            MySqlSchemaBuilder schemaBuilder = new MySqlSchemaBuilder(_Sanitizer, _DataTypeConverter);
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

            MySqlSchemaBuilder schemaBuilder = new MySqlSchemaBuilder(_Sanitizer, _DataTypeConverter);
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

            EntityAttribute? entityAttr = typeof(T).GetCustomAttribute<EntityAttribute>();
            if (entityAttr == null)
                throw new InvalidOperationException($"Type '{typeof(T).Name}' must have an Entity attribute");

            string tableName = entityAttr.Name;
            string sql = $"DROP INDEX {_Sanitizer.SanitizeIdentifier(indexName)} ON {_Sanitizer.SanitizeIdentifier(tableName)}";

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

            EntityAttribute? entityAttr = typeof(T).GetCustomAttribute<EntityAttribute>();
            if (entityAttr == null)
                throw new InvalidOperationException($"Type '{typeof(T).Name}' must have an Entity attribute");

            string tableName = entityAttr.Name;
            string sql = $"DROP INDEX {_Sanitizer.SanitizeIdentifier(indexName)} ON {_Sanitizer.SanitizeIdentifier(tableName)}";

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

            using (DbConnection conn = _ConnectionFactory.GetConnection())
            {
                MySqlConnection connection = (MySqlConnection)PooledConnectionHandle.Unwrap(conn);

                string databaseName = Settings?.Database ?? connection.Database;
                List<IndexInfo> indexes = MySqlSchemaBuilder.GetExistingIndexes(tableName, databaseName, connection);
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

            using (DbConnection conn = _ConnectionFactory.GetConnection())
            {
                MySqlConnection connection = (MySqlConnection)PooledConnectionHandle.Unwrap(conn);

                await Task.Run(() =>
                {
                    // MySQL connector operations
                }, cancellationToken).ConfigureAwait(false);

                string databaseName = Settings?.Database ?? connection.Database;
                List<IndexInfo> indexes = MySqlSchemaBuilder.GetExistingIndexes(tableName, databaseName, connection);
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

            // Create a connection string without database specified
            MySqlRepositorySettings tempSettings = new MySqlRepositorySettings
            {
                Hostname = Settings.Hostname,
                Port = Settings.Port,
                Username = Settings.Username,
                Password = Settings.Password,
                Database = string.Empty // No database specified
            };

            string connectionString = tempSettings.BuildConnectionString();

            using (MySqlConnection connection = new MySqlConnection(connectionString))
            {
                connection.Open();

                // Check if database exists
                bool exists = MySqlSchemaBuilder.DatabaseExists(databaseName, connection);

                if (!exists)
                {
                    // Create the database
                    using (MySqlCommand command = connection.CreateCommand())
                    {
                        command.CommandText = $"CREATE DATABASE `{databaseName}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci";
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

            // Create a connection string without database specified
            MySqlRepositorySettings tempSettings = new MySqlRepositorySettings
            {
                Hostname = Settings.Hostname,
                Port = Settings.Port,
                Username = Settings.Username,
                Password = Settings.Password,
                Database = string.Empty // No database specified
            };

            string connectionString = tempSettings.BuildConnectionString();

            using (MySqlConnection connection = new MySqlConnection(connectionString))
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                // Check if database exists
                bool exists = MySqlSchemaBuilder.DatabaseExists(databaseName, connection);

                if (!exists)
                {
                    // Create the database
                    using (MySqlCommand command = connection.CreateCommand())
                    {
                        command.CommandText = $"CREATE DATABASE `{databaseName}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci";
                        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }
                }
            }
        }

        #endregion
    }
}