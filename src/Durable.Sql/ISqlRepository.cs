namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;

    /// <summary>
    /// A repository backed by a SQL database: the backend-neutral <see cref="IRepository{T}"/> plus raw SQL, stored
    /// procedures, multiple result sets, bulk insert, schema management and SQL capture.
    /// Raw SQL parameters are positional and referenced as <c>@p0</c>, <c>@p1</c>...; pass a <see cref="SqlParameterValue"/>
    /// to use an explicit name.
    /// Thread safety: safe for concurrent use; see <see cref="IRepository{T}"/>.
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    public interface ISqlRepository<[DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] T> : IRepository<T>, ISqlCapture where T : class, new()
    {
        #region Configuration

        /// <summary>
        /// Gets the dialect. Never null.
        /// </summary>
        ISqlDialect Dialect { get; }

        /// <summary>
        /// Gets the connection factory. Never null.
        /// </summary>
        IConnectionFactory ConnectionFactory { get; }

        /// <summary>
        /// Gets the options. Never null.
        /// </summary>
        SqlRepositoryOptions Options { get; }

        /// <summary>
        /// Gets the settings the repository was created from, or null when created from a connection factory.
        /// </summary>
        RepositorySettings? Settings { get; }

        #endregion

        #region Query-and-Transactions

        /// <summary>
        /// Starts a SQL query.
        /// </summary>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>A new query builder.</returns>
        new ISqlQueryBuilder<T> Query(ITransaction? transaction = null);

        /// <summary>
        /// Begins a transaction on a new connection.
        /// </summary>
        /// <returns>The transaction. Dispose it; an uncommitted transaction rolls back on dispose.</returns>
        new ISqlTransaction BeginTransaction();

        /// <summary>
        /// Begins a transaction on a new connection.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The transaction.</returns>
        new Task<ISqlTransaction> BeginTransactionAsync(CancellationToken token = default);

        #endregion

        #region Raw-SQL

        /// <summary>
        /// Executes raw SQL and maps rows to entities by column name. Results are streamed.
        /// </summary>
        /// <param name="sql">SQL. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="parameters">Values for @p0, @p1...; <see cref="SqlParameterValue"/> instances keep their names.</param>
        /// <returns>Entities.</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null.</exception>
        IEnumerable<T> FromSql(string sql, ITransaction? transaction = null, params object?[] parameters);

        /// <summary>
        /// Executes raw SQL and maps rows to <typeparamref name="TResult"/>: by column name for classes (matching
        /// column names, property names, or names ignoring case and underscores), or the first column for scalar types
        /// (including <see cref="string"/>). Class result types need a parameterless constructor.
        /// </summary>
        /// <typeparam name="TResult">Result type.</typeparam>
        /// <param name="sql">SQL. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="parameters">Values for @p0, @p1....</param>
        /// <returns>Results, streamed.</returns>
        IEnumerable<TResult> FromSql<[DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] TResult>(string sql, ITransaction? transaction = null, params object?[] parameters);

        /// <summary>
        /// Executes raw SQL that returns no rows.
        /// </summary>
        /// <param name="sql">SQL. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="parameters">Values for @p0, @p1....</param>
        /// <returns>Rows affected.</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null.</exception>
        int ExecuteSql(string sql, ITransaction? transaction = null, params object?[] parameters);

        /// <summary>
        /// Executes raw SQL and returns the first column of the first row converted to <typeparamref name="TResult"/>.
        /// </summary>
        /// <typeparam name="TResult">Result type.</typeparam>
        /// <param name="sql">SQL. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="parameters">Values for @p0, @p1....</param>
        /// <returns>The value, or default when no row or null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null.</exception>
        TResult? ExecuteScalar<TResult>(string sql, ITransaction? transaction = null, params object?[] parameters);

        /// <summary>
        /// Executes raw SQL and streams entities.
        /// </summary>
        /// <param name="sql">SQL. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <param name="parameters">Values for @p0, @p1....</param>
        /// <returns>Entities.</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null.</exception>
        IAsyncEnumerable<T> FromSqlAsync(string sql, ITransaction? transaction = null, CancellationToken token = default, params object?[] parameters);

        /// <summary>
        /// Executes raw SQL and streams <typeparamref name="TResult"/> rows.
        /// </summary>
        /// <typeparam name="TResult">Result type.</typeparam>
        /// <param name="sql">SQL. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <param name="parameters">Values for @p0, @p1....</param>
        /// <returns>Results.</returns>
        IAsyncEnumerable<TResult> FromSqlAsync<[DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] TResult>(string sql, ITransaction? transaction = null, CancellationToken token = default, params object?[] parameters);

        /// <summary>
        /// Executes raw SQL that returns no rows.
        /// </summary>
        /// <param name="sql">SQL. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <param name="parameters">Values for @p0, @p1....</param>
        /// <returns>Rows affected.</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null.</exception>
        Task<int> ExecuteSqlAsync(string sql, ITransaction? transaction = null, CancellationToken token = default, params object?[] parameters);

        /// <summary>
        /// Executes raw SQL and returns the first column of the first row.
        /// </summary>
        /// <typeparam name="TResult">Result type.</typeparam>
        /// <param name="sql">SQL. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <param name="parameters">Values for @p0, @p1....</param>
        /// <returns>The value, or default.</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null.</exception>
        Task<TResult?> ExecuteScalarAsync<TResult>(string sql, ITransaction? transaction = null, CancellationToken token = default, params object?[] parameters);

        /// <summary>
        /// Executes raw SQL returning several result sets; read them in order from the returned reader.
        /// </summary>
        /// <param name="sql">SQL with multiple SELECT statements. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="parameters">Values for @p0, @p1....</param>
        /// <returns>A reader over the result sets. Dispose it to release the connection.</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null.</exception>
        SqlMultipleResultReader QueryMultiple(string sql, ITransaction? transaction = null, params object?[] parameters);

        /// <summary>
        /// Executes raw SQL returning several result sets.
        /// </summary>
        /// <param name="sql">SQL. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <param name="parameters">Values for @p0, @p1....</param>
        /// <returns>A reader over the result sets. Dispose it to release the connection.</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null.</exception>
        Task<SqlMultipleResultReader> QueryMultipleAsync(string sql, ITransaction? transaction = null, CancellationToken token = default, params object?[] parameters);

        #endregion

        #region Stored-Procedures

        /// <summary>
        /// Executes a stored procedure that returns no rows. Output parameters are populated after execution.
        /// </summary>
        /// <param name="procedureName">Procedure name. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="parameters">Named parameters.</param>
        /// <returns>Rows affected as reported by the database.</returns>
        /// <exception cref="NotSupportedException">Thrown on databases without stored procedures (SQLite).</exception>
        /// <exception cref="ArgumentNullException">Thrown when procedureName is null.</exception>
        int ExecuteProcedure(string procedureName, ITransaction? transaction = null, params SqlParameterValue[] parameters);

        /// <summary>
        /// Executes a stored procedure and maps its first result set to <typeparamref name="TResult"/>.
        /// </summary>
        /// <typeparam name="TResult">Result type.</typeparam>
        /// <param name="procedureName">Procedure name. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="parameters">Named parameters.</param>
        /// <returns>Results, buffered.</returns>
        /// <exception cref="NotSupportedException">Thrown on databases without stored procedures (SQLite).</exception>
        List<TResult> FromProcedure<[DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] TResult>(string procedureName, ITransaction? transaction = null, params SqlParameterValue[] parameters);

        /// <summary>
        /// Executes a stored procedure that returns no rows.
        /// </summary>
        /// <param name="procedureName">Procedure name. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <param name="parameters">Named parameters.</param>
        /// <returns>Rows affected.</returns>
        /// <exception cref="ArgumentNullException">Thrown when procedureName is null.</exception>
        Task<int> ExecuteProcedureAsync(string procedureName, ITransaction? transaction = null, CancellationToken token = default, params SqlParameterValue[] parameters);

        /// <summary>
        /// Executes a stored procedure and maps its first result set.
        /// </summary>
        /// <typeparam name="TResult">Result type.</typeparam>
        /// <param name="procedureName">Procedure name. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <param name="parameters">Named parameters.</param>
        /// <returns>Results, buffered.</returns>
        Task<List<TResult>> FromProcedureAsync<[DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] TResult>(string procedureName, ITransaction? transaction = null, CancellationToken token = default, params SqlParameterValue[] parameters);

        #endregion

        #region Bulk

        /// <summary>
        /// Inserts many rows with the database's fastest bulk path (SqlBulkCopy, PostgreSQL COPY, MySQL bulk copy, or
        /// prepared batched inserts on SQLite). Generated keys are not written back; use <see cref="IRepository{T}.CreateMany"/> when you need them.
        /// Default values and initial versions are applied before insert.
        /// </summary>
        /// <param name="entities">Entities. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>Rows inserted.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entities is null.</exception>
        long BulkInsert(IEnumerable<T> entities, ITransaction? transaction = null);

        /// <summary>
        /// Inserts many rows with the database's fastest bulk path.
        /// </summary>
        /// <param name="entities">Entities. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Rows inserted.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entities is null.</exception>
        Task<long> BulkInsertAsync(IEnumerable<T> entities, ITransaction? transaction = null, CancellationToken token = default);

        #endregion

        #region Schema

        /// <summary>
        /// Creates the table (and its declared indexes) for an entity type if it does not exist, then validates it.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the mapping is invalid or the existing table lacks mapped columns.</exception>
        void InitializeTable([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType, ITransaction? transaction = null);

        /// <summary>
        /// Creates the table for an entity type if it does not exist, then validates it.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        Task InitializeTableAsync([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType, ITransaction? transaction = null, CancellationToken token = default);

        /// <summary>
        /// Initializes tables for several entity types in order.
        /// </summary>
        /// <param name="entityTypes">Entity types. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when entityTypes is null.</exception>
        [RequiresUnreferencedCode("Entity types passed in a collection cannot be analyzed by trimming, so their public properties may be removed. Under trimming or Native AOT, call the single-type overload for each entity type.")]
        void InitializeTables(IEnumerable<Type> entityTypes, ITransaction? transaction = null);

        /// <summary>
        /// Initializes tables for several entity types in order.
        /// </summary>
        /// <param name="entityTypes">Entity types. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityTypes is null.</exception>
        [RequiresUnreferencedCode("Entity types passed in a collection cannot be analyzed by trimming, so their public properties may be removed. Under trimming or Native AOT, call the single-type overload for each entity type.")]
        Task InitializeTablesAsync(IEnumerable<Type> entityTypes, ITransaction? transaction = null, CancellationToken token = default);

        /// <summary>
        /// Validates an entity mapping and, when its table exists, compares it with the table's columns.
        /// Mapping problems and mapped columns missing from the table are errors; table columns the entity does not map
        /// are warnings. A missing table is not an error (<see cref="InitializeTable"/> creates it).
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>The validation result. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        TableValidationResult ValidateTable([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType, ITransaction? transaction = null);

        /// <summary>
        /// Validates an entity mapping and, when its table exists, compares it with the table's columns.
        /// Mapping problems and mapped columns missing from the table are errors; table columns the entity does not map
        /// are warnings. A missing table is not an error (<see cref="InitializeTableAsync"/> creates it).
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The validation result. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        /// <exception cref="OperationCanceledException">Thrown when <paramref name="token"/> is canceled.</exception>
        Task<TableValidationResult> ValidateTableAsync([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType, ITransaction? transaction = null, CancellationToken token = default);

        /// <summary>
        /// Validates several entity mappings against the database (see <see cref="ValidateTable"/>).
        /// </summary>
        /// <param name="entityTypes">Entity types. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>The combined validation result. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityTypes is null.</exception>
        [RequiresUnreferencedCode("Entity types passed in a collection cannot be analyzed by trimming, so their public properties may be removed. Under trimming or Native AOT, call the single-type overload for each entity type.")]
        SchemaValidationResult ValidateTables(IEnumerable<Type> entityTypes, ITransaction? transaction = null);

        /// <summary>
        /// Validates several entity mappings against the database (see <see cref="ValidateTableAsync"/>).
        /// </summary>
        /// <param name="entityTypes">Entity types. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The combined validation result. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityTypes is null.</exception>
        /// <exception cref="OperationCanceledException">Thrown when <paramref name="token"/> is canceled.</exception>
        [RequiresUnreferencedCode("Entity types passed in a collection cannot be analyzed by trimming, so their public properties may be removed. Under trimming or Native AOT, call the single-type overload for each entity type.")]
        Task<SchemaValidationResult> ValidateTablesAsync(IEnumerable<Type> entityTypes, ITransaction? transaction = null, CancellationToken token = default);

        /// <summary>
        /// Creates the indexes declared with <see cref="IndexAttribute"/> and <see cref="CompositeIndexAttribute"/> that do not exist yet.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        void CreateIndexes([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType, ITransaction? transaction = null);

        /// <summary>
        /// Creates declared indexes that do not exist yet.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        Task CreateIndexesAsync([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType, ITransaction? transaction = null, CancellationToken token = default);

        /// <summary>
        /// Drops an index on this repository's table.
        /// </summary>
        /// <param name="indexName">Index name. Must not be null or empty.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when indexName is null.</exception>
        void DropIndex(string indexName, ITransaction? transaction = null);

        /// <summary>
        /// Drops an index on this repository's table.
        /// </summary>
        /// <param name="indexName">Index name. Must not be null or empty.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task.</returns>
        /// <exception cref="ArgumentNullException">Thrown when indexName is null.</exception>
        Task DropIndexAsync(string indexName, ITransaction? transaction = null, CancellationToken token = default);

        /// <summary>
        /// Lists index names on an entity's table (excluding primary key indexes).
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <returns>Index names. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        List<string> GetIndexes([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType);

        /// <summary>
        /// Lists index names on an entity's table.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Index names. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        Task<List<string>> GetIndexesAsync([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType, CancellationToken token = default);

        /// <summary>
        /// Creates the configured database when it does not exist (for SQLite, creates the file).
        /// </summary>
        void CreateDatabaseIfNotExists();

        /// <summary>
        /// Creates the configured database when it does not exist.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task.</returns>
        Task CreateDatabaseIfNotExistsAsync(CancellationToken token = default);

        #endregion
    }
}
