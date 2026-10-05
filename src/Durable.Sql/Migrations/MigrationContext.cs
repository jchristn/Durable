namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Passed to <see cref="Migration.Up(MigrationContext)"/> and <see cref="Migration.Down(MigrationContext)"/>: executes
    /// parameterized SQL and synchronizes schema on the migration's connection and transaction.
    /// <para>
    /// While a script is being generated (<see cref="IsScripting"/>), statements are written to the script instead of being
    /// executed, schema reads still query the live database, and <see cref="ExecuteScalar"/> is unavailable.
    /// </para>
    /// Thread safety: not thread-safe; valid only for the duration of the migration call.
    /// </summary>
    public sealed class MigrationContext
    {
        #region Public-Members

        /// <summary>
        /// Gets the dialect. Never null.
        /// </summary>
        public ISqlDialect Dialect => _Session.Dialect;

        /// <summary>
        /// Gets the migration being run. Never null.
        /// </summary>
        public Migration Migration { get; }

        /// <summary>
        /// Gets whether statements are being written to a script rather than executed.
        /// </summary>
        public bool IsScripting => _Script != null;

        /// <summary>
        /// Gets the migration's connection and transaction, for passing to repository methods so they run inside the
        /// migration (for example to seed data). When the migration runs without a transaction this wraps the connection only.
        /// Null while scripting.
        /// </summary>
        public ISqlTransaction? Transaction => IsScripting ? null : _Session.Context;

        #endregion

        #region Private-Members

        private readonly MigrationSession _Session;
        private readonly StringBuilder? _Script;

        #endregion

        #region Constructors-and-Factories

        internal MigrationContext(MigrationSession session, Migration migration, StringBuilder? script)
        {
            _Session = session ?? throw new ArgumentNullException(nameof(session));
            Migration = migration ?? throw new ArgumentNullException(nameof(migration));
            _Script = script;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Executes a statement that returns no rows. Parameters are referenced by the dialect's positional names
        /// (@p0, @p1, ...), as with <see cref="ISqlRepository{T}.ExecuteSql"/>, and are always bound, never inlined
        /// (except in generated scripts).
        /// </summary>
        /// <param name="sql">SQL. Must not be null or empty.</param>
        /// <param name="parameters">Parameter values; may be empty.</param>
        /// <returns>Rows affected; 0 while scripting.</returns>
        /// <exception cref="ArgumentException">Thrown when sql is null or empty.</exception>
        public int ExecuteSql(string sql, params object?[] parameters)
        {
            SqlStatement statement = Build(sql, parameters);
            if (_Script != null)
            {
                MigrationScriptWriter.AppendStatement(_Script, Dialect, statement);
                return 0;
            }

            return _Session.Execute(statement, "MIGRATION");
        }

        /// <summary>
        /// Executes a statement that returns no rows. See <see cref="ExecuteSql"/> for parameter syntax.
        /// </summary>
        /// <param name="sql">SQL. Must not be null or empty.</param>
        /// <param name="parameters">Parameter values; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Rows affected; 0 while scripting.</returns>
        /// <exception cref="ArgumentException">Thrown when sql is null or empty.</exception>
        public Task<int> ExecuteSqlAsync(string sql, object?[]? parameters = null, CancellationToken token = default)
        {
            SqlStatement statement = Build(sql, parameters);
            token.ThrowIfCancellationRequested();
            if (_Script != null)
            {
                MigrationScriptWriter.AppendStatement(_Script, Dialect, statement);
                return Task.FromResult(0);
            }

            return _Session.ExecuteAsync(statement, "MIGRATION", token);
        }

        /// <summary>
        /// Executes a query and returns the first column of the first row. See <see cref="ExecuteSql"/> for parameter syntax.
        /// </summary>
        /// <param name="sql">SQL. Must not be null or empty.</param>
        /// <param name="parameters">Parameter values; may be empty.</param>
        /// <returns>The value, or null for no row or a database null.</returns>
        /// <exception cref="ArgumentException">Thrown when sql is null or empty.</exception>
        /// <exception cref="InvalidOperationException">Thrown while scripting.</exception>
        public object? ExecuteScalar(string sql, params object?[] parameters)
        {
            SqlStatement statement = Build(sql, parameters);
            RequireNotScripting();
            return _Session.Scalar(statement, "MIGRATION");
        }

        /// <summary>
        /// Executes a query and returns the first column of the first row. See <see cref="ExecuteSql"/> for parameter syntax.
        /// </summary>
        /// <param name="sql">SQL. Must not be null or empty.</param>
        /// <param name="parameters">Parameter values; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The value, or null for no row or a database null.</returns>
        /// <exception cref="ArgumentException">Thrown when sql is null or empty.</exception>
        /// <exception cref="InvalidOperationException">Thrown while scripting.</exception>
        public Task<object?> ExecuteScalarAsync(string sql, object?[]? parameters = null, CancellationToken token = default)
        {
            SqlStatement statement = Build(sql, parameters);
            RequireNotScripting();
            return _Session.ScalarAsync(statement, "MIGRATION", token);
        }

        /// <summary>
        /// Brings the tables of the given entity types up to date with additive operations (create tables, add columns,
        /// create indexes) using default <see cref="SchemaSyncOptions"/>. Destructive operations are skipped and
        /// differences are reported in the result.
        /// </summary>
        /// <param name="entityTypes">Entity types. Must not be null.</param>
        /// <returns>The result.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityTypes is null.</exception>
        public SchemaSyncResult EnsureSchema(params Type[] entityTypes)
        {
            return EnsureSchema(new SchemaSyncOptions(), entityTypes);
        }

        /// <summary>
        /// Brings the tables of the given entity types up to date. Runs inside the migration's transaction (the
        /// <see cref="SchemaSyncOptions.UseTransaction"/> option does not apply).
        /// </summary>
        /// <param name="options">Options. Must not be null.</param>
        /// <param name="entityTypes">Entity types. Must not be null.</param>
        /// <returns>The result.</returns>
        /// <exception cref="ArgumentNullException">Thrown when options or entityTypes is null.</exception>
        public SchemaSyncResult EnsureSchema(SchemaSyncOptions options, params Type[] entityTypes)
        {
            ArgumentNullException.ThrowIfNull(options);
            ArgumentNullException.ThrowIfNull(entityTypes);
            List<Type> types = entityTypes.ToList();
            SchemaDiff diff = SchemaSynchronizer.Diff(_Session, types, options);
            if (_Script != null) return SchemaSynchronizer.Script(diff, options, _Script);
            return SchemaSynchronizer.Apply(_Session, diff, options, new List<MigrationOperation>());
        }

        /// <summary>
        /// Brings the tables of the given entity types up to date. Runs inside the migration's transaction.
        /// </summary>
        /// <param name="entityTypes">Entity types. Must not be null.</param>
        /// <param name="options">Options; null uses defaults (additive only).</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The result.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityTypes is null.</exception>
        public async Task<SchemaSyncResult> EnsureSchemaAsync(IEnumerable<Type> entityTypes, SchemaSyncOptions? options = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entityTypes);
            options ??= new SchemaSyncOptions();
            List<Type> types = entityTypes.ToList();
            SchemaDiff diff = await SchemaSynchronizer.DiffAsync(_Session, types, options, token).ConfigureAwait(false);
            if (_Script != null) return SchemaSynchronizer.Script(diff, options, _Script);
            return await SchemaSynchronizer.ApplyAsync(_Session, diff, options, new List<MigrationOperation>(), token).ConfigureAwait(false);
        }

        /// <summary>
        /// Reads a table's live schema on the migration's connection (sees uncommitted changes of this migration).
        /// </summary>
        /// <param name="tableName">Table name. Must not be null or empty.</param>
        /// <returns>The table, or null when it does not exist.</returns>
        /// <exception cref="ArgumentException">Thrown when tableName is null or empty.</exception>
        public TableSchema? ReadTable(string tableName)
        {
            if (string.IsNullOrEmpty(tableName)) throw new ArgumentException("Table name cannot be null or empty.", nameof(tableName));
            return DatabaseSchemaReader.ReadTable(_Session, tableName);
        }

        /// <summary>
        /// Reads a table's live schema on the migration's connection (sees uncommitted changes of this migration).
        /// </summary>
        /// <param name="tableName">Table name. Must not be null or empty.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The table, or null when it does not exist.</returns>
        /// <exception cref="ArgumentException">Thrown when tableName is null or empty.</exception>
        public Task<TableSchema?> ReadTableAsync(string tableName, CancellationToken token = default)
        {
            if (string.IsNullOrEmpty(tableName)) throw new ArgumentException("Table name cannot be null or empty.", nameof(tableName));
            return DatabaseSchemaReader.ReadTableAsync(_Session, tableName, token);
        }

        #endregion

        #region Private-Methods

        private SqlStatement Build(string sql, object?[]? parameters)
        {
            if (string.IsNullOrEmpty(sql)) throw new ArgumentException("SQL cannot be null or empty.", nameof(sql));
            return RawSql.Positional(sql, parameters, Dialect, Dialect.Converter);
        }

        private void RequireNotScripting()
        {
            if (_Script != null)
                throw new InvalidOperationException("Migration " + Migration.Id + " reads data, which is not possible while generating a script; check MigrationContext.IsScripting.");
        }

        #endregion
    }
}
