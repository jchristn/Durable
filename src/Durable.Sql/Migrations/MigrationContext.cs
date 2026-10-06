namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
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
        /// Executes an interpolated statement that returns no rows. Every interpolation hole becomes a bound parameter
        /// (inlined as a literal only in generated scripts); see <see cref="RawSql"/>.
        /// </summary>
        /// <param name="sql">Interpolated SQL. Must not be null.</param>
        /// <returns>Rows affected; 0 while scripting.</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null.</exception>
        /// <exception cref="ArgumentException">Thrown when the SQL is empty.</exception>
        public int ExecuteSql(FormattableString sql)
        {
            return Execute(BuildInterpolated(sql));
        }

        /// <summary>
        /// Executes an interpolated statement that returns no rows. Every hole becomes a bound parameter.
        /// </summary>
        /// <param name="sql">Interpolated SQL. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Rows affected; 0 while scripting.</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null.</exception>
        /// <exception cref="ArgumentException">Thrown when the SQL is empty.</exception>
        public Task<int> ExecuteSqlAsync(FormattableString sql, CancellationToken token = default)
        {
            return ExecuteAsync(BuildInterpolated(sql), token);
        }

        /// <summary>
        /// Executes SQL text that returns no rows. <c>{0}</c>, <c>{1}</c>... bind the corresponding
        /// <paramref name="parameters"/> (see <see cref="RawSql"/>); without parameters the text is used verbatim.
        /// </summary>
        /// <param name="sql">SQL. Must not be null or empty.</param>
        /// <param name="parameters">Placeholder values; null for none.</param>
        /// <returns>Rows affected; 0 while scripting.</returns>
        /// <exception cref="ArgumentException">Thrown when sql is null or empty.</exception>
        public int ExecuteSqlRaw(string sql, IEnumerable<object?>? parameters = null)
        {
            return Execute(Build(sql, parameters));
        }

        /// <summary>
        /// Executes SQL text that returns no rows. Placeholders as for <see cref="ExecuteSqlRaw"/>.
        /// </summary>
        /// <param name="sql">SQL. Must not be null or empty.</param>
        /// <param name="parameters">Placeholder values; null for none.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Rows affected; 0 while scripting.</returns>
        /// <exception cref="ArgumentException">Thrown when sql is null or empty.</exception>
        public Task<int> ExecuteSqlRawAsync(string sql, IEnumerable<object?>? parameters = null, CancellationToken token = default)
        {
            return ExecuteAsync(Build(sql, parameters), token);
        }

        /// <summary>
        /// Executes an interpolated query and returns the first column of the first row. Every hole becomes a bound parameter.
        /// </summary>
        /// <param name="sql">Interpolated SQL. Must not be null.</param>
        /// <returns>The value, or null for no row or a database null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown while scripting.</exception>
        public object? ExecuteScalar(FormattableString sql)
        {
            SqlStatement statement = BuildInterpolated(sql);
            RequireNotScripting();
            return _Session.Scalar(statement, "MIGRATION");
        }

        /// <summary>
        /// Executes an interpolated query and returns the first column of the first row.
        /// </summary>
        /// <param name="sql">Interpolated SQL. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The value, or null for no row or a database null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown while scripting.</exception>
        public Task<object?> ExecuteScalarAsync(FormattableString sql, CancellationToken token = default)
        {
            SqlStatement statement = BuildInterpolated(sql);
            RequireNotScripting();
            return _Session.ScalarAsync(statement, "MIGRATION", token);
        }

        /// <summary>
        /// Executes a query and returns the first column of the first row. Placeholders as for <see cref="ExecuteSqlRaw"/>.
        /// </summary>
        /// <param name="sql">SQL. Must not be null or empty.</param>
        /// <param name="parameters">Placeholder values; null for none.</param>
        /// <returns>The value, or null for no row or a database null.</returns>
        /// <exception cref="ArgumentException">Thrown when sql is null or empty.</exception>
        /// <exception cref="InvalidOperationException">Thrown while scripting.</exception>
        public object? ExecuteScalarRaw(string sql, IEnumerable<object?>? parameters = null)
        {
            SqlStatement statement = Build(sql, parameters);
            RequireNotScripting();
            return _Session.Scalar(statement, "MIGRATION");
        }

        /// <summary>
        /// Executes a query and returns the first column of the first row. Placeholders as for <see cref="ExecuteSqlRaw"/>.
        /// </summary>
        /// <param name="sql">SQL. Must not be null or empty.</param>
        /// <param name="parameters">Placeholder values; null for none.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The value, or null for no row or a database null.</returns>
        /// <exception cref="ArgumentException">Thrown when sql is null or empty.</exception>
        /// <exception cref="InvalidOperationException">Thrown while scripting.</exception>
        public Task<object?> ExecuteScalarRawAsync(string sql, IEnumerable<object?>? parameters = null, CancellationToken token = default)
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
        [RequiresUnreferencedCode("Entity types passed as Type values cannot be analyzed by trimming, so their public properties may be removed. Under trimming or Native AOT, use the overload taking EntityMetadata values created with EntityMetadata.For<T>().")]
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
        [RequiresUnreferencedCode("Entity types passed as Type values cannot be analyzed by trimming, so their public properties may be removed. Under trimming or Native AOT, use the overload taking EntityMetadata values created with EntityMetadata.For<T>().")]
        public SchemaSyncResult EnsureSchema(SchemaSyncOptions options, params Type[] entityTypes)
        {
            ArgumentNullException.ThrowIfNull(entityTypes);
            return EnsureSchema(options, SchemaSynchronizer.ToMetadata(entityTypes).ToArray());
        }

        /// <summary>
        /// Brings the tables of the given entities up to date with additive operations using default
        /// <see cref="SchemaSyncOptions"/>. Trimming and Native AOT safe: create the metadata with
        /// <c>EntityMetadata.For&lt;T&gt;()</c>.
        /// </summary>
        /// <param name="entities">Entity metadata. Must not be null.</param>
        /// <returns>The result.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entities is null.</exception>
        public SchemaSyncResult EnsureSchema(params EntityMetadata[] entities)
        {
            return EnsureSchema(new SchemaSyncOptions(), entities);
        }

        /// <summary>
        /// Brings the tables of the given entities up to date. Runs inside the migration's transaction (the
        /// <see cref="SchemaSyncOptions.UseTransaction"/> option does not apply). Trimming and Native AOT safe.
        /// </summary>
        /// <param name="options">Options. Must not be null.</param>
        /// <param name="entities">Entity metadata. Must not be null.</param>
        /// <returns>The result.</returns>
        /// <exception cref="ArgumentNullException">Thrown when options or entities is null.</exception>
        public SchemaSyncResult EnsureSchema(SchemaSyncOptions options, params EntityMetadata[] entities)
        {
            ArgumentNullException.ThrowIfNull(options);
            ArgumentNullException.ThrowIfNull(entities);
            List<EntityMetadata> list = entities.ToList();
            SchemaDiff diff = SchemaSynchronizer.Diff(_Session, list, options);
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
        [RequiresUnreferencedCode("Entity types passed as Type values cannot be analyzed by trimming, so their public properties may be removed. Under trimming or Native AOT, use the overload taking EntityMetadata values created with EntityMetadata.For<T>().")]
        public Task<SchemaSyncResult> EnsureSchemaAsync(IEnumerable<Type> entityTypes, SchemaSyncOptions? options = null, CancellationToken token = default)
        {
            return EnsureSchemaAsync(SchemaSynchronizer.ToMetadata(entityTypes), options, token);
        }

        /// <summary>
        /// Brings the tables of the given entities up to date. Runs inside the migration's transaction. Trimming and
        /// Native AOT safe: create the metadata with <c>EntityMetadata.For&lt;T&gt;()</c>.
        /// </summary>
        /// <param name="entities">Entity metadata. Must not be null.</param>
        /// <param name="options">Options; null uses defaults (additive only).</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The result.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entities is null.</exception>
        public async Task<SchemaSyncResult> EnsureSchemaAsync(IEnumerable<EntityMetadata> entities, SchemaSyncOptions? options = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entities);
            options ??= new SchemaSyncOptions();
            List<EntityMetadata> list = entities.ToList();
            SchemaDiff diff = await SchemaSynchronizer.DiffAsync(_Session, list, options, token).ConfigureAwait(false);
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

        private SqlStatement Build(string sql, IEnumerable<object?>? parameters)
        {
            if (string.IsNullOrEmpty(sql)) throw new ArgumentException("SQL cannot be null or empty.", nameof(sql));
            return RawSql.ToStatement(sql, parameters, Dialect, Dialect.Converter);
        }

        private SqlStatement BuildInterpolated(FormattableString sql)
        {
            ArgumentNullException.ThrowIfNull(sql);
            if (string.IsNullOrEmpty(sql.Format)) throw new ArgumentException("SQL cannot be empty.", nameof(sql));
            return RawSql.ToStatement(sql, Dialect, Dialect.Converter);
        }

        private int Execute(SqlStatement statement)
        {
            if (_Script != null)
            {
                MigrationScriptWriter.AppendStatement(_Script, Dialect, statement);
                return 0;
            }

            return _Session.Execute(statement, "MIGRATION");
        }

        private Task<int> ExecuteAsync(SqlStatement statement, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            if (_Script != null)
            {
                MigrationScriptWriter.AppendStatement(_Script, Dialect, statement);
                return Task.FromResult(0);
            }

            return _Session.ExecuteAsync(statement, "MIGRATION", token);
        }

        private void RequireNotScripting()
        {
            if (_Script != null)
                throw new InvalidOperationException("Migration " + Migration.Id + " reads data, which is not possible while generating a script; check MigrationContext.IsScripting.");
        }

        #endregion
    }
}
