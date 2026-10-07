namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using Durable;
    using Durable.Query;

    /// <summary>
    /// Everything that differs between SQL databases. The shared engine (<see cref="SqlRepository{T}"/>,
    /// <see cref="SqlQueryBuilder{T}"/>, <see cref="SqlExpressionTranslator"/>) generates SQL only through this contract,
    /// so adding a database means implementing a dialect, a connection factory and a thin repository subclass.
    /// Derive from <see cref="SqlDialect"/>, which supplies ANSI defaults.
    /// Thread safety: implementations must be immutable and thread-safe; one instance is shared by all repositories of a provider.
    /// </summary>
    public interface ISqlDialect
    {
        #region General

        /// <summary>
        /// Gets the repository type identifying this dialect.
        /// </summary>
        RepositoryType RepositoryType { get; }

        /// <summary>
        /// Gets the OpenTelemetry <c>db.system</c> name (for example, "postgresql").
        /// </summary>
        string DbSystemName { get; }

        /// <summary>
        /// Gets the converter between CLR values and database values. Never null.
        /// </summary>
        IDataTypeConverter Converter { get; }

        /// <summary>
        /// Gets the maximum number of parameters the engine will place in one command. Lists and batches are split to stay below it.
        /// </summary>
        int MaxParameters { get; }

        /// <summary>
        /// Quotes an identifier, escaping embedded quote characters. Dotted names (schema.table) are quoted per part.
        /// </summary>
        /// <param name="identifier">Identifier. Must not be null or empty.</param>
        /// <returns>The quoted identifier.</returns>
        /// <exception cref="ArgumentException">Thrown when identifier is null or empty.</exception>
        string QuoteIdentifier(string identifier);

        /// <summary>
        /// Returns the placeholder for the parameter at an index (for example, "@p0").
        /// </summary>
        /// <param name="index">Zero-based index.</param>
        /// <returns>The parameter name including prefix.</returns>
        string FormatParameterName(int index);

        /// <summary>
        /// Applies provider-specific typing to a parameter before execution (for example, jsonb for JSON columns).
        /// </summary>
        /// <param name="parameter">The provider parameter. Must not be null.</param>
        /// <param name="value">The engine parameter. Must not be null.</param>
        void ConfigureParameter(DbParameter parameter, SqlParameterValue value);

        /// <summary>
        /// Gets whether the ADO.NET driver enforces <see cref="DbCommand.CommandTimeout"/>. When false (DuckDB.NET ignores
        /// it), the engine enforces <see cref="SqlRepositoryOptions.CommandTimeoutSeconds"/> itself: it cancels the command
        /// when the timeout elapses and throws a <see cref="TimeoutException"/>. Only an explicitly configured timeout is
        /// enforced this way; without one, commands run until they finish. Default: true.
        /// </summary>
        bool DriverEnforcesCommandTimeout { get; }

        /// <summary>
        /// Gets how many times a statement that runs outside a transaction (autocommit) and fails with a
        /// <see cref="IsRetryableConflict"/> error is run again, after a short randomized delay. Databases with optimistic
        /// concurrency (DuckDB) fail a write that conflicts with a concurrent transaction instead of waiting for it; a
        /// statement on its own was rolled back as a whole, so running it again gives the waiting behavior of other
        /// databases. Statements inside a transaction are never retried (the transaction is aborted). Default: 0.
        /// </summary>
        int AutocommitConflictRetries { get; }

        /// <summary>
        /// Returns whether an exception is a transient write-write conflict with a concurrent transaction (see
        /// <see cref="AutocommitConflictRetries"/>). Default: false.
        /// </summary>
        /// <param name="exception">Exception thrown by a statement. Must not be null.</param>
        /// <returns>True when the statement may be run again.</returns>
        bool IsRetryableConflict(Exception exception);

        #endregion

        #region Expressions

        /// <summary>
        /// Returns the SQL literal for a boolean (TRUE/FALSE or 1/0).
        /// </summary>
        /// <param name="value">Value.</param>
        /// <returns>The literal.</returns>
        string BooleanLiteral(bool value);

        /// <summary>
        /// Gets the escape character used for LIKE patterns; emitted as an ESCAPE clause.
        /// </summary>
        char LikeEscapeCharacter { get; }

        /// <summary>
        /// Escapes LIKE wildcards in a literal value using <see cref="LikeEscapeCharacter"/>.
        /// </summary>
        /// <param name="value">Literal value. Must not be null.</param>
        /// <returns>The escaped value.</returns>
        string EscapeLikePattern(string value);

        /// <summary>
        /// Concatenates SQL string expressions.
        /// </summary>
        /// <param name="parts">SQL fragments. Must contain at least two items.</param>
        /// <returns>The concatenation expression.</returns>
        string Concat(IReadOnlyList<string> parts);

        /// <summary>
        /// Divides two SQL expressions. When both operands are integers the result must truncate like C# integer division.
        /// </summary>
        /// <param name="left">Dividend SQL.</param>
        /// <param name="right">Divisor SQL.</param>
        /// <param name="integerOperands">Whether both operands are integer-typed.</param>
        /// <returns>The division expression.</returns>
        string Divide(string left, string right, bool integerOperands);

        /// <summary>
        /// Returns a condition that is true when a non-null string expression is empty (zero characters, not blank).
        /// </summary>
        /// <param name="expression">String SQL expression.</param>
        /// <returns>The condition.</returns>
        string IsEmptyString(string expression);

        /// <summary>
        /// Returns the ORDER BY direction suffix for a sort key, with LINQ's null ordering: NULLs first when ascending and
        /// last when descending (the default for SQLite, MySQL and SQL Server; PostgreSQL needs NULLS FIRST / NULLS LAST).
        /// </summary>
        /// <param name="descending">Whether the key sorts descending.</param>
        /// <returns>The suffix, including a leading space (for example " ASC").</returns>
        string OrderDirection(bool descending);

        /// <summary>
        /// Gets the type name used to convert a non-string value to text in concatenations (<c>CAST(x AS ...)</c>).
        /// </summary>
        string StringCastType { get; }

        /// <summary>
        /// Applies ordinal (binary, case- and accent-sensitive) comparison to a string expression, for
        /// <see cref="StringMatchMode.Ordinal"/> and <see cref="StringMatchMode.IgnoreCase"/>; typically
        /// <c>expression COLLATE &lt;binary collation&gt;</c>. Applied to one operand of =, &lt;&gt;, &lt;, &gt;, IN and LIKE.
        /// </summary>
        /// <param name="expression">String SQL expression.</param>
        /// <returns>The expression with ordinal comparison semantics.</returns>
        string OrdinalCollation(string expression);

        /// <summary>
        /// Gets whether LIKE honours <see cref="OrdinalCollation"/>. When false (SQLite, whose LIKE folds ASCII case
        /// regardless of collation), ordinal Contains/StartsWith/EndsWith use <see cref="OrdinalStringMatch"/>.
        /// </summary>
        bool SupportsOrdinalLike { get; }

        /// <summary>
        /// Returns an exact, case-sensitive substring test without LIKE. Used only when <see cref="SupportsOrdinalLike"/> is false.
        /// </summary>
        /// <param name="kind">Kind of test.</param>
        /// <param name="target">Searched string SQL expression.</param>
        /// <param name="value">Literal text SQL expression (a parameter or expression; not a LIKE pattern).</param>
        /// <returns>The condition.</returns>
        /// <exception cref="NotSupportedException">Thrown when the dialect supports ordinal LIKE instead.</exception>
        string OrdinalStringMatch(StringMatchKind kind, string target, string value);

        /// <summary>
        /// Translates a scalar function.
        /// </summary>
        /// <param name="function">Function.</param>
        /// <param name="arguments">SQL fragments for the arguments.</param>
        /// <returns>The SQL expression.</returns>
        /// <exception cref="NotSupportedException">Thrown when the dialect cannot express the function.</exception>
        string TranslateFunction(QueryFunction function, IReadOnlyList<string> arguments);

        /// <summary>
        /// Gets the keyword introducing a recursive CTE ("WITH RECURSIVE" or "WITH").
        /// </summary>
        string RecursiveCteKeyword { get; }

        /// <summary>
        /// Appends LIMIT/OFFSET (or equivalent) to a SELECT. Called after ORDER BY has been appended.
        /// </summary>
        /// <param name="builder">Statement builder. Must not be null.</param>
        /// <param name="skip">Rows to skip; null for none.</param>
        /// <param name="take">Rows to take; null for unlimited.</param>
        /// <param name="hasOrderBy">Whether an ORDER BY was emitted.</param>
        void AppendPaging(SqlStatementBuilder builder, int? skip, int? take, bool hasOrderBy);

        #endregion

        #region Writes

        /// <summary>
        /// Gets how generated values are returned from INSERT.
        /// </summary>
        InsertKeyStrategy InsertKeyStrategy { get; }

        /// <summary>
        /// Gets the statement returning the last generated identity for <see cref="InsertKeyStrategy.LastInsertId"/>; null otherwise.
        /// </summary>
        string? LastInsertIdSql { get; }

        /// <summary>
        /// Appends an upsert statement (insert, or update on key conflict) without a terminating separator.
        /// Values are supplied as placeholders already registered with the builder.
        /// </summary>
        /// <param name="builder">Statement builder. Must not be null.</param>
        /// <param name="metadata">Entity metadata. Must not be null.</param>
        /// <param name="insertColumns">Columns being inserted. Must not be null.</param>
        /// <param name="placeholders">Placeholders aligned with insertColumns. Must not be null.</param>
        /// <param name="conflictColumns">Key columns identifying the row. Must not be null or empty.</param>
        /// <param name="updateColumns">Columns updated on conflict; may be empty.</param>
        /// <param name="versionPlaceholder">Placeholder holding the incremented version assigned to the version column on the update path; null when the entity has no version column.</param>
        void AppendUpsert(SqlStatementBuilder builder, EntityMetadata metadata, IReadOnlyList<ColumnMetadata> insertColumns, IReadOnlyList<string> placeholders, IReadOnlyList<ColumnMetadata> conflictColumns, IReadOnlyList<ColumnMetadata> updateColumns, string? versionPlaceholder = null);

        /// <summary>
        /// Gets the statement separator used when batching statements in one command. Default ";".
        /// </summary>
        string StatementSeparator { get; }

        /// <summary>
        /// Gets the clause inserting a row with only default values, appended after "INSERT INTO table "
        /// (for example "DEFAULT VALUES", or "() VALUES ()" on MySQL).
        /// </summary>
        string InsertDefaultValuesClause { get; }

        /// <summary>
        /// Gets whether the database supports stored procedures.
        /// </summary>
        bool SupportsStoredProcedures { get; }

        /// <summary>
        /// Gets whether stored procedure calls may pass arguments by name. When false (CockroachDB), procedure parameters are
        /// sent positionally, in the order supplied, and their names are ignored.
        /// </summary>
        bool SupportsNamedProcedureArguments { get; }

        #endregion

        #region Transactions

        /// <summary>
        /// Gets whether the database supports savepoints inside a transaction. When false (DuckDB),
        /// <see cref="ISqlTransaction.CreateSavepoint"/> throws <see cref="NotSupportedException"/>.
        /// </summary>
        bool SupportsSavepoints { get; }

        /// <summary>
        /// Returns SQL that creates a savepoint.
        /// </summary>
        /// <param name="name">Savepoint name. Must be a valid identifier.</param>
        /// <returns>SQL text.</returns>
        string CreateSavepointSql(string name);

        /// <summary>
        /// Returns SQL that rolls back to a savepoint.
        /// </summary>
        /// <param name="name">Savepoint name.</param>
        /// <returns>SQL text.</returns>
        string RollbackToSavepointSql(string name);

        /// <summary>
        /// Returns SQL that releases a savepoint, or null when the database has no release statement (SQL Server).
        /// </summary>
        /// <param name="name">Savepoint name.</param>
        /// <returns>SQL text or null.</returns>
        string? ReleaseSavepointSql(string name);

        #endregion

        #region Schema

        /// <summary>
        /// Returns the column type used in CREATE TABLE for a mapped column.
        /// </summary>
        /// <param name="column">Column. Must not be null.</param>
        /// <returns>The SQL type.</returns>
        string GetColumnType(ColumnMetadata column);

        /// <summary>
        /// Appends a CREATE TABLE statement that is a no-op when the table already exists.
        /// </summary>
        /// <param name="builder">Statement builder. Must not be null.</param>
        /// <param name="metadata">Entity metadata. Must not be null.</param>
        void AppendCreateTable(SqlStatementBuilder builder, EntityMetadata metadata);

        /// <summary>
        /// Returns a query yielding one row when the table exists and none otherwise.
        /// </summary>
        /// <param name="tableName">Table name. Must not be null.</param>
        /// <returns>The statement.</returns>
        SqlStatement TableExistsQuery(string tableName);

        /// <summary>
        /// Returns a query whose first column is the name of each column of the table.
        /// </summary>
        /// <param name="tableName">Table name. Must not be null.</param>
        /// <returns>The statement.</returns>
        SqlStatement ColumnNamesQuery(string tableName);

        /// <summary>
        /// Returns a query whose first column is the name of each index on the table (excluding primary key indexes).
        /// </summary>
        /// <param name="tableName">Table name. Must not be null.</param>
        /// <returns>The statement.</returns>
        SqlStatement IndexNamesQuery(string tableName);

        /// <summary>
        /// Returns SQL creating an index.
        /// </summary>
        /// <param name="indexName">Index name. Must not be null.</param>
        /// <param name="tableName">Table name. Must not be null.</param>
        /// <param name="columns">Column names in order. Must not be empty.</param>
        /// <param name="unique">Whether the index is unique.</param>
        /// <param name="includedColumns">Covering columns where supported; may be null.</param>
        /// <returns>SQL text.</returns>
        string CreateIndexSql(string indexName, string tableName, IReadOnlyList<string> columns, bool unique, IReadOnlyList<string>? includedColumns);

        /// <summary>
        /// Returns SQL dropping an index.
        /// </summary>
        /// <param name="indexName">Index name. Must not be null.</param>
        /// <param name="tableName">Owning table name; required by some databases. May be null when unknown.</param>
        /// <returns>SQL text.</returns>
        string DropIndexSql(string indexName, string? tableName);

        // Migrations

        /// <summary>
        /// Gets whether DDL statements participate in transactions (they can be rolled back). When false (MySQL), DDL
        /// commits implicitly and a failed migration may leave partial changes behind.
        /// </summary>
        bool SupportsTransactionalDdl { get; }

        /// <summary>
        /// Gets whether ALTER TABLE ... DROP COLUMN is supported. When false, column drops are reported instead of generated.
        /// </summary>
        bool SupportsDropColumn { get; }

        /// <summary>
        /// Gets whether string column types carry a maximum length that the database stores and reports through schema
        /// introspection (VARCHAR(n)). When false (SQLite, DuckDB), <see cref="ColumnMetadata.MaxLength"/> is not part of
        /// the column type, so schema comparison reports no length differences and scaffolding cannot recover lengths.
        /// </summary>
        bool SupportsStringMaxLength { get; }

        /// <summary>
        /// Gets whether the database refuses to alter a table's existing columns (drop a column, or make an added column
        /// NOT NULL) while the table has secondary indexes. When true (DuckDB), schema comparison wraps such operations so
        /// they drop the table's indexes first and re-create the ones that remain afterwards. Default: false.
        /// </summary>
        bool AlterTableRequiresDroppingIndexes { get; }

        /// <summary>
        /// Gets whether the NTH_VALUE window function is supported. When false,
        /// <see cref="IWindowedQueryBuilder{T}.NthValue"/> throws <see cref="System.NotSupportedException"/>.
        /// </summary>
        bool SupportsNthValue { get; }

        /// <summary>
        /// Gets whether LEAD and LAG accept a third (default value) argument. When false (MariaDB), a default passed to
        /// <see cref="IWindowedQueryBuilder{T}.Lead"/> or <see cref="IWindowedQueryBuilder{T}.Lag"/> is applied with an
        /// equivalent CASE over a one-row frame at the offset, so a NULL value at an existing row is still returned as NULL.
        /// </summary>
        bool SupportsOffsetFunctionDefault { get; }

        /// <summary>
        /// Gets whether RANGE window frames accept numeric offsets (<c>RANGE BETWEEN n PRECEDING AND m FOLLOWING</c>).
        /// When false, <see cref="IWindowedQueryBuilder{T}.Range"/> throws <see cref="System.NotSupportedException"/>;
        /// UNBOUNDED and CURRENT ROW bounds remain available.
        /// </summary>
        bool SupportsRangeFrameOffsets { get; }

        /// <summary>
        /// Gets the maximum identifier length. Longer names are truncated (PostgreSQL) or rejected by the database;
        /// schema comparison truncates expected index names to this length.
        /// </summary>
        int MaxIdentifierLength { get; }

        /// <summary>
        /// Gets the batch separator written after each statement in generated migration scripts (for example "GO" on
        /// SQL Server), or null when statements are separated by <see cref="StatementSeparator"/> only.
        /// </summary>
        string? ScriptBatchSeparator { get; }

        /// <summary>
        /// Gets a SQL expression evaluating to the current UTC date and time, used in generated scripts.
        /// </summary>
        string CurrentUtcTimestampSql { get; }

        /// <summary>
        /// Gets the statement that starts a transaction in generated migration scripts (for example "BEGIN TRANSACTION").
        /// </summary>
        string ScriptBeginTransactionSql { get; }

        /// <summary>
        /// Gets the statement that commits a transaction in generated migration scripts (for example "COMMIT").
        /// </summary>
        string ScriptCommitTransactionSql { get; }

        /// <summary>
        /// Returns a query describing the columns of a table, one row per column in ordinal order, with the columns:
        /// 0 name (string), 1 declared type including length/precision (string), 2 nullable (1/0),
        /// 3 maximum character length (integer, -1 for unbounded/MAX, or null), 4 primary key member (1/0), and optionally
        /// 5 database-generated key (identity / auto-increment, 1/0; treated as 0 when the column is absent).
        /// Returns no rows when the table does not exist.
        /// </summary>
        /// <param name="tableName">Table name. Must not be null.</param>
        /// <returns>The statement.</returns>
        /// <exception cref="NotSupportedException">Thrown when the dialect does not support schema introspection.</exception>
        SqlStatement ColumnSchemaQuery(string tableName);

        /// <summary>
        /// Returns a query whose first column is the name of each user table in the current schema/database, ordered by
        /// name. System and internal tables are excluded.
        /// </summary>
        /// <returns>The statement.</returns>
        /// <exception cref="NotSupportedException">Thrown when the dialect does not support schema introspection.</exception>
        SqlStatement TableNamesQuery();

        /// <summary>
        /// Returns a query describing the secondary indexes of a table (excluding the primary key and indexes that back
        /// constraints), one row per index column ordered by index name then position, with the columns:
        /// 0 index name (string), 1 column name (string), 2 unique (1/0), 3 position (integer), 4 included/non-key column (1/0).
        /// </summary>
        /// <param name="tableName">Table name. Must not be null.</param>
        /// <returns>The statement.</returns>
        /// <exception cref="NotSupportedException">Thrown when the dialect does not support schema introspection.</exception>
        SqlStatement IndexSchemaQuery(string tableName);

        /// <summary>
        /// Normalizes a column type to a canonical form so that a declared type (from <see cref="GetColumnType"/>) and the
        /// type reported by <see cref="ColumnSchemaQuery"/> compare equal when they denote the same type
        /// (lower case, no redundant whitespace, synonyms resolved).
        /// </summary>
        /// <param name="columnType">Column type. Must not be null.</param>
        /// <returns>The canonical type.</returns>
        /// <exception cref="ArgumentNullException">Thrown when columnType is null.</exception>
        string NormalizeColumnType(string columnType);

        /// <summary>
        /// Returns SQL adding a column to an existing table.
        /// </summary>
        /// <param name="tableName">Table name. Must not be null.</param>
        /// <param name="column">Column. Must not be null.</param>
        /// <param name="nullable">Whether the column is declared nullable (may differ from the mapping when added as nullable).</param>
        /// <param name="defaultLiteral">SQL literal for the column default, from <see cref="FormatLiteral"/>; null for none.</param>
        /// <returns>SQL text.</returns>
        /// <exception cref="ArgumentNullException">Thrown when tableName or column is null.</exception>
        string AddColumnSql(string tableName, ColumnMetadata column, bool nullable, string? defaultLiteral);

        /// <summary>
        /// Returns SQL dropping a column (including anything the database requires to be dropped first, such as a
        /// SQL Server default constraint).
        /// </summary>
        /// <param name="tableName">Table name. Must not be null.</param>
        /// <param name="columnName">Column name. Must not be null.</param>
        /// <returns>SQL text.</returns>
        /// <exception cref="ArgumentNullException">Thrown when tableName or columnName is null.</exception>
        /// <exception cref="NotSupportedException">Thrown when <see cref="SupportsDropColumn"/> is false.</exception>
        string DropColumnSql(string tableName, string columnName);

        /// <summary>
        /// Renders a database value (already converted with <see cref="Converter"/>) as a SQL literal, escaping as required.
        /// Used for column defaults in DDL and for inlining parameters in generated scripts.
        /// </summary>
        /// <param name="value">Value; null or <see cref="DBNull"/> renders NULL.</param>
        /// <returns>The literal.</returns>
        /// <exception cref="NotSupportedException">Thrown when the value type cannot be rendered as a literal.</exception>
        string FormatLiteral(object? value);

        /// <summary>
        /// Returns SQL creating the migration history table when it does not exist, with the columns
        /// id (string key, up to 150 characters), description (nullable string, up to 1000 characters),
        /// applied_utc (date/time, not null) and duration_ms (64-bit integer, not null).
        /// </summary>
        /// <param name="tableName">History table name. Must not be null.</param>
        /// <returns>SQL text.</returns>
        /// <exception cref="ArgumentNullException">Thrown when tableName is null.</exception>
        string CreateMigrationHistoryTableSql(string tableName);

        /// <summary>
        /// Returns a statement that tries to take a session-level exclusive lock and yields a single value of 1 when the
        /// lock was acquired and 0 otherwise, waiting up to waitSeconds where the database supports waiting. Returns null
        /// when the database has no session lock; the migrator then relies on each migration's write transaction
        /// (SQLite's BEGIN IMMEDIATE) for mutual exclusion.
        /// </summary>
        /// <param name="lockName">Lock name. Must not be null.</param>
        /// <param name="waitSeconds">Seconds to wait inside the statement. Minimum: 0.</param>
        /// <returns>The statement, or null.</returns>
        SqlStatement? AcquireMigrationLockSql(string lockName, int waitSeconds);

        /// <summary>
        /// Returns a statement the migrator runs once, outside any transaction, before its first
        /// <see cref="AcquireMigrationLockSql"/> attempt (for example creating a lock table on a database without session
        /// locks), or null when nothing is needed (the default).
        /// </summary>
        /// <returns>The statement, or null.</returns>
        SqlStatement? PrepareMigrationLockSql();

        /// <summary>
        /// Returns a statement releasing the lock taken by <see cref="AcquireMigrationLockSql"/>, or null when there is none.
        /// </summary>
        /// <param name="lockName">Lock name. Must not be null.</param>
        /// <returns>The statement, or null.</returns>
        SqlStatement? ReleaseMigrationLockSql(string lockName);

        /// <summary>
        /// Returns whether an exception thrown by the <see cref="AcquireMigrationLockSql"/> statement means that another
        /// session holds (or is concurrently taking) the lock, rather than a real failure. Databases with optimistic
        /// concurrency (DuckDB) report a concurrent writer as a write-write conflict or key violation instead of waiting;
        /// the migrator then treats the attempt as "not acquired" and polls again. Default: false (the exception propagates).
        /// </summary>
        /// <param name="exception">Exception thrown by the lock statement. Must not be null.</param>
        /// <returns>True when the attempt should count as "not acquired".</returns>
        bool IsMigrationLockContention(Exception exception);

        #endregion

        #region Statement-Shapes

        /// <summary>
        /// Applies provider-specific settings to every command before its parameters are bound (for example, binding
        /// parameters by name on Oracle). The default does nothing.
        /// </summary>
        /// <param name="command">The provider command. Must not be null.</param>
        void ConfigureCommand(DbCommand command);

        /// <summary>
        /// Gets the text written before several statements sent in one command (for example "BEGIN " to open a PL/SQL
        /// block on Oracle). Default: empty, statements are simply joined with <see cref="StatementSeparator"/>. Never null.
        /// </summary>
        string StatementBatchPrefix { get; }

        /// <summary>
        /// Gets the text written after several statements sent in one command (for example "; END;" on Oracle).
        /// Default: empty. Never null.
        /// </summary>
        string StatementBatchSuffix { get; }

        /// <summary>
        /// Gets whether one INSERT may carry several rows (<c>VALUES (...), (...)</c>). When false the engine sends one
        /// INSERT per row as a statement batch (see <see cref="StatementBatchPrefix"/>). Default: true.
        /// </summary>
        bool SupportsMultiRowInsert { get; }

        /// <summary>
        /// Gets the clause completing a SELECT that reads no table (for example " FROM DUAL" on Oracle). Default: empty.
        /// Never null.
        /// </summary>
        string SingleRowFromClause { get; }

        /// <summary>
        /// Gets the maximum number of items in one <c>IN (...)</c> list. Longer lists are split into several lists joined
        /// with OR (AND for NOT IN), and include loading chunks keys to it. Default: <see cref="int.MaxValue"/> (only
        /// <see cref="MaxParameters"/> applies); Oracle: 1000. Minimum: 1.
        /// </summary>
        int MaxInListItems { get; }

        /// <summary>
        /// Returns the remainder of dividing two SQL expressions, with C# semantics (the sign follows the dividend).
        /// Default: <c>(left % right)</c>.
        /// </summary>
        /// <param name="left">Dividend SQL.</param>
        /// <param name="right">Divisor SQL.</param>
        /// <returns>The remainder expression.</returns>
        string Modulo(string left, string right);

        /// <summary>
        /// Returns the keyword for a set operation (for example "EXCEPT", or "MINUS" on Oracle).
        /// </summary>
        /// <param name="operation">Set operation.</param>
        /// <returns>The keyword.</returns>
        string SetOperationKeyword(SetOperationType operation);

        /// <summary>
        /// Returns whether the column created for a mapped column accepts NULL. Default: <see cref="ColumnMetadata.IsNullable"/>.
        /// A dialect that stores empty strings as NULL (<see cref="TreatsEmptyStringAsNull"/>) also declares non-key string
        /// columns nullable, so a non-nullable string property can still hold an empty string. Used by CREATE TABLE, ADD
        /// COLUMN and schema comparison.
        /// </summary>
        /// <param name="column">Column. Must not be null.</param>
        /// <returns>True when the column is declared NULL.</returns>
        /// <exception cref="ArgumentNullException">Thrown when column is null.</exception>
        bool ColumnAllowsNull(ColumnMetadata column);

        /// <summary>
        /// Gets whether the database stores an empty string as NULL (Oracle). When true, an empty string written to a
        /// column reads back as NULL (a null or unchanged property), and a comparison with an empty string matches no row;
        /// the database cannot tell the two apart. Default: false.
        /// </summary>
        bool TreatsEmptyStringAsNull { get; }

        /// <summary>
        /// Appends one statement, already rendered with literals, to a generated migration script, including its
        /// terminator and any batch separator. The default writes the statement, <see cref="StatementSeparator"/> when the
        /// statement does not already end with it, a line break, and <see cref="ScriptBatchSeparator"/> on its own line
        /// when set.
        /// </summary>
        /// <param name="script">Script being written. Must not be null.</param>
        /// <param name="sql">Statement text with parameters inlined. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when script or sql is null.</exception>
        void AppendScriptStatement(System.Text.StringBuilder script, string sql);

        #endregion
    }
}
