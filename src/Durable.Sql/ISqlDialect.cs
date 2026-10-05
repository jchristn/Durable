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

        #endregion

        #region Transactions

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
        /// 3 maximum character length (integer, -1 for unbounded/MAX, or null), 4 primary key member (1/0).
        /// Returns no rows when the table does not exist.
        /// </summary>
        /// <param name="tableName">Table name. Must not be null.</param>
        /// <returns>The statement.</returns>
        /// <exception cref="NotSupportedException">Thrown when the dialect does not support schema introspection.</exception>
        SqlStatement ColumnSchemaQuery(string tableName);

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
        /// Returns a statement releasing the lock taken by <see cref="AcquireMigrationLockSql"/>, or null when there is none.
        /// </summary>
        /// <param name="lockName">Lock name. Must not be null.</param>
        /// <returns>The statement, or null.</returns>
        SqlStatement? ReleaseMigrationLockSql(string lockName);

        #endregion
    }
}
