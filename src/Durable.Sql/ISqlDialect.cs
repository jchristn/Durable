namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;

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
        /// Translates a scalar function.
        /// </summary>
        /// <param name="function">Function.</param>
        /// <param name="arguments">SQL fragments for the arguments.</param>
        /// <returns>The SQL expression.</returns>
        /// <exception cref="NotSupportedException">Thrown when the dialect cannot express the function.</exception>
        string TranslateFunction(SqlFunction function, IReadOnlyList<string> arguments);

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
        void AppendUpsert(SqlStatementBuilder builder, EntityMetadata metadata, IReadOnlyList<ColumnMetadata> insertColumns, IReadOnlyList<string> placeholders, IReadOnlyList<ColumnMetadata> conflictColumns, IReadOnlyList<ColumnMetadata> updateColumns);

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

        #endregion
    }
}
