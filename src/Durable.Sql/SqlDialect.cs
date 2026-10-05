namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using System.Text.RegularExpressions;
    using Durable;
    using Durable.Query;

    /// <summary>
    /// Base dialect with ANSI SQL defaults. Providers override what differs.
    /// Thread safety: instances are immutable and thread-safe.
    /// </summary>
    public abstract class SqlDialect : ISqlDialect
    {
        #region Public-Members

        /// <inheritdoc />
        public abstract RepositoryType RepositoryType { get; }

        /// <inheritdoc />
        public abstract string DbSystemName { get; }

        /// <inheritdoc />
        public IDataTypeConverter Converter { get; }

        /// <inheritdoc />
        public virtual int MaxParameters => 2000;

        /// <inheritdoc />
        public virtual char LikeEscapeCharacter => '!';

        /// <inheritdoc />
        public virtual string RecursiveCteKeyword => "WITH RECURSIVE";

        /// <inheritdoc />
        public virtual InsertKeyStrategy InsertKeyStrategy => InsertKeyStrategy.Returning;

        /// <inheritdoc />
        public virtual string? LastInsertIdSql => null;

        /// <inheritdoc />
        public virtual string StatementSeparator => ";";

        /// <inheritdoc />
        public virtual string InsertDefaultValuesClause => "DEFAULT VALUES";

        /// <inheritdoc />
        public virtual bool SupportsStoredProcedures => true;

        /// <inheritdoc />
        public virtual string StringCastType => "TEXT";

        /// <inheritdoc />
        public virtual bool SupportsOrdinalLike => true;

        // Migrations

        /// <inheritdoc />
        public virtual bool SupportsTransactionalDdl => true;

        /// <inheritdoc />
        public virtual bool SupportsDropColumn => true;

        /// <inheritdoc />
        public virtual int MaxIdentifierLength => int.MaxValue;

        /// <inheritdoc />
        public virtual string? ScriptBatchSeparator => null;

        /// <inheritdoc />
        public virtual string CurrentUtcTimestampSql => "CURRENT_TIMESTAMP";

        /// <inheritdoc />
        public virtual string ScriptBeginTransactionSql => "BEGIN TRANSACTION";

        /// <inheritdoc />
        public virtual string ScriptCommitTransactionSql => "COMMIT";

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the dialect.
        /// </summary>
        /// <param name="converter">Data type converter. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when converter is null.</exception>
        protected SqlDialect(IDataTypeConverter converter)
        {
            Converter = converter ?? throw new ArgumentNullException(nameof(converter));
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public virtual string QuoteIdentifier(string identifier)
        {
            if (string.IsNullOrEmpty(identifier)) throw new ArgumentException("Identifier cannot be null or empty.", nameof(identifier));
            if (identifier == "*") return identifier;
            string open = IdentifierOpen;
            string close = IdentifierClose;
            if (identifier.IndexOf('.') < 0)
                return open + identifier.Replace(close, close + close, StringComparison.Ordinal) + close;

            string[] parts = identifier.Split('.');
            StringBuilder sb = new StringBuilder(identifier.Length + parts.Length * 2);
            for (int i = 0; i < parts.Length; i++)
            {
                if (i > 0) sb.Append('.');
                sb.Append(open).Append(parts[i].Replace(close, close + close, StringComparison.Ordinal)).Append(close);
            }

            return sb.ToString();
        }

        /// <inheritdoc />
        public virtual string FormatParameterName(int index)
        {
            return "@p" + index.ToString(CultureInfo.InvariantCulture);
        }

        /// <inheritdoc />
        public virtual void ConfigureParameter(DbParameter parameter, SqlParameterValue value)
        {
        }

        /// <inheritdoc />
        public virtual string BooleanLiteral(bool value)
        {
            return value ? "TRUE" : "FALSE";
        }

        /// <inheritdoc />
        public virtual string EscapeLikePattern(string value)
        {
            ArgumentNullException.ThrowIfNull(value);
            char escape = LikeEscapeCharacter;
            StringBuilder sb = new StringBuilder(value.Length + 4);
            foreach (char c in value)
            {
                if (c == escape || c == '%' || c == '_' || IsAdditionalLikeWildcard(c)) sb.Append(escape);
                sb.Append(c);
            }

            return sb.ToString();
        }

        /// <inheritdoc />
        public virtual string Concat(IReadOnlyList<string> parts)
        {
            return "(" + string.Join(" || ", parts) + ")";
        }

        /// <inheritdoc />
        public virtual string Divide(string left, string right, bool integerOperands)
        {
            return "(" + left + " / " + right + ")";
        }

        /// <inheritdoc />
        public virtual string IsEmptyString(string expression)
        {
            return expression + " = ''";
        }

        /// <inheritdoc />
        public virtual string OrderDirection(bool descending)
        {
            return descending ? " DESC" : " ASC";
        }

        /// <inheritdoc />
        public virtual string OrdinalCollation(string expression)
        {
            return expression;
        }

        /// <inheritdoc />
        public virtual string OrdinalStringMatch(StringMatchKind kind, string target, string value)
        {
            throw new NotSupportedException(GetType().Name + " supports ordinal LIKE; OrdinalStringMatch is not used.");
        }

        /// <inheritdoc />
        public virtual string TranslateFunction(QueryFunction function, IReadOnlyList<string> arguments)
        {
            string a0 = arguments.Count > 0 ? arguments[0] : string.Empty;
            string a1 = arguments.Count > 1 ? arguments[1] : string.Empty;
            switch (function)
            {
                case QueryFunction.Length: return "LENGTH(" + a0 + ")";
                case QueryFunction.Upper: return "UPPER(" + a0 + ")";
                case QueryFunction.Lower: return "LOWER(" + a0 + ")";
                case QueryFunction.Trim: return "TRIM(" + a0 + ")";
                case QueryFunction.TrimStart: return "LTRIM(" + a0 + ")";
                case QueryFunction.TrimEnd: return "RTRIM(" + a0 + ")";
                case QueryFunction.Substring:
                    return arguments.Count > 2
                        ? "SUBSTR(" + a0 + ", (" + a1 + ") + 1, " + arguments[2] + ")"
                        : "SUBSTR(" + a0 + ", (" + a1 + ") + 1)";
                case QueryFunction.Replace: return "REPLACE(" + a0 + ", " + a1 + ", " + arguments[2] + ")";
                case QueryFunction.IndexOf: return "(INSTR(" + a0 + ", " + a1 + ") - 1)";
                case QueryFunction.Abs: return "ABS(" + a0 + ")";
                case QueryFunction.Round: return arguments.Count > 1 ? "ROUND(" + a0 + ", " + a1 + ")" : "ROUND(" + a0 + ")";
                case QueryFunction.Ceiling: return "CEILING(" + a0 + ")";
                case QueryFunction.Floor: return "FLOOR(" + a0 + ")";
                case QueryFunction.Power: return "POWER(" + a0 + ", " + a1 + ")";
                case QueryFunction.Sqrt: return "SQRT(" + a0 + ")";
                case QueryFunction.Year: return "EXTRACT(YEAR FROM " + a0 + ")";
                case QueryFunction.Month: return "EXTRACT(MONTH FROM " + a0 + ")";
                case QueryFunction.Day: return "EXTRACT(DAY FROM " + a0 + ")";
                case QueryFunction.Hour: return "EXTRACT(HOUR FROM " + a0 + ")";
                case QueryFunction.Minute: return "EXTRACT(MINUTE FROM " + a0 + ")";
                case QueryFunction.Second: return "FLOOR(EXTRACT(SECOND FROM " + a0 + "))";
                default:
                    throw new NotSupportedException("Function " + function + " is not supported by " + RepositoryType.DisplayName + ".");
            }
        }

        /// <inheritdoc />
        public virtual void AppendPaging(SqlStatementBuilder builder, int? skip, int? take, bool hasOrderBy)
        {
            ArgumentNullException.ThrowIfNull(builder);
            if (take.HasValue)
                builder.Append(" LIMIT ").Append(take.Value.ToString(CultureInfo.InvariantCulture));
            else if (skip.HasValue && UnboundedLimit != null)
                builder.Append(" LIMIT ").Append(UnboundedLimit);
            if (skip.HasValue && skip.Value > 0)
                builder.Append(" OFFSET ").Append(skip.Value.ToString(CultureInfo.InvariantCulture));
        }

        /// <inheritdoc />
        public virtual void AppendUpsert(SqlStatementBuilder builder, EntityMetadata metadata, IReadOnlyList<ColumnMetadata> insertColumns, IReadOnlyList<string> placeholders, IReadOnlyList<ColumnMetadata> conflictColumns, IReadOnlyList<ColumnMetadata> updateColumns, string? versionPlaceholder = null)
        {
            ArgumentNullException.ThrowIfNull(builder);
            ArgumentNullException.ThrowIfNull(metadata);
            builder.Append("INSERT INTO ").AppendIdentifier(metadata.TableName).Append(" (")
                .Append(string.Join(", ", insertColumns.Select(c => QuoteIdentifier(c.Name))))
                .Append(") VALUES (").Append(string.Join(", ", placeholders)).Append(") ON CONFLICT (")
                .Append(string.Join(", ", conflictColumns.Select(c => QuoteIdentifier(c.Name))))
                .Append(")");
            if (updateColumns.Count == 0)
            {
                builder.Append(" DO NOTHING");
                return;
            }

            builder.Append(" DO UPDATE SET ")
                .Append(string.Join(", ", updateColumns.Select(c => QuoteIdentifier(c.Name) + " = " + (c.IsVersion && versionPlaceholder != null ? versionPlaceholder : "EXCLUDED." + QuoteIdentifier(c.Name)))));
        }

        /// <inheritdoc />
        public virtual string CreateSavepointSql(string name)
        {
            return "SAVEPOINT " + QuoteIdentifier(name);
        }

        /// <inheritdoc />
        public virtual string RollbackToSavepointSql(string name)
        {
            return "ROLLBACK TO SAVEPOINT " + QuoteIdentifier(name);
        }

        /// <inheritdoc />
        public virtual string? ReleaseSavepointSql(string name)
        {
            return "RELEASE SAVEPOINT " + QuoteIdentifier(name);
        }

        /// <inheritdoc />
        public abstract string GetColumnType(ColumnMetadata column);

        /// <inheritdoc />
        public virtual void AppendCreateTable(SqlStatementBuilder builder, EntityMetadata metadata)
        {
            ArgumentNullException.ThrowIfNull(builder);
            ArgumentNullException.ThrowIfNull(metadata);
            builder.Append("CREATE TABLE IF NOT EXISTS ").AppendIdentifier(metadata.TableName).Append(" (");
            AppendTableBody(builder, metadata);
            builder.Append(")");
        }

        /// <inheritdoc />
        public abstract SqlStatement TableExistsQuery(string tableName);

        /// <inheritdoc />
        public abstract SqlStatement ColumnNamesQuery(string tableName);

        /// <inheritdoc />
        public abstract SqlStatement IndexNamesQuery(string tableName);

        /// <inheritdoc />
        public virtual string CreateIndexSql(string indexName, string tableName, IReadOnlyList<string> columns, bool unique, IReadOnlyList<string>? includedColumns)
        {
            return "CREATE " + (unique ? "UNIQUE " : string.Empty) + "INDEX " + QuoteIdentifier(indexName)
                + " ON " + QuoteIdentifier(tableName) + " (" + string.Join(", ", columns.Select(QuoteIdentifier)) + ")";
        }

        /// <inheritdoc />
        public virtual string DropIndexSql(string indexName, string? tableName)
        {
            return "DROP INDEX " + QuoteIdentifier(indexName);
        }

        // Migrations

        /// <inheritdoc />
        public virtual SqlStatement ColumnSchemaQuery(string tableName)
        {
            throw new NotSupportedException(RepositoryType.DisplayName + " does not support column schema introspection.");
        }

        /// <inheritdoc />
        public virtual SqlStatement IndexSchemaQuery(string tableName)
        {
            throw new NotSupportedException(RepositoryType.DisplayName + " does not support index schema introspection.");
        }

        /// <inheritdoc />
        public virtual string NormalizeColumnType(string columnType)
        {
            return CanonicalizeColumnType(columnType);
        }

        /// <inheritdoc />
        public virtual string AddColumnSql(string tableName, ColumnMetadata column, bool nullable, string? defaultLiteral)
        {
            ArgumentNullException.ThrowIfNull(tableName);
            ArgumentNullException.ThrowIfNull(column);
            StringBuilder sb = new StringBuilder();
            sb.Append("ALTER TABLE ").Append(QuoteIdentifier(tableName)).Append(' ').Append(AddColumnKeyword).Append(' ')
                .Append(QuoteIdentifier(column.Name)).Append(' ').Append(GetColumnType(column));
            if (defaultLiteral != null) sb.Append(" DEFAULT ").Append(defaultLiteral);
            sb.Append(nullable ? " NULL" : " NOT NULL");
            return sb.ToString();
        }

        /// <inheritdoc />
        public virtual string DropColumnSql(string tableName, string columnName)
        {
            ArgumentNullException.ThrowIfNull(tableName);
            ArgumentNullException.ThrowIfNull(columnName);
            if (!SupportsDropColumn) throw new NotSupportedException(RepositoryType.DisplayName + " does not support dropping columns.");
            return "ALTER TABLE " + QuoteIdentifier(tableName) + " DROP COLUMN " + QuoteIdentifier(columnName);
        }

        /// <inheritdoc />
        public virtual string FormatLiteral(object? value)
        {
            if (value == null || value == DBNull.Value) return "NULL";
            switch (value)
            {
                case string text: return StringLiteral(text);
                case char character: return StringLiteral(character.ToString());
                case bool flag: return BooleanLiteral(flag);
                case byte or sbyte or short or ushort or int or uint or long or ulong:
                    return Convert.ToString(value, CultureInfo.InvariantCulture)!;
                case decimal number: return number.ToString(CultureInfo.InvariantCulture);
                case double number:
                    if (double.IsNaN(number) || double.IsInfinity(number)) throw new NotSupportedException("Non-finite numbers cannot be rendered as SQL literals.");
                    return number.ToString("R", CultureInfo.InvariantCulture);
                case float number:
                    if (float.IsNaN(number) || float.IsInfinity(number)) throw new NotSupportedException("Non-finite numbers cannot be rendered as SQL literals.");
                    return number.ToString("R", CultureInfo.InvariantCulture);
                case Guid guid: return StringLiteral(guid.ToString("D"));
                case DateTime dateTime: return StringLiteral(dateTime.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture));
                case DateTimeOffset offset: return StringLiteral(offset.ToString("yyyy-MM-dd HH:mm:ss.ffffffzzz", CultureInfo.InvariantCulture));
                case DateOnly date: return StringLiteral(date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
                case TimeOnly time: return StringLiteral(time.ToString("HH:mm:ss.ffffff", CultureInfo.InvariantCulture));
                case TimeSpan span: return StringLiteral(span.ToString("c", CultureInfo.InvariantCulture));
                case byte[] bytes: return BinaryLiteral(bytes);
                default:
                    throw new NotSupportedException("Values of type " + value.GetType().Name + " cannot be rendered as SQL literals.");
            }
        }

        /// <inheritdoc />
        public virtual string CreateMigrationHistoryTableSql(string tableName)
        {
            ArgumentNullException.ThrowIfNull(tableName);
            return "CREATE TABLE IF NOT EXISTS " + QuoteIdentifier(tableName) + " ("
                + QuoteIdentifier("id") + " VARCHAR(150) NOT NULL PRIMARY KEY, "
                + QuoteIdentifier("description") + " VARCHAR(1000) NULL, "
                + QuoteIdentifier("applied_utc") + " TIMESTAMP NOT NULL, "
                + QuoteIdentifier("duration_ms") + " BIGINT NOT NULL)";
        }

        /// <inheritdoc />
        public virtual SqlStatement? AcquireMigrationLockSql(string lockName, int waitSeconds)
        {
            return null;
        }

        /// <inheritdoc />
        public virtual SqlStatement? ReleaseMigrationLockSql(string lockName)
        {
            return null;
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Gets the opening identifier quote. Default: a double quote.
        /// </summary>
        protected virtual string IdentifierOpen => "\"";

        /// <summary>
        /// Gets the closing identifier quote. Default: a double quote.
        /// </summary>
        protected virtual string IdentifierClose => "\"";

        /// <summary>
        /// Gets the LIMIT value meaning "no limit" for skip-only paging, or null when OFFSET may stand alone. Default: null.
        /// </summary>
        protected virtual string? UnboundedLimit => null;

        /// <summary>
        /// Returns true for dialect-specific LIKE wildcard characters beyond % and _ (for example, [ on SQL Server).
        /// </summary>
        /// <param name="c">Character.</param>
        /// <returns>True when the character must be escaped.</returns>
        protected virtual bool IsAdditionalLikeWildcard(char c)
        {
            return false;
        }

        /// <summary>
        /// Appends column definitions and the primary key constraint for CREATE TABLE.
        /// </summary>
        /// <param name="builder">Builder.</param>
        /// <param name="metadata">Entity metadata.</param>
        protected virtual void AppendTableBody(SqlStatementBuilder builder, EntityMetadata metadata)
        {
            bool inlineKey = metadata.KeyColumns.Count == 1 && metadata.KeyColumns[0].IsAutoIncrement && InlinesAutoIncrementKey;
            for (int i = 0; i < metadata.Columns.Count; i++)
            {
                if (i > 0) builder.Append(", ");
                builder.Append(ColumnDefinition(metadata.Columns[i], inlineKey && metadata.Columns[i].IsPrimaryKey));
            }

            if (metadata.KeyColumns.Count > 0 && !inlineKey)
            {
                builder.Append(", PRIMARY KEY (")
                    .Append(string.Join(", ", metadata.KeyColumns.Select(c => QuoteIdentifier(c.Name))))
                    .Append(")");
            }
        }

        /// <summary>
        /// Gets whether an auto-increment single-column key is declared inline as "PRIMARY KEY" (SQLite requirement). Default: false.
        /// </summary>
        protected virtual bool InlinesAutoIncrementKey => false;

        /// <summary>
        /// Returns the column definition for CREATE TABLE.
        /// </summary>
        /// <param name="column">Column.</param>
        /// <param name="inlinePrimaryKey">Whether to declare the primary key inline.</param>
        /// <returns>The definition.</returns>
        protected virtual string ColumnDefinition(ColumnMetadata column, bool inlinePrimaryKey)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(QuoteIdentifier(column.Name)).Append(' ');
            if (column.IsAutoIncrement)
            {
                sb.Append(AutoIncrementColumnType(column, inlinePrimaryKey));
                return sb.ToString();
            }

            sb.Append(GetColumnType(column));
            if (!column.IsNullable) sb.Append(" NOT NULL");
            if (inlinePrimaryKey) sb.Append(" PRIMARY KEY");
            return sb.ToString();
        }

        /// <summary>
        /// Returns the type and modifiers for an auto-increment column.
        /// </summary>
        /// <param name="column">Column.</param>
        /// <param name="inlinePrimaryKey">Whether the primary key is declared inline.</param>
        /// <returns>Type and modifiers.</returns>
        protected abstract string AutoIncrementColumnType(ColumnMetadata column, bool inlinePrimaryKey);

        /// <summary>
        /// Returns the string type for a column, honoring MaxLength.
        /// </summary>
        /// <param name="column">Column.</param>
        /// <param name="boundedType">Type name for bounded strings, for example "VARCHAR".</param>
        /// <param name="unboundedType">Type for unbounded strings, for example "TEXT".</param>
        /// <param name="defaultLength">Length used for indexed or key strings without MaxLength; 0 for unbounded.</param>
        /// <returns>The SQL type.</returns>
        protected static string StringType(ColumnMetadata column, string boundedType, string unboundedType, int defaultLength)
        {
            int length = column.MaxLength > 0 ? column.MaxLength : (column.IsPrimaryKey || column.Indexes.Count > 0 || column.ForeignKey != null ? defaultLength : 0);
            return length > 0 ? boundedType + "(" + length.ToString(CultureInfo.InvariantCulture) + ")" : unboundedType;
        }

        // Migrations

        /// <summary>
        /// Gets the keyword(s) following the table name when adding a column. Default: "ADD COLUMN".
        /// </summary>
        protected virtual string AddColumnKeyword => "ADD COLUMN";

        /// <summary>
        /// Renders a string literal, doubling embedded single quotes.
        /// </summary>
        /// <param name="value">Text. Must not be null.</param>
        /// <returns>The literal.</returns>
        protected virtual string StringLiteral(string value)
        {
            return "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";
        }

        /// <summary>
        /// Renders a binary literal. Default: X'hex'.
        /// </summary>
        /// <param name="value">Bytes. Must not be null.</param>
        /// <returns>The literal.</returns>
        protected virtual string BinaryLiteral(byte[] value)
        {
            return "X'" + Convert.ToHexString(value) + "'";
        }

        /// <summary>
        /// Canonicalizes type text: lower case, single spaces, no spaces around parentheses and commas.
        /// </summary>
        /// <param name="columnType">Type text. Must not be null.</param>
        /// <returns>The canonical text.</returns>
        /// <exception cref="ArgumentNullException">Thrown when columnType is null.</exception>
        protected static string CanonicalizeColumnType(string columnType)
        {
            ArgumentNullException.ThrowIfNull(columnType);
            string text = Regex.Replace(columnType.Trim().ToLowerInvariant(), "\\s+", " ");
            text = Regex.Replace(text, "\\s*\\(\\s*", "(");
            text = Regex.Replace(text, "\\s*,\\s*", ",");
            return Regex.Replace(text, "\\s*\\)", ")");
        }

        #endregion
    }
}
