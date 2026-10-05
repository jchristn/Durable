namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using Durable;

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
        public virtual string TranslateFunction(SqlFunction function, IReadOnlyList<string> arguments)
        {
            string a0 = arguments.Count > 0 ? arguments[0] : string.Empty;
            string a1 = arguments.Count > 1 ? arguments[1] : string.Empty;
            switch (function)
            {
                case SqlFunction.Length: return "LENGTH(" + a0 + ")";
                case SqlFunction.Upper: return "UPPER(" + a0 + ")";
                case SqlFunction.Lower: return "LOWER(" + a0 + ")";
                case SqlFunction.Trim: return "TRIM(" + a0 + ")";
                case SqlFunction.TrimStart: return "LTRIM(" + a0 + ")";
                case SqlFunction.TrimEnd: return "RTRIM(" + a0 + ")";
                case SqlFunction.Substring:
                    return arguments.Count > 2
                        ? "SUBSTR(" + a0 + ", (" + a1 + ") + 1, " + arguments[2] + ")"
                        : "SUBSTR(" + a0 + ", (" + a1 + ") + 1)";
                case SqlFunction.Replace: return "REPLACE(" + a0 + ", " + a1 + ", " + arguments[2] + ")";
                case SqlFunction.IndexOf: return "(INSTR(" + a0 + ", " + a1 + ") - 1)";
                case SqlFunction.Abs: return "ABS(" + a0 + ")";
                case SqlFunction.Round: return arguments.Count > 1 ? "ROUND(" + a0 + ", " + a1 + ")" : "ROUND(" + a0 + ")";
                case SqlFunction.Ceiling: return "CEILING(" + a0 + ")";
                case SqlFunction.Floor: return "FLOOR(" + a0 + ")";
                case SqlFunction.Power: return "POWER(" + a0 + ", " + a1 + ")";
                case SqlFunction.Sqrt: return "SQRT(" + a0 + ")";
                case SqlFunction.Year: return "EXTRACT(YEAR FROM " + a0 + ")";
                case SqlFunction.Month: return "EXTRACT(MONTH FROM " + a0 + ")";
                case SqlFunction.Day: return "EXTRACT(DAY FROM " + a0 + ")";
                case SqlFunction.Hour: return "EXTRACT(HOUR FROM " + a0 + ")";
                case SqlFunction.Minute: return "EXTRACT(MINUTE FROM " + a0 + ")";
                case SqlFunction.Second: return "FLOOR(EXTRACT(SECOND FROM " + a0 + "))";
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
        public virtual void AppendUpsert(SqlStatementBuilder builder, EntityMetadata metadata, IReadOnlyList<ColumnMetadata> insertColumns, IReadOnlyList<string> placeholders, IReadOnlyList<ColumnMetadata> conflictColumns, IReadOnlyList<ColumnMetadata> updateColumns)
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
                .Append(string.Join(", ", updateColumns.Select(c => QuoteIdentifier(c.Name) + " = EXCLUDED." + QuoteIdentifier(c.Name))));
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

        #endregion
    }
}
