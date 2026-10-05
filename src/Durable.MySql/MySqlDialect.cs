namespace Durable.MySql
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text.RegularExpressions;
    using Durable;
    using Durable.Query;
    using Durable.Sql;

    /// <summary>
    /// MySQL dialect (MySQL 8.0.31+ for INTERSECT/EXCEPT; 8.0.19+ for the upsert row alias).
    /// Thread safety: immutable; safe to share.
    /// </summary>
    public class MySqlDialect : SqlDialect
    {
        #region Public-Members

        /// <summary>
        /// Gets the shared default instance.
        /// </summary>
        public static MySqlDialect Default { get; } = new MySqlDialect();

        /// <inheritdoc />
        public override RepositoryType RepositoryType => RepositoryType.MySql;

        /// <inheritdoc />
        public override string DbSystemName => "mysql";

        /// <inheritdoc />
        public override int MaxParameters => 32000;

        /// <inheritdoc />
        public override InsertKeyStrategy InsertKeyStrategy => InsertKeyStrategy.LastInsertId;

        /// <inheritdoc />
        public override string? LastInsertIdSql => "SELECT LAST_INSERT_ID()";

        /// <inheritdoc />
        public override string InsertDefaultValuesClause => "() VALUES ()";

        /// <summary>
        /// Gets the binary collation applied by <see cref="OrdinalCollation"/> for ordinal and ignore-case string matching.
        /// Default: utf8mb4_bin. Columns using another character set need a matching binary collation (for example latin1_bin).
        /// </summary>
        public string OrdinalCollationName { get; }

        /// <inheritdoc />
        public override string StringCastType => "CHAR";

        // Migrations

        /// <summary>
        /// Gets false: MySQL commits DDL implicitly, so a failed migration may leave earlier statements applied.
        /// </summary>
        public override bool SupportsTransactionalDdl => false;

        /// <inheritdoc />
        public override int MaxIdentifierLength => 64;

        /// <inheritdoc />
        public override string CurrentUtcTimestampSql => "UTC_TIMESTAMP(6)";

        /// <inheritdoc />
        public override string ScriptBeginTransactionSql => "START TRANSACTION";

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the dialect.
        /// </summary>
        /// <param name="converter">Converter; null uses <see cref="MySqlDataTypeConverter"/>.</param>
        /// <param name="ordinalCollation">Binary collation for ordinal string matching; must be valid for the character set of
        /// the compared columns. Default: utf8mb4_bin.</param>
        /// <exception cref="ArgumentException">Thrown when ordinalCollation is not a simple collation name.</exception>
        public MySqlDialect(IDataTypeConverter? converter = null, string ordinalCollation = "utf8mb4_bin") : base(converter ?? new MySqlDataTypeConverter())
        {
            OrdinalCollationName = SqlIdentifierValidator.RequireIdentifier(ordinalCollation, nameof(ordinalCollation));
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override string OrdinalCollation(string expression)
        {
            return expression + " COLLATE " + OrdinalCollationName;
        }

        /// <inheritdoc />
        public override string BooleanLiteral(bool value)
        {
            return value ? "1" : "0";
        }

        /// <inheritdoc />
        public override string Concat(IReadOnlyList<string> parts)
        {
            return "CONCAT(" + string.Join(", ", parts) + ")";
        }

        /// <inheritdoc />
        public override string Divide(string left, string right, bool integerOperands)
        {
            return integerOperands ? "(" + left + " DIV " + right + ")" : base.Divide(left, right, integerOperands);
        }

        /// <inheritdoc />
        public override string TranslateFunction(QueryFunction function, IReadOnlyList<string> arguments)
        {
            string a0 = arguments.Count > 0 ? arguments[0] : string.Empty;
            string a1 = arguments.Count > 1 ? arguments[1] : string.Empty;
            switch (function)
            {
                case QueryFunction.Length: return "CHAR_LENGTH(" + a0 + ")";
                case QueryFunction.IndexOf: return "(LOCATE(" + a1 + ", " + a0 + ") - 1)";
                case QueryFunction.Year: return "YEAR(" + a0 + ")";
                case QueryFunction.Month: return "MONTH(" + a0 + ")";
                case QueryFunction.Day: return "DAY(" + a0 + ")";
                case QueryFunction.Hour: return "HOUR(" + a0 + ")";
                case QueryFunction.Minute: return "MINUTE(" + a0 + ")";
                case QueryFunction.Second: return "SECOND(" + a0 + ")";
                case QueryFunction.DayOfYear: return "DAYOFYEAR(" + a0 + ")";
                case QueryFunction.DayOfWeek: return "(DAYOFWEEK(" + a0 + ") - 1)";
                case QueryFunction.Date: return "CAST(DATE(" + a0 + ") AS DATETIME(6))";
                case QueryFunction.AddYears: return "DATE_ADD(" + a0 + ", INTERVAL " + a1 + " YEAR)";
                case QueryFunction.AddMonths: return "DATE_ADD(" + a0 + ", INTERVAL " + a1 + " MONTH)";
                case QueryFunction.AddDays: return "DATE_ADD(" + a0 + ", INTERVAL " + a1 + " DAY)";
                case QueryFunction.AddHours: return "DATE_ADD(" + a0 + ", INTERVAL " + a1 + " HOUR)";
                case QueryFunction.AddMinutes: return "DATE_ADD(" + a0 + ", INTERVAL " + a1 + " MINUTE)";
                case QueryFunction.AddSeconds: return "DATE_ADD(" + a0 + ", INTERVAL " + a1 + " SECOND)";
                default:
                    return base.TranslateFunction(function, arguments);
            }
        }

        /// <inheritdoc />
        public override void AppendUpsert(SqlStatementBuilder builder, EntityMetadata metadata, IReadOnlyList<ColumnMetadata> insertColumns, IReadOnlyList<string> placeholders, IReadOnlyList<ColumnMetadata> conflictColumns, IReadOnlyList<ColumnMetadata> updateColumns, string? versionPlaceholder = null)
        {
            ArgumentNullException.ThrowIfNull(builder);
            ArgumentNullException.ThrowIfNull(metadata);
            builder.Append("INSERT INTO ").AppendIdentifier(metadata.TableName).Append(" (")
                .Append(string.Join(", ", insertColumns.Select(c => QuoteIdentifier(c.Name))))
                .Append(") VALUES (").Append(string.Join(", ", placeholders)).Append(") AS durable_new ON DUPLICATE KEY UPDATE ");
            IReadOnlyList<ColumnMetadata> assigned = updateColumns.Count > 0 ? updateColumns : conflictColumns;
            builder.Append(string.Join(", ", assigned.Select(c => QuoteIdentifier(c.Name) + " = " + (c.IsVersion && versionPlaceholder != null ? versionPlaceholder : "durable_new." + QuoteIdentifier(c.Name)))));
        }

        /// <inheritdoc />
        public override string GetColumnType(ColumnMetadata column)
        {
            ArgumentNullException.ThrowIfNull(column);
            Type type = column.Converter?.ProviderType ?? column.ClrType;
            type = Nullable.GetUnderlyingType(type) ?? type;
            if (column.IsJson) return "JSON";
            if (type.IsEnum) return column.EnumAsString ? StringType(column, "VARCHAR", "VARCHAR(64)", 64) : "INT";
            if (type == typeof(bool)) return "TINYINT(1)";
            if (type == typeof(sbyte)) return "TINYINT";
            if (type == typeof(byte)) return "TINYINT UNSIGNED";
            if (type == typeof(short)) return "SMALLINT";
            if (type == typeof(ushort)) return "SMALLINT UNSIGNED";
            if (type == typeof(int)) return "INT";
            if (type == typeof(uint)) return "INT UNSIGNED";
            if (type == typeof(long)) return "BIGINT";
            if (type == typeof(ulong)) return "BIGINT UNSIGNED";
            if (type == typeof(float)) return "FLOAT";
            if (type == typeof(double)) return "DOUBLE";
            if (type == typeof(decimal)) return "DECIMAL(38, 10)";
            if (type == typeof(DateTime) || type == typeof(DateTimeOffset)) return "DATETIME(6)";
            if (type == typeof(DateOnly)) return "DATE";
            if (type == typeof(TimeOnly)) return "TIME(6)";
            if (type == typeof(TimeSpan)) return "BIGINT";
            if (type == typeof(Guid)) return "CHAR(36)";
            if (type == typeof(byte[])) return "LONGBLOB";
            if (type == typeof(char)) return "CHAR(1)";
            return StringType(column, "VARCHAR", "LONGTEXT", 255);
        }

        /// <inheritdoc />
        public override SqlStatement TableExistsQuery(string tableName)
        {
            return new SqlStatement(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = @p0",
                new[] { new SqlParameterValue("@p0", tableName) });
        }

        /// <inheritdoc />
        public override SqlStatement ColumnNamesQuery(string tableName)
        {
            return new SqlStatement(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = @p0 ORDER BY ordinal_position",
                new[] { new SqlParameterValue("@p0", tableName) });
        }

        /// <inheritdoc />
        public override SqlStatement IndexNamesQuery(string tableName)
        {
            return new SqlStatement(
                "SELECT DISTINCT index_name FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = @p0 AND index_name <> 'PRIMARY'",
                new[] { new SqlParameterValue("@p0", tableName) });
        }

        /// <inheritdoc />
        public override string CreateIndexSql(string indexName, string tableName, IReadOnlyList<string> columns, bool unique, IReadOnlyList<string>? includedColumns)
        {
            return base.CreateIndexSql(indexName, tableName, columns, unique, null);
        }

        /// <inheritdoc />
        public override string DropIndexSql(string indexName, string? tableName)
        {
            if (tableName == null) throw new ArgumentNullException(nameof(tableName), "MySQL requires the table name to drop an index.");
            return "DROP INDEX " + QuoteIdentifier(indexName) + " ON " + QuoteIdentifier(tableName);
        }

        // Migrations

        /// <inheritdoc />
        public override SqlStatement ColumnSchemaQuery(string tableName)
        {
            return new SqlStatement(
                "SELECT column_name, column_type, CASE WHEN is_nullable = 'YES' THEN 1 ELSE 0 END, character_maximum_length, " +
                "CASE WHEN column_key = 'PRI' THEN 1 ELSE 0 END FROM information_schema.columns " +
                "WHERE table_schema = DATABASE() AND table_name = @p0 ORDER BY ordinal_position",
                new[] { new SqlParameterValue("@p0", tableName) });
        }

        /// <inheritdoc />
        public override SqlStatement IndexSchemaQuery(string tableName)
        {
            return new SqlStatement(
                "SELECT index_name, column_name, CASE WHEN non_unique = 0 THEN 1 ELSE 0 END, seq_in_index, 0 " +
                "FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = @p0 AND index_name <> 'PRIMARY' " +
                "ORDER BY index_name, seq_in_index",
                new[] { new SqlParameterValue("@p0", tableName) });
        }

        /// <inheritdoc />
        public override string NormalizeColumnType(string columnType)
        {
            string type = base.NormalizeColumnType(columnType);
            if (type == "bool" || type == "boolean") return "tinyint(1)";
            Match integer = Regex.Match(type, "^(tinyint|smallint|mediumint|integer|bigint|int)(\\((\\d+)\\))?(.*)$");
            if (integer.Success)
            {
                string name = integer.Groups[1].Value == "integer" ? "int" : integer.Groups[1].Value;
                string width = name == "tinyint" && integer.Groups[3].Value == "1" ? "(1)" : string.Empty;
                return name + width + integer.Groups[4].Value;
            }

            if (type.StartsWith("numeric", StringComparison.Ordinal)) return "decimal" + type.Substring(7);
            if (type == "double precision" || type == "real") return "double";
            return type;
        }

        /// <inheritdoc />
        public override string CreateMigrationHistoryTableSql(string tableName)
        {
            ArgumentNullException.ThrowIfNull(tableName);
            return "CREATE TABLE IF NOT EXISTS " + QuoteIdentifier(tableName) + " ("
                + QuoteIdentifier("id") + " VARCHAR(150) NOT NULL PRIMARY KEY, "
                + QuoteIdentifier("description") + " VARCHAR(1000) NULL, "
                + QuoteIdentifier("applied_utc") + " DATETIME(6) NOT NULL, "
                + QuoteIdentifier("duration_ms") + " BIGINT NOT NULL)";
        }

        /// <inheritdoc />
        public override SqlStatement? AcquireMigrationLockSql(string lockName, int waitSeconds)
        {
            ArgumentNullException.ThrowIfNull(lockName);
            return new SqlStatement(
                "SELECT COALESCE(GET_LOCK(@p0, @p1), 0)",
                new[] { new SqlParameterValue("@p0", lockName), new SqlParameterValue("@p1", Math.Max(0, waitSeconds)) });
        }

        /// <inheritdoc />
        public override SqlStatement? ReleaseMigrationLockSql(string lockName)
        {
            ArgumentNullException.ThrowIfNull(lockName);
            return new SqlStatement("SELECT RELEASE_LOCK(@p0)", new[] { new SqlParameterValue("@p0", lockName) });
        }

        #endregion

        #region Private-Methods

        /// <inheritdoc />
        protected override string IdentifierOpen => "`";

        /// <inheritdoc />
        protected override string IdentifierClose => "`";

        /// <inheritdoc />
        protected override string? UnboundedLimit => "18446744073709551615";

        /// <inheritdoc />
        protected override string AutoIncrementColumnType(ColumnMetadata column, bool inlinePrimaryKey)
        {
            Type type = column.ClrType;
            string baseType = type == typeof(long) ? "BIGINT" : type == typeof(short) ? "SMALLINT" : "INT";
            return baseType + " NOT NULL AUTO_INCREMENT" + (inlinePrimaryKey ? " PRIMARY KEY" : string.Empty);
        }

        // Migrations

        /// <summary>
        /// Renders a string literal, escaping backslashes (MySQL treats them as escape characters by default) and quotes.
        /// </summary>
        /// <param name="value">Text. Must not be null.</param>
        /// <returns>The literal.</returns>
        protected override string StringLiteral(string value)
        {
            return "'" + value.Replace("\\", "\\\\", StringComparison.Ordinal).Replace("'", "''", StringComparison.Ordinal) + "'";
        }

        #endregion
    }
}
