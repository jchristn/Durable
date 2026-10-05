namespace Durable.MySql
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Durable;
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

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the dialect.
        /// </summary>
        /// <param name="converter">Converter; null uses <see cref="MySqlDataTypeConverter"/>.</param>
        public MySqlDialect(IDataTypeConverter? converter = null) : base(converter ?? new MySqlDataTypeConverter())
        {
        }

        #endregion

        #region Public-Methods

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
        public override string TranslateFunction(SqlFunction function, IReadOnlyList<string> arguments)
        {
            string a0 = arguments.Count > 0 ? arguments[0] : string.Empty;
            string a1 = arguments.Count > 1 ? arguments[1] : string.Empty;
            switch (function)
            {
                case SqlFunction.Length: return "CHAR_LENGTH(" + a0 + ")";
                case SqlFunction.IndexOf: return "(LOCATE(" + a1 + ", " + a0 + ") - 1)";
                case SqlFunction.Year: return "YEAR(" + a0 + ")";
                case SqlFunction.Month: return "MONTH(" + a0 + ")";
                case SqlFunction.Day: return "DAY(" + a0 + ")";
                case SqlFunction.Hour: return "HOUR(" + a0 + ")";
                case SqlFunction.Minute: return "MINUTE(" + a0 + ")";
                case SqlFunction.Second: return "SECOND(" + a0 + ")";
                case SqlFunction.DayOfYear: return "DAYOFYEAR(" + a0 + ")";
                case SqlFunction.DayOfWeek: return "(DAYOFWEEK(" + a0 + ") - 1)";
                case SqlFunction.Date: return "CAST(DATE(" + a0 + ") AS DATETIME(6))";
                case SqlFunction.AddYears: return "DATE_ADD(" + a0 + ", INTERVAL " + a1 + " YEAR)";
                case SqlFunction.AddMonths: return "DATE_ADD(" + a0 + ", INTERVAL " + a1 + " MONTH)";
                case SqlFunction.AddDays: return "DATE_ADD(" + a0 + ", INTERVAL " + a1 + " DAY)";
                case SqlFunction.AddHours: return "DATE_ADD(" + a0 + ", INTERVAL " + a1 + " HOUR)";
                case SqlFunction.AddMinutes: return "DATE_ADD(" + a0 + ", INTERVAL " + a1 + " MINUTE)";
                case SqlFunction.AddSeconds: return "DATE_ADD(" + a0 + ", INTERVAL " + a1 + " SECOND)";
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

        #endregion
    }
}
