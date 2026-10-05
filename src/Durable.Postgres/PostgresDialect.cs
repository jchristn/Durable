namespace Durable.Postgres
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Text.RegularExpressions;
    using Npgsql;
    using NpgsqlTypes;
    using Durable;
    using Durable.Query;
    using Durable.Sql;

    /// <summary>
    /// PostgreSQL dialect (PostgreSQL 12+). Tables are resolved in <c>current_schema()</c>, so the connection's
    /// search path selects the schema. JSON columns are bound as <c>jsonb</c>.
    /// Thread safety: immutable; safe to share.
    /// </summary>
    public class PostgresDialect : SqlDialect
    {
        #region Public-Members

        /// <summary>
        /// Gets the shared default instance.
        /// </summary>
        public static PostgresDialect Default { get; } = new PostgresDialect();

        /// <inheritdoc />
        public override RepositoryType RepositoryType => RepositoryType.Postgres;

        /// <inheritdoc />
        public override string DbSystemName => "postgresql";

        /// <inheritdoc />
        public override int MaxParameters => 32000;

        /// <summary>
        /// Gets the binary collation applied by <see cref="OrdinalCollation"/> for ordinal and ignore-case string matching.
        /// Default: C. PostgreSQL equality and LIKE are already case- and accent-sensitive under deterministic collations; the collation mainly fixes ordering comparisons.
        /// </summary>
        public string OrdinalCollationName { get; }

        // Migrations

        /// <inheritdoc />
        public override int MaxIdentifierLength => 63;

        /// <inheritdoc />
        public override string CurrentUtcTimestampSql => "(now() AT TIME ZONE 'utc')";

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the dialect.
        /// </summary>
        /// <param name="converter">Converter; null uses <see cref="PostgresDataTypeConverter"/>.</param>
        /// <param name="ordinalCollation">Binary collation for ordinal string matching. Default: C (byte order of UTF-8, which is code-point order).</param>
        /// <exception cref="ArgumentException">Thrown when ordinalCollation is not a simple collation name.</exception>
        public PostgresDialect(IDataTypeConverter? converter = null, string ordinalCollation = "C") : base(converter ?? new PostgresDataTypeConverter())
        {
            OrdinalCollationName = SqlIdentifierValidator.RequireIdentifier(ordinalCollation, nameof(ordinalCollation));
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        /// <remarks>PostgreSQL sorts NULLs as larger than any value; NULLS FIRST / NULLS LAST restore LINQ ordering.</remarks>
        public override string OrderDirection(bool descending)
        {
            return descending ? " DESC NULLS LAST" : " ASC NULLS FIRST";
        }

        /// <inheritdoc />
        public override string OrdinalCollation(string expression)
        {
            return expression + " COLLATE \"" + OrdinalCollationName + "\"";
        }

        /// <inheritdoc />
        public override void ConfigureParameter(DbParameter parameter, SqlParameterValue value)
        {
            ArgumentNullException.ThrowIfNull(parameter);
            ArgumentNullException.ThrowIfNull(value);
            if (parameter is NpgsqlParameter npgsql && value.Column != null && value.Column.IsJson && value.Column.Converter == null)
                npgsql.NpgsqlDbType = NpgsqlDbType.Jsonb;
        }

        /// <inheritdoc />
        public override string TranslateFunction(QueryFunction function, IReadOnlyList<string> arguments)
        {
            string a0 = arguments.Count > 0 ? arguments[0] : string.Empty;
            string a1 = arguments.Count > 1 ? arguments[1] : string.Empty;
            switch (function)
            {
                case QueryFunction.IndexOf: return "(STRPOS(" + a0 + ", " + a1 + ") - 1)";
                case QueryFunction.Round:
                    return arguments.Count > 1 ? "ROUND(CAST(" + a0 + " AS NUMERIC), " + a1 + ")" : "ROUND(CAST(" + a0 + " AS NUMERIC))";
                case QueryFunction.Year: return "CAST(EXTRACT(YEAR FROM " + a0 + ") AS INTEGER)";
                case QueryFunction.Month: return "CAST(EXTRACT(MONTH FROM " + a0 + ") AS INTEGER)";
                case QueryFunction.Day: return "CAST(EXTRACT(DAY FROM " + a0 + ") AS INTEGER)";
                case QueryFunction.Hour: return "CAST(EXTRACT(HOUR FROM " + a0 + ") AS INTEGER)";
                case QueryFunction.Minute: return "CAST(EXTRACT(MINUTE FROM " + a0 + ") AS INTEGER)";
                case QueryFunction.Second: return "CAST(FLOOR(EXTRACT(SECOND FROM " + a0 + ")) AS INTEGER)";
                case QueryFunction.DayOfYear: return "CAST(EXTRACT(DOY FROM " + a0 + ") AS INTEGER)";
                case QueryFunction.DayOfWeek: return "CAST(EXTRACT(DOW FROM " + a0 + ") AS INTEGER)";
                case QueryFunction.Date: return "DATE_TRUNC('day', " + a0 + ")";
                case QueryFunction.AddYears: return "(" + a0 + " + (" + a1 + ") * INTERVAL '1 year')";
                case QueryFunction.AddMonths: return "(" + a0 + " + (" + a1 + ") * INTERVAL '1 month')";
                case QueryFunction.AddDays: return "(" + a0 + " + (" + a1 + ") * INTERVAL '1 day')";
                case QueryFunction.AddHours: return "(" + a0 + " + (" + a1 + ") * INTERVAL '1 hour')";
                case QueryFunction.AddMinutes: return "(" + a0 + " + (" + a1 + ") * INTERVAL '1 minute')";
                case QueryFunction.AddSeconds: return "(" + a0 + " + (" + a1 + ") * INTERVAL '1 second')";
                default:
                    return base.TranslateFunction(function, arguments);
            }
        }

        /// <inheritdoc />
        public override string GetColumnType(ColumnMetadata column)
        {
            ArgumentNullException.ThrowIfNull(column);
            Type type = column.Converter?.ProviderType ?? column.ClrType;
            type = Nullable.GetUnderlyingType(type) ?? type;
            if (column.IsJson) return "JSONB";
            if (type.IsEnum) return column.EnumAsString ? StringType(column, "VARCHAR", "TEXT", 64) : "INTEGER";
            if (type == typeof(bool)) return "BOOLEAN";
            if (type == typeof(byte) || type == typeof(sbyte) || type == typeof(short)) return "SMALLINT";
            if (type == typeof(ushort) || type == typeof(int)) return "INTEGER";
            if (type == typeof(uint) || type == typeof(long)) return "BIGINT";
            if (type == typeof(ulong)) return "NUMERIC(20, 0)";
            if (type == typeof(float)) return "REAL";
            if (type == typeof(double)) return "DOUBLE PRECISION";
            if (type == typeof(decimal)) return "NUMERIC(38, 10)";
            if (type == typeof(DateTime)) return "TIMESTAMP";
            if (type == typeof(DateTimeOffset)) return "TIMESTAMPTZ";
            if (type == typeof(DateOnly)) return "DATE";
            if (type == typeof(TimeOnly)) return "TIME";
            if (type == typeof(TimeSpan)) return "INTERVAL";
            if (type == typeof(Guid)) return "UUID";
            if (type == typeof(byte[])) return "BYTEA";
            if (type == typeof(char)) return "CHAR(1)";
            return StringType(column, "VARCHAR", "TEXT", 0);
        }

        /// <inheritdoc />
        public override SqlStatement TableExistsQuery(string tableName)
        {
            return new SqlStatement(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = @p0",
                new[] { new SqlParameterValue("@p0", tableName) });
        }

        /// <inheritdoc />
        public override SqlStatement ColumnNamesQuery(string tableName)
        {
            return new SqlStatement(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = @p0 ORDER BY ordinal_position",
                new[] { new SqlParameterValue("@p0", tableName) });
        }

        /// <inheritdoc />
        public override SqlStatement IndexNamesQuery(string tableName)
        {
            return new SqlStatement(
                "SELECT i.indexname FROM pg_indexes i WHERE i.schemaname = current_schema() AND i.tablename = @p0 " +
                "AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conname = i.indexname AND c.contype = 'p')",
                new[] { new SqlParameterValue("@p0", tableName) });
        }

        /// <inheritdoc />
        public override string CreateIndexSql(string indexName, string tableName, IReadOnlyList<string> columns, bool unique, IReadOnlyList<string>? includedColumns)
        {
            string sql = base.CreateIndexSql(indexName, tableName, columns, unique, null);
            if (includedColumns != null && includedColumns.Count > 0)
            {
                List<string> quoted = new List<string>();
                foreach (string column in includedColumns) quoted.Add(QuoteIdentifier(column));
                sql += " INCLUDE (" + string.Join(", ", quoted) + ")";
            }

            return sql;
        }

        // Migrations

        /// <inheritdoc />
        public override SqlStatement ColumnSchemaQuery(string tableName)
        {
            return new SqlStatement(
                "SELECT a.attname, format_type(a.atttypid, a.atttypmod), CASE WHEN a.attnotnull THEN 0 ELSE 1 END, " +
                "CASE WHEN a.atttypid IN (1042, 1043) AND a.atttypmod > 4 THEN a.atttypmod - 4 ELSE NULL END, " +
                "CASE WHEN EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary AND a.attnum = ANY(i.indkey)) THEN 1 ELSE 0 END " +
                "FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace " +
                "WHERE n.nspname = current_schema() AND c.relname = @p0 AND c.relkind IN ('r', 'p') AND a.attnum > 0 AND NOT a.attisdropped " +
                "ORDER BY a.attnum",
                new[] { new SqlParameterValue("@p0", tableName) });
        }

        /// <inheritdoc />
        public override SqlStatement IndexSchemaQuery(string tableName)
        {
            return new SqlStatement(
                "SELECT ic.relname, a.attname, CASE WHEN ix.indisunique THEN 1 ELSE 0 END, k.ord, CASE WHEN k.ord > ix.indnkeyatts THEN 1 ELSE 0 END " +
                "FROM pg_index ix JOIN pg_class t ON t.oid = ix.indrelid JOIN pg_namespace n ON n.oid = t.relnamespace " +
                "JOIN pg_class ic ON ic.oid = ix.indexrelid " +
                "CROSS JOIN LATERAL unnest(ix.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord) " +
                "JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum " +
                "WHERE n.nspname = current_schema() AND t.relname = @p0 AND NOT ix.indisprimary " +
                "AND NOT EXISTS (SELECT 1 FROM pg_constraint con WHERE con.conindid = ix.indexrelid) " +
                "ORDER BY ic.relname, k.ord",
                new[] { new SqlParameterValue("@p0", tableName) });
        }

        /// <inheritdoc />
        public override string NormalizeColumnType(string columnType)
        {
            string type = base.NormalizeColumnType(columnType);
            Match temporal = Regex.Match(type, "^(timestamp|time)(\\(\\d+\\))? (with|without) time zone$");
            if (temporal.Success)
                return temporal.Groups[1].Value + (temporal.Groups[3].Value == "with" ? "tz" : string.Empty) + temporal.Groups[2].Value;

            int paren = type.IndexOf('(');
            string name = paren < 0 ? type : type.Substring(0, paren);
            string suffix = paren < 0 ? string.Empty : type.Substring(paren);
            switch (name)
            {
                case "character varying": name = "varchar"; break;
                case "character": name = "char"; break;
                case "int":
                case "int4":
                case "serial":
                case "serial4": name = "integer"; break;
                case "int8":
                case "bigserial":
                case "serial8": name = "bigint"; break;
                case "int2":
                case "smallserial":
                case "serial2": name = "smallint"; break;
                case "float8": name = "double precision"; break;
                case "float4": name = "real"; break;
                case "bool": name = "boolean"; break;
                case "decimal": name = "numeric"; break;
            }

            return name + suffix;
        }

        /// <inheritdoc />
        public override SqlStatement? AcquireMigrationLockSql(string lockName, int waitSeconds)
        {
            ArgumentNullException.ThrowIfNull(lockName);
            return new SqlStatement(
                "SELECT CASE WHEN pg_try_advisory_lock(hashtext(@p0)) THEN 1 ELSE 0 END",
                new[] { new SqlParameterValue("@p0", lockName) });
        }

        /// <inheritdoc />
        public override SqlStatement? ReleaseMigrationLockSql(string lockName)
        {
            ArgumentNullException.ThrowIfNull(lockName);
            return new SqlStatement("SELECT pg_advisory_unlock(hashtext(@p0))", new[] { new SqlParameterValue("@p0", lockName) });
        }

        #endregion

        #region Private-Methods

        /// <inheritdoc />
        protected override string AutoIncrementColumnType(ColumnMetadata column, bool inlinePrimaryKey)
        {
            Type type = column.ClrType;
            string baseType = type == typeof(long) ? "BIGINT" : type == typeof(short) ? "SMALLINT" : "INTEGER";
            return baseType + " GENERATED BY DEFAULT AS IDENTITY" + (inlinePrimaryKey ? " PRIMARY KEY" : string.Empty);
        }

        // Migrations

        /// <inheritdoc />
        protected override string BinaryLiteral(byte[] value)
        {
            return "'\\x" + Convert.ToHexString(value) + "'::bytea";
        }

        #endregion
    }
}
