namespace Durable.Postgres
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using Npgsql;
    using NpgsqlTypes;
    using Durable;
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

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the dialect.
        /// </summary>
        /// <param name="converter">Converter; null uses <see cref="PostgresDataTypeConverter"/>.</param>
        public PostgresDialect(IDataTypeConverter? converter = null) : base(converter ?? new PostgresDataTypeConverter())
        {
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override void ConfigureParameter(DbParameter parameter, SqlParameterValue value)
        {
            ArgumentNullException.ThrowIfNull(parameter);
            ArgumentNullException.ThrowIfNull(value);
            if (parameter is NpgsqlParameter npgsql && value.Column != null && value.Column.IsJson && value.Column.Converter == null)
                npgsql.NpgsqlDbType = NpgsqlDbType.Jsonb;
        }

        /// <inheritdoc />
        public override string TranslateFunction(SqlFunction function, IReadOnlyList<string> arguments)
        {
            string a0 = arguments.Count > 0 ? arguments[0] : string.Empty;
            string a1 = arguments.Count > 1 ? arguments[1] : string.Empty;
            switch (function)
            {
                case SqlFunction.IndexOf: return "(STRPOS(" + a0 + ", " + a1 + ") - 1)";
                case SqlFunction.Round:
                    return arguments.Count > 1 ? "ROUND(CAST(" + a0 + " AS NUMERIC), " + a1 + ")" : "ROUND(CAST(" + a0 + " AS NUMERIC))";
                case SqlFunction.Year: return "CAST(EXTRACT(YEAR FROM " + a0 + ") AS INTEGER)";
                case SqlFunction.Month: return "CAST(EXTRACT(MONTH FROM " + a0 + ") AS INTEGER)";
                case SqlFunction.Day: return "CAST(EXTRACT(DAY FROM " + a0 + ") AS INTEGER)";
                case SqlFunction.Hour: return "CAST(EXTRACT(HOUR FROM " + a0 + ") AS INTEGER)";
                case SqlFunction.Minute: return "CAST(EXTRACT(MINUTE FROM " + a0 + ") AS INTEGER)";
                case SqlFunction.Second: return "CAST(FLOOR(EXTRACT(SECOND FROM " + a0 + ")) AS INTEGER)";
                case SqlFunction.DayOfYear: return "CAST(EXTRACT(DOY FROM " + a0 + ") AS INTEGER)";
                case SqlFunction.DayOfWeek: return "CAST(EXTRACT(DOW FROM " + a0 + ") AS INTEGER)";
                case SqlFunction.Date: return "DATE_TRUNC('day', " + a0 + ")";
                case SqlFunction.AddYears: return "(" + a0 + " + (" + a1 + ") * INTERVAL '1 year')";
                case SqlFunction.AddMonths: return "(" + a0 + " + (" + a1 + ") * INTERVAL '1 month')";
                case SqlFunction.AddDays: return "(" + a0 + " + (" + a1 + ") * INTERVAL '1 day')";
                case SqlFunction.AddHours: return "(" + a0 + " + (" + a1 + ") * INTERVAL '1 hour')";
                case SqlFunction.AddMinutes: return "(" + a0 + " + (" + a1 + ") * INTERVAL '1 minute')";
                case SqlFunction.AddSeconds: return "(" + a0 + " + (" + a1 + ") * INTERVAL '1 second')";
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

        #endregion

        #region Private-Methods

        /// <inheritdoc />
        protected override string AutoIncrementColumnType(ColumnMetadata column, bool inlinePrimaryKey)
        {
            Type type = column.ClrType;
            string baseType = type == typeof(long) ? "BIGINT" : type == typeof(short) ? "SMALLINT" : "INTEGER";
            return baseType + " GENERATED BY DEFAULT AS IDENTITY" + (inlinePrimaryKey ? " PRIMARY KEY" : string.Empty);
        }

        #endregion
    }
}
