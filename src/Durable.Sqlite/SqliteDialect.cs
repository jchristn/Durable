namespace Durable.Sqlite
{
    using System;
    using System.Collections.Generic;
    using Durable;
    using Durable.Sql;

    /// <summary>
    /// SQLite dialect (SQLite 3.35+, as bundled with Microsoft.Data.Sqlite).
    /// Thread safety: immutable; safe to share.
    /// </summary>
    public class SqliteDialect : SqlDialect
    {
        #region Public-Members

        /// <summary>
        /// Gets the shared default instance.
        /// </summary>
        public static SqliteDialect Default { get; } = new SqliteDialect();

        /// <inheritdoc />
        public override RepositoryType RepositoryType => RepositoryType.Sqlite;

        /// <inheritdoc />
        public override string DbSystemName => "sqlite";

        /// <inheritdoc />
        public override int MaxParameters => 32000;

        /// <inheritdoc />
        public override bool SupportsStoredProcedures => false;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the dialect.
        /// </summary>
        /// <param name="converter">Converter; null uses <see cref="SqliteDataTypeConverter"/>.</param>
        public SqliteDialect(IDataTypeConverter? converter = null) : base(converter ?? new SqliteDataTypeConverter())
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
        public override string TranslateFunction(SqlFunction function, IReadOnlyList<string> arguments)
        {
            string a0 = arguments.Count > 0 ? arguments[0] : string.Empty;
            string a1 = arguments.Count > 1 ? arguments[1] : string.Empty;
            switch (function)
            {
                case SqlFunction.Ceiling:
                    return "(CAST(" + a0 + " AS INTEGER) + (" + a0 + " > CAST(" + a0 + " AS INTEGER)))";
                case SqlFunction.Floor:
                    return "(CAST(" + a0 + " AS INTEGER) - (" + a0 + " < CAST(" + a0 + " AS INTEGER)))";
                case SqlFunction.Year: return "CAST(strftime('%Y', " + a0 + ") AS INTEGER)";
                case SqlFunction.Month: return "CAST(strftime('%m', " + a0 + ") AS INTEGER)";
                case SqlFunction.Day: return "CAST(strftime('%d', " + a0 + ") AS INTEGER)";
                case SqlFunction.Hour: return "CAST(strftime('%H', " + a0 + ") AS INTEGER)";
                case SqlFunction.Minute: return "CAST(strftime('%M', " + a0 + ") AS INTEGER)";
                case SqlFunction.Second: return "CAST(strftime('%S', " + a0 + ") AS INTEGER)";
                case SqlFunction.DayOfYear: return "CAST(strftime('%j', " + a0 + ") AS INTEGER)";
                case SqlFunction.DayOfWeek: return "CAST(strftime('%w', " + a0 + ") AS INTEGER)";
                case SqlFunction.Date: return "strftime('%Y-%m-%d 00:00:00.0000000', " + a0 + ")";
                case SqlFunction.AddYears: return AddInterval(a0, a1, "years");
                case SqlFunction.AddMonths: return AddInterval(a0, a1, "months");
                case SqlFunction.AddDays: return AddInterval(a0, a1, "days");
                case SqlFunction.AddHours: return AddInterval(a0, a1, "hours");
                case SqlFunction.AddMinutes: return AddInterval(a0, a1, "minutes");
                case SqlFunction.AddSeconds: return AddInterval(a0, a1, "seconds");
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
            if (column.IsJson) return "TEXT";
            if (type.IsEnum) return column.EnumAsString ? "TEXT" : "INTEGER";
            if (type == typeof(bool) || type == typeof(byte) || type == typeof(sbyte) || type == typeof(short) || type == typeof(ushort)
                || type == typeof(int) || type == typeof(uint) || type == typeof(long) || type == typeof(ulong))
                return "INTEGER";
            if (type == typeof(float) || type == typeof(double) || type == typeof(decimal)) return "REAL";
            if (type == typeof(byte[])) return "BLOB";
            return "TEXT";
        }

        /// <inheritdoc />
        public override SqlStatement TableExistsQuery(string tableName)
        {
            return new SqlStatement("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = @p0", new[] { new SqlParameterValue("@p0", tableName) });
        }

        /// <inheritdoc />
        public override SqlStatement ColumnNamesQuery(string tableName)
        {
            return new SqlStatement("SELECT name FROM pragma_table_info(@p0)", new[] { new SqlParameterValue("@p0", tableName) });
        }

        /// <inheritdoc />
        public override SqlStatement IndexNamesQuery(string tableName)
        {
            return new SqlStatement(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = @p0 AND name NOT LIKE 'sqlite_autoindex%'",
                new[] { new SqlParameterValue("@p0", tableName) });
        }

        /// <inheritdoc />
        public override string CreateIndexSql(string indexName, string tableName, IReadOnlyList<string> columns, bool unique, IReadOnlyList<string>? includedColumns)
        {
            return base.CreateIndexSql(indexName, tableName, columns, unique, null);
        }

        #endregion

        #region Private-Methods

        /// <inheritdoc />
        protected override string? UnboundedLimit => "-1";

        /// <inheritdoc />
        protected override bool InlinesAutoIncrementKey => true;

        /// <inheritdoc />
        protected override string AutoIncrementColumnType(ColumnMetadata column, bool inlinePrimaryKey)
        {
            return inlinePrimaryKey ? "INTEGER PRIMARY KEY AUTOINCREMENT" : "INTEGER NOT NULL";
        }

        private static string AddInterval(string date, string amount, string unit)
        {
            return "(strftime('%Y-%m-%d %H:%M:%f', " + date + ", printf('%+d " + unit + "', " + amount + ")) || '0000')";
        }

        #endregion
    }
}
