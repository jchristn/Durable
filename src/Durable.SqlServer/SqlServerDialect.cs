namespace Durable.SqlServer
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Globalization;
    using System.Linq;
    using Microsoft.Data.SqlClient;
    using Durable;
    using Durable.Query;
    using Durable.Sql;

    /// <summary>
    /// SQL Server dialect (SQL Server 2017+). Paging uses OFFSET/FETCH, generated keys use OUTPUT INSERTED, upsert uses
    /// MERGE, and <see cref="DateTime"/> parameters are sent as datetime2.
    /// Thread safety: immutable; safe to share.
    /// </summary>
    public class SqlServerDialect : SqlDialect
    {
        #region Public-Members

        /// <summary>
        /// Gets the shared default instance.
        /// </summary>
        public static SqlServerDialect Default { get; } = new SqlServerDialect();

        /// <inheritdoc />
        public override RepositoryType RepositoryType => RepositoryType.SqlServer;

        /// <inheritdoc />
        public override string DbSystemName => "mssql";

        /// <inheritdoc />
        public override int MaxParameters => 2000;

        /// <inheritdoc />
        public override string RecursiveCteKeyword => "WITH";

        /// <inheritdoc />
        public override InsertKeyStrategy InsertKeyStrategy => InsertKeyStrategy.Output;

        /// <summary>
        /// Gets the binary collation applied by <see cref="OrdinalCollation"/> for ordinal and ignore-case string matching.
        /// Default: Latin1_General_100_BIN2. BIN2 collations compare Unicode code points, so the Latin1 name does not restrict the languages compared.
        /// </summary>
        public string OrdinalCollationName { get; }

        /// <inheritdoc />
        public override string StringCastType => "NVARCHAR(MAX)";

        // Migrations

        /// <inheritdoc />
        public override int MaxIdentifierLength => 128;

        /// <summary>
        /// Gets "GO": generated scripts separate statements into batches so later statements can reference columns added earlier.
        /// </summary>
        public override string? ScriptBatchSeparator => "GO";

        /// <inheritdoc />
        public override string CurrentUtcTimestampSql => "SYSUTCDATETIME()";

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the dialect.
        /// </summary>
        /// <param name="converter">Converter; null uses <see cref="SqlServerDataTypeConverter"/>.</param>
        /// <param name="ordinalCollation">Binary collation for ordinal string matching. Default: Latin1_General_100_BIN2 (code-point order).</param>
        /// <exception cref="ArgumentException">Thrown when ordinalCollation is not a simple collation name.</exception>
        public SqlServerDialect(IDataTypeConverter? converter = null, string ordinalCollation = "Latin1_General_100_BIN2") : base(converter ?? new SqlServerDataTypeConverter())
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
        public override void ConfigureParameter(DbParameter parameter, SqlParameterValue value)
        {
            ArgumentNullException.ThrowIfNull(parameter);
            ArgumentNullException.ThrowIfNull(value);
            if (parameter is not SqlParameter sqlParameter) return;
            if (value.Value is DateTime) sqlParameter.SqlDbType = SqlDbType.DateTime2;
            else if (value.Value is string text && text.Length > 4000) sqlParameter.SqlDbType = SqlDbType.NVarChar;
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
        public override string IsEmptyString(string expression)
        {
            return "DATALENGTH(" + expression + ") = 0";
        }

        /// <inheritdoc />
        public override string TranslateFunction(QueryFunction function, IReadOnlyList<string> arguments)
        {
            string a0 = arguments.Count > 0 ? arguments[0] : string.Empty;
            string a1 = arguments.Count > 1 ? arguments[1] : string.Empty;
            switch (function)
            {
                case QueryFunction.Length: return "(LEN(" + a0 + " + N'.') - 1)";
                case QueryFunction.Substring:
                    return arguments.Count > 2
                        ? "SUBSTRING(" + a0 + ", (" + a1 + ") + 1, " + arguments[2] + ")"
                        : "SUBSTRING(" + a0 + ", (" + a1 + ") + 1, LEN(" + a0 + "))";
                case QueryFunction.IndexOf: return "(CHARINDEX(" + a1 + ", " + a0 + ") - 1)";
                case QueryFunction.Round: return arguments.Count > 1 ? "ROUND(" + a0 + ", " + a1 + ")" : "ROUND(" + a0 + ", 0)";
                case QueryFunction.Year: return "DATEPART(year, " + a0 + ")";
                case QueryFunction.Month: return "DATEPART(month, " + a0 + ")";
                case QueryFunction.Day: return "DATEPART(day, " + a0 + ")";
                case QueryFunction.Hour: return "DATEPART(hour, " + a0 + ")";
                case QueryFunction.Minute: return "DATEPART(minute, " + a0 + ")";
                case QueryFunction.Second: return "DATEPART(second, " + a0 + ")";
                case QueryFunction.DayOfYear: return "DATEPART(dayofyear, " + a0 + ")";
                case QueryFunction.DayOfWeek: return "((DATEPART(weekday, " + a0 + ") + @@DATEFIRST - 1) % 7)";
                case QueryFunction.Date: return "CAST(CAST(" + a0 + " AS DATE) AS DATETIME2)";
                case QueryFunction.AddYears: return "DATEADD(year, " + a1 + ", " + a0 + ")";
                case QueryFunction.AddMonths: return "DATEADD(month, " + a1 + ", " + a0 + ")";
                case QueryFunction.AddDays: return "DATEADD(day, " + a1 + ", " + a0 + ")";
                case QueryFunction.AddHours: return "DATEADD(hour, " + a1 + ", " + a0 + ")";
                case QueryFunction.AddMinutes: return "DATEADD(minute, " + a1 + ", " + a0 + ")";
                case QueryFunction.AddSeconds: return "DATEADD(second, " + a1 + ", " + a0 + ")";
                default:
                    return base.TranslateFunction(function, arguments);
            }
        }

        /// <inheritdoc />
        public override void AppendPaging(SqlStatementBuilder builder, int? skip, int? take, bool hasOrderBy)
        {
            ArgumentNullException.ThrowIfNull(builder);
            if (!hasOrderBy) builder.Append(" ORDER BY (SELECT NULL)");
            builder.Append(" OFFSET ").Append((skip ?? 0).ToString(CultureInfo.InvariantCulture)).Append(" ROWS");
            if (take.HasValue) builder.Append(" FETCH NEXT ").Append(take.Value.ToString(CultureInfo.InvariantCulture)).Append(" ROWS ONLY");
        }

        /// <inheritdoc />
        public override void AppendUpsert(SqlStatementBuilder builder, EntityMetadata metadata, IReadOnlyList<ColumnMetadata> insertColumns, IReadOnlyList<string> placeholders, IReadOnlyList<ColumnMetadata> conflictColumns, IReadOnlyList<ColumnMetadata> updateColumns, string? versionPlaceholder = null)
        {
            ArgumentNullException.ThrowIfNull(builder);
            ArgumentNullException.ThrowIfNull(metadata);
            string table = QuoteIdentifier(metadata.TableName);
            bool identityInsert = insertColumns.Any(c => c.IsAutoIncrement);
            if (identityInsert) builder.Append("SET IDENTITY_INSERT ").Append(table).Append(" ON; ");

            builder.Append("MERGE INTO ").Append(table).Append(" WITH (HOLDLOCK) AS durable_target USING (SELECT ");
            for (int i = 0; i < insertColumns.Count; i++)
            {
                if (i > 0) builder.Append(", ");
                builder.Append(placeholders[i]).Append(" AS ").Append(QuoteIdentifier(insertColumns[i].Name));
            }

            builder.Append(") AS durable_source ON ")
                .Append(string.Join(" AND ", conflictColumns.Select(c => "durable_target." + QuoteIdentifier(c.Name) + " = durable_source." + QuoteIdentifier(c.Name))));
            if (updateColumns.Count > 0)
            {
                builder.Append(" WHEN MATCHED THEN UPDATE SET ")
                    .Append(string.Join(", ", updateColumns.Select(c => "durable_target." + QuoteIdentifier(c.Name) + " = " + (c.IsVersion && versionPlaceholder != null ? versionPlaceholder : "durable_source." + QuoteIdentifier(c.Name)))));
            }

            builder.Append(" WHEN NOT MATCHED THEN INSERT (")
                .Append(string.Join(", ", insertColumns.Select(c => QuoteIdentifier(c.Name))))
                .Append(") VALUES (")
                .Append(string.Join(", ", insertColumns.Select(c => "durable_source." + QuoteIdentifier(c.Name))))
                .Append(");");
            if (identityInsert) builder.Append(" SET IDENTITY_INSERT ").Append(table).Append(" OFF;");
        }

        /// <inheritdoc />
        public override string CreateSavepointSql(string name)
        {
            return "SAVE TRANSACTION " + QuoteIdentifier(name);
        }

        /// <inheritdoc />
        public override string RollbackToSavepointSql(string name)
        {
            return "ROLLBACK TRANSACTION " + QuoteIdentifier(name);
        }

        /// <inheritdoc />
        public override string? ReleaseSavepointSql(string name)
        {
            return null;
        }

        /// <inheritdoc />
        public override string GetColumnType(ColumnMetadata column)
        {
            ArgumentNullException.ThrowIfNull(column);
            Type type = column.Converter?.ProviderType ?? column.ClrType;
            type = Nullable.GetUnderlyingType(type) ?? type;
            if (column.IsJson) return "NVARCHAR(MAX)";
            if (type.IsEnum) return column.EnumAsString ? StringType(column, "NVARCHAR", "NVARCHAR(64)", 64) : "INT";
            if (type == typeof(bool)) return "BIT";
            if (type == typeof(byte)) return "TINYINT";
            if (type == typeof(sbyte) || type == typeof(short)) return "SMALLINT";
            if (type == typeof(ushort) || type == typeof(int)) return "INT";
            if (type == typeof(uint) || type == typeof(long)) return "BIGINT";
            if (type == typeof(ulong)) return "DECIMAL(20, 0)";
            if (type == typeof(float)) return "REAL";
            if (type == typeof(double)) return "FLOAT";
            if (type == typeof(decimal)) return "DECIMAL(38, 10)";
            if (type == typeof(DateTime)) return "DATETIME2";
            if (type == typeof(DateTimeOffset)) return "DATETIMEOFFSET";
            if (type == typeof(DateOnly)) return "DATE";
            if (type == typeof(TimeOnly)) return "TIME";
            if (type == typeof(TimeSpan)) return "BIGINT";
            if (type == typeof(Guid)) return "UNIQUEIDENTIFIER";
            if (type == typeof(byte[])) return "VARBINARY(MAX)";
            if (type == typeof(char)) return "NCHAR(1)";
            return StringType(column, "NVARCHAR", "NVARCHAR(MAX)", 450);
        }

        /// <inheritdoc />
        public override void AppendCreateTable(SqlStatementBuilder builder, EntityMetadata metadata)
        {
            ArgumentNullException.ThrowIfNull(builder);
            ArgumentNullException.ThrowIfNull(metadata);
            builder.Append("IF OBJECT_ID(").AppendParameter(QuoteIdentifier(metadata.TableName)).Append(", N'U') IS NULL CREATE TABLE ")
                .AppendIdentifier(metadata.TableName).Append(" (");
            AppendTableBody(builder, metadata);
            builder.Append(")");
        }

        /// <inheritdoc />
        public override SqlStatement TableExistsQuery(string tableName)
        {
            return new SqlStatement(
                "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = SCHEMA_NAME() AND TABLE_NAME = @p0",
                new[] { new SqlParameterValue("@p0", tableName) });
        }

        /// <inheritdoc />
        public override SqlStatement ColumnNamesQuery(string tableName)
        {
            return new SqlStatement(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = SCHEMA_NAME() AND TABLE_NAME = @p0 ORDER BY ORDINAL_POSITION",
                new[] { new SqlParameterValue("@p0", tableName) });
        }

        /// <inheritdoc />
        public override SqlStatement IndexNamesQuery(string tableName)
        {
            return new SqlStatement(
                "SELECT i.name FROM sys.indexes i WHERE i.object_id = OBJECT_ID(QUOTENAME(@p0)) AND i.is_primary_key = 0 AND i.name IS NOT NULL",
                new[] { new SqlParameterValue("@p0", tableName) });
        }

        /// <inheritdoc />
        public override string CreateIndexSql(string indexName, string tableName, IReadOnlyList<string> columns, bool unique, IReadOnlyList<string>? includedColumns)
        {
            string sql = base.CreateIndexSql(indexName, tableName, columns, unique, null);
            if (includedColumns != null && includedColumns.Count > 0)
                sql += " INCLUDE (" + string.Join(", ", includedColumns.Select(QuoteIdentifier)) + ")";
            return sql;
        }

        /// <inheritdoc />
        public override string DropIndexSql(string indexName, string? tableName)
        {
            if (tableName == null) throw new ArgumentNullException(nameof(tableName), "SQL Server requires the table name to drop an index.");
            return "DROP INDEX " + QuoteIdentifier(indexName) + " ON " + QuoteIdentifier(tableName);
        }

        // Migrations

        /// <inheritdoc />
        public override SqlStatement ColumnSchemaQuery(string tableName)
        {
            return new SqlStatement(
                "SELECT c.COLUMN_NAME, " +
                "CASE WHEN c.CHARACTER_MAXIMUM_LENGTH = -1 THEN c.DATA_TYPE + '(max)' " +
                "WHEN c.CHARACTER_MAXIMUM_LENGTH IS NOT NULL AND c.DATA_TYPE NOT IN ('text', 'ntext', 'image', 'xml') THEN c.DATA_TYPE + '(' + CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR(10)) + ')' " +
                "WHEN c.DATA_TYPE IN ('decimal', 'numeric') THEN c.DATA_TYPE + '(' + CAST(c.NUMERIC_PRECISION AS VARCHAR(10)) + ',' + CAST(c.NUMERIC_SCALE AS VARCHAR(10)) + ')' " +
                "ELSE c.DATA_TYPE END, " +
                "CASE WHEN c.IS_NULLABLE = 'YES' THEN 1 ELSE 0 END, c.CHARACTER_MAXIMUM_LENGTH, " +
                "CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k " +
                "ON k.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND k.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA AND k.TABLE_NAME = tc.TABLE_NAME " +
                "WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND tc.TABLE_SCHEMA = c.TABLE_SCHEMA AND tc.TABLE_NAME = c.TABLE_NAME AND k.COLUMN_NAME = c.COLUMN_NAME) THEN 1 ELSE 0 END " +
                "FROM INFORMATION_SCHEMA.COLUMNS c WHERE c.TABLE_SCHEMA = SCHEMA_NAME() AND c.TABLE_NAME = @p0 ORDER BY c.ORDINAL_POSITION",
                new[] { new SqlParameterValue("@p0", tableName) });
        }

        /// <inheritdoc />
        public override SqlStatement IndexSchemaQuery(string tableName)
        {
            return new SqlStatement(
                "SELECT i.name, c.name, CAST(i.is_unique AS INT), " +
                "CASE WHEN ic.is_included_column = 1 THEN 1000 + ic.index_column_id ELSE ic.key_ordinal END, CAST(ic.is_included_column AS INT) " +
                "FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id " +
                "JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id " +
                "WHERE i.object_id = OBJECT_ID(QUOTENAME(SCHEMA_NAME()) + '.' + QUOTENAME(@p0)) AND i.is_primary_key = 0 " +
                "AND i.is_unique_constraint = 0 AND i.name IS NOT NULL " +
                "ORDER BY i.name, CASE WHEN ic.is_included_column = 1 THEN 1000 + ic.index_column_id ELSE ic.key_ordinal END",
                new[] { new SqlParameterValue("@p0", tableName) });
        }

        /// <inheritdoc />
        public override string NormalizeColumnType(string columnType)
        {
            string type = base.NormalizeColumnType(columnType);
            int paren = type.IndexOf('(');
            string name = paren < 0 ? type : type.Substring(0, paren);
            string suffix = paren < 0 ? string.Empty : type.Substring(paren);
            switch (name)
            {
                case "integer": name = "int"; break;
                case "dec":
                case "numeric": name = "decimal"; break;
                case "double precision": name = "float"; break;
                case "rowversion": name = "timestamp"; break;
                case "national character varying":
                case "national char varying": name = "nvarchar"; break;
                case "national character":
                case "national char": name = "nchar"; break;
            }

            if (name == "float" && suffix == "(53)") suffix = string.Empty;
            return name + suffix;
        }

        /// <inheritdoc />
        public override string DropColumnSql(string tableName, string columnName)
        {
            ArgumentNullException.ThrowIfNull(tableName);
            ArgumentNullException.ThrowIfNull(columnName);
            string table = QuoteIdentifier(tableName);
            return "DECLARE @durable_default SYSNAME; DECLARE @durable_sql NVARCHAR(MAX); " +
                "SELECT @durable_default = d.name FROM sys.default_constraints d JOIN sys.columns c " +
                "ON c.object_id = d.parent_object_id AND c.column_id = d.parent_column_id " +
                "WHERE d.parent_object_id = OBJECT_ID(" + FormatLiteral(table) + ") AND c.name = " + FormatLiteral(columnName) + "; " +
                "IF @durable_default IS NOT NULL BEGIN SET @durable_sql = " + FormatLiteral("ALTER TABLE " + table + " DROP CONSTRAINT ") +
                " + QUOTENAME(@durable_default); EXEC sp_executesql @durable_sql; END; " +
                "ALTER TABLE " + table + " DROP COLUMN " + QuoteIdentifier(columnName);
        }

        /// <inheritdoc />
        public override string CreateMigrationHistoryTableSql(string tableName)
        {
            ArgumentNullException.ThrowIfNull(tableName);
            string table = QuoteIdentifier(tableName);
            return "IF OBJECT_ID(" + FormatLiteral(table) + ", N'U') IS NULL CREATE TABLE " + table + " ("
                + QuoteIdentifier("id") + " NVARCHAR(150) NOT NULL PRIMARY KEY, "
                + QuoteIdentifier("description") + " NVARCHAR(1000) NULL, "
                + QuoteIdentifier("applied_utc") + " DATETIME2 NOT NULL, "
                + QuoteIdentifier("duration_ms") + " BIGINT NOT NULL)";
        }

        /// <inheritdoc />
        public override SqlStatement? AcquireMigrationLockSql(string lockName, int waitSeconds)
        {
            ArgumentNullException.ThrowIfNull(lockName);
            return new SqlStatement(
                "DECLARE @durable_lock INT; " +
                "EXEC @durable_lock = sp_getapplock @Resource = @p0, @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = @p1; " +
                "SELECT CASE WHEN @durable_lock >= 0 THEN 1 ELSE 0 END",
                new[] { new SqlParameterValue("@p0", lockName), new SqlParameterValue("@p1", Math.Max(0, waitSeconds) * 1000) });
        }

        /// <inheritdoc />
        public override SqlStatement? ReleaseMigrationLockSql(string lockName)
        {
            ArgumentNullException.ThrowIfNull(lockName);
            return new SqlStatement(
                "EXEC sp_releaseapplock @Resource = @p0, @LockOwner = 'Session'",
                new[] { new SqlParameterValue("@p0", lockName) });
        }

        #endregion

        #region Private-Methods

        /// <inheritdoc />
        protected override string IdentifierOpen => "[";

        /// <inheritdoc />
        protected override string IdentifierClose => "]";

        /// <inheritdoc />
        protected override bool IsAdditionalLikeWildcard(char c)
        {
            return c == '[';
        }

        /// <inheritdoc />
        protected override string AutoIncrementColumnType(ColumnMetadata column, bool inlinePrimaryKey)
        {
            Type type = column.ClrType;
            string baseType = type == typeof(long) ? "BIGINT" : type == typeof(short) ? "SMALLINT" : "INT";
            return baseType + " IDENTITY(1,1) NOT NULL" + (inlinePrimaryKey ? " PRIMARY KEY" : string.Empty);
        }

        // Migrations

        /// <summary>
        /// Gets "ADD": SQL Server's ALTER TABLE does not use the COLUMN keyword when adding columns.
        /// </summary>
        protected override string AddColumnKeyword => "ADD";

        /// <summary>
        /// Renders a Unicode string literal (N'...').
        /// </summary>
        /// <param name="value">Text. Must not be null.</param>
        /// <returns>The literal.</returns>
        protected override string StringLiteral(string value)
        {
            return "N" + base.StringLiteral(value);
        }

        /// <summary>
        /// Renders a binary literal (0x...).
        /// </summary>
        /// <param name="value">Bytes. Must not be null.</param>
        /// <returns>The literal.</returns>
        protected override string BinaryLiteral(byte[] value)
        {
            return "0x" + Convert.ToHexString(value);
        }

        #endregion
    }
}
