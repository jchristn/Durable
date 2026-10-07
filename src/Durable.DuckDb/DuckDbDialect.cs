namespace Durable.DuckDb
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Globalization;
    using System.Linq;
    using System.Numerics;
    using System.Text;
    using System.Text.RegularExpressions;
    using DuckDB.NET.Data;
    using Durable;
    using Durable.Query;
    using Durable.Sql;

    /// <summary>
    /// DuckDB dialect (DuckDB 1.1+, as bundled with DuckDB.NET.Data.Full). Tables are resolved in <c>current_database()</c>
    /// and <c>current_schema()</c> unless the name is schema-qualified. Notable differences from PostgreSQL, which DuckDB's
    /// SQL otherwise follows: auto-increment keys use a sequence per column (<c>DEFAULT nextval(...)</c>, because DuckDB has
    /// no identity columns), savepoints are not supported (<see cref="SupportsSavepoints"/> is false), integer division
    /// uses <c>//</c>, and the migration lock is a row in <see cref="MigrationLockTableName"/> (DuckDB has no advisory
    /// locks; see <see cref="AcquireMigrationLockSql"/>).
    /// Thread safety: immutable; safe to share.
    /// </summary>
    public class DuckDbDialect : SqlDialect
    {
        #region Public-Members

        /// <summary>
        /// Gets the shared default instance.
        /// </summary>
        public static DuckDbDialect Default { get; } = new DuckDbDialect();

        /// <inheritdoc />
        public override RepositoryType RepositoryType => RepositoryType.DuckDb;

        /// <inheritdoc />
        public override string DbSystemName => "duckdb";

        /// <inheritdoc />
        public override int MaxParameters => 10000;

        /// <inheritdoc />
        /// <remarks>DuckDB has macros but no stored procedures.</remarks>
        public override bool SupportsStoredProcedures => false;

        /// <inheritdoc />
        /// <remarks>DuckDB (through 1.5) does not support SAVEPOINT.</remarks>
        public override bool SupportsSavepoints => false;

        /// <inheritdoc />
        public override string StringCastType => "VARCHAR";

        /// <inheritdoc />
        /// <remarks>DuckDB.NET ignores <see cref="DbCommand.CommandTimeout"/>; the engine cancels commands that exceed <see cref="SqlRepositoryOptions.CommandTimeoutSeconds"/>.</remarks>
        public override bool DriverEnforcesCommandTimeout => false;

        /// <inheritdoc />
        /// <remarks>
        /// DuckDB fails an UPDATE or DELETE of a row that a concurrent transaction changed ("Conflict on update") instead of
        /// waiting. Statements outside a transaction are run again up to this many times, so concurrent autocommit writes to
        /// the same rows behave as on other databases. Writes inside a transaction still fail with the conflict. Default: 10.
        /// </remarks>
        public override int AutocommitConflictRetries { get; }

        /// <summary>
        /// Gets the collation applied by <see cref="OrdinalCollation"/> for ordinal and ignore-case string matching.
        /// Default: binary. DuckDB compares strings by byte (code point) order unless a default collation such as
        /// <c>nocase</c> is configured; the explicit collation keeps ordinal semantics in that case too.
        /// </summary>
        public string OrdinalCollationName { get; }

        /// <summary>
        /// Gets the name of the table holding migration locks (one row per held lock; see <see cref="AcquireMigrationLockSql"/>).
        /// Default: durable_migration_lock. Created on first use and excluded from <see cref="TableNamesQuery"/>.
        /// </summary>
        public string MigrationLockTableName { get; }

        // Migrations

        /// <inheritdoc />
        /// <remarks>
        /// Reported as false although DuckDB can roll back DDL: DuckDB cannot create an index in a transaction that has
        /// already changed rows of the table (for example by adding a column with a default) and cannot drop a column in
        /// the transaction that dropped its index, which schema synchronization and typical migrations need. Durable
        /// therefore applies migrations and schema changes statement by statement (each commits on its own), as on MySQL;
        /// a failed migration may leave its earlier statements applied.
        /// </remarks>
        public override bool SupportsTransactionalDdl => false;

        /// <inheritdoc />
        /// <remarks>DuckDB accepts VARCHAR(n) but neither stores nor enforces the length.</remarks>
        public override bool SupportsStringMaxLength => false;

        /// <inheritdoc />
        /// <remarks>DuckDB reports a dependency error for DROP COLUMN, SET NOT NULL and type changes on a table with indexes.</remarks>
        public override bool AlterTableRequiresDroppingIndexes => true;

        /// <inheritdoc />
        public override string CurrentUtcTimestampSql => "CAST(timezone('UTC', current_timestamp) AS TIMESTAMP)";

        #endregion

        #region Private-Members

        // Identifies this process in migration lock rows. A DuckDB database file can be opened read-write by one process
        // at a time, so a lock row written by any other process id is stale (left behind by a process that exited).
        private static readonly string _ProcessLockOwner = Guid.NewGuid().ToString("N");

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the dialect.
        /// </summary>
        /// <param name="converter">Converter; null uses <see cref="DuckDbDataTypeConverter"/>.</param>
        /// <param name="ordinalCollation">Collation for ordinal string matching. Default: binary.</param>
        /// <param name="migrationLockTableName">Table holding migration locks. Default: durable_migration_lock.</param>
        /// <param name="autocommitConflictRetries">How often a conflicting statement outside a transaction is run again (see <see cref="AutocommitConflictRetries"/>). Default: 10. Minimum: 0.</param>
        /// <exception cref="ArgumentException">Thrown when ordinalCollation or migrationLockTableName is not a simple identifier.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when autocommitConflictRetries is negative.</exception>
        public DuckDbDialect(IDataTypeConverter? converter = null, string ordinalCollation = "binary", string migrationLockTableName = "durable_migration_lock", int autocommitConflictRetries = 10)
            : base(converter ?? new DuckDbDataTypeConverter())
        {
            if (autocommitConflictRetries < 0) throw new ArgumentOutOfRangeException(nameof(autocommitConflictRetries), "autocommitConflictRetries cannot be negative.");
            OrdinalCollationName = SqlIdentifierValidator.RequireIdentifier(ordinalCollation, nameof(ordinalCollation));
            MigrationLockTableName = SqlIdentifierValidator.RequireIdentifier(migrationLockTableName, nameof(migrationLockTableName));
            AutocommitConflictRetries = autocommitConflictRetries;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        /// <remarks>DuckDB binds named parameters written as <c>$name</c>.</remarks>
        public override string FormatParameterName(int index)
        {
            return "$p" + index.ToString(CultureInfo.InvariantCulture);
        }

        /// <inheritdoc />
        /// <remarks>DuckDB.NET matches parameters by name without the <c>$</c> prefix.</remarks>
        public override void ConfigureParameter(DbParameter parameter, SqlParameterValue value)
        {
            ArgumentNullException.ThrowIfNull(parameter);
            ArgumentNullException.ThrowIfNull(value);
            string? name = parameter.ParameterName;
            if (!string.IsNullOrEmpty(name) && name[0] == '$') parameter.ParameterName = name.Substring(1);
        }

        /// <inheritdoc />
        /// <remarks>DuckDB sorts NULLs last by default; NULLS FIRST / NULLS LAST restore LINQ ordering.</remarks>
        public override string OrderDirection(bool descending)
        {
            return descending ? " DESC NULLS LAST" : " ASC NULLS FIRST";
        }

        /// <inheritdoc />
        /// <remarks>DuckDB's <c>/</c> always returns DOUBLE; <c>//</c> is integer division truncating toward zero, like C#.</remarks>
        public override string Divide(string left, string right, bool integerOperands)
        {
            return integerOperands ? "(" + left + " // " + right + ")" : "(" + left + " / " + right + ")";
        }

        /// <inheritdoc />
        public override string OrdinalCollation(string expression)
        {
            return expression + " COLLATE \"" + OrdinalCollationName + "\"";
        }

        /// <inheritdoc />
        public override string TranslateFunction(QueryFunction function, IReadOnlyList<string> arguments)
        {
            string a0 = arguments.Count > 0 ? arguments[0] : string.Empty;
            string a1 = arguments.Count > 1 ? arguments[1] : string.Empty;
            switch (function)
            {
                case QueryFunction.IndexOf: return "(STRPOS(" + a0 + ", " + a1 + ") - 1)";
                case QueryFunction.Year: return "CAST(EXTRACT(YEAR FROM " + a0 + ") AS INTEGER)";
                case QueryFunction.Month: return "CAST(EXTRACT(MONTH FROM " + a0 + ") AS INTEGER)";
                case QueryFunction.Day: return "CAST(EXTRACT(DAY FROM " + a0 + ") AS INTEGER)";
                case QueryFunction.Hour: return "CAST(EXTRACT(HOUR FROM " + a0 + ") AS INTEGER)";
                case QueryFunction.Minute: return "CAST(EXTRACT(MINUTE FROM " + a0 + ") AS INTEGER)";
                case QueryFunction.Second: return "CAST(EXTRACT(SECOND FROM " + a0 + ") AS INTEGER)";
                case QueryFunction.DayOfYear: return "CAST(EXTRACT(DOY FROM " + a0 + ") AS INTEGER)";
                case QueryFunction.DayOfWeek: return "CAST(EXTRACT(DOW FROM " + a0 + ") AS INTEGER)";
                case QueryFunction.Date: return "CAST(DATE_TRUNC('day', " + a0 + ") AS TIMESTAMP)";
                case QueryFunction.AddYears: return "(" + a0 + " + to_years(CAST(" + a1 + " AS INTEGER)))";
                case QueryFunction.AddMonths: return "(" + a0 + " + to_months(CAST(" + a1 + " AS INTEGER)))";
                case QueryFunction.AddDays: return "(" + a0 + " + to_days(CAST(" + a1 + " AS INTEGER)))";
                case QueryFunction.AddHours: return "(" + a0 + " + to_microseconds(CAST(ROUND((" + a1 + ") * 3600000000) AS BIGINT)))";
                case QueryFunction.AddMinutes: return "(" + a0 + " + to_microseconds(CAST(ROUND((" + a1 + ") * 60000000) AS BIGINT)))";
                case QueryFunction.AddSeconds: return "(" + a0 + " + to_microseconds(CAST(ROUND((" + a1 + ") * 1000000) AS BIGINT)))";
                default:
                    return base.TranslateFunction(function, arguments);
            }
        }

        /// <inheritdoc />
        /// <remarks>
        /// DuckDB ignores declared string lengths, so strings are always <c>VARCHAR</c> (MaxLength is not enforced by the
        /// database). Unsigned integers use DuckDB's native unsigned types, <see cref="BigInteger"/> (for example from a
        /// value converter) maps to <c>HUGEINT</c>, and JSON columns use the <c>JSON</c> type.
        /// </remarks>
        public override string GetColumnType(ColumnMetadata column)
        {
            ArgumentNullException.ThrowIfNull(column);
            Type type = column.Converter?.ProviderType ?? column.ClrType;
            type = Nullable.GetUnderlyingType(type) ?? type;
            if (column.IsJson) return "JSON";
            if (type.IsEnum) return column.EnumAsString ? "VARCHAR" : "INTEGER";
            if (type == typeof(bool)) return "BOOLEAN";
            if (type == typeof(sbyte)) return "TINYINT";
            if (type == typeof(byte)) return "UTINYINT";
            if (type == typeof(short)) return "SMALLINT";
            if (type == typeof(ushort)) return "USMALLINT";
            if (type == typeof(int)) return "INTEGER";
            if (type == typeof(uint)) return "UINTEGER";
            if (type == typeof(long)) return "BIGINT";
            if (type == typeof(ulong)) return "UBIGINT";
            if (type == typeof(BigInteger)) return "HUGEINT";
            if (type == typeof(float)) return "FLOAT";
            if (type == typeof(double)) return "DOUBLE";
            if (type == typeof(decimal)) return "DECIMAL(38,10)";
            if (type == typeof(DateTime)) return "TIMESTAMP";
            if (type == typeof(DateTimeOffset)) return "TIMESTAMPTZ";
            if (type == typeof(DateOnly)) return "DATE";
            if (type == typeof(TimeOnly)) return "TIME";
            if (type == typeof(TimeSpan)) return "INTERVAL";
            if (type == typeof(Guid)) return "UUID";
            if (type == typeof(byte[])) return "BLOB";
            return "VARCHAR";
        }

        /// <inheritdoc />
        /// <remarks>
        /// Each auto-increment column gets a sequence named <c>{table}_{column}_seq</c> (in the table's schema), and the
        /// column defaults to <c>nextval</c> of it. DuckDB does not drop a sequence with the table that uses it, so the
        /// statement first drops a sequence of that name left behind by an earlier, dropped table and recreates it: a
        /// recreated table numbers its rows from 1 again, as on the other databases. The engine emits this statement only
        /// for tables that do not exist; when the table does exist, DuckDB refuses to drop the sequence it depends on and
        /// the statement fails instead of being a no-op.
        /// </remarks>
        public override void AppendCreateTable(SqlStatementBuilder builder, EntityMetadata metadata)
        {
            ArgumentNullException.ThrowIfNull(builder);
            ArgumentNullException.ThrowIfNull(metadata);
            foreach (ColumnMetadata column in metadata.Columns)
            {
                if (!column.IsAutoIncrement) continue;
                string sequence = QuoteIdentifier(SequenceName(metadata.TableName, column.Name));
                builder.Append("DROP SEQUENCE IF EXISTS ").Append(sequence).Append(StatementSeparator).Append(" ");
                builder.Append("CREATE SEQUENCE ").Append(sequence).Append(StatementSeparator).Append(" ");
            }

            builder.Append("CREATE TABLE IF NOT EXISTS ").AppendIdentifier(metadata.TableName).Append(" (");
            for (int i = 0; i < metadata.Columns.Count; i++)
            {
                ColumnMetadata column = metadata.Columns[i];
                if (i > 0) builder.Append(", ");
                if (column.IsAutoIncrement)
                {
                    builder.Append(QuoteIdentifier(column.Name)).Append(" ").Append(AutoIncrementBaseType(column))
                        .Append(" NOT NULL DEFAULT nextval(").Append(StringLiteral(QuoteIdentifier(SequenceName(metadata.TableName, column.Name)))).Append(")");
                }
                else
                {
                    builder.Append(ColumnDefinition(column, false));
                }
            }

            if (metadata.KeyColumns.Count > 0)
            {
                builder.Append(", PRIMARY KEY (")
                    .Append(string.Join(", ", metadata.KeyColumns.Select(c => QuoteIdentifier(c.Name))))
                    .Append(")");
            }

            builder.Append(")");
        }

        /// <inheritdoc />
        public override SqlStatement TableExistsQuery(string tableName)
        {
            ArgumentNullException.ThrowIfNull(tableName);
            return new SqlStatement(
                "SELECT 1 FROM information_schema.tables WHERE table_catalog = current_database() AND table_schema = " + SchemaExpression(tableName) + " AND table_name = $p0",
                TableParameters(tableName));
        }

        /// <inheritdoc />
        public override SqlStatement ColumnNamesQuery(string tableName)
        {
            ArgumentNullException.ThrowIfNull(tableName);
            return new SqlStatement(
                "SELECT column_name FROM information_schema.columns WHERE table_catalog = current_database() AND table_schema = " + SchemaExpression(tableName) + " AND table_name = $p0 ORDER BY ordinal_position",
                TableParameters(tableName));
        }

        /// <inheritdoc />
        public override SqlStatement IndexNamesQuery(string tableName)
        {
            ArgumentNullException.ThrowIfNull(tableName);
            return new SqlStatement(
                "SELECT index_name FROM duckdb_indexes() WHERE database_name = current_database() AND schema_name = " + SchemaExpression(tableName) + " AND table_name = $p0 AND NOT is_primary ORDER BY index_name",
                TableParameters(tableName));
        }

        /// <inheritdoc />
        /// <remarks>DuckDB has no covering (INCLUDE) indexes; included columns are ignored.</remarks>
        public override string CreateIndexSql(string indexName, string tableName, IReadOnlyList<string> columns, bool unique, IReadOnlyList<string>? includedColumns)
        {
            ArgumentNullException.ThrowIfNull(indexName);
            ArgumentNullException.ThrowIfNull(tableName);
            ArgumentNullException.ThrowIfNull(columns);
            return base.CreateIndexSql(indexName, tableName, columns, unique, null);
        }

        /// <inheritdoc />
        /// <remarks>Index names are schema-scoped; the index is qualified with the table's schema when the table name is qualified.</remarks>
        public override string DropIndexSql(string indexName, string? tableName)
        {
            ArgumentNullException.ThrowIfNull(indexName);
            string? schema = tableName != null ? SchemaOf(tableName) : null;
            return "DROP INDEX " + (schema != null ? QuoteIdentifier(schema) + "." : string.Empty) + QuoteIdentifier(indexName);
        }

        // Transactions

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Always: DuckDB does not support savepoints.</exception>
        public override string CreateSavepointSql(string name)
        {
            throw new NotSupportedException("DuckDB does not support savepoints.");
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Always: DuckDB does not support savepoints.</exception>
        public override string RollbackToSavepointSql(string name)
        {
            throw new NotSupportedException("DuckDB does not support savepoints.");
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Always: DuckDB does not support savepoints.</exception>
        public override string? ReleaseSavepointSql(string name)
        {
            throw new NotSupportedException("DuckDB does not support savepoints.");
        }

        // Migrations

        /// <inheritdoc />
        /// <remarks>
        /// Reads <c>duckdb_columns()</c> and <c>duckdb_constraints()</c>. DuckDB reports no character lengths (VARCHAR
        /// lengths are not stored), so column 3 is always null; a column whose default is <c>nextval(...)</c> is reported as
        /// database-generated.
        /// </remarks>
        public override SqlStatement ColumnSchemaQuery(string tableName)
        {
            ArgumentNullException.ThrowIfNull(tableName);
            string schema = SchemaExpression(tableName);
            return new SqlStatement(
                "SELECT c.column_name, c.data_type, CASE WHEN c.is_nullable THEN 1 ELSE 0 END, CAST(NULL AS INTEGER), " +
                "CASE WHEN EXISTS (SELECT 1 FROM duckdb_constraints() k WHERE k.database_name = c.database_name AND k.schema_name = c.schema_name " +
                "AND k.table_name = c.table_name AND k.constraint_type = 'PRIMARY KEY' AND list_contains(k.constraint_column_names, c.column_name)) THEN 1 ELSE 0 END, " +
                "CASE WHEN c.column_default LIKE 'nextval(%' THEN 1 ELSE 0 END " +
                "FROM duckdb_columns() c WHERE c.database_name = current_database() AND c.schema_name = " + schema + " AND c.table_name = $p0 " +
                "ORDER BY c.column_index",
                TableParameters(tableName));
        }

        /// <inheritdoc />
        public override SqlStatement TableNamesQuery()
        {
            return new SqlStatement(
                "SELECT table_name FROM information_schema.tables WHERE table_catalog = current_database() AND table_schema = current_schema() " +
                "AND table_type = 'BASE TABLE' AND table_name <> $p0 ORDER BY table_name",
                new[] { new SqlParameterValue(FormatParameterName(0), MigrationLockTableName) });
        }

        /// <inheritdoc />
        /// <remarks>DuckDB indexes have no included columns; column 4 is always 0.</remarks>
        public override SqlStatement IndexSchemaQuery(string tableName)
        {
            ArgumentNullException.ThrowIfNull(tableName);
            return new SqlStatement(
                "SELECT index_name, trim(expression, '\"'), is_unique, position, 0 FROM (" +
                "SELECT index_name, CASE WHEN is_unique THEN 1 ELSE 0 END AS is_unique, " +
                "unnest(CAST(expressions AS VARCHAR[])) AS expression, generate_subscripts(CAST(expressions AS VARCHAR[]), 1) AS position " +
                "FROM duckdb_indexes() WHERE database_name = current_database() AND schema_name = " + SchemaExpression(tableName) + " AND table_name = $p0 AND NOT is_primary) " +
                "ORDER BY index_name, position",
                TableParameters(tableName));
        }

        /// <inheritdoc />
        public override string NormalizeColumnType(string columnType)
        {
            string type = base.NormalizeColumnType(columnType);
            switch (type)
            {
                case "timestamp with time zone": return "timestamptz";
                case "timestamp without time zone":
                case "datetime": return "timestamp";
                case "time without time zone": return "time";
                case "double precision": return "double";
            }

            int paren = type.IndexOf('(');
            string name = paren < 0 ? type : type.Substring(0, paren);
            string suffix = paren < 0 ? string.Empty : type.Substring(paren);
            switch (name)
            {
                case "varchar":
                case "char":
                case "bpchar":
                case "text":
                case "string":
                case "nvarchar":
                    return "varchar";
                case "int":
                case "int4":
                case "signed":
                case "integer": name = "integer"; break;
                case "int8":
                case "long": name = "bigint"; break;
                case "int2":
                case "short": name = "smallint"; break;
                case "int1": name = "tinyint"; break;
                case "int128": name = "hugeint"; break;
                case "float4":
                case "real": name = "float"; break;
                case "float8": name = "double"; break;
                case "bool":
                case "logical": name = "boolean"; break;
                case "bytea":
                case "binary":
                case "varbinary": name = "blob"; break;
                case "numeric": name = "decimal"; break;
            }

            return name + suffix;
        }

        /// <inheritdoc />
        /// <remarks>DuckDB cannot add a column with a constraint, so a NOT NULL column is added with its default and then altered.</remarks>
        public override string AddColumnSql(string tableName, ColumnMetadata column, bool nullable, string? defaultLiteral)
        {
            ArgumentNullException.ThrowIfNull(tableName);
            ArgumentNullException.ThrowIfNull(column);
            StringBuilder sb = new StringBuilder();
            sb.Append("ALTER TABLE ").Append(QuoteIdentifier(tableName)).Append(" ADD COLUMN ")
                .Append(QuoteIdentifier(column.Name)).Append(' ').Append(GetColumnType(column));
            if (defaultLiteral != null) sb.Append(" DEFAULT ").Append(defaultLiteral);
            if (!nullable)
            {
                sb.Append(StatementSeparator).Append(" ALTER TABLE ").Append(QuoteIdentifier(tableName))
                    .Append(" ALTER COLUMN ").Append(QuoteIdentifier(column.Name)).Append(" SET NOT NULL");
            }

            return sb.ToString();
        }

        /// <inheritdoc />
        /// <remarks>
        /// DuckDB has no advisory or session locks. The lock is a row in <see cref="MigrationLockTableName"/> (created on
        /// first use): the statement inserts the row and yields 1, or yields no row when another session holds the lock.
        /// Concurrent attempts that DuckDB reports as write-write conflicts or key violations count as "not acquired"
        /// (<see cref="IsMigrationLockContention"/>). A DuckDB file is opened read-write by one process at a time, so rows
        /// left by another process (one that exited without releasing) are stale and removed first. waitSeconds is not
        /// used: the migrator polls.
        /// </remarks>
        public override SqlStatement? AcquireMigrationLockSql(string lockName, int waitSeconds)
        {
            ArgumentNullException.ThrowIfNull(lockName);
            string table = QuoteIdentifier(MigrationLockTableName);
            return new SqlStatement(
                "CREATE TABLE IF NOT EXISTS " + table + " (" + QuoteIdentifier("name") + " VARCHAR PRIMARY KEY, " + QuoteIdentifier("owner") + " VARCHAR NOT NULL, " +
                QuoteIdentifier("acquired_utc") + " TIMESTAMP NOT NULL)" + StatementSeparator + " " +
                "DELETE FROM " + table + " WHERE " + QuoteIdentifier("name") + " = $p0 AND " + QuoteIdentifier("owner") + " <> $p1" + StatementSeparator + " " +
                "INSERT INTO " + table + " VALUES ($p0, $p1, " + CurrentUtcTimestampSql + ") ON CONFLICT DO NOTHING RETURNING 1",
                new[] { new SqlParameterValue(FormatParameterName(0), lockName), new SqlParameterValue(FormatParameterName(1), _ProcessLockOwner) });
        }

        /// <inheritdoc />
        public override SqlStatement? ReleaseMigrationLockSql(string lockName)
        {
            ArgumentNullException.ThrowIfNull(lockName);
            return new SqlStatement(
                "DELETE FROM " + QuoteIdentifier(MigrationLockTableName) + " WHERE " + QuoteIdentifier("name") + " = $p0",
                new[] { new SqlParameterValue(FormatParameterName(0), lockName) });
        }

        /// <inheritdoc />
        /// <remarks>True for DuckDB write-write conflicts ("Conflict on update", "Conflict on tuple deletion", catalog write-write conflicts).</remarks>
        public override bool IsRetryableConflict(Exception exception)
        {
            ArgumentNullException.ThrowIfNull(exception);
            for (Exception? current = exception; current != null; current = current.InnerException)
            {
                if (current is DuckDBException duck)
                {
                    string message = duck.Message ?? string.Empty;
                    return message.Contains("Conflict on", StringComparison.OrdinalIgnoreCase)
                        || message.Contains("write-write conflict", StringComparison.OrdinalIgnoreCase);
                }
            }

            return false;
        }

        /// <inheritdoc />
        /// <remarks>True for DuckDB transaction conflicts and constraint violations, which is how a concurrent insert of the lock row fails.</remarks>
        public override bool IsMigrationLockContention(Exception exception)
        {
            ArgumentNullException.ThrowIfNull(exception);
            for (Exception? current = exception; current != null; current = current.InnerException)
            {
                if (current is DuckDBException duck)
                {
                    string message = duck.Message ?? string.Empty;
                    return message.Contains("conflict", StringComparison.OrdinalIgnoreCase)
                        || message.Contains("Constraint Error", StringComparison.Ordinal)
                        || message.Contains("constraint violation", StringComparison.OrdinalIgnoreCase);
                }
            }

            return false;
        }

        #endregion

        #region Private-Methods

        /// <inheritdoc />
        protected override string AutoIncrementColumnType(ColumnMetadata column, bool inlinePrimaryKey)
        {
            return AutoIncrementBaseType(column) + " NOT NULL";
        }

        /// <inheritdoc />
        /// <remarks>DuckDB blob literals spell every byte as <c>\xHH</c>.</remarks>
        protected override string BinaryLiteral(byte[] value)
        {
            StringBuilder sb = new StringBuilder(value.Length * 4 + 10);
            sb.Append('\'');
            foreach (byte b in value) sb.Append("\\x").Append(b.ToString("X2", CultureInfo.InvariantCulture));
            return sb.Append("'::BLOB").ToString();
        }

        private static string AutoIncrementBaseType(ColumnMetadata column)
        {
            Type type = Nullable.GetUnderlyingType(column.ClrType) ?? column.ClrType;
            return type == typeof(long) ? "BIGINT" : type == typeof(short) ? "SMALLINT" : "INTEGER";
        }

        private static string SequenceName(string tableName, string columnName)
        {
            return tableName + "_" + columnName + "_seq";
        }

        private static string? SchemaOf(string tableName)
        {
            int dot = tableName.LastIndexOf('.');
            return dot > 0 ? tableName.Substring(0, dot) : null;
        }

        private static string TableOf(string tableName)
        {
            int dot = tableName.LastIndexOf('.');
            return dot >= 0 ? tableName.Substring(dot + 1) : tableName;
        }

        private static string SchemaExpression(string tableName)
        {
            return SchemaOf(tableName) != null ? "$p1" : "current_schema()";
        }

        private SqlParameterValue[] TableParameters(string tableName)
        {
            string? schema = SchemaOf(tableName);
            if (schema == null) return new[] { new SqlParameterValue(FormatParameterName(0), TableOf(tableName)) };
            return new[] { new SqlParameterValue(FormatParameterName(0), TableOf(tableName)), new SqlParameterValue(FormatParameterName(1), schema) };
        }

        #endregion
    }
}
