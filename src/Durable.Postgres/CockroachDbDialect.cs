namespace Durable.Postgres
{
    using System;
    using System.Collections.Generic;
    using Durable;
    using Durable.Query;
    using Durable.Sql;

    /// <summary>
    /// CockroachDB dialect (CockroachDB 24.1+ over the PostgreSQL wire protocol; tested on 26.3). Differences from
    /// <see cref="PostgresDialect"/>: 32-bit integers are declared <c>INT4</c> (CockroachDB's <c>INTEGER</c> is 64-bit),
    /// integer division uses <c>div()</c> (CockroachDB's <c>/</c> returns DECIMAL), floating-point functions cast their
    /// argument to <c>FLOAT8</c>, and the migration lock is a row in a lock table (CockroachDB accepts
    /// <c>pg_try_advisory_lock</c> but does not lock).
    /// Thread safety: immutable; safe to share.
    /// </summary>
    public class CockroachDbDialect : PostgresDialect
    {
        #region Public-Members

        /// <summary>
        /// Gets the shared default instance.
        /// </summary>
        public static new CockroachDbDialect Default { get; } = new CockroachDbDialect();

        /// <inheritdoc />
        public override PostgresFlavor Flavor => PostgresFlavor.CockroachDb;

        /// <inheritdoc />
        public override string DbSystemName => "cockroachdb";

        /// <summary>
        /// Gets false: CockroachDB commits an open transaction before a schema change (the <c>autocommit_before_ddl</c>
        /// session default), so migrations and schema synchronization run without a transaction and a failed migration may
        /// leave earlier statements applied.
        /// </summary>
        public override bool SupportsTransactionalDdl => false;

        /// <summary>
        /// Gets false: CockroachDB's CALL accepts positional arguments only, so procedure parameters are sent in order.
        /// </summary>
        public override bool SupportsNamedProcedureArguments => false;

        /// <summary>
        /// Gets the table holding migration locks (one row per held lock; created on first use). Default: durable_migration_locks.
        /// </summary>
        public string MigrationLockTableName { get; }

        /// <summary>
        /// Gets the age in seconds after which a migration lock row left behind by a crashed migrator may be taken over.
        /// Default: 900. Minimum: 1. Must exceed the longest expected migration run.
        /// </summary>
        public int MigrationLockExpirySeconds { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the dialect.
        /// </summary>
        /// <param name="converter">Converter; null uses <see cref="PostgresDataTypeConverter"/>.</param>
        /// <param name="ordinalCollation">Binary collation for ordinal string matching. Default: C.</param>
        /// <param name="migrationLockTableName">Table holding migration locks. Default: durable_migration_locks.</param>
        /// <param name="migrationLockExpirySeconds">Seconds after which an abandoned migration lock may be taken over. Default: 900. Minimum: 1.</param>
        /// <exception cref="ArgumentException">Thrown when ordinalCollation or migrationLockTableName is not a simple identifier.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when migrationLockExpirySeconds is less than 1.</exception>
        public CockroachDbDialect(IDataTypeConverter? converter = null, string ordinalCollation = "C", string migrationLockTableName = "durable_migration_locks", int migrationLockExpirySeconds = 900)
            : base(converter, ordinalCollation)
        {
            MigrationLockTableName = SqlIdentifierValidator.RequireIdentifier(migrationLockTableName, nameof(migrationLockTableName));
            if (migrationLockExpirySeconds < 1) throw new ArgumentOutOfRangeException(nameof(migrationLockExpirySeconds), "migrationLockExpirySeconds must be at least 1.");
            MigrationLockExpirySeconds = migrationLockExpirySeconds;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        /// <remarks>CockroachDB evaluates <c>int / int</c> as DECIMAL; <c>div()</c> truncates toward zero like C#.</remarks>
        public override string Divide(string left, string right, bool integerOperands)
        {
            return integerOperands ? "div(" + left + ", " + right + ")" : base.Divide(left, right, integerOperands);
        }

        /// <inheritdoc />
        /// <remarks>CockroachDB has no implicit integer-to-float conversion for SQRT, so the argument is cast to FLOAT8.</remarks>
        public override string TranslateFunction(QueryFunction function, IReadOnlyList<string> arguments)
        {
            ArgumentNullException.ThrowIfNull(arguments);
            if (function == QueryFunction.Sqrt && arguments.Count > 0) return "SQRT(CAST(" + arguments[0] + " AS FLOAT8))";
            return base.TranslateFunction(function, arguments);
        }

        /// <inheritdoc />
        public override string GetColumnType(ColumnMetadata column)
        {
            string type = base.GetColumnType(column);
            return type == "INTEGER" ? "INT4" : type;
        }

        /// <inheritdoc />
        public override SqlStatement? PrepareMigrationLockSql()
        {
            return new SqlStatement("CREATE TABLE IF NOT EXISTS " + QuoteIdentifier(MigrationLockTableName) + " ("
                + QuoteIdentifier("lock_name") + " VARCHAR(64) PRIMARY KEY, "
                + QuoteIdentifier("owner_id") + " VARCHAR(128) NOT NULL, "
                + QuoteIdentifier("acquired_utc") + " TIMESTAMPTZ NOT NULL)");
        }

        /// <inheritdoc />
        /// <remarks>
        /// Inserts the lock row for this session, or takes it over when this session already owns it or it is older than
        /// <see cref="MigrationLockExpirySeconds"/>; returns 1 when the row is now owned by this session.
        /// </remarks>
        public override SqlStatement? AcquireMigrationLockSql(string lockName, int waitSeconds)
        {
            ArgumentNullException.ThrowIfNull(lockName);
            string table = QuoteIdentifier(MigrationLockTableName);
            string owner = QuoteIdentifier("owner_id");
            string acquired = QuoteIdentifier("acquired_utc");
            return new SqlStatement(
                "INSERT INTO " + table + " (" + QuoteIdentifier("lock_name") + ", " + owner + ", " + acquired + ") "
                + "VALUES (@p0, " + SessionIdentitySql + ", now()) "
                + "ON CONFLICT (" + QuoteIdentifier("lock_name") + ") DO UPDATE SET " + owner + " = excluded." + owner + ", " + acquired + " = excluded." + acquired + " "
                + "WHERE " + table + "." + owner + " = excluded." + owner + " OR " + table + "." + acquired + " < now() - (@p1 * INTERVAL '1 second') "
                + "RETURNING 1",
                new[] { new SqlParameterValue("@p0", lockName), new SqlParameterValue("@p1", (long)MigrationLockExpirySeconds) });
        }

        /// <inheritdoc />
        public override SqlStatement? ReleaseMigrationLockSql(string lockName)
        {
            ArgumentNullException.ThrowIfNull(lockName);
            return new SqlStatement(
                "DELETE FROM " + QuoteIdentifier(MigrationLockTableName) + " WHERE " + QuoteIdentifier("lock_name") + " = @p0 AND "
                + QuoteIdentifier("owner_id") + " = " + SessionIdentitySql,
                new[] { new SqlParameterValue("@p0", lockName) });
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Gets an empty filter: CockroachDB reports every unique index as a unique constraint, so indexes cannot be told
        /// apart from constraint-backed ones and all secondary indexes are introspected.
        /// </summary>
        protected override string IndexSchemaConstraintFilter => string.Empty;

        /// <summary>
        /// Gets a SQL expression that identifies the current session uniquely across the cluster.
        /// </summary>
        protected virtual string SessionIdentitySql => "current_setting('session_id')";

        /// <inheritdoc />
        protected override string AutoIncrementColumnType(ColumnMetadata column, bool inlinePrimaryKey)
        {
            string type = base.AutoIncrementColumnType(column, inlinePrimaryKey);
            return type.StartsWith("INTEGER ", StringComparison.Ordinal) ? "INT4" + type.Substring(7) : type;
        }

        #endregion
    }
}
