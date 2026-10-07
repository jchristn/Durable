namespace Durable.Postgres
{
    using System;
    using Durable.Sql;

    /// <summary>
    /// YugabyteDB YSQL dialect (YugabyteDB 2025.1+ over the PostgreSQL wire protocol; tested on 2026.1). The only
    /// difference from <see cref="PostgresDialect"/> is that DDL is treated as non-transactional. Advisory locks (used for
    /// the migration lock) require <c>yb_enable_advisory_locks</c>, which is on by default in the tested release.
    /// Thread safety: immutable; safe to share.
    /// </summary>
    public class YugabyteDbDialect : PostgresDialect
    {
        #region Public-Members

        /// <summary>
        /// Gets the shared default instance.
        /// </summary>
        public static new YugabyteDbDialect Default { get; } = new YugabyteDbDialect();

        /// <inheritdoc />
        public override PostgresFlavor Flavor => PostgresFlavor.YugabyteDb;

        /// <summary>
        /// Gets false: YugabyteDB runs DDL inside a transaction block in its own transaction unless
        /// <c>yb_ddl_transaction_block_enabled</c> is on (off by default), so migrations and schema synchronization run
        /// without a transaction and a failed migration may leave earlier statements applied.
        /// </summary>
        public override bool SupportsTransactionalDdl => false;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the dialect.
        /// </summary>
        /// <param name="converter">Converter; null uses <see cref="PostgresDataTypeConverter"/>.</param>
        /// <param name="ordinalCollation">Binary collation for ordinal string matching. Default: C.</param>
        /// <exception cref="ArgumentException">Thrown when ordinalCollation is not a simple collation name.</exception>
        public YugabyteDbDialect(IDataTypeConverter? converter = null, string ordinalCollation = "C") : base(converter, ordinalCollation)
        {
        }

        #endregion
    }
}
