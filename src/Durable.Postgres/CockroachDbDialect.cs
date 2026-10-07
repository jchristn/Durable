namespace Durable.Postgres
{
    using System;
    using Durable.Sql;

    /// <summary>
    /// CockroachDB dialect (CockroachDB 24.1+ over the PostgreSQL wire protocol). Differences from <see cref="PostgresDialect"/>
    /// are limited to what CockroachDB does not support.
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

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the dialect.
        /// </summary>
        /// <param name="converter">Converter; null uses <see cref="PostgresDataTypeConverter"/>.</param>
        /// <param name="ordinalCollation">Binary collation for ordinal string matching. Default: C.</param>
        /// <exception cref="ArgumentException">Thrown when ordinalCollation is not a simple collation name.</exception>
        public CockroachDbDialect(IDataTypeConverter? converter = null, string ordinalCollation = "C") : base(converter, ordinalCollation)
        {
        }

        #endregion
    }
}
