namespace Durable.Postgres
{
    using System;
    using Durable.Sql;

    /// <summary>
    /// YugabyteDB YSQL dialect (YugabyteDB 2024.2+ over the PostgreSQL wire protocol). Differences from
    /// <see cref="PostgresDialect"/> are limited to what YSQL does not support.
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
