namespace Durable.Postgres
{
    /// <summary>
    /// The PostgreSQL-compatible database a repository talks to. Each flavor selects a dialect
    /// (<see cref="PostgresDialect.For(PostgresFlavor)"/>) that adjusts the SQL Durable generates for that database.
    /// </summary>
    public enum PostgresFlavor
    {
        /// <summary>
        /// PostgreSQL (default). Uses <see cref="PostgresDialect"/>.
        /// </summary>
        PostgreSql = 0,

        /// <summary>
        /// CockroachDB (PostgreSQL wire protocol). Uses <see cref="CockroachDbDialect"/>.
        /// </summary>
        CockroachDb = 1,

        /// <summary>
        /// YugabyteDB YSQL (PostgreSQL wire protocol). Uses <see cref="YugabyteDbDialect"/>.
        /// </summary>
        YugabyteDb = 2
    }
}
