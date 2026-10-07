namespace Test.Shared
{
    /// <summary>
    /// Enumerates the database providers and backends supported by the Durable test suites. Names, tags, families and
    /// command-line aliases for each value live in <see cref="TestDatabaseTypes"/>.
    /// </summary>
    public enum TestDatabaseType
    {
        /// <summary>
        /// SQLite provider. Runs fully in-process and requires no external server.
        /// </summary>
        Sqlite,

        /// <summary>
        /// MySQL provider. Requires a reachable MySQL server (or a dockerized instance).
        /// </summary>
        MySql,

        /// <summary>
        /// PostgreSQL provider. Requires a reachable PostgreSQL server (or a dockerized instance).
        /// </summary>
        Postgres,

        /// <summary>
        /// Microsoft SQL Server provider. Requires a reachable SQL Server instance (or a dockerized instance).
        /// </summary>
        SqlServer,

        /// <summary>
        /// Oracle Database provider (Durable.Oracle). Requires a reachable Oracle server (or a dockerized instance).
        /// </summary>
        Oracle,

        /// <summary>
        /// DuckDB provider (Durable.DuckDb). Runs fully in-process (in-memory or a file) and requires no external server.
        /// </summary>
        DuckDb,

        /// <summary>
        /// MariaDB, served by the MySQL provider (Durable.MySql). Requires a reachable MariaDB server (or a dockerized instance).
        /// </summary>
        MariaDb,

        /// <summary>
        /// CockroachDB, served by the PostgreSQL provider (Durable.Postgres). Requires a reachable CockroachDB node
        /// (or a dockerized instance).
        /// </summary>
        CockroachDb,

        /// <summary>
        /// YugabyteDB (YSQL API), served by the PostgreSQL provider (Durable.Postgres). Requires a reachable YugabyteDB
        /// node (or a dockerized instance).
        /// </summary>
        YugabyteDb,

        /// <summary>
        /// MongoDB document backend (Durable.MongoDb, not a SQL provider). Runs the conformance kit and the backend suites
        /// instead of the SQL suites. Requires a reachable MongoDB server (or a dockerized instance).
        /// </summary>
        MongoDb,

        /// <summary>
        /// Azure Cosmos DB for NoSQL document backend (Durable.CosmosDb, not a SQL provider). Runs the conformance kit and
        /// the backend suites instead of the SQL suites. Requires a reachable Cosmos DB account or emulator (or a
        /// dockerized emulator).
        /// </summary>
        CosmosDb
    }
}
