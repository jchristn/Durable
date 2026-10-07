namespace Test.Shared
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// The one place that describes each <see cref="TestDatabaseType"/>: display name, tag, command-line names and family.
    /// The CLI runner, the environment-variable configuration of the xUnit/NUnit adapters and the suite registry all read
    /// these, so a new target is named here once.
    /// Thread safety: stateless; safe for concurrent use.
    /// </summary>
    public static class TestDatabaseTypes
    {
        #region Public-Members

        /// <summary>
        /// Gets every value of <see cref="TestDatabaseType"/> in declaration order. Never null.
        /// </summary>
        public static IReadOnlyList<TestDatabaseType> All { get; } = (TestDatabaseType[])Enum.GetValues(typeof(TestDatabaseType));

        /// <summary>
        /// Gets the canonical command-line names separated by " | " (for usage text), for example "sqlite | mysql | ...".
        /// Never null.
        /// </summary>
        public static string CommandLineNames
        {
            get
            {
                List<string> names = new List<string>();
                foreach (TestDatabaseType type in All) names.Add(ProviderTag(type));
                return string.Join(" | ", names);
            }
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns the display name of a target (for example "PostgreSQL" or "Cosmos DB"), used in suite names.
        /// </summary>
        /// <param name="databaseType">The target.</param>
        /// <returns>The display name; "Unknown" for an undefined value. Never null.</returns>
        public static string ProviderName(TestDatabaseType databaseType)
        {
            switch (databaseType)
            {
                case TestDatabaseType.Sqlite: return "SQLite";
                case TestDatabaseType.MySql: return "MySQL";
                case TestDatabaseType.Postgres: return "PostgreSQL";
                case TestDatabaseType.SqlServer: return "SQL Server";
                case TestDatabaseType.Oracle: return "Oracle";
                case TestDatabaseType.DuckDb: return "DuckDB";
                case TestDatabaseType.MariaDb: return "MariaDB";
                case TestDatabaseType.CockroachDb: return "CockroachDB";
                case TestDatabaseType.YugabyteDb: return "YugabyteDB";
                case TestDatabaseType.MongoDb: return "MongoDB";
                case TestDatabaseType.CosmosDb: return "Cosmos DB";
                default: return "Unknown";
            }
        }

        /// <summary>
        /// Returns the tag applied to every test case of a run against the target; it is also the canonical
        /// <c>--type</c> / <c>DURABLE_TEST_DB</c> name (for example "postgres" or "cosmosdb").
        /// </summary>
        /// <param name="databaseType">The target.</param>
        /// <returns>The lower-case tag; "unknown" for an undefined value. Never null.</returns>
        public static string ProviderTag(TestDatabaseType databaseType)
        {
            switch (databaseType)
            {
                case TestDatabaseType.Sqlite: return "sqlite";
                case TestDatabaseType.MySql: return "mysql";
                case TestDatabaseType.Postgres: return "postgres";
                case TestDatabaseType.SqlServer: return "sqlserver";
                case TestDatabaseType.Oracle: return "oracle";
                case TestDatabaseType.DuckDb: return "duckdb";
                case TestDatabaseType.MariaDb: return "mariadb";
                case TestDatabaseType.CockroachDb: return "cockroachdb";
                case TestDatabaseType.YugabyteDb: return "yugabytedb";
                case TestDatabaseType.MongoDb: return "mongodb";
                case TestDatabaseType.CosmosDb: return "cosmosdb";
                default: return "unknown";
            }
        }

        /// <summary>
        /// Parses a command-line or environment-variable name (case-insensitive, surrounding white space ignored): the
        /// canonical tag (see <see cref="ProviderTag"/>) or an alias such as "postgresql", "mssql", "crdb" or "mongo".
        /// </summary>
        /// <param name="value">The name. Null or empty returns null.</param>
        /// <returns>The target, or null when the name is not recognized.</returns>
        public static TestDatabaseType? Parse(string? value)
        {
            if (string.IsNullOrWhiteSpace(value)) return null;

            switch (value.Trim().ToLowerInvariant())
            {
                case "sqlite":
                    return TestDatabaseType.Sqlite;
                case "mysql":
                    return TestDatabaseType.MySql;
                case "postgres":
                case "postgresql":
                case "pgsql":
                    return TestDatabaseType.Postgres;
                case "sqlserver":
                case "mssql":
                    return TestDatabaseType.SqlServer;
                case "oracle":
                    return TestDatabaseType.Oracle;
                case "duckdb":
                    return TestDatabaseType.DuckDb;
                case "mariadb":
                    return TestDatabaseType.MariaDb;
                case "cockroachdb":
                case "cockroach":
                case "crdb":
                    return TestDatabaseType.CockroachDb;
                case "yugabytedb":
                case "yugabyte":
                    return TestDatabaseType.YugabyteDb;
                case "mongodb":
                case "mongo":
                    return TestDatabaseType.MongoDb;
                case "cosmosdb":
                case "cosmos":
                    return TestDatabaseType.CosmosDb;
                default:
                    return null;
            }
        }

        /// <summary>
        /// Returns true for targets that run in-process and need no server or container (SQLite, DuckDB).
        /// </summary>
        /// <param name="databaseType">The target.</param>
        /// <returns>True when the target is in-process.</returns>
        public static bool IsInProcess(TestDatabaseType databaseType)
        {
            return databaseType == TestDatabaseType.Sqlite || databaseType == TestDatabaseType.DuckDb;
        }

        /// <summary>
        /// Returns true for non-SQL document backends (MongoDB, Cosmos DB). Those runs build no
        /// <see cref="IRepositoryProvider"/>: they run the Durable.Conformance kit and the backend's own suites through an
        /// <see cref="IDocumentBackendTestTarget"/> (see <see cref="DocumentBackendTestTargets"/>) instead of the SQL suites.
        /// </summary>
        /// <param name="databaseType">The target.</param>
        /// <returns>True when the target is a document backend.</returns>
        public static bool IsDocumentBackend(TestDatabaseType databaseType)
        {
            return databaseType == TestDatabaseType.MongoDb || databaseType == TestDatabaseType.CosmosDb;
        }

        /// <summary>
        /// Returns true for targets served by the MySQL provider (MySQL, MariaDB). Suites use it instead of comparing with
        /// <see cref="TestDatabaseType.MySql"/> so wire-compatible databases share MySQL-specific SQL and expectations.
        /// </summary>
        /// <param name="databaseType">The target.</param>
        /// <returns>True for MySQL and MariaDB.</returns>
        public static bool IsMySqlFamily(TestDatabaseType databaseType)
        {
            return databaseType == TestDatabaseType.MySql || databaseType == TestDatabaseType.MariaDb;
        }

        /// <summary>
        /// Returns true for targets served by the PostgreSQL provider (PostgreSQL, CockroachDB, YugabyteDB). Suites use it
        /// instead of comparing with <see cref="TestDatabaseType.Postgres"/> so wire-compatible databases share
        /// PostgreSQL-specific SQL and expectations.
        /// </summary>
        /// <param name="databaseType">The target.</param>
        /// <returns>True for PostgreSQL, CockroachDB and YugabyteDB.</returns>
        public static bool IsPostgresFamily(TestDatabaseType databaseType)
        {
            return databaseType == TestDatabaseType.Postgres
                || databaseType == TestDatabaseType.CockroachDb
                || databaseType == TestDatabaseType.YugabyteDb;
        }

        /// <summary>
        /// Creates the exception thrown by a placeholder for a target whose test wiring has not been added yet.
        /// </summary>
        /// <param name="databaseType">The target.</param>
        /// <param name="what">What is missing, for example "repository provider" or "docker settings". Must not be null.</param>
        /// <returns>A <see cref="NotSupportedException"/> naming the target. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="what"/> is null.</exception>
        public static NotSupportedException NotYetAvailable(TestDatabaseType databaseType, string what)
        {
            ArgumentNullException.ThrowIfNull(what);
            return new NotSupportedException(
                "Test wiring for " + ProviderName(databaseType) + " (" + what + ") is added by a later v0.7.0 change (--type " + ProviderTag(databaseType) + ").");
        }

        #endregion
    }
}
