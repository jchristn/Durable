namespace Test.Shared
{
    using System;
    using Microsoft.Data.SqlClient;
    using MySqlConnector;
    using Npgsql;

    /// <summary>
    /// Creates <see cref="IRepositoryProvider"/> instances and provider connection strings from a
    /// <see cref="TestRuntimeConfiguration"/>. Centralizes the mapping between configuration values and
    /// provider-specific connection strings so all runners behave identically.
    /// </summary>
    public static class RepositoryProviderFactory
    {
        #region Public-Methods

        /// <summary>
        /// Creates a repository provider for the database described by the supplied configuration.
        /// </summary>
        /// <param name="configuration">The runtime configuration. Cannot be null.</param>
        /// <returns>A repository provider for the configured database type.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="configuration"/> is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown for a document backend (see <see cref="DocumentBackendTestTargets"/>).</exception>
        /// <exception cref="NotSupportedException">Thrown when the target's provider wiring has not been added yet.</exception>
        public static IRepositoryProvider Create(TestRuntimeConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));

            switch (configuration.DatabaseType)
            {
                case TestDatabaseType.Sqlite:
                    return new SqliteRepositoryProvider(BuildSqliteConnectionString(configuration));
                case TestDatabaseType.MySql:
                    return new MySqlRepositoryProvider(BuildMySqlConnectionString(configuration));
                case TestDatabaseType.Postgres:
                    return new PostgresRepositoryProvider(BuildPostgresConnectionString(configuration));
                case TestDatabaseType.SqlServer:
                    return new SqlServerRepositoryProvider(BuildSqlServerConnectionString(configuration));

                // v0.7.0 targets: each one fills in its own Create*Provider / Build*ConnectionString pair below.
                case TestDatabaseType.Oracle:
                    return CreateOracleProvider(configuration);

                case TestDatabaseType.DuckDb:
                    return CreateDuckDbProvider(configuration);

                case TestDatabaseType.MariaDb:
                    return CreateMariaDbProvider(configuration);

                case TestDatabaseType.CockroachDb:
                    return CreateCockroachDbProvider(configuration);

                case TestDatabaseType.YugabyteDb:
                    return CreateYugabyteDbProvider(configuration);

                case TestDatabaseType.MongoDb:
                case TestDatabaseType.CosmosDb:
                    throw new InvalidOperationException(
                        TestDatabaseTypes.ProviderName(configuration.DatabaseType) + " is a document backend, not a SQL provider; use DocumentBackendTestTargets.");

                default:
                    throw new ArgumentOutOfRangeException(nameof(configuration), "Unsupported database type " + configuration.DatabaseType + ".");
            }
        }

        /// <summary>
        /// Builds the provider-specific connection string for the supplied configuration.
        /// </summary>
        /// <param name="configuration">The runtime configuration. Cannot be null.</param>
        /// <returns>The connection string for the configured database type.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="configuration"/> is null.</exception>
        /// <exception cref="NotSupportedException">Thrown when the target's provider wiring has not been added yet.</exception>
        public static string BuildConnectionString(TestRuntimeConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));

            switch (configuration.DatabaseType)
            {
                case TestDatabaseType.Sqlite:
                    return BuildSqliteConnectionString(configuration);
                case TestDatabaseType.MySql:
                    return BuildMySqlConnectionString(configuration);
                case TestDatabaseType.Postgres:
                    return BuildPostgresConnectionString(configuration);
                case TestDatabaseType.SqlServer:
                    return BuildSqlServerConnectionString(configuration);

                case TestDatabaseType.Oracle:
                    return BuildOracleConnectionString(configuration);

                case TestDatabaseType.DuckDb:
                    return BuildDuckDbConnectionString(configuration);

                case TestDatabaseType.MariaDb:
                    return BuildMariaDbConnectionString(configuration);

                case TestDatabaseType.CockroachDb:
                    return BuildCockroachDbConnectionString(configuration);

                case TestDatabaseType.YugabyteDb:
                    return BuildYugabyteDbConnectionString(configuration);

                default:
                    throw new ArgumentOutOfRangeException(nameof(configuration), "Unsupported database type " + configuration.DatabaseType + ".");
            }
        }

        #endregion

        #region Private-Methods

        private static string BuildSqliteConnectionString(TestRuntimeConfiguration configuration)
        {
            if (!string.IsNullOrWhiteSpace(configuration.Filename))
            {
                return "Data Source=" + configuration.Filename;
            }

            return "Data Source=file:/InMemorySharedTest?vfs=memdb";
        }

        private static string BuildMySqlConnectionString(TestRuntimeConfiguration configuration)
        {
            MySqlConnectionStringBuilder builder = new MySqlConnectionStringBuilder
            {
                Server = string.IsNullOrWhiteSpace(configuration.Hostname) ? "127.0.0.1" : configuration.Hostname,
                Port = (uint)(configuration.Port ?? 3306),
                UserID = string.IsNullOrWhiteSpace(configuration.Username) ? "root" : configuration.Username,
                Password = configuration.Password ?? string.Empty,
                Database = configuration.DatabaseName,
                AllowUserVariables = true,
                Pooling = true
            };

            return builder.ConnectionString;
        }

        private static string BuildPostgresConnectionString(TestRuntimeConfiguration configuration)
        {
            NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder
            {
                Host = string.IsNullOrWhiteSpace(configuration.Hostname) ? "127.0.0.1" : configuration.Hostname,
                Port = configuration.Port ?? 5432,
                Username = string.IsNullOrWhiteSpace(configuration.Username) ? "postgres" : configuration.Username,
                Password = configuration.Password ?? string.Empty,
                Database = configuration.DatabaseName,
                Pooling = true
            };

            return builder.ConnectionString;
        }

        private static string BuildSqlServerConnectionString(TestRuntimeConfiguration configuration)
        {
            string host = string.IsNullOrWhiteSpace(configuration.Hostname) ? "127.0.0.1" : configuration.Hostname;
            string dataSource;

            if (!string.IsNullOrWhiteSpace(configuration.Instance))
            {
                dataSource = host + "\\" + configuration.Instance;
            }
            else if (configuration.Port.HasValue)
            {
                dataSource = host + "," + configuration.Port.Value;
            }
            else
            {
                dataSource = host;
            }

            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder
            {
                DataSource = dataSource,
                InitialCatalog = configuration.DatabaseName,
                Encrypt = false,
                TrustServerCertificate = true,
                MultipleActiveResultSets = true,
                // SqlClient applies the connection's command timeout (default 30 s) to COMMIT; under the stress suites a
                // containerized server on a shared CI runner has stalled a commit's log flush past 30 s.
                CommandTimeout = 120
            };

            // "integrated" uses Windows authentication (e.g. LocalDB: host "(localdb)", instance "MSSQLLocalDB")
            if (string.Equals(configuration.Username, "integrated", StringComparison.OrdinalIgnoreCase))
            {
                builder.IntegratedSecurity = true;
            }
            else
            {
                builder.UserID = string.IsNullOrWhiteSpace(configuration.Username) ? "sa" : configuration.Username;
                builder.Password = configuration.Password ?? string.Empty;
            }

            return builder.ConnectionString;
        }

        private static IRepositoryProvider CreateOracleProvider(TestRuntimeConfiguration configuration)
        {
            throw TestDatabaseTypes.NotYetAvailable(TestDatabaseType.Oracle, "repository provider");
        }

        private static string BuildOracleConnectionString(TestRuntimeConfiguration configuration)
        {
            throw TestDatabaseTypes.NotYetAvailable(TestDatabaseType.Oracle, "connection string builder");
        }

        private static IRepositoryProvider CreateDuckDbProvider(TestRuntimeConfiguration configuration)
        {
            return new DuckDbRepositoryProvider(BuildDuckDbConnectionString(configuration));
        }

        private static string BuildDuckDbConnectionString(TestRuntimeConfiguration configuration)
        {
            // In-process like SQLite: a database file when --filename is given, otherwise the process-wide shared in-memory
            // database (kept alive by DuckDbRepositoryProvider for the run).
            if (!string.IsNullOrWhiteSpace(configuration.Filename))
            {
                return Durable.DuckDb.DuckDbRepositorySettings.ForFile(configuration.Filename).BuildConnectionString();
            }

            return DuckDbRepositoryProvider.DefaultConnectionString;
        }

        private static IRepositoryProvider CreateMariaDbProvider(TestRuntimeConfiguration configuration)
        {
            throw TestDatabaseTypes.NotYetAvailable(TestDatabaseType.MariaDb, "repository provider");
        }

        private static string BuildMariaDbConnectionString(TestRuntimeConfiguration configuration)
        {
            throw TestDatabaseTypes.NotYetAvailable(TestDatabaseType.MariaDb, "connection string builder");
        }

        private static IRepositoryProvider CreateCockroachDbProvider(TestRuntimeConfiguration configuration)
        {
            throw TestDatabaseTypes.NotYetAvailable(TestDatabaseType.CockroachDb, "repository provider");
        }

        private static string BuildCockroachDbConnectionString(TestRuntimeConfiguration configuration)
        {
            throw TestDatabaseTypes.NotYetAvailable(TestDatabaseType.CockroachDb, "connection string builder");
        }

        private static IRepositoryProvider CreateYugabyteDbProvider(TestRuntimeConfiguration configuration)
        {
            throw TestDatabaseTypes.NotYetAvailable(TestDatabaseType.YugabyteDb, "repository provider");
        }

        private static string BuildYugabyteDbConnectionString(TestRuntimeConfiguration configuration)
        {
            throw TestDatabaseTypes.NotYetAvailable(TestDatabaseType.YugabyteDb, "connection string builder");
        }

        #endregion
    }
}
