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
                MultipleActiveResultSets = true
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

        #endregion
    }
}
