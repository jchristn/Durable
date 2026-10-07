namespace Durable.Tool
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Durable.DuckDb;
    using Durable.MySql;
    using Durable.Oracle;
    using Durable.Postgres;
    using Durable.Sql;
    using Durable.Sqlite;
    using Durable.SqlServer;

    /// <summary>
    /// A database the tool talks to: the provider's dialect and a connection factory for the connection string.
    /// Dispose it to release the factory.
    /// </summary>
    internal sealed class DatabaseTarget : IAsyncDisposable
    {
        // Canonical provider names, one per line so each provider adds its own line (keep in step with Create and
        // NormalizeProvider).
        private static readonly IReadOnlyList<string> _CanonicalProviders = new List<string>
        {
            "sqlite",
            "duckdb",
            "postgres",
            "cockroachdb",
            "yugabytedb",
            "mysql",
            "mariadb",
            "sqlserver",
            "oracle"
        };

        /// <summary>
        /// Gets the canonical provider names accepted by --provider, in display order. Never null.
        /// </summary>
        public static IReadOnlyList<string> CanonicalProviders => _CanonicalProviders;

        /// <summary>
        /// Gets the provider names accepted by --provider, for help and error messages (for example
        /// "sqlite, postgres, mysql or sqlserver").
        /// </summary>
        public static string ProviderNames
        {
            get
            {
                if (_CanonicalProviders.Count == 1) return _CanonicalProviders[0];
                List<string> names = new List<string>(_CanonicalProviders);
                string last = names[names.Count - 1];
                names.RemoveAt(names.Count - 1);
                return string.Join(", ", names) + " or " + last;
            }
        }

        /// <summary>
        /// Gets the provider names separated by '|', for usage lines (for example "sqlite|postgres|mysql|sqlserver").
        /// </summary>
        public static string ProviderChoices => string.Join("|", _CanonicalProviders);

        /// <summary>
        /// Gets the dialect.
        /// </summary>
        public ISqlDialect Dialect { get; }

        /// <summary>
        /// Gets the connection factory.
        /// </summary>
        public IConnectionFactory ConnectionFactory { get; }

        /// <summary>
        /// Gets the display name of the database, for example "PostgreSQL".
        /// </summary>
        public string DisplayName => Dialect.RepositoryType.DisplayName;

        private DatabaseTarget(ISqlDialect dialect, IConnectionFactory connectionFactory)
        {
            Dialect = dialect;
            ConnectionFactory = connectionFactory;
        }

        /// <summary>
        /// Creates a target for a provider name and connection string.
        /// </summary>
        /// <param name="provider">Provider name (sqlite, duckdb, postgres/postgresql, mysql, sqlserver/mssql, mariadb, cockroachdb/cockroach/crdb, yugabytedb/yugabyte/ysql, oracle/odp/odpnet). Must not be null.</param>
        /// <param name="connectionString">Connection string. Must not be null.</param>
        /// <returns>The target.</returns>
        /// <exception cref="DurableCliException">Thrown when the provider is unknown or the connection string is invalid.</exception>
        public static DatabaseTarget Create(string provider, string connectionString)
        {
            ArgumentNullException.ThrowIfNull(provider);
            ArgumentNullException.ThrowIfNull(connectionString);
            try
            {
                switch (NormalizeProvider(provider))
                {
                    case "sqlite":
                        return new DatabaseTarget(SqliteDialect.Default, new SqliteConnectionFactory(connectionString));
                    case "duckdb":
                        return new DatabaseTarget(DuckDbDialect.Default, new DuckDbConnectionFactory(connectionString));
                    case "postgres":
                        return new DatabaseTarget(PostgresDialect.Default, new PostgresConnectionFactory(connectionString));
                    case "cockroachdb":
                        return new DatabaseTarget(CockroachDbDialect.Default, new PostgresConnectionFactory(connectionString) { Flavor = PostgresFlavor.CockroachDb });
                    case "yugabytedb":
                        return new DatabaseTarget(YugabyteDbDialect.Default, new PostgresConnectionFactory(connectionString) { Flavor = PostgresFlavor.YugabyteDb });
                    case "mysql":
                        return new DatabaseTarget(MySqlDialect.Default, new MySqlConnectionFactory(connectionString));
                    case "mariadb":
                        return new DatabaseTarget(MariaDbDialect.Default, new MySqlConnectionFactory(connectionString) { Flavor = MySqlFlavor.MariaDb });
                    case "sqlserver":
                        return new DatabaseTarget(SqlServerDialect.Default, new SqlServerConnectionFactory(connectionString));
                    case "oracle":
                        return new DatabaseTarget(OracleDialect.Default, new OracleConnectionFactory(connectionString));
                    default:
                        throw new DurableCliException("Unknown provider '" + provider + "'. Use " + ProviderNames + ".", null, true);
                }
            }
            catch (ArgumentException e)
            {
                throw new DurableCliException("The connection string is not valid for " + provider + ": " + e.Message);
            }
        }

        /// <summary>
        /// Returns the canonical provider name for an accepted alias, or null when the name is unknown.
        /// </summary>
        /// <param name="provider">Provider name. Must not be null.</param>
        /// <returns>A name from <see cref="CanonicalProviders"/>, or null.</returns>
        public static string? NormalizeProvider(string provider)
        {
            ArgumentNullException.ThrowIfNull(provider);
            switch (provider.Trim().ToLowerInvariant())
            {
                case "sqlite": return "sqlite";
                case "duckdb": return "duckdb";
                case "postgres":
                case "postgresql":
                case "pgsql":
                case "npgsql": return "postgres";
                case "cockroachdb":
                case "cockroach":
                case "crdb": return "cockroachdb";
                case "yugabytedb":
                case "yugabyte":
                case "ysql": return "yugabytedb";
                case "mysql": return "mysql";
                case "mariadb": return "mariadb";
                case "sqlserver":
                case "mssql": return "sqlserver";
                case "oracle":
                case "odp":
                case "odpnet": return "oracle";
                default: return null;
            }
        }

        /// <summary>
        /// Disposes the connection factory.
        /// </summary>
        /// <returns>A task.</returns>
        public ValueTask DisposeAsync()
        {
            return ConnectionFactory.DisposeAsync();
        }
    }
}
