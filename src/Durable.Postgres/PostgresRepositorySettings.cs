namespace Durable.Postgres
{

    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using Durable;
    using Durable.Sql;
    using Npgsql;

    /// <summary>
    /// Connection settings for PostgreSQL repositories
    /// Thread safety: immutable after construction (init-only properties); safe to share.
    /// </summary>
    public sealed class PostgresRepositorySettings : RepositorySettings
    {

        #region Public-Members

        /// <summary>
        /// Gets <see cref="RepositoryType.Postgres"/>.
        /// </summary>
        public override RepositoryType Type => RepositoryType.Postgres;

        /// <summary>
        /// Gets the connection (login) timeout in seconds. Default: null (the driver default, 15 seconds). Minimum: 0
        /// (0 waits indefinitely on drivers that allow it). Maps to the driver's connection-timeout keyword.
        /// </summary>
        public int? ConnectionTimeout { get; init; }

        /// <summary>
        /// Gets the minimum number of pooled connections the driver keeps open. Default: null (the driver default, 0).
        /// Minimum: 0. Must not exceed <see cref="MaxPoolSize"/>.
        /// </summary>
        public int? MinPoolSize { get; init; }

        /// <summary>
        /// Gets the maximum number of pooled connections. Default: null (the driver default, 100). Minimum: 1.
        /// </summary>
        public int? MaxPoolSize { get; init; }

        /// <summary>
        /// Gets whether the driver pools connections. Default: null (the driver default, true).
        /// </summary>
        public bool? Pooling { get; init; }

        /// <summary>
        /// Gets the TLS mode (Npgsql's <see cref="Npgsql.SslMode"/>). Default: null (the driver default, Prefer).
        /// </summary>
        public SslMode? SslMode { get; init; }

        /// <summary>
        /// Gets the PostgreSQL-compatible database the settings connect to, which selects the dialect of repositories and
        /// connection factories built from these settings (<see cref="PostgresDialect.For(PostgresFlavor)"/>).
        /// Default: <see cref="PostgresFlavor.PostgreSql"/>. Not part of the connection string: <c>Parse(connectionString)</c> returns the default,
        /// <c>Parse(connectionString, flavor)</c> sets it, and <see cref="BuildConnectionString"/> ignores it.
        /// </summary>
        public PostgresFlavor Flavor { get; init; } = PostgresFlavor.PostgreSql;

        #endregion


        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the PostgresRepositorySettings class
        /// </summary>
        public PostgresRepositorySettings()
        {
        }

        /// <summary>
        /// Parses a PostgreSQL connection string and returns a PostgresRepositorySettings instance
        /// </summary>
        /// <param name="connectionString">The connection string to parse</param>
        /// <returns>A PostgresRepositorySettings instance</returns>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null</exception>
        /// <exception cref="ArgumentException">Thrown when connectionString is empty or whitespace, or when the connection string is invalid</exception>
        public static PostgresRepositorySettings Parse(string connectionString)
        {
            return Parse(connectionString, PostgresFlavor.PostgreSql);
        }

        /// <summary>
        /// Parses a PostgreSQL connection string and returns a PostgresRepositorySettings instance
        /// </summary>
        /// <param name="connectionString">The connection string to parse</param>
        /// <param name="flavor">Database flavor stored in <see cref="Flavor"/>.</param>
        /// <returns>A PostgresRepositorySettings instance</returns>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when flavor is not a defined value.</exception>
        /// <exception cref="ArgumentException">Thrown when connectionString is empty or whitespace, or when the connection string is invalid</exception>
        public static PostgresRepositorySettings Parse(string connectionString, PostgresFlavor flavor)
        {
            if (!Enum.IsDefined(flavor)) throw new ArgumentOutOfRangeException(nameof(flavor), flavor, "Unknown PostgreSQL flavor.");
            ArgumentNullException.ThrowIfNull(connectionString);

            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentException("Connection string cannot be empty or whitespace", nameof(connectionString));
            }

            NpgsqlConnectionStringBuilder builder;

            try
            {
                builder = new NpgsqlConnectionStringBuilder(connectionString);
            }
            catch (Exception ex)
            {
                throw new ArgumentException($"Invalid PostgreSQL connection string: {ex.Message}", nameof(connectionString), ex);
            }

            Dictionary<string, string>? additionalProperties = null;

            foreach (string key in builder.Keys)
            {
                string lowerKey = key.ToLowerInvariant();

                if (lowerKey != "host" &&
                    lowerKey != "server" &&
                    lowerKey != "port" &&
                    lowerKey != "username" &&
                    lowerKey != "user id" &&
                    lowerKey != "userid" &&
                    lowerKey != "user" &&
                    lowerKey != "password" &&
                    lowerKey != "pwd" &&
                    lowerKey != "database" &&
                    lowerKey != "db" &&
                    lowerKey != "timeout" &&
                    lowerKey != "connection timeout" &&
                    lowerKey != "minpoolsize" &&
                    lowerKey != "minimum pool size" &&
                    lowerKey != "maxpoolsize" &&
                    lowerKey != "maximum pool size" &&
                    lowerKey != "pooling" &&
                    lowerKey != "sslmode" &&
                    lowerKey != "ssl mode")
                {
                    if (additionalProperties == null)
                    {
                        additionalProperties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    }

                    additionalProperties[key] = builder[key]?.ToString() ?? string.Empty;
                }
            }

            return new PostgresRepositorySettings
            {
                Hostname = builder.Host,
                Port = builder.Port != 5432 ? builder.Port : null,
                Username = builder.Username,
                Password = builder.Password,
                Database = builder.Database,
                ConnectionTimeout = builder.Timeout != 15 ? builder.Timeout : null,
                MinPoolSize = builder.MinPoolSize != 0 ? builder.MinPoolSize : null,
                MaxPoolSize = builder.MaxPoolSize != 100 ? builder.MaxPoolSize : null,
                Pooling = builder.Pooling != true ? builder.Pooling : null,
                SslMode = builder.SslMode != Npgsql.SslMode.Prefer ? builder.SslMode : null,
                AdditionalProperties = additionalProperties,
                Flavor = flavor
            };
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Builds a PostgreSQL connection string from the current settings
        /// </summary>
        /// <returns>A PostgreSQL connection string</returns>
        /// <exception cref="InvalidOperationException">Thrown when required properties (Hostname, Database) are null or empty</exception>
        public override string BuildConnectionString()
        {
            if (string.IsNullOrWhiteSpace(Hostname))
            {
                throw new InvalidOperationException("Hostname is required for PostgreSQL connection string");
            }

            if (string.IsNullOrWhiteSpace(Database))
            {
                throw new InvalidOperationException("Database is required for PostgreSQL connection string");
            }

            NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder
            {
                Host = Hostname,
                Database = Database
            };

            if (Port.HasValue)
            {
                builder.Port = Port.Value;
            }

            if (!string.IsNullOrWhiteSpace(Username))
            {
                builder.Username = Username;
            }

            if (!string.IsNullOrWhiteSpace(Password))
            {
                builder.Password = Password;
            }

            if (ConnectionTimeout.HasValue)
            {
                builder.Timeout = ConnectionTimeout.Value;
            }

            if (MinPoolSize.HasValue)
            {
                builder.MinPoolSize = MinPoolSize.Value;
            }

            if (MaxPoolSize.HasValue)
            {
                builder.MaxPoolSize = MaxPoolSize.Value;
            }

            if (Pooling.HasValue)
            {
                builder.Pooling = Pooling.Value;
            }

            if (SslMode.HasValue)
            {
                builder.SslMode = SslMode.Value;
            }

            if (AdditionalProperties != null)
            {
                foreach (KeyValuePair<string, string> kvp in AdditionalProperties)
                {
                    builder[kvp.Key] = kvp.Value;
                }
            }

            return builder.ConnectionString;
        }

        #endregion


    }

}
