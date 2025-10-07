namespace Durable.Postgres
{

    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using Durable;
    using Npgsql;

    /// <summary>
    /// Connection settings for PostgreSQL repositories
    /// </summary>
    public sealed class PostgresRepositorySettings : RepositorySettings
    {

        #region Public-Members

        /// <summary>
        /// The type of repository
        /// </summary>
        public override RepositoryType Type => RepositoryType.Postgres;

        /// <summary>
        /// The connection timeout in seconds. Default: null (uses PostgreSQL default of 15 seconds)
        /// </summary>
        public int? ConnectionTimeout { get; init; }

        /// <summary>
        /// The command timeout in seconds. Default: null (uses PostgreSQL default of 30 seconds)
        /// </summary>
        public int? CommandTimeout { get; init; }

        /// <summary>
        /// The minimum pool size. Default: null (uses PostgreSQL default of 0)
        /// </summary>
        public int? MinPoolSize { get; init; }

        /// <summary>
        /// The maximum pool size. Default: null (uses PostgreSQL default of 100)
        /// </summary>
        public int? MaxPoolSize { get; init; }

        /// <summary>
        /// Whether to use connection pooling. Default: null (uses PostgreSQL default of true)
        /// </summary>
        public bool? Pooling { get; init; }

        /// <summary>
        /// The SSL mode. Default: null (uses PostgreSQL default)
        /// </summary>
        public SslMode? SslMode { get; init; }

        #endregion

        #region Private-Members

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
                    lowerKey != "commandtimeout" &&
                    lowerKey != "command timeout" &&
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
                CommandTimeout = builder.CommandTimeout != 30 ? builder.CommandTimeout : null,
                MinPoolSize = builder.MinPoolSize != 0 ? builder.MinPoolSize : null,
                MaxPoolSize = builder.MaxPoolSize != 100 ? builder.MaxPoolSize : null,
                Pooling = builder.Pooling != true ? builder.Pooling : null,
                SslMode = builder.SslMode,
                AdditionalProperties = additionalProperties
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

            if (CommandTimeout.HasValue)
            {
                builder.CommandTimeout = CommandTimeout.Value;
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

        #region Private-Methods

        #endregion

    }

}
