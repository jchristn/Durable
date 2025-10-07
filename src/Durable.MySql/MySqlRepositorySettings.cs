namespace Durable.MySql
{

    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using Durable;
    using MySqlConnector;

    /// <summary>
    /// Connection settings for MySQL repositories
    /// </summary>
    public sealed class MySqlRepositorySettings : RepositorySettings
    {

        #region Public-Members

        /// <summary>
        /// The type of repository
        /// </summary>
        public override RepositoryType Type => RepositoryType.MySql;

        /// <summary>
        /// The connection timeout in seconds. Default: null (uses MySQL default of 15 seconds)
        /// </summary>
        public uint? ConnectionTimeout { get; init; }

        /// <summary>
        /// The minimum pool size. Default: null (uses MySQL default of 0)
        /// </summary>
        public uint? MinimumPoolSize { get; init; }

        /// <summary>
        /// The maximum pool size. Default: null (uses MySQL default of 100)
        /// </summary>
        public uint? MaximumPoolSize { get; init; }

        /// <summary>
        /// Whether to use connection pooling. Default: null (uses MySQL default of true)
        /// </summary>
        public bool? Pooling { get; init; }

        /// <summary>
        /// The SSL mode. Default: null (uses MySQL default)
        /// </summary>
        public MySqlSslMode? SslMode { get; init; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the MySqlRepositorySettings class
        /// </summary>
        public MySqlRepositorySettings()
        {
        }

        /// <summary>
        /// Parses a MySQL connection string and returns a MySqlRepositorySettings instance
        /// </summary>
        /// <param name="connectionString">The connection string to parse</param>
        /// <returns>A MySqlRepositorySettings instance</returns>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null</exception>
        /// <exception cref="ArgumentException">Thrown when connectionString is empty or whitespace, or when the connection string is invalid</exception>
        public static MySqlRepositorySettings Parse(string connectionString)
        {
            ArgumentNullException.ThrowIfNull(connectionString);

            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentException("Connection string cannot be empty or whitespace", nameof(connectionString));
            }

            MySqlConnectionStringBuilder builder;

            try
            {
                builder = new MySqlConnectionStringBuilder(connectionString);
            }
            catch (Exception ex)
            {
                throw new ArgumentException($"Invalid MySQL connection string: {ex.Message}", nameof(connectionString), ex);
            }

            Dictionary<string, string>? additionalProperties = null;

            foreach (string key in builder.Keys)
            {
                string lowerKey = key.ToLowerInvariant();

                if (lowerKey != "server" &&
                    lowerKey != "host" &&
                    lowerKey != "port" &&
                    lowerKey != "user" &&
                    lowerKey != "userid" &&
                    lowerKey != "uid" &&
                    lowerKey != "username" &&
                    lowerKey != "password" &&
                    lowerKey != "pwd" &&
                    lowerKey != "database" &&
                    lowerKey != "initial catalog" &&
                    lowerKey != "connectiontimeout" &&
                    lowerKey != "connection timeout" &&
                    lowerKey != "minpoolsize" &&
                    lowerKey != "minimumpoolsize" &&
                    lowerKey != "min pool size" &&
                    lowerKey != "maxpoolsize" &&
                    lowerKey != "maximumpoolsize" &&
                    lowerKey != "max pool size" &&
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

            return new MySqlRepositorySettings
            {
                Hostname = builder.Server,
                Port = builder.Port != 3306 ? (int)builder.Port : null,
                Username = !string.IsNullOrEmpty(builder.UserID) ? builder.UserID : null,
                Password = !string.IsNullOrEmpty(builder.Password) ? builder.Password : null,
                Database = builder.Database,
                ConnectionTimeout = builder.ConnectionTimeout != 15 ? builder.ConnectionTimeout : null,
                MinimumPoolSize = builder.MinimumPoolSize != 0 ? builder.MinimumPoolSize : null,
                MaximumPoolSize = builder.MaximumPoolSize != 100 ? builder.MaximumPoolSize : null,
                Pooling = builder.Pooling != true ? builder.Pooling : null,
                SslMode = builder.SslMode,
                AdditionalProperties = additionalProperties
            };
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Builds a MySQL connection string from the current settings
        /// </summary>
        /// <returns>A MySQL connection string</returns>
        /// <exception cref="InvalidOperationException">Thrown when required properties (Hostname, Database) are null or empty</exception>
        public override string BuildConnectionString()
        {
            if (string.IsNullOrWhiteSpace(Hostname))
            {
                throw new InvalidOperationException("Hostname is required for MySQL connection string");
            }

            if (string.IsNullOrWhiteSpace(Database))
            {
                throw new InvalidOperationException("Database is required for MySQL connection string");
            }

            MySqlConnectionStringBuilder builder = new MySqlConnectionStringBuilder
            {
                Server = Hostname,
                Database = Database
            };

            if (Port.HasValue)
            {
                builder.Port = (uint)Port.Value;
            }

            if (!string.IsNullOrWhiteSpace(Username))
            {
                builder.UserID = Username;
            }

            if (!string.IsNullOrWhiteSpace(Password))
            {
                builder.Password = Password;
            }

            if (ConnectionTimeout.HasValue)
            {
                builder.ConnectionTimeout = ConnectionTimeout.Value;
            }

            if (MinimumPoolSize.HasValue)
            {
                builder.MinimumPoolSize = MinimumPoolSize.Value;
            }

            if (MaximumPoolSize.HasValue)
            {
                builder.MaximumPoolSize = MaximumPoolSize.Value;
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
