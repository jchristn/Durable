namespace Durable.MySql
{

    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using Durable;
    using Durable.Sql;
    using MySqlConnector;

    /// <summary>
    /// Connection settings for MySQL repositories
    /// Thread safety: immutable after construction (init-only properties); safe to share.
    /// </summary>
    public sealed class MySqlRepositorySettings : RepositorySettings
    {

        #region Public-Members

        /// <summary>
        /// Gets <see cref="RepositoryType.MySql"/>.
        /// </summary>
        public override RepositoryType Type => RepositoryType.MySql;

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
        /// Gets the TLS mode (MySqlConnector's <see cref="MySqlSslMode"/>). Default: null (the driver default, Preferred).
        /// </summary>
        public MySqlSslMode? SslMode { get; init; }

        /// <summary>
        /// Gets the MySQL-compatible database the settings connect to, which selects the dialect of repositories and
        /// connection factories built from these settings (<see cref="MySqlDialect.For(MySqlFlavor)"/>).
        /// Default: <see cref="MySqlFlavor.MySql"/>. Not part of the connection string: <c>Parse(connectionString)</c> returns the default,
        /// <c>Parse(connectionString, flavor)</c> sets it, and <see cref="BuildConnectionString"/> ignores it.
        /// </summary>
        public MySqlFlavor Flavor { get; init; } = MySqlFlavor.MySql;

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
            return Parse(connectionString, MySqlFlavor.MySql);
        }

        /// <summary>
        /// Parses a MySQL connection string and returns a MySqlRepositorySettings instance
        /// </summary>
        /// <param name="connectionString">The connection string to parse</param>
        /// <param name="flavor">Database flavor stored in <see cref="Flavor"/>.</param>
        /// <returns>A MySqlRepositorySettings instance</returns>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when flavor is not a defined value.</exception>
        /// <exception cref="ArgumentException">Thrown when connectionString is empty or whitespace, or when the connection string is invalid</exception>
        public static MySqlRepositorySettings Parse(string connectionString, MySqlFlavor flavor)
        {
            if (!Enum.IsDefined(flavor)) throw new ArgumentOutOfRangeException(nameof(flavor), flavor, "Unknown MySQL flavor.");
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
                ConnectionTimeout = builder.ConnectionTimeout != 15 ? (int)builder.ConnectionTimeout : null,
                MinPoolSize = builder.MinimumPoolSize != 0 ? (int)builder.MinimumPoolSize : null,
                MaxPoolSize = builder.MaximumPoolSize != 100 ? (int)builder.MaximumPoolSize : null,
                Pooling = builder.Pooling != true ? builder.Pooling : null,
                SslMode = builder.SslMode != MySqlSslMode.Preferred ? builder.SslMode : null,
                AdditionalProperties = additionalProperties,
                Flavor = flavor
            };
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Builds a MySQL connection string from the current settings
        /// </summary>
        /// <returns>A MySQL connection string</returns>
        /// <exception cref="InvalidOperationException">Thrown when Hostname is null or empty</exception>
        /// <exception cref="OverflowException">Thrown when ConnectionTimeout, MinPoolSize or MaxPoolSize is negative.</exception>
        public override string BuildConnectionString()
        {
            if (string.IsNullOrWhiteSpace(Hostname))
            {
                throw new InvalidOperationException("Hostname is required for MySQL connection string");
            }

            MySqlConnectionStringBuilder builder = new MySqlConnectionStringBuilder
            {
                Server = Hostname
            };

            // Database is optional - only add if specified
            if (!string.IsNullOrWhiteSpace(Database))
            {
                builder.Database = Database;
            }

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
                builder.ConnectionTimeout = checked((uint)ConnectionTimeout.Value);
            }

            if (MinPoolSize.HasValue)
            {
                builder.MinimumPoolSize = checked((uint)MinPoolSize.Value);
            }

            if (MaxPoolSize.HasValue)
            {
                builder.MaximumPoolSize = checked((uint)MaxPoolSize.Value);
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
