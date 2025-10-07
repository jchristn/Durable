namespace Durable.SqlServer
{

    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using Durable;
    using Microsoft.Data.SqlClient;

    /// <summary>
    /// Connection settings for SQL Server repositories
    /// </summary>
    public sealed class SqlServerRepositorySettings : RepositorySettings
    {

        #region Public-Members

        /// <summary>
        /// The type of repository
        /// </summary>
        public override RepositoryType Type => RepositoryType.SqlServer;

        /// <summary>
        /// The connection timeout in seconds. Default: null (uses SQL Server default of 15 seconds)
        /// </summary>
        public int? ConnectionTimeout { get; init; }

        /// <summary>
        /// The minimum pool size. Default: null (uses SQL Server default of 0)
        /// </summary>
        public int? MinPoolSize { get; init; }

        /// <summary>
        /// The maximum pool size. Default: null (uses SQL Server default of 100)
        /// </summary>
        public int? MaxPoolSize { get; init; }

        /// <summary>
        /// Whether to use connection pooling. Default: null (uses SQL Server default of true)
        /// </summary>
        public bool? Pooling { get; init; }

        /// <summary>
        /// Whether to encrypt the connection. Default: null (uses SQL Server default)
        /// </summary>
        public bool? Encrypt { get; init; }

        /// <summary>
        /// Whether to trust the server certificate. Default: null (uses SQL Server default of false)
        /// </summary>
        public bool? TrustServerCertificate { get; init; }

        /// <summary>
        /// Whether to use integrated security (Windows authentication). Default: null (uses SQL Server default of false)
        /// </summary>
        public bool? IntegratedSecurity { get; init; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the SqlServerRepositorySettings class
        /// </summary>
        public SqlServerRepositorySettings()
        {
        }

        /// <summary>
        /// Parses a SQL Server connection string and returns a SqlServerRepositorySettings instance
        /// </summary>
        /// <param name="connectionString">The connection string to parse</param>
        /// <returns>A SqlServerRepositorySettings instance</returns>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null</exception>
        /// <exception cref="ArgumentException">Thrown when connectionString is empty or whitespace, or when the connection string is invalid</exception>
        public static SqlServerRepositorySettings Parse(string connectionString)
        {
            ArgumentNullException.ThrowIfNull(connectionString);

            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentException("Connection string cannot be empty or whitespace", nameof(connectionString));
            }

            SqlConnectionStringBuilder builder;

            try
            {
                builder = new SqlConnectionStringBuilder(connectionString);
            }
            catch (Exception ex)
            {
                throw new ArgumentException($"Invalid SQL Server connection string: {ex.Message}", nameof(connectionString), ex);
            }

            Dictionary<string, string>? additionalProperties = null;

            foreach (string key in builder.Keys)
            {
                string lowerKey = key.ToLowerInvariant();

                if (lowerKey != "data source" &&
                    lowerKey != "server" &&
                    lowerKey != "address" &&
                    lowerKey != "addr" &&
                    lowerKey != "network address" &&
                    lowerKey != "user id" &&
                    lowerKey != "uid" &&
                    lowerKey != "user" &&
                    lowerKey != "password" &&
                    lowerKey != "pwd" &&
                    lowerKey != "initial catalog" &&
                    lowerKey != "database" &&
                    lowerKey != "connection timeout" &&
                    lowerKey != "connect timeout" &&
                    lowerKey != "timeout" &&
                    lowerKey != "min pool size" &&
                    lowerKey != "minpoolsize" &&
                    lowerKey != "max pool size" &&
                    lowerKey != "maxpoolsize" &&
                    lowerKey != "pooling" &&
                    lowerKey != "encrypt" &&
                    lowerKey != "trustservercertificate" &&
                    lowerKey != "trust server certificate" &&
                    lowerKey != "integrated security" &&
                    lowerKey != "trusted_connection" &&
                    lowerKey != "authentication")
                {
                    if (additionalProperties == null)
                    {
                        additionalProperties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    }

                    additionalProperties[key] = builder[key]?.ToString() ?? string.Empty;
                }
            }

            string dataSource = builder.DataSource;
            string? hostname = null;
            int? port = null;

            if (!string.IsNullOrWhiteSpace(dataSource))
            {
                string[] parts = dataSource.Split(',');
                hostname = parts[0];

                if (parts.Length > 1 && int.TryParse(parts[1], out int parsedPort))
                {
                    port = parsedPort;
                }
            }

            return new SqlServerRepositorySettings
            {
                Hostname = hostname,
                Port = port,
                Username = !string.IsNullOrEmpty(builder.UserID) ? builder.UserID : null,
                Password = !string.IsNullOrEmpty(builder.Password) ? builder.Password : null,
                Database = builder.InitialCatalog,
                ConnectionTimeout = builder.ConnectTimeout != 15 ? builder.ConnectTimeout : null,
                MinPoolSize = builder.MinPoolSize != 0 ? builder.MinPoolSize : null,
                MaxPoolSize = builder.MaxPoolSize != 100 ? builder.MaxPoolSize : null,
                Pooling = builder.Pooling != true ? builder.Pooling : null,
                Encrypt = builder.Encrypt != false ? builder.Encrypt : null,
                TrustServerCertificate = builder.TrustServerCertificate != false ? builder.TrustServerCertificate : null,
                IntegratedSecurity = builder.IntegratedSecurity != false ? builder.IntegratedSecurity : null,
                AdditionalProperties = additionalProperties
            };
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Builds a SQL Server connection string from the current settings
        /// </summary>
        /// <returns>A SQL Server connection string</returns>
        /// <exception cref="InvalidOperationException">Thrown when required properties are missing</exception>
        public override string BuildConnectionString()
        {
            if (string.IsNullOrWhiteSpace(Hostname))
            {
                throw new InvalidOperationException("Hostname is required for SQL Server connection string");
            }

            if (string.IsNullOrWhiteSpace(Database))
            {
                throw new InvalidOperationException("Database is required for SQL Server connection string");
            }

            string dataSource = Hostname;

            if (Port.HasValue)
            {
                dataSource = $"{Hostname},{Port.Value}";
            }

            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder
            {
                DataSource = dataSource,
                InitialCatalog = Database
            };

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
                builder.ConnectTimeout = ConnectionTimeout.Value;
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

            if (Encrypt.HasValue)
            {
                builder.Encrypt = Encrypt.Value;
            }

            if (TrustServerCertificate.HasValue)
            {
                builder.TrustServerCertificate = TrustServerCertificate.Value;
            }

            if (IntegratedSecurity.HasValue)
            {
                builder.IntegratedSecurity = IntegratedSecurity.Value;
            }

            if (AdditionalProperties != null)
            {
                foreach (KeyValuePair<string, string> kvp in AdditionalProperties)
                {
                    try
                    {
                        builder[kvp.Key] = kvp.Value;
                    }
                    catch (ArgumentException)
                    {
                        // Skip properties that can't be set as strings (enums, etc.)
                    }
                }
            }

            return builder.ConnectionString;
        }

        #endregion

        #region Private-Methods

        #endregion

    }

}
