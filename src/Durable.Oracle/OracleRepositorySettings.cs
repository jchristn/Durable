namespace Durable.Oracle
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using Durable;
    using Durable.Sql;
    using global::Oracle.ManagedDataAccess.Client;

    /// <summary>
    /// Connection settings for Oracle repositories (ODP.NET managed driver). The server is addressed either with
    /// <see cref="RepositorySettings.Hostname"/>, <see cref="RepositorySettings.Port"/> and
    /// <see cref="RepositorySettings.Database"/> (the service name, for example FREEPDB1), written as an EZConnect data
    /// source <c>host:port/service</c>, or with <see cref="DataSource"/> (a TNS alias, EZConnect string or full connect
    /// descriptor), which takes precedence.
    /// Thread safety: immutable after construction (init-only properties); safe to share.
    /// </summary>
    public sealed class OracleRepositorySettings : RepositorySettings
    {
        #region Public-Members

        /// <summary>
        /// Gets <see cref="RepositoryType.Oracle"/>.
        /// </summary>
        public override RepositoryType Type => RepositoryType.Oracle;

        /// <summary>
        /// Gets the driver data source (a TNS alias, an EZConnect string such as <c>db.example:1521/ORCLPDB1</c>, or a full
        /// connect descriptor). Default: null (built from Hostname, Port and Database). When set it is written verbatim and
        /// Hostname, Port and Database are ignored.
        /// </summary>
        public string? DataSource { get; init; }

        /// <summary>
        /// Gets the connection (login) timeout in seconds. Default: null (the driver default, 15 seconds). Minimum: 0.
        /// Maps to the driver's "Connection Timeout" keyword.
        /// </summary>
        public int? ConnectionTimeout { get; init; }

        /// <summary>
        /// Gets the minimum number of pooled connections the driver keeps open. Default: null (the driver default, 1).
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

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the OracleRepositorySettings class.
        /// </summary>
        public OracleRepositorySettings()
        {
        }

        /// <summary>
        /// Parses an ODP.NET connection string. An EZConnect data source of the form <c>host[:port]/service</c> is split
        /// into Hostname, Port and Database; any other data source is kept in <see cref="DataSource"/>. Keywords without a
        /// typed property are kept in <see cref="RepositorySettings.AdditionalProperties"/>.
        /// </summary>
        /// <param name="connectionString">The connection string. Must not be null.</param>
        /// <returns>The settings. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null.</exception>
        /// <exception cref="ArgumentException">Thrown when connectionString is empty, whitespace or invalid.</exception>
        public static OracleRepositorySettings Parse(string connectionString)
        {
            ArgumentNullException.ThrowIfNull(connectionString);
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentException("Connection string cannot be empty or whitespace", nameof(connectionString));

            OracleConnectionStringBuilder builder;
            try
            {
                builder = new OracleConnectionStringBuilder(connectionString);
            }
            catch (Exception ex)
            {
                throw new ArgumentException("Invalid Oracle connection string: " + ex.Message, nameof(connectionString), ex);
            }

            Dictionary<string, string>? additionalProperties = null;
            foreach (string key in builder.Keys)
            {
                if (!builder.ContainsKey(key) || !builder.ShouldSerialize(key)) continue;
                string lowerKey = key.ToLowerInvariant();
                if (lowerKey == "data source" || lowerKey == "user id" || lowerKey == "password" || lowerKey == "connection timeout" ||
                    lowerKey == "min pool size" || lowerKey == "max pool size" || lowerKey == "pooling")
                {
                    continue;
                }

                additionalProperties ??= new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                additionalProperties[key] = Convert.ToString(builder[key], CultureInfo.InvariantCulture) ?? string.Empty;
            }

            string? hostname = null;
            int? port = null;
            string? database = null;
            string? dataSource = string.IsNullOrWhiteSpace(builder.DataSource) ? null : builder.DataSource;
            if (dataSource != null && TrySplitEzConnect(dataSource, out string? parsedHost, out int? parsedPort, out string? parsedService))
            {
                hostname = parsedHost;
                port = parsedPort;
                database = parsedService;
                dataSource = null;
            }

            return new OracleRepositorySettings
            {
                Hostname = hostname,
                Port = port,
                Database = database,
                DataSource = dataSource,
                Username = string.IsNullOrEmpty(builder.UserID) ? null : builder.UserID,
                Password = string.IsNullOrEmpty(builder.Password) ? null : builder.Password,
                ConnectionTimeout = builder.ShouldSerialize("Connection Timeout") ? builder.ConnectionTimeout : null,
                MinPoolSize = builder.ShouldSerialize("Min Pool Size") ? builder.MinPoolSize : null,
                MaxPoolSize = builder.ShouldSerialize("Max Pool Size") ? builder.MaxPoolSize : null,
                Pooling = builder.ShouldSerialize("Pooling") ? builder.Pooling : null,
                AdditionalProperties = additionalProperties
            };
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Builds an ODP.NET connection string from the settings.
        /// </summary>
        /// <returns>The connection string. Never null.</returns>
        /// <exception cref="InvalidOperationException">Thrown when neither <see cref="DataSource"/> nor Hostname and
        /// Database (the service name) are set.</exception>
        public override string BuildConnectionString()
        {
            string dataSource;
            if (!string.IsNullOrWhiteSpace(DataSource))
            {
                dataSource = DataSource;
            }
            else
            {
                if (string.IsNullOrWhiteSpace(Hostname)) throw new InvalidOperationException("Hostname (or DataSource) is required for an Oracle connection string");
                if (string.IsNullOrWhiteSpace(Database)) throw new InvalidOperationException("Database (the service name, or DataSource) is required for an Oracle connection string");
                dataSource = Hostname + (Port.HasValue ? ":" + Port.Value.ToString(CultureInfo.InvariantCulture) : string.Empty) + "/" + Database;
            }

            OracleConnectionStringBuilder builder = new OracleConnectionStringBuilder { DataSource = dataSource };
            if (!string.IsNullOrWhiteSpace(Username)) builder.UserID = Username;
            if (!string.IsNullOrWhiteSpace(Password)) builder.Password = Password;
            if (ConnectionTimeout.HasValue) builder.ConnectionTimeout = ConnectionTimeout.Value;
            if (MinPoolSize.HasValue) builder.MinPoolSize = MinPoolSize.Value;
            if (MaxPoolSize.HasValue) builder.MaxPoolSize = MaxPoolSize.Value;
            if (Pooling.HasValue) builder.Pooling = Pooling.Value;
            if (AdditionalProperties != null)
            {
                foreach (KeyValuePair<string, string> property in AdditionalProperties)
                {
                    try
                    {
                        builder[property.Key] = property.Value;
                    }
                    catch (ArgumentException)
                    {
                        // Keywords the driver rejects as strings are skipped, as for the other providers.
                    }
                }
            }

            return builder.ConnectionString;
        }

        #endregion

        #region Private-Methods

        private static bool TrySplitEzConnect(string dataSource, out string? host, out int? port, out string? service)
        {
            host = null;
            port = null;
            service = null;
            string text = dataSource.Trim();
            if (text.StartsWith("//", StringComparison.Ordinal)) text = text.Substring(2);
            if (text.IndexOfAny(new[] { '(', ')', '=', ' ', '?' }) >= 0 || text.Contains("://", StringComparison.Ordinal)) return false;
            int slash = text.IndexOf('/');
            if (slash <= 0 || slash == text.Length - 1 || text.IndexOf('/', slash + 1) >= 0) return false;
            string address = text.Substring(0, slash);
            service = text.Substring(slash + 1);
            int colon = address.LastIndexOf(':');
            if (colon > 0 && address.IndexOf(':') == colon)
            {
                if (!int.TryParse(address.Substring(colon + 1), NumberStyles.None, CultureInfo.InvariantCulture, out int parsed)) return false;
                port = parsed;
                address = address.Substring(0, colon);
            }
            else if (colon >= 0)
            {
                return false;
            }

            host = address;
            return host.Length > 0;
        }

        #endregion
    }
}
