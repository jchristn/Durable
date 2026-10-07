namespace Durable.MongoDb
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Text.Json;
    using Microsoft.Extensions.Logging;
    using Durable;
    using MongoDB.Driver;

    /// <summary>
    /// Settings for a <see cref="MongoDbBackend"/>: which server and database to use (an existing <see cref="IMongoClient"/>,
    /// a connection string, or host, port and credentials), how the client connects (pool sizes, timeouts, TLS, replica set,
    /// direct connection), and how the backend behaves (transactions, the sequence collection, JSON columns, logging).
    /// <para>
    /// Location: <see cref="Client"/> (never disposed by Durable), otherwise <see cref="ConnectionString"/>, otherwise
    /// <see cref="Hostname"/> and <see cref="Port"/> with the connection options below. A client created from the settings is
    /// owned and disposed by the backend. When <see cref="Client"/> or <see cref="ConnectionString"/> is set, the host and
    /// connection options (<see cref="Hostname"/>, <see cref="Port"/>, <see cref="Username"/>, <see cref="Password"/>,
    /// <see cref="AuthenticationDatabase"/>, <see cref="ReplicaSetName"/>, <see cref="DirectConnection"/>,
    /// <see cref="UseTls"/>, <see cref="ConnectionTimeout"/>, <see cref="ServerSelectionTimeout"/>,
    /// <see cref="MinPoolSize"/>, <see cref="MaxPoolSize"/>, <see cref="ApplicationName"/>) are not used.
    /// </para>
    /// Thread safety: not thread-safe; configure before creating the backend. The backend reads the settings once, when it
    /// is created; later changes have no effect on it.
    /// </summary>
    public sealed class MongoDbRepositorySettings
    {
        #region Public-Members

        /// <summary>
        /// Gets or sets an existing MongoDB client to use; null to create one from <see cref="ConnectionString"/> or the host
        /// settings. The backend never disposes a client passed here.
        /// Default: null.
        /// </summary>
        public IMongoClient? Client { get; set; }

        /// <summary>
        /// Gets or sets a MongoDB connection string (for example <c>mongodb://host:27017/?replicaSet=rs0</c>); null to use the
        /// host settings. Ignored when <see cref="Client"/> is set.
        /// Default: null.
        /// </summary>
        /// <exception cref="ArgumentException">Thrown when value is empty or whitespace.</exception>
        public string? ConnectionString
        {
            get => _ConnectionString;
            set
            {
                if (value != null && string.IsNullOrWhiteSpace(value)) throw new ArgumentException("ConnectionString cannot be empty; use null for none.", nameof(value));
                _ConnectionString = value;
            }
        }

        /// <summary>
        /// Gets or sets the database that holds the collections. Letters, digits, '_' and '-'; at most 63 characters.
        /// Default: "durable".
        /// </summary>
        /// <exception cref="ArgumentNullException">Thrown when value is null.</exception>
        /// <exception cref="ArgumentException">Thrown when value is not a valid MongoDB database name.</exception>
        public string DatabaseName
        {
            get => _DatabaseName;
            set
            {
                ArgumentNullException.ThrowIfNull(value);
                ValidateDatabaseName(value);
                _DatabaseName = value;
            }
        }

        /// <summary>
        /// Gets or sets the server host name.
        /// Default: "localhost".
        /// </summary>
        /// <exception cref="ArgumentNullException">Thrown when value is null.</exception>
        /// <exception cref="ArgumentException">Thrown when value is empty or whitespace.</exception>
        public string Hostname
        {
            get => _Hostname;
            set
            {
                ArgumentNullException.ThrowIfNull(value);
                if (string.IsNullOrWhiteSpace(value)) throw new ArgumentException("Hostname cannot be empty.", nameof(value));
                _Hostname = value;
            }
        }

        /// <summary>
        /// Gets or sets the server port.
        /// Default: 27017. Minimum: 1. Maximum: 65535.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when value is outside the allowed range.</exception>
        public int Port
        {
            get => _Port;
            set
            {
                if (value < 1 || value > 65535) throw new ArgumentOutOfRangeException(nameof(value), "Port must be between 1 and 65535.");
                _Port = value;
            }
        }

        /// <summary>
        /// Gets or sets the user name; null to connect without authentication.
        /// Default: null.
        /// </summary>
        public string? Username { get; set; }

        /// <summary>
        /// Gets or sets the password of <see cref="Username"/>; null for none.
        /// Default: null.
        /// </summary>
        public string? Password { get; set; }

        /// <summary>
        /// Gets or sets the database that holds the user's credentials; null for "admin".
        /// Default: null.
        /// </summary>
        public string? AuthenticationDatabase { get; set; }

        /// <summary>
        /// Gets or sets the replica set name to require; null to accept any topology.
        /// Default: null.
        /// </summary>
        public string? ReplicaSetName { get; set; }

        /// <summary>
        /// Gets or sets whether to talk to <see cref="Hostname"/> only, without discovering the other members of a replica
        /// set (useful when the members advertise host names the client cannot resolve, for example inside containers).
        /// Default: false.
        /// </summary>
        public bool DirectConnection { get; set; } = false;

        /// <summary>
        /// Gets or sets whether to connect with TLS.
        /// Default: false.
        /// </summary>
        public bool UseTls { get; set; } = false;

        /// <summary>
        /// Gets or sets how long opening a connection may take.
        /// Default: 30 seconds. Minimum: 1 second. Maximum: 10 minutes.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when value is outside the allowed range.</exception>
        public TimeSpan ConnectionTimeout
        {
            get => _ConnectionTimeout;
            set
            {
                if (value < TimeSpan.FromSeconds(1) || value > TimeSpan.FromMinutes(10))
                    throw new ArgumentOutOfRangeException(nameof(value), "ConnectionTimeout must be between 1 second and 10 minutes.");
                _ConnectionTimeout = value;
            }
        }

        /// <summary>
        /// Gets or sets how long an operation waits for a suitable server (for example while the server starts or a replica
        /// set elects a primary) before failing.
        /// Default: 30 seconds. Minimum: 100 milliseconds. Maximum: 10 minutes.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when value is outside the allowed range.</exception>
        public TimeSpan ServerSelectionTimeout
        {
            get => _ServerSelectionTimeout;
            set
            {
                if (value < TimeSpan.FromMilliseconds(100) || value > TimeSpan.FromMinutes(10))
                    throw new ArgumentOutOfRangeException(nameof(value), "ServerSelectionTimeout must be between 100 milliseconds and 10 minutes.");
                _ServerSelectionTimeout = value;
            }
        }

        /// <summary>
        /// Gets or sets the minimum number of pooled connections per server.
        /// Default: 0. Minimum: 0. Maximum: <see cref="MaxPoolSize"/> (checked by <see cref="Validate"/>).
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when value is negative.</exception>
        public int MinPoolSize
        {
            get => _MinPoolSize;
            set
            {
                if (value < 0) throw new ArgumentOutOfRangeException(nameof(value), "MinPoolSize cannot be negative.");
                _MinPoolSize = value;
            }
        }

        /// <summary>
        /// Gets or sets the maximum number of pooled connections per server.
        /// Default: 100. Minimum: 1. Maximum: 10000.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when value is outside the allowed range.</exception>
        public int MaxPoolSize
        {
            get => _MaxPoolSize;
            set
            {
                if (value < 1 || value > 10000) throw new ArgumentOutOfRangeException(nameof(value), "MaxPoolSize must be between 1 and 10000.");
                _MaxPoolSize = value;
            }
        }

        /// <summary>
        /// Gets or sets the application name reported to the server (visible in server logs and <c>currentOp</c>); null for none.
        /// Default: null.
        /// </summary>
        public string? ApplicationName { get; set; }

        /// <summary>
        /// Gets or sets whether the backend offers transactions (<see cref="RepositoryCapabilities.Transactions"/>). Null
        /// detects it when the backend is created: MongoDB supports multi-document transactions on replica sets and sharded
        /// clusters, not on a standalone server. Set false to never use transactions (writes of several documents, such as
        /// CreateMany, then are not atomic); set true to skip detection when the deployment is known to support them.
        /// Default: null.
        /// </summary>
        public bool? TransactionsEnabled { get; set; }

        /// <summary>
        /// Gets or sets the collection that holds the auto-increment sequences (one document per entity collection).
        /// Default: "durable_sequences".
        /// </summary>
        /// <exception cref="ArgumentNullException">Thrown when value is null.</exception>
        /// <exception cref="ArgumentException">Thrown when value is empty, contains '$' or a null character, or starts with "system.".</exception>
        public string SequenceCollectionName
        {
            get => _SequenceCollectionName;
            set
            {
                ArgumentNullException.ThrowIfNull(value);
                if (value.Length == 0 || value.IndexOf('$') >= 0 || value.IndexOf('\0') >= 0 || value.StartsWith("system.", StringComparison.Ordinal))
                    throw new ArgumentException("SequenceCollectionName '" + value + "' is not a valid MongoDB collection name.", nameof(value));
                _SequenceCollectionName = value;
            }
        }

        /// <summary>
        /// Gets whether the settings select a database held only in memory: always false (MongoDB is a server; use a
        /// disposable database or container for tests). Present so every backend's settings have the same shape.
        /// </summary>
        public bool IsInMemory => false;

        /// <summary>
        /// Gets or sets the logger for query plans (Debug) and index maintenance problems (Warning); null for none.
        /// Default: null.
        /// </summary>
        public ILogger? Logger { get; set; }

        /// <summary>
        /// Gets or sets the JSON options used to store JSON columns; null for camelCase, non-indented output (the same as
        /// the SQL providers).
        /// Default: null.
        /// </summary>
        public JsonSerializerOptions? JsonOptions { get; set; }

        #endregion

        #region Private-Members

        private string? _ConnectionString = null;
        private string _DatabaseName = "durable";
        private string _Hostname = "localhost";
        private int _Port = 27017;
        private TimeSpan _ConnectionTimeout = TimeSpan.FromSeconds(30);
        private TimeSpan _ServerSelectionTimeout = TimeSpan.FromSeconds(30);
        private int _MinPoolSize = 0;
        private int _MaxPoolSize = 100;
        private string _SequenceCollectionName = "durable_sequences";

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates settings for database "durable" on localhost:27017.
        /// </summary>
        public MongoDbRepositorySettings()
        {
        }

        /// <summary>
        /// Creates settings for an existing client, which the backend does not dispose.
        /// </summary>
        /// <param name="client">Client. Must not be null.</param>
        /// <param name="databaseName">Database name. Must not be null; see <see cref="DatabaseName"/>.</param>
        /// <returns>The settings. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when client or databaseName is null.</exception>
        /// <exception cref="ArgumentException">Thrown when databaseName is invalid.</exception>
        public static MongoDbRepositorySettings ForClient(IMongoClient client, string databaseName)
        {
            ArgumentNullException.ThrowIfNull(client);
            return new MongoDbRepositorySettings { Client = client, DatabaseName = databaseName };
        }

        /// <summary>
        /// Creates settings for a connection string.
        /// </summary>
        /// <param name="connectionString">Connection string. Must not be null or empty.</param>
        /// <param name="databaseName">Database name; null uses the database of the connection string, or "durable" when it names none.</param>
        /// <returns>The settings. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null.</exception>
        /// <exception cref="ArgumentException">Thrown when connectionString is empty or databaseName is invalid.</exception>
        /// <exception cref="MongoConfigurationException">Thrown when the connection string cannot be parsed.</exception>
        public static MongoDbRepositorySettings ForConnectionString(string connectionString, string? databaseName = null)
        {
            ArgumentNullException.ThrowIfNull(connectionString);
            MongoDbRepositorySettings settings = new MongoDbRepositorySettings { ConnectionString = connectionString };
            string? fromUrl = databaseName == null ? new MongoUrl(connectionString).DatabaseName : null;
            settings.DatabaseName = databaseName ?? (string.IsNullOrEmpty(fromUrl) ? "durable" : fromUrl);
            return settings;
        }

        /// <summary>
        /// Creates settings for a server host.
        /// </summary>
        /// <param name="hostname">Host name. Must not be null or empty.</param>
        /// <param name="port">Port. Minimum: 1. Maximum: 65535.</param>
        /// <param name="databaseName">Database name. Must not be null; see <see cref="DatabaseName"/>.</param>
        /// <returns>The settings. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when hostname or databaseName is null.</exception>
        /// <exception cref="ArgumentException">Thrown when hostname is empty or databaseName is invalid.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when port is outside the allowed range.</exception>
        public static MongoDbRepositorySettings ForHost(string hostname, int port = 27017, string databaseName = "durable")
        {
            return new MongoDbRepositorySettings { Hostname = hostname, Port = port, DatabaseName = databaseName };
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Validates the settings. <see cref="MongoDbBackend.Create"/> calls it.
        /// </summary>
        /// <exception cref="ArgumentException">Thrown when <see cref="Client"/> is combined with <see cref="ConnectionString"/>, when <see cref="MinPoolSize"/> exceeds <see cref="MaxPoolSize"/>, or when <see cref="Password"/> is set without <see cref="Username"/>.</exception>
        public void Validate()
        {
            if (Client != null && _ConnectionString != null)
                throw new ArgumentException("MongoDB settings must set either Client or ConnectionString, not both.");
            if (_MinPoolSize > _MaxPoolSize)
                throw new ArgumentException("MinPoolSize (" + _MinPoolSize.ToString(CultureInfo.InvariantCulture) + ") cannot exceed MaxPoolSize (" + _MaxPoolSize.ToString(CultureInfo.InvariantCulture) + ").");
            if (Password != null && string.IsNullOrEmpty(Username))
                throw new ArgumentException("MongoDB settings set a Password without a Username.");
        }

        /// <summary>
        /// Builds the driver settings for a client created from these settings (not used when <see cref="Client"/> is set).
        /// </summary>
        /// <returns>The client settings. Never null.</returns>
        /// <exception cref="MongoConfigurationException">Thrown when <see cref="ConnectionString"/> cannot be parsed.</exception>
        public MongoClientSettings ToClientSettings()
        {
            if (_ConnectionString != null) return MongoClientSettings.FromConnectionString(_ConnectionString);

            MongoClientSettings settings = new MongoClientSettings
            {
                Server = new MongoServerAddress(_Hostname, _Port),
                DirectConnection = DirectConnection,
                UseTls = UseTls,
                ConnectTimeout = _ConnectionTimeout,
                ServerSelectionTimeout = _ServerSelectionTimeout,
                MinConnectionPoolSize = _MinPoolSize,
                MaxConnectionPoolSize = _MaxPoolSize
            };

            if (!string.IsNullOrEmpty(ReplicaSetName)) settings.ReplicaSetName = ReplicaSetName;
            if (!string.IsNullOrEmpty(ApplicationName)) settings.ApplicationName = ApplicationName;
            if (!string.IsNullOrEmpty(Username))
                settings.Credential = MongoCredential.CreateCredential(string.IsNullOrEmpty(AuthenticationDatabase) ? "admin" : AuthenticationDatabase, Username, Password ?? string.Empty);
            return settings;
        }

        /// <inheritdoc />
        public override string ToString()
        {
            if (Client != null) return "MongoDB (existing client) database '" + _DatabaseName + "'";
            if (_ConnectionString != null) return "MongoDB (connection string) database '" + _DatabaseName + "'";
            return "MongoDB " + _Hostname + ":" + _Port.ToString(CultureInfo.InvariantCulture) + " database '" + _DatabaseName + "'"
                + (Username != null ? " as " + Username : string.Empty) + (UseTls ? " (TLS)" : string.Empty);
        }

        #endregion

        #region Private-Methods

        private static void ValidateDatabaseName(string name)
        {
            if (name.Length == 0 || name.Length > 63)
                throw new ArgumentException("DatabaseName must have between 1 and 63 characters.", nameof(name));
            foreach (char c in name)
            {
                bool valid = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-';
                if (!valid) throw new ArgumentException("DatabaseName '" + name + "' may contain only letters, digits, '_' and '-'.", nameof(name));
            }
        }

        #endregion
    }
}
