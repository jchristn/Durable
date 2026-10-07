namespace Durable.CosmosDb
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Text.Json;
    using Microsoft.Extensions.Logging;
    using Durable;
    using Microsoft.Azure.Cosmos;

    /// <summary>
    /// Settings for a <see cref="CosmosDbBackend"/>: which account and database to use (an existing
    /// <see cref="CosmosClient"/>, an endpoint and key, a connection string, or the local emulator), how an owned client
    /// connects (connection mode, endpoint discovery, timeouts), how storage is provisioned (database and container
    /// throughput, creation on first use), how entities are partitioned (<see cref="PartitionKeys"/>), and how the backend
    /// behaves (JSON columns, logging, conflict retries).
    /// <para>
    /// Account: exactly one of <see cref="Client"/> (never disposed by Durable; the client options below then do not apply),
    /// <see cref="Endpoint"/> with <see cref="AccountKey"/>, or <see cref="ConnectionString"/>. A client built from the
    /// endpoint or connection string is owned and disposed by the backend.
    /// </para>
    /// <para>
    /// Partitioning: by default every entity is partitioned by its document id (<c>/id</c>, one logical partition per
    /// document), which spreads load evenly, makes every key lookup a point read and needs no design work, but means every
    /// filter that does not name a key is a cross-partition query. Map an entity to one of its properties (for example a
    /// tenant or customer id) with <see cref="WithPartitionKey{T}"/> to keep related documents together and make queries
    /// that filter on that property single-partition; key lookups then need the partition key value (point reads when the
    /// property is part of the primary key, otherwise a cross-partition query by id), and Durable checks key uniqueness
    /// across partitions with an extra query on insert (Cosmos DB ids are unique only within a partition).
    /// </para>
    /// Thread safety: not thread-safe; configure before creating the backend. The backend reads the settings once, when it
    /// is created; later changes have no effect on it.
    /// </summary>
    public sealed class CosmosDbRepositorySettings
    {
        #region Public-Members

        /// <summary>
        /// The well-known account key of the Azure Cosmos DB emulator (public; never use it for a real account).
        /// </summary>
        public const string EmulatorAccountKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

        /// <summary>
        /// The default emulator endpoint.
        /// </summary>
        public const string EmulatorEndpoint = "http://localhost:8081/";

        /// <summary>
        /// The default database name.
        /// </summary>
        public const string DefaultDatabaseName = "durable";

        /// <summary>
        /// Gets or sets an existing client to use; null to create one from <see cref="Endpoint"/> and
        /// <see cref="AccountKey"/> or from <see cref="ConnectionString"/>. The backend never disposes a client passed here.
        /// Default: null.
        /// </summary>
        public CosmosClient? Client { get; set; }

        /// <summary>
        /// Gets or sets the account endpoint, for example <c>https://myaccount.documents.azure.com:443/</c>; used with
        /// <see cref="AccountKey"/>. Default: null.
        /// </summary>
        /// <exception cref="ArgumentException">Thrown when value is not an absolute http or https URI.</exception>
        public string? Endpoint
        {
            get => _Endpoint;
            set
            {
                if (value != null && (!Uri.TryCreate(value, UriKind.Absolute, out Uri? uri) || (uri.Scheme != Uri.UriSchemeHttp && uri.Scheme != Uri.UriSchemeHttps)))
                    throw new ArgumentException("Endpoint must be an absolute http or https URI.", nameof(value));
                _Endpoint = value;
            }
        }

        /// <summary>
        /// Gets or sets the account key (primary or secondary key, or a resource token) used with <see cref="Endpoint"/>.
        /// Never logged or included in <see cref="ToString"/>. Default: null.
        /// </summary>
        public string? AccountKey { get; set; }

        /// <summary>
        /// Gets or sets an account connection string (<c>AccountEndpoint=...;AccountKey=...;</c>); an alternative to
        /// <see cref="Endpoint"/> and <see cref="AccountKey"/>. Never logged or included in <see cref="ToString"/>.
        /// Default: null.
        /// </summary>
        public string? ConnectionString { get; set; }

        /// <summary>
        /// Gets or sets the database that holds the containers (created on first use unless <see cref="CreateIfNotExists"/>
        /// is false). Default: <see cref="DefaultDatabaseName"/>.
        /// </summary>
        /// <exception cref="ArgumentNullException">Thrown when value is null.</exception>
        /// <exception cref="ArgumentException">Thrown when value is not a valid Cosmos DB database name (1 to 255 characters, no '/', '\', '#' or '?', no trailing space).</exception>
        public string DatabaseName
        {
            get => _DatabaseName;
            set
            {
                ArgumentNullException.ThrowIfNull(value);
                ValidateResourceName(value, nameof(value), "DatabaseName");
                _DatabaseName = value;
            }
        }

        /// <summary>
        /// Gets or sets the connection mode of an owned client; null uses the SDK default (<see cref="ConnectionMode.Direct"/>).
        /// The emulator supports only <see cref="ConnectionMode.Gateway"/>. Default: null.
        /// </summary>
        public ConnectionMode? ConnectionMode { get; set; }

        /// <summary>
        /// Gets or sets whether an owned client talks only to <see cref="Endpoint"/> and does not discover regional
        /// endpoints. Required when the endpoint is reached through a mapped port (for example a container's random host
        /// port). Default: false.
        /// </summary>
        public bool LimitToEndpoint { get; set; } = false;

        /// <summary>
        /// Gets or sets whether an owned client accepts any server TLS certificate (only for the emulator's self-signed
        /// certificate in development; never in production). Default: false.
        /// </summary>
        public bool AcceptAnyServerCertificate { get; set; } = false;

        /// <summary>
        /// Gets or sets the request timeout of an owned client; null uses the SDK default (6 seconds per network request,
        /// retried by the SDK). Default: null. Minimum: 1 second. Maximum: 10 minutes.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when value is outside the allowed range.</exception>
        public TimeSpan? RequestTimeout
        {
            get => _RequestTimeout;
            set
            {
                if (value.HasValue && (value.Value < TimeSpan.FromSeconds(1) || value.Value > TimeSpan.FromMinutes(10)))
                    throw new ArgumentOutOfRangeException(nameof(value), "RequestTimeout must be between 1 second and 10 minutes.");
                _RequestTimeout = value;
            }
        }

        /// <summary>
        /// Gets or sets the application name an owned client adds to its user agent; null for none. Default: null.
        /// </summary>
        public string? ApplicationName { get; set; }

        /// <summary>
        /// Gets or sets whether the database and containers are created when missing (database when the backend is created,
        /// each container on its first use). When false, a missing database or container fails the operation. Default: true.
        /// </summary>
        public bool CreateIfNotExists { get; set; } = true;

        /// <summary>
        /// Gets or sets the manual throughput (RU/s) of a database created by the backend, shared by its containers; null for
        /// none (serverless accounts, or per-container throughput). Applies only when the database is created.
        /// Default: null. Minimum: 400. Must be a multiple of 100.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when value is below 400 or not a multiple of 100.</exception>
        public int? DatabaseThroughput
        {
            get => _DatabaseThroughput;
            set
            {
                ValidateManualThroughput(value, "DatabaseThroughput");
                _DatabaseThroughput = value;
            }
        }

        /// <summary>
        /// Gets or sets the autoscale maximum throughput (RU/s) of a database created by the backend; null for none. Cannot be
        /// combined with <see cref="DatabaseThroughput"/>. Default: null. Minimum: 1000. Must be a multiple of 1000.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when value is below 1000 or not a multiple of 1000.</exception>
        public int? DatabaseAutoscaleMaxThroughput
        {
            get => _DatabaseAutoscaleMaxThroughput;
            set
            {
                ValidateAutoscaleThroughput(value, "DatabaseAutoscaleMaxThroughput");
                _DatabaseAutoscaleMaxThroughput = value;
            }
        }

        /// <summary>
        /// Gets or sets the manual throughput (RU/s) of each container created by the backend (including the sequence
        /// container); null for none (shared database throughput or serverless). Applies only when a container is created.
        /// Default: null. Minimum: 400. Must be a multiple of 100.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when value is below 400 or not a multiple of 100.</exception>
        public int? ContainerThroughput
        {
            get => _ContainerThroughput;
            set
            {
                ValidateManualThroughput(value, "ContainerThroughput");
                _ContainerThroughput = value;
            }
        }

        /// <summary>
        /// Gets or sets the autoscale maximum throughput (RU/s) of each container created by the backend; null for none.
        /// Cannot be combined with <see cref="ContainerThroughput"/>. Default: null. Minimum: 1000. Must be a multiple of 1000.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when value is below 1000 or not a multiple of 1000.</exception>
        public int? ContainerAutoscaleMaxThroughput
        {
            get => _ContainerAutoscaleMaxThroughput;
            set
            {
                ValidateAutoscaleThroughput(value, "ContainerAutoscaleMaxThroughput");
                _ContainerAutoscaleMaxThroughput = value;
            }
        }

        /// <summary>
        /// Gets the partition key of each entity type that is not partitioned by its document id: the entity type mapped to
        /// the name of one of its mapped properties (or columns). The property's values must be strings, integers, Guids,
        /// dates or booleans. Entities not listed are partitioned by <c>/id</c>. Never null. Default: empty.
        /// </summary>
        public IDictionary<Type, string> PartitionKeys { get; } = new Dictionary<Type, string>();

        /// <summary>
        /// Gets or sets the container that holds the auto-increment counters (one document per entity container, partitioned
        /// by <c>/id</c>), created on first use. Default: "durable_sequences".
        /// </summary>
        /// <exception cref="ArgumentNullException">Thrown when value is null.</exception>
        /// <exception cref="ArgumentException">Thrown when value is not a valid container name.</exception>
        public string SequenceContainerName
        {
            get => _SequenceContainerName;
            set
            {
                ArgumentNullException.ThrowIfNull(value);
                ValidateResourceName(value, nameof(value), "SequenceContainerName");
                _SequenceContainerName = value;
            }
        }

        /// <summary>
        /// Gets or sets how many times a conditional write (ETag-checked replace, delete or counter increment) is retried after
        /// another writer changed the document first. Default: 100. Minimum: 1. Maximum: 10000.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when value is outside the allowed range.</exception>
        public int MaxConflictRetries
        {
            get => _MaxConflictRetries;
            set
            {
                if (value < 1 || value > 10000) throw new ArgumentOutOfRangeException(nameof(value), "MaxConflictRetries must be between 1 and 10000.");
                _MaxConflictRetries = value;
            }
        }

        /// <summary>
        /// Gets or sets how many documents <see cref="CosmosDbBackend.Clear(Type)"/> deletes concurrently.
        /// Default: 8. Minimum: 1. Maximum: 256.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when value is outside the allowed range.</exception>
        public int MaxConcurrentDeletes
        {
            get => _MaxConcurrentDeletes;
            set
            {
                if (value < 1 || value > 256) throw new ArgumentOutOfRangeException(nameof(value), "MaxConcurrentDeletes must be between 1 and 256.");
                _MaxConcurrentDeletes = value;
            }
        }

        /// <summary>
        /// Gets whether the settings select a database held only in memory. Always false: Cosmos DB has no in-memory mode
        /// (use the emulator, <see cref="ForEmulator"/>, for local development and tests).
        /// </summary>
        public bool IsInMemory => false;

        /// <summary>
        /// Gets or sets the logger for query plans (Debug) and conflict retries (Debug); null for none.
        /// Default: null.
        /// </summary>
        public ILogger? Logger { get; set; }

        /// <summary>
        /// Gets or sets the JSON options used to store JSON columns; null for camelCase, non-indented output (the same as
        /// the SQL providers). Documents themselves are always written by Durable from entity metadata. Under Native AOT,
        /// pass options with a source-generated context (<see cref="DurableJson.CreateOptions"/>).
        /// Default: null.
        /// </summary>
        public JsonSerializerOptions? JsonOptions { get; set; }

        #endregion

        #region Private-Members

        private string? _Endpoint = null;
        private string _DatabaseName = DefaultDatabaseName;
        private TimeSpan? _RequestTimeout = null;
        private int? _DatabaseThroughput = null;
        private int? _DatabaseAutoscaleMaxThroughput = null;
        private int? _ContainerThroughput = null;
        private int? _ContainerAutoscaleMaxThroughput = null;
        private string _SequenceContainerName = "durable_sequences";
        private int _MaxConflictRetries = 100;
        private int _MaxConcurrentDeletes = 8;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates empty settings; set <see cref="Client"/>, <see cref="Endpoint"/> and <see cref="AccountKey"/>, or
        /// <see cref="ConnectionString"/> before use.
        /// </summary>
        public CosmosDbRepositorySettings()
        {
        }

        /// <summary>
        /// Creates settings for the local Azure Cosmos DB emulator: the well-known emulator key, gateway mode, no endpoint
        /// discovery (so a mapped port works), and any server certificate accepted (the emulator's certificate is
        /// self-signed).
        /// </summary>
        /// <param name="endpoint">Emulator endpoint. Default: <see cref="EmulatorEndpoint"/>.</param>
        /// <param name="databaseName">Database name. Default: <see cref="DefaultDatabaseName"/>.</param>
        /// <returns>The settings. Never null.</returns>
        /// <exception cref="ArgumentException">Thrown when endpoint or databaseName is invalid.</exception>
        public static CosmosDbRepositorySettings ForEmulator(string endpoint = EmulatorEndpoint, string databaseName = DefaultDatabaseName)
        {
            return new CosmosDbRepositorySettings
            {
                Endpoint = endpoint,
                AccountKey = EmulatorAccountKey,
                DatabaseName = databaseName,
                ConnectionMode = Microsoft.Azure.Cosmos.ConnectionMode.Gateway,
                LimitToEndpoint = true,
                AcceptAnyServerCertificate = true
            };
        }

        /// <summary>
        /// Creates settings for an account endpoint and key.
        /// </summary>
        /// <param name="endpoint">Account endpoint. Must not be null.</param>
        /// <param name="accountKey">Account key. Must not be null or empty.</param>
        /// <param name="databaseName">Database name. Default: <see cref="DefaultDatabaseName"/>.</param>
        /// <returns>The settings. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when endpoint or accountKey is null.</exception>
        /// <exception cref="ArgumentException">Thrown when an argument is invalid.</exception>
        public static CosmosDbRepositorySettings ForEndpoint(string endpoint, string accountKey, string databaseName = DefaultDatabaseName)
        {
            ArgumentNullException.ThrowIfNull(endpoint);
            ArgumentNullException.ThrowIfNull(accountKey);
            if (accountKey.Length == 0) throw new ArgumentException("The account key cannot be empty.", nameof(accountKey));
            return new CosmosDbRepositorySettings { Endpoint = endpoint, AccountKey = accountKey, DatabaseName = databaseName };
        }

        /// <summary>
        /// Creates settings for an account connection string.
        /// </summary>
        /// <param name="connectionString">Connection string. Must not be null or empty.</param>
        /// <param name="databaseName">Database name. Default: <see cref="DefaultDatabaseName"/>.</param>
        /// <returns>The settings. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null.</exception>
        /// <exception cref="ArgumentException">Thrown when an argument is invalid.</exception>
        public static CosmosDbRepositorySettings ForConnectionString(string connectionString, string databaseName = DefaultDatabaseName)
        {
            ArgumentNullException.ThrowIfNull(connectionString);
            if (connectionString.Trim().Length == 0) throw new ArgumentException("The connection string cannot be empty.", nameof(connectionString));
            return new CosmosDbRepositorySettings { ConnectionString = connectionString, DatabaseName = databaseName };
        }

        /// <summary>
        /// Creates settings for an existing client, which the backend does not dispose.
        /// </summary>
        /// <param name="client">Client. Must not be null.</param>
        /// <param name="databaseName">Database name. Default: <see cref="DefaultDatabaseName"/>.</param>
        /// <returns>The settings. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when client is null.</exception>
        /// <exception cref="ArgumentException">Thrown when databaseName is invalid.</exception>
        public static CosmosDbRepositorySettings ForClient(CosmosClient client, string databaseName = DefaultDatabaseName)
        {
            ArgumentNullException.ThrowIfNull(client);
            return new CosmosDbRepositorySettings { Client = client, DatabaseName = databaseName };
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Partitions an entity type by one of its properties instead of its document id (see <see cref="PartitionKeys"/>).
        /// </summary>
        /// <typeparam name="T">Entity type.</typeparam>
        /// <param name="propertyName">Name of a mapped property (use <c>nameof</c>). Must not be null or empty.</param>
        /// <returns>These settings, for chaining.</returns>
        /// <exception cref="ArgumentNullException">Thrown when propertyName is null.</exception>
        /// <exception cref="ArgumentException">Thrown when propertyName is empty.</exception>
        public CosmosDbRepositorySettings WithPartitionKey<[DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] T>(string propertyName) where T : class
        {
            ArgumentNullException.ThrowIfNull(propertyName);
            if (propertyName.Trim().Length == 0) throw new ArgumentException("The partition key property name cannot be empty.", nameof(propertyName));
            PartitionKeys[typeof(T)] = propertyName;
            return this;
        }

        /// <summary>
        /// Validates the settings. <see cref="CosmosDbBackend.Create"/> calls it.
        /// </summary>
        /// <exception cref="ArgumentException">
        /// Thrown when not exactly one of <see cref="Client"/>, <see cref="Endpoint"/> with <see cref="AccountKey"/>, or
        /// <see cref="ConnectionString"/> is set; when <see cref="Client"/> is combined with options for an owned client; or
        /// when manual and autoscale throughput are both set for the database or for containers.
        /// </exception>
        public void Validate()
        {
            int sources = (Client != null ? 1 : 0) + (_Endpoint != null || AccountKey != null ? 1 : 0) + (ConnectionString != null ? 1 : 0);
            if (sources != 1)
                throw new ArgumentException("Cosmos DB settings must set exactly one of Client, Endpoint with AccountKey, or ConnectionString.");
            if ((_Endpoint != null) != (AccountKey != null))
                throw new ArgumentException("Cosmos DB settings must set Endpoint and AccountKey together.");
            if (AccountKey != null && AccountKey.Length == 0)
                throw new ArgumentException("Cosmos DB settings: AccountKey cannot be empty.");
            if (Client != null && (ConnectionMode != null || LimitToEndpoint || AcceptAnyServerCertificate || _RequestTimeout != null || ApplicationName != null))
                throw new ArgumentException("Cosmos DB settings: ConnectionMode, LimitToEndpoint, AcceptAnyServerCertificate, RequestTimeout and ApplicationName configure a client the backend creates; they cannot be combined with Client.");
            if (_DatabaseThroughput != null && _DatabaseAutoscaleMaxThroughput != null)
                throw new ArgumentException("Cosmos DB settings: set DatabaseThroughput or DatabaseAutoscaleMaxThroughput, not both.");
            if (_ContainerThroughput != null && _ContainerAutoscaleMaxThroughput != null)
                throw new ArgumentException("Cosmos DB settings: set ContainerThroughput or ContainerAutoscaleMaxThroughput, not both.");
            foreach (KeyValuePair<Type, string> partitionKey in PartitionKeys)
            {
                if (partitionKey.Key == null || string.IsNullOrWhiteSpace(partitionKey.Value))
                    throw new ArgumentException("Cosmos DB settings: every PartitionKeys entry needs an entity type and a property name.");
            }
        }

        /// <inheritdoc />
        public override string ToString()
        {
            string account = Client != null ? "existing client" : _Endpoint != null ? _Endpoint : ConnectionString != null ? "connection string" : "no account";
            return "Cosmos DB (" + account + ", database '" + _DatabaseName + "')";
        }

        #endregion

        #region Private-Methods

        private static void ValidateResourceName(string value, string parameter, string property)
        {
            if (value.Length == 0 || value.Length > 255 || value.EndsWith(" ", StringComparison.Ordinal) || value.IndexOfAny(new[] { '/', '\\', '#', '?' }) >= 0)
                throw new ArgumentException(property + " must be 1 to 255 characters without '/', '\\', '#', '?' or a trailing space.", parameter);
        }

        private static void ValidateManualThroughput(int? value, string property)
        {
            if (value.HasValue && (value.Value < 400 || value.Value % 100 != 0))
                throw new ArgumentOutOfRangeException(nameof(value), property + " must be at least 400 RU/s and a multiple of 100.");
        }

        private static void ValidateAutoscaleThroughput(int? value, string property)
        {
            if (value.HasValue && (value.Value < 1000 || value.Value % 1000 != 0))
                throw new ArgumentOutOfRangeException(nameof(value), property + " must be at least 1000 RU/s and a multiple of 1000.");
        }

        #endregion
    }
}
