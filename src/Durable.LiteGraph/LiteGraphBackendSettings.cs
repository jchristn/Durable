namespace Durable.LiteGraph
{
    using System;
    using System.Text.Json;
    using Microsoft.Extensions.Logging;
    using global::LiteGraph;

    /// <summary>
    /// Settings for a <see cref="LiteGraphBackend"/>: where the graph lives (an existing <see cref="LiteGraphClient"/>, a
    /// SQLite file, or LiteGraph's in-memory SQLite mode), which tenant and graph hold the data (created when missing),
    /// and how the backend behaves (edge maintenance, push-down, logging, transaction limits).
    /// <para>
    /// Exactly one location is required: <see cref="Client"/>, or <see cref="Filename"/> and/or <see cref="InMemory"/>.
    /// A client passed in is never disposed by Durable and must already be initialized
    /// (<see cref="LiteGraphClient.InitializeRepository"/>); a client created from a filename is initialized and owned by
    /// the backend. LiteGraph keeps vector index files in an <c>indexes</c> directory next to the database file.
    /// </para>
    /// Thread safety: not thread-safe; configure before creating the backend. The backend reads the settings once, when
    /// it is created; later changes have no effect on it.
    /// </summary>
    public sealed class LiteGraphBackendSettings
    {
        #region Public-Members

        /// <summary>
        /// Gets or sets an existing LiteGraph client to store data in; null to create one from <see cref="Filename"/> /
        /// <see cref="InMemory"/>. Default: null. The backend never disposes a client passed here.
        /// </summary>
        public LiteGraphClient? Client { get; set; }

        /// <summary>
        /// Gets or sets the SQLite database file the backend creates its own client on; null for none. With
        /// <see cref="InMemory"/>, the file is loaded at start (when it exists) and written back when the backend is
        /// disposed (LiteGraph's in-memory mode). Default: null.
        /// </summary>
        public string? Filename { get; set; }

        /// <summary>
        /// Gets or sets whether the backend's own client uses LiteGraph's in-memory SQLite mode. Without a
        /// <see cref="Filename"/>, a private temporary file location is used and deleted on dispose, so the data is
        /// ephemeral, and (unless <see cref="GraphGuid"/> is set) a new graph is created, because LiteGraph shares one
        /// in-memory database per process (opening an existing file in this mode loads it into that shared database).
        /// The mode uses SQLite shared-cache in-memory storage, whose locking is coarser than a file database's; prefer a
        /// file for concurrent workloads. Default: false.
        /// </summary>
        public bool InMemory { get; set; }

        /// <summary>
        /// Gets or sets the tenant GUID; null to use the first tenant named <see cref="TenantName"/> (created when none
        /// exists). A tenant with this GUID is created when missing. Default: null.
        /// </summary>
        public Guid? TenantGuid { get; set; }

        /// <summary>
        /// Gets or sets the tenant name used to find or create the tenant when <see cref="TenantGuid"/> is null, and to
        /// name a created tenant. Default: "Durable". Must not be null or empty.
        /// </summary>
        /// <exception cref="ArgumentException">Thrown when set to null or empty.</exception>
        public string TenantName
        {
            get => _TenantName;
            set => _TenantName = string.IsNullOrEmpty(value) ? throw new ArgumentException("TenantName cannot be null or empty.", nameof(value)) : value;
        }

        /// <summary>
        /// Gets or sets the graph GUID; null to use the first graph named <see cref="GraphName"/> in the tenant (created
        /// when none exists). A graph with this GUID is created when missing. Default: null.
        /// </summary>
        public Guid? GraphGuid { get; set; }

        /// <summary>
        /// Gets or sets the graph name used to find or create the graph when <see cref="GraphGuid"/> is null, and to name
        /// a created graph. Default: "Durable". Must not be null or empty.
        /// </summary>
        /// <exception cref="ArgumentException">Thrown when set to null or empty.</exception>
        public string GraphName
        {
            get => _GraphName;
            set => _GraphName = string.IsNullOrEmpty(value) ? throw new ArgumentException("GraphName cannot be null or empty.", nameof(value)) : value;
        }

        /// <summary>
        /// Gets or sets whether foreign keys are maintained as edges (see <see cref="LiteGraphRelationship"/>). Default:
        /// true. When false, no edges are written; queries, navigations and includes are unaffected (they always resolve
        /// relationships from the stored foreign key values).
        /// </summary>
        public bool MaintainEdges { get; set; } = true;

        /// <summary>
        /// Gets or sets whether exact string and non-negative integer equality (and string <c>Contains</c> lists) on root
        /// columns are pushed down to LiteGraph as data filters. Labels and primary keys are always pushed down. Default:
        /// true. Push-down only narrows candidates; the full filter is always evaluated client-side.
        /// </summary>
        public bool PushDownDataFilters { get; set; } = true;

        /// <summary>
        /// Gets or sets whether labels, tags and vectors added to Durable nodes outside Durable (for example with the
        /// LiteGraph dashboard) are kept when Durable updates the node. Costs one extra read per updated row. Default: true.
        /// </summary>
        public bool PreserveNodeSubordinates { get; set; } = true;

        /// <summary>
        /// Gets or sets the logger for push-down plans (debug level) and backend events; null for none. Default: null.
        /// </summary>
        public ILogger? Logger { get; set; }

        /// <summary>
        /// Gets or sets the JSON options used to store JSON columns; null for camelCase, non-indented output (the same as
        /// the SQL providers). Default: null.
        /// </summary>
        public JsonSerializerOptions? JsonOptions { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of LiteGraph operations (node and edge writes) in one LiteGraph graph
        /// transaction. A Durable transaction whose writes exceed it cannot commit atomically and fails at commit (this
        /// includes the transaction <see cref="Durable.Query.RepositoryBase{T}"/> opens for CreateMany, UpsertMany and
        /// UpdateMany, so split very large batches: each inserted row costs one operation plus one per foreign key edge);
        /// a single set-based update or delete outside a transaction that exceeds it is applied in several graph
        /// transactions. Default: 10000.
        /// Minimum: 1. Maximum: 10000 (LiteGraph's limit).
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is outside 1..10000.</exception>
        public int MaxOperationsPerTransaction
        {
            get => _MaxOperationsPerTransaction;
            set
            {
                if (value < 1 || value > 10000) throw new ArgumentOutOfRangeException(nameof(value), "MaxOperationsPerTransaction must be between 1 and 10000.");
                _MaxOperationsPerTransaction = value;
            }
        }

        /// <summary>
        /// Gets or sets the timeout of one LiteGraph graph transaction, in seconds. Default: 60. Minimum: 1. Maximum: 3600.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is outside 1..3600.</exception>
        public int TransactionTimeoutSeconds
        {
            get => _TransactionTimeoutSeconds;
            set
            {
                if (value < 1 || value > 3600) throw new ArgumentOutOfRangeException(nameof(value), "TransactionTimeoutSeconds must be between 1 and 3600.");
                _TransactionTimeoutSeconds = value;
            }
        }

        #endregion

        #region Private-Members

        private string _TenantName = "Durable";
        private string _GraphName = "Durable";
        private int _MaxOperationsPerTransaction = 10000;
        private int _TransactionTimeoutSeconds = 60;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates settings with defaults (no location; set <see cref="Client"/>, <see cref="Filename"/> or
        /// <see cref="InMemory"/>).
        /// </summary>
        public LiteGraphBackendSettings()
        {
        }

        /// <summary>
        /// Creates settings for an existing, initialized client. The backend does not dispose it.
        /// </summary>
        /// <param name="client">Client. Must not be null.</param>
        /// <param name="tenantGuid">Tenant GUID; null to find or create the tenant named "Durable".</param>
        /// <param name="graphGuid">Graph GUID; null to find or create the graph named "Durable".</param>
        /// <returns>The settings.</returns>
        /// <exception cref="ArgumentNullException">Thrown when client is null.</exception>
        public static LiteGraphBackendSettings ForClient(LiteGraphClient client, Guid? tenantGuid = null, Guid? graphGuid = null)
        {
            ArgumentNullException.ThrowIfNull(client);
            return new LiteGraphBackendSettings { Client = client, TenantGuid = tenantGuid, GraphGuid = graphGuid };
        }

        /// <summary>
        /// Creates settings for a SQLite database file (created when missing).
        /// </summary>
        /// <param name="filename">Database file path. Must not be null or empty.</param>
        /// <returns>The settings.</returns>
        /// <exception cref="ArgumentException">Thrown when filename is null or empty.</exception>
        public static LiteGraphBackendSettings ForFile(string filename)
        {
            if (string.IsNullOrEmpty(filename)) throw new ArgumentException("Filename cannot be null or empty.", nameof(filename));
            return new LiteGraphBackendSettings { Filename = filename };
        }

        /// <summary>
        /// Creates settings for an ephemeral in-memory graph (LiteGraph's in-memory SQLite mode, nothing kept after dispose).
        /// </summary>
        /// <returns>The settings.</returns>
        public static LiteGraphBackendSettings ForInMemory()
        {
            return new LiteGraphBackendSettings { InMemory = true };
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Validates the settings.
        /// </summary>
        /// <exception cref="ArgumentException">Thrown when no location or more than one kind of location is configured.</exception>
        public void Validate()
        {
            bool own = !string.IsNullOrEmpty(Filename) || InMemory;
            if (Client == null && !own)
                throw new ArgumentException("LiteGraph backend settings need a location: set Client, Filename or InMemory.");
            if (Client != null && own)
                throw new ArgumentException("LiteGraph backend settings must set either Client or Filename/InMemory, not both.");
            if (Filename != null && Filename.Length == 0)
                throw new ArgumentException("Filename cannot be empty.");
            if (TenantGuid.HasValue && TenantGuid.Value == Guid.Empty)
                throw new ArgumentException("TenantGuid cannot be Guid.Empty; use null to find or create the tenant by name.");
            if (GraphGuid.HasValue && GraphGuid.Value == Guid.Empty)
                throw new ArgumentException("GraphGuid cannot be Guid.Empty; use null to find or create the graph by name.");
        }

        #endregion
    }
}
