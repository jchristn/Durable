namespace Durable.LiteGraph
{
    using System;
    using System.Text.Json;
    using Microsoft.Extensions.Logging;
    using Durable;
    using global::LiteGraph;

    /// <summary>
    /// Settings for a <see cref="LiteGraphBackend"/>: where the graph lives (an existing <see cref="LiteGraphClient"/>, a
    /// SQLite file, or an ephemeral in-memory database), which tenant and graph hold the data (created when missing), and
    /// how the backend behaves (edge maintenance, push-down, JSON columns, logging, transaction limits).
    /// <para>
    /// Location: <see cref="Client"/>, or <see cref="Filename"/>, or neither for an ephemeral in-memory graph (the
    /// default, <see cref="IsInMemory"/>). A client passed in is never disposed by Durable and must already be initialized
    /// (<see cref="LiteGraphClient.InitializeRepository"/>); a client created by the backend is initialized and owned by
    /// it. LiteGraph keeps vector index files in an <c>indexes</c> directory next to the database file.
    /// </para>
    /// Thread safety: not thread-safe; configure before creating the backend. The backend reads the settings once, when
    /// it is created; later changes have no effect on it.
    /// </summary>
    public sealed class LiteGraphRepositorySettings
    {
        #region Public-Members

        /// <summary>
        /// Gets or sets an existing LiteGraph client to store data in; null to create one (on <see cref="Filename"/>, or
        /// in memory). The backend never disposes a client passed here.
        /// Default: null.
        /// </summary>
        public LiteGraphClient? Client { get; set; }

        /// <summary>
        /// Gets or sets the SQLite database file the backend creates its own client on (created when missing); null for an
        /// ephemeral in-memory graph. Must be null when <see cref="Client"/> is set.
        /// Default: null.
        /// </summary>
        public string? Filename { get; set; }

        /// <summary>
        /// Gets whether the settings select an ephemeral graph held only in memory and discarded on dispose (neither
        /// <see cref="Client"/> nor <see cref="Filename"/> is set). The backend then uses LiteGraph's in-memory SQLite mode
        /// on a private temporary location deleted on dispose and, unless <see cref="GraphGuid"/> is set, creates a new
        /// graph, because LiteGraph shares one in-memory database per process. The mode uses SQLite shared-cache in-memory
        /// storage, whose locking is coarser than a file database's; prefer a file for concurrent workloads.
        /// </summary>
        public bool IsInMemory => Client == null && string.IsNullOrEmpty(Filename);

        /// <summary>
        /// Gets or sets whether the backend's own client keeps the <see cref="Filename"/> database in memory (LiteGraph's
        /// in-memory SQLite mode): the file is loaded at start (when it exists) and written back when the backend is
        /// disposed. Requires <see cref="Filename"/>.
        /// Default: false.
        /// </summary>
        public bool LoadIntoMemory { get; set; }

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
        /// Gets or sets the logger for push-down plans (Debug) and handler failures (Warning); null for none.
        /// Default: null.
        /// </summary>
        public ILogger? Logger { get; set; }

        /// <summary>
        /// Gets or sets the JSON options used to store JSON columns; null for camelCase, non-indented output (the same as
        /// the SQL providers). Under Native AOT, pass options with a source-generated context
        /// (<see cref="DurableJson.CreateOptions"/>).
        /// Default: null.
        /// </summary>
        public JsonSerializerOptions? JsonOptions { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of LiteGraph operations (node and edge writes) in one LiteGraph graph
        /// transaction. A Durable transaction whose writes exceed it cannot commit atomically and fails at commit (this
        /// includes the transaction <see cref="Durable.Query.RepositoryBase{T}"/> opens for CreateMany, UpsertMany and
        /// UpdateMany, so split very large batches: each inserted row costs one operation plus one per foreign key edge);
        /// a single set-based update or delete outside a transaction that exceeds it is applied in several graph
        /// transactions.
        /// Default: 10000. Minimum: 1. Maximum: 10000 (LiteGraph's limit).
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
        /// Gets or sets the timeout of one LiteGraph graph transaction. LiteGraph takes whole seconds (the value is rounded
        /// down).
        /// Default: 1 minute. Minimum: 1 second. Maximum: 1 hour.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is outside the allowed range.</exception>
        public TimeSpan TransactionTimeout
        {
            get => _TransactionTimeout;
            set
            {
                if (value < TimeSpan.FromSeconds(1) || value > TimeSpan.FromHours(1))
                    throw new ArgumentOutOfRangeException(nameof(value), "TransactionTimeout must be between 1 second and 1 hour.");
                _TransactionTimeout = value;
            }
        }

        #endregion

        #region Private-Members

        private string _TenantName = "Durable";
        private string _GraphName = "Durable";
        private int _MaxOperationsPerTransaction = 10000;
        private TimeSpan _TransactionTimeout = TimeSpan.FromMinutes(1);

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates settings for an ephemeral in-memory graph (set <see cref="Client"/> or <see cref="Filename"/> to
        /// store it elsewhere).
        /// </summary>
        public LiteGraphRepositorySettings()
        {
        }

        /// <summary>
        /// Creates settings for an ephemeral in-memory graph (LiteGraph's in-memory SQLite mode, nothing kept after dispose).
        /// </summary>
        /// <returns>The settings. Never null.</returns>
        public static LiteGraphRepositorySettings ForInMemory()
        {
            return new LiteGraphRepositorySettings();
        }

        /// <summary>
        /// Creates settings for a SQLite database file (created when missing).
        /// </summary>
        /// <param name="filename">Database file path. Must not be null or empty.</param>
        /// <returns>The settings. Never null.</returns>
        /// <exception cref="ArgumentException">Thrown when filename is null or empty.</exception>
        public static LiteGraphRepositorySettings ForFile(string filename)
        {
            if (string.IsNullOrEmpty(filename)) throw new ArgumentException("Filename cannot be null or empty.", nameof(filename));
            return new LiteGraphRepositorySettings { Filename = filename };
        }

        /// <summary>
        /// Creates settings for an existing, initialized client, which the backend does not dispose.
        /// </summary>
        /// <param name="client">Client. Must not be null.</param>
        /// <param name="tenantGuid">Tenant GUID; null to find or create the tenant named "Durable".</param>
        /// <param name="graphGuid">Graph GUID; null to find or create the graph named "Durable".</param>
        /// <returns>The settings. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when client is null.</exception>
        public static LiteGraphRepositorySettings ForClient(LiteGraphClient client, Guid? tenantGuid = null, Guid? graphGuid = null)
        {
            ArgumentNullException.ThrowIfNull(client);
            return new LiteGraphRepositorySettings { Client = client, TenantGuid = tenantGuid, GraphGuid = graphGuid };
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Validates the settings. <see cref="LiteGraphBackend.CreateAsync"/> calls it.
        /// </summary>
        /// <exception cref="ArgumentException">Thrown when <see cref="Client"/> is combined with <see cref="Filename"/> or <see cref="LoadIntoMemory"/>, <see cref="Filename"/> is empty, <see cref="LoadIntoMemory"/> is set without a <see cref="Filename"/>, or a tenant or graph GUID is <see cref="Guid.Empty"/>.</exception>
        public void Validate()
        {
            if (Client != null && (Filename != null || LoadIntoMemory))
                throw new ArgumentException("LiteGraph settings must set either Client or Filename/LoadIntoMemory, not both.");
            if (Filename != null && Filename.Length == 0)
                throw new ArgumentException("Filename cannot be empty; use null for an in-memory graph.");
            if (LoadIntoMemory && Filename == null)
                throw new ArgumentException("LoadIntoMemory requires a Filename; without one the graph is already in memory.");
            if (TenantGuid.HasValue && TenantGuid.Value == Guid.Empty)
                throw new ArgumentException("TenantGuid cannot be Guid.Empty; use null to find or create the tenant by name.");
            if (GraphGuid.HasValue && GraphGuid.Value == Guid.Empty)
                throw new ArgumentException("GraphGuid cannot be Guid.Empty; use null to find or create the graph by name.");
        }

        /// <inheritdoc />
        public override string ToString()
        {
            string location = Client != null ? "existing client" : IsInMemory ? "in-memory" : "'" + Filename + "'" + (LoadIntoMemory ? " (loaded into memory)" : string.Empty);
            return "LiteGraph " + location + ", graph " + (GraphGuid?.ToString() ?? "'" + GraphName + "'");
        }

        #endregion
    }
}
