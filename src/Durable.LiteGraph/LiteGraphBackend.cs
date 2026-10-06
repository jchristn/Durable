namespace Durable.LiteGraph
{
    using System;
    using System.Buffers;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Runtime.CompilerServices;
    using System.Text.Json;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using Durable;
    using Durable.Query;
    using ExpressionTree;
    using global::LiteGraph;
    using global::LiteGraph.GraphRepositories.Sqlite;

    /// <summary>
    /// A LiteGraph <see cref="IRepositoryBackend"/>: stores Durable entities in a LiteGraph property graph so they are
    /// first-class graph data. Use it through <see cref="LiteGraphRepository{T}"/> (or any <see cref="RepositoryBase{T}"/>);
    /// one backend serves every entity type of one graph, so includes, navigation predicates and transactions work across
    /// repositories that share it.
    /// <para>
    /// Storage model: each row is one node in the configured tenant and graph, labelled with the entity's table name
    /// (<see cref="EntityMetadata.TableName"/>) and named <c>table:key</c>. The node's data is a JSON object of the column
    /// values keyed by column name, built from Durable's mapping (value converters store their provider value, JSON
    /// columns their serialized text, enums their name or number with <see cref="Flags.Integer"/>), encoded losslessly
    /// (see <see cref="LiteGraphRepositorySettings"/> and the README for the encoding). The node GUID is derived from the
    /// graph, the table and the primary key (<see cref="GetNodeGuid"/>), so key lookups are lookups by GUID and LiteGraph's
    /// unique node GUIDs enforce unique keys, including composite keys. Foreign keys are maintained as edges from the
    /// dependent node to the principal node (<see cref="LiteGraphRelationship"/>, <see cref="LiteGraphRepositorySettings.MaintainEdges"/>):
    /// an edge exists exactly when the foreign key holds the key of an existing principal row; it is created on insert of
    /// either side, moved when the foreign key changes and removed with either node. Many-to-many junction rows are nodes
    /// with an edge to each side. Labels, tags and vectors added to these nodes outside Durable are kept across updates
    /// (<see cref="LiteGraphRepositorySettings.PreserveNodeSubordinates"/>). Do not change key values or data of Durable
    /// nodes outside Durable.
    /// </para>
    /// <para>
    /// Queries: candidates are read by deterministic GUID when the filter pins the key, otherwise by label, narrowed by an
    /// exact data filter when part of the filter can be pushed down (<see cref="LiteGraphRepositorySettings.PushDownDataFilters"/>);
    /// the complete filter, ordering, paging, navigation members, collection predicates and aggregates are then evaluated
    /// client-side with the C# semantics of <see cref="QueryEvaluator{TRow}"/> (null equals only null, ordinal strings,
    /// stable ordering with nulls first, insertion order when unordered). Related rows for navigations are read once per
    /// operation and indexed. Every candidate node is read before the first row is returned, so memory and time grow with
    /// the candidates of a query, not its results. A client created by the backend reads each label scan in one
    /// statement; for a client you supply, raise its repository's <c>SelectBatchSize</c> for the same consistency under
    /// concurrent writes. <see cref="QueryPlanned"/> and the configured logger report what was pushed down.
    /// </para>
    /// <para>
    /// Writes: every write is applied as one LiteGraph graph transaction (node and edge operations together). Writes made
    /// outside a transaction are serialized by a backend-wide lock, which also makes key generation and duplicate-key
    /// checks atomic within the process. Transactions are interactive (see <see cref="LiteGraphTransaction"/>): writes are
    /// kept in the transaction, read back by it, and applied atomically at commit, which limits a transaction (including
    /// the implicit one CreateMany, UpsertMany and UpdateMany run in) to
    /// <see cref="LiteGraphRepositorySettings.MaxOperationsPerTransaction"/> node and edge operations. Auto-increment keys come from a
    /// per-type sequence that starts after the largest stored key (read once per process), never reuses a value within the
    /// process and skips values already present in the graph. Several processes writing one graph cannot share the
    /// sequence: a key generated concurrently by two processes is rejected by LiteGraph for the second writer (the write
    /// fails; nothing is overwritten).
    /// </para>
    /// <para>
    /// Lifetime: create a backend with <see cref="CreateAsync"/> (or <see cref="Create"/>), share it across repositories,
    /// and dispose it when done. It disposes the client only when it created it (<see cref="OwnsClient"/>); a client passed
    /// in with <see cref="LiteGraphRepositorySettings.Client"/> is never disposed. Repositories never dispose the backend.
    /// </para>
    /// <para>
    /// Trimming and Native AOT: not supported. The LiteGraph library uses reflection-based System.Text.Json for its own
    /// records and <c>DataTable</c> for query results, so <see cref="CreateAsync"/> and <see cref="Create"/> carry
    /// <see cref="RequiresUnreferencedCodeAttribute"/> and <see cref="RequiresDynamicCodeAttribute"/>. Durable's own code
    /// in this package is trim-annotated and warning-free.
    /// </para>
    /// Thread safety: safe for concurrent use by any number of repositories and threads.
    /// </summary>
    public sealed class LiteGraphBackend : IRepositoryBackend, IDisposable, IAsyncDisposable
    {
        #region Public-Members

        /// <summary>
        /// Gets the optional features the backend supports: <see cref="RepositoryCapabilities.All"/>.
        /// </summary>
        public RepositoryCapabilities Capabilities => RepositoryCapabilities.All;

        /// <summary>
        /// Gets the LiteGraph client. Never null. Use it to traverse, search or analyze the graph with LiteGraph's own APIs;
        /// writes to Durable nodes should go through Durable.
        /// </summary>
        /// <exception cref="ObjectDisposedException">Thrown when the backend is disposed and owned the client.</exception>
        public LiteGraphClient Client
        {
            get
            {
                if (OwnsClient) ThrowIfDisposed();
                return _Client;
            }
        }

        /// <summary>
        /// Gets the tenant GUID holding the graph.
        /// </summary>
        public Guid TenantGuid { get; }

        /// <summary>
        /// Gets the graph GUID holding the data.
        /// </summary>
        public Guid GraphGuid { get; }

        /// <summary>
        /// Gets whether the backend created (and disposes) the client.
        /// </summary>
        public bool OwnsClient { get; }

        /// <summary>
        /// Gets whether foreign keys are maintained as edges.
        /// </summary>
        public bool MaintainEdges { get; }

        /// <summary>
        /// Gets the JSON options used to store JSON columns. Never null.
        /// </summary>
        public JsonSerializerOptions JsonOptions => _Values.JsonOptions;

        /// <summary>
        /// Gets the plan of the most recent read of candidate nodes by any thread, or null before the first one. Use it in
        /// single-threaded diagnostics and tests; use <see cref="QueryPlanned"/> to observe every plan.
        /// </summary>
        public LiteGraphQueryPlan? LastQueryPlan => _LastQueryPlan;

        /// <summary>
        /// Raised whenever the backend reads candidate nodes from LiteGraph, describing what was pushed down, synchronously
        /// on the thread that executed the read; the sender is the backend. Handlers must be thread-safe and fast and must
        /// not call the backend; an exception thrown by a handler is logged (Warning) and otherwise ignored.
        /// </summary>
        public event EventHandler<LiteGraphQueryPlan>? QueryPlanned;

        #endregion

        #region Private-Members

        private const string _NotAotCompatible =
            "The LiteGraph library serializes its own records with reflection-based System.Text.Json and loads query results "
            + "through DataTable, which trimming and Native AOT do not support (the backend fails while LiteGraph initializes "
            + "its repository). Durable.LiteGraph itself is trim-annotated; use LiteGraph on the JIT runtime.";

        private static readonly JsonSerializerOptions _DefaultJsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };

        private readonly LiteGraphClient _Client;
        private readonly string? _TemporaryDirectory;
        private readonly LiteGraphValueConverter _Values;
        private readonly LiteGraphPlanner _Planner;
        private readonly LiteGraphRelationshipRegistry _Registry = new LiteGraphRelationshipRegistry();
        private readonly Dictionary<Type, LiteGraphIdentitySequence> _Sequences = new Dictionary<Type, LiteGraphIdentitySequence>();
        private readonly SemaphoreSlim _WriteGate = new SemaphoreSlim(1, 1);
        private readonly SemaphoreSlim _SequenceGate = new SemaphoreSlim(1, 1);
        private readonly ILogger? _Logger;
        private readonly bool _PreserveSubordinates;
        private readonly int _MaxOperations;
        private readonly int _TimeoutSeconds;
        private readonly int _ReadChunkSize = 500;
        private volatile LiteGraphQueryPlan? _LastQueryPlan;
        private long _LastCreatedTicks;
        private int _Disposed;

        #endregion

        #region Constructors-and-Factories

        private LiteGraphBackend(LiteGraphRepositorySettings settings, LiteGraphClient client, bool ownsClient, string? temporaryDirectory, Guid tenantGuid, Guid graphGuid)
        {
            _Client = client;
            OwnsClient = ownsClient;
            _TemporaryDirectory = temporaryDirectory;
            TenantGuid = tenantGuid;
            GraphGuid = graphGuid;
            MaintainEdges = settings.MaintainEdges;
            _PreserveSubordinates = settings.PreserveNodeSubordinates;
            _MaxOperations = settings.MaxOperationsPerTransaction;
            _TimeoutSeconds = (int)Math.Floor(settings.TransactionTimeout.TotalSeconds);
            _Logger = settings.Logger;
            _Values = new LiteGraphValueConverter(settings.JsonOptions ?? _DefaultJsonOptions);
            _Planner = new LiteGraphPlanner(_Values, graphGuid, settings.PushDownDataFilters);
        }

        /// <summary>
        /// Creates a backend: wraps <see cref="LiteGraphRepositorySettings.Client"/> when set (not owned, never disposed),
        /// otherwise creates and initializes its own client on <see cref="LiteGraphRepositorySettings.Filename"/> or an
        /// ephemeral in-memory database (owned and disposed by the backend); then finds or creates the tenant and graph.
        /// </summary>
        /// <param name="settings">Settings; null uses <see cref="LiteGraphRepositorySettings.ForInMemory"/>.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The backend. Never null. Dispose it when done.</returns>
        /// <exception cref="ArgumentException">Thrown when the settings are invalid (see <see cref="LiteGraphRepositorySettings.Validate"/>).</exception>
        /// <exception cref="OperationCanceledException">Thrown when the token is canceled.</exception>
        [RequiresUnreferencedCode(_NotAotCompatible)]
        [RequiresDynamicCode(_NotAotCompatible)]
        public static async Task<LiteGraphBackend> CreateAsync(LiteGraphRepositorySettings? settings = null, CancellationToken token = default)
        {
            settings ??= LiteGraphRepositorySettings.ForInMemory();
            settings.Validate();
            token.ThrowIfCancellationRequested();

            LiteGraphClient client;
            bool owns = settings.Client == null;
            string? temporary = null;
            if (settings.Client != null)
            {
                client = settings.Client;
            }
            else
            {
                string filename;
                if (!string.IsNullOrEmpty(settings.Filename))
                {
                    filename = settings.Filename;
                }
                else
                {
                    temporary = Path.Combine(Path.GetTempPath(), "durable-litegraph-" + Guid.NewGuid().ToString("N"));
                    Directory.CreateDirectory(temporary);
                    filename = Path.Combine(temporary, "litegraph.db");
                }

                // One page per scan: LiteGraph pages label scans with OFFSET, so a single page makes every scan one
                // statement (a consistent snapshot) instead of pages that shift under concurrent writes.
                SqliteGraphRepository repository = new SqliteGraphRepository(filename, settings.IsInMemory || settings.LoadIntoMemory) { SelectBatchSize = int.MaxValue };
                client = new LiteGraphClient(repository, new LoggingSettings { Enable = false }, null, null);
            }

            try
            {
                if (owns) await client.InitializeRepositoryAsync(token).ConfigureAwait(false);
                Guid tenant = await ResolveTenantAsync(client, settings, token).ConfigureAwait(false);
                Guid? requestedGraph = settings.GraphGuid;
                if (requestedGraph == null && settings.IsInMemory) requestedGraph = Guid.NewGuid();
                Guid graph = await ResolveGraphAsync(client, tenant, requestedGraph, settings.GraphName, token).ConfigureAwait(false);
                return new LiteGraphBackend(settings, client, owns, temporary, tenant, graph);
            }
            catch
            {
                if (owns)
                {
                    await client.DisposeAsync().ConfigureAwait(false);
                    DeleteDirectory(temporary);
                }

                throw;
            }
        }

        /// <summary>
        /// Creates a backend (see <see cref="CreateAsync"/>). Blocks the calling thread while LiteGraph initializes its
        /// storage; prefer <see cref="CreateAsync"/> in asynchronous code.
        /// </summary>
        /// <param name="settings">Settings; null uses <see cref="LiteGraphRepositorySettings.ForInMemory"/>.</param>
        /// <returns>The backend. Never null. Dispose it when done.</returns>
        /// <exception cref="ArgumentException">Thrown when the settings are invalid (see <see cref="LiteGraphRepositorySettings.Validate"/>).</exception>
        [RequiresUnreferencedCode(_NotAotCompatible)]
        [RequiresDynamicCode(_NotAotCompatible)]
        public static LiteGraphBackend Create(LiteGraphRepositorySettings? settings = null)
        {
            settings ??= LiteGraphRepositorySettings.ForInMemory();
            settings.Validate();
            return Task.Run(() => CreateAsync(settings, CancellationToken.None)).GetAwaiter().GetResult();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Creates a repository over this backend.
        /// </summary>
        /// <typeparam name="T">Entity type.</typeparam>
        /// <param name="options">Options; null uses defaults.</param>
        /// <returns>A new repository.</returns>
        /// <exception cref="InvalidOperationException">Thrown when <typeparamref name="T"/> has no primary key or an invalid mapping.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend is disposed.</exception>
        public LiteGraphRepository<T> CreateRepository<[DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] T>(RepositoryOptions? options = null) where T : class, new()
        {
            ThrowIfDisposed();
            return new LiteGraphRepository<T>(this, options);
        }

        /// <summary>
        /// Determines whether a transaction was created by this backend.
        /// </summary>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>True when the transaction belongs to this backend.</returns>
        public bool Owns(ITransaction? transaction)
        {
            return transaction is LiteGraphTransaction liteGraph && ReferenceEquals(liteGraph.Backend, this);
        }

        /// <summary>
        /// Returns the deterministic node GUID of an entity row in this graph.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="id">Key: a scalar, or an object array with one value per key column for composite keys. Must not be null.</param>
        /// <returns>The node GUID (the node exists only when the row does).</returns>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        /// <exception cref="ArgumentException">Thrown when the key does not match the entity's key columns.</exception>
        public Guid GetNodeGuid([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType, object id)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            ArgumentNullException.ThrowIfNull(id);
            EntityMetadata metadata = EntityMetadata.For(entityType);
            object?[] key = metadata.SplitKey(id);
            object?[] stored = new object?[key.Length];
            for (int i = 0; i < key.Length; i++)
            {
                stored[i] = _Values.ToStored(metadata.KeyColumns[i], key[i]);
                if (stored[i] == null) throw new ArgumentException("Key column '" + metadata.KeyColumns[i].Name + "' of " + entityType.Name + " cannot be null.", nameof(id));
            }

            return LiteGraphIdentity.Node(GraphGuid, metadata.TableName, stored);
        }

        /// <summary>
        /// Returns the deterministic node GUID of an entity row in this graph.
        /// </summary>
        /// <typeparam name="T">Entity type.</typeparam>
        /// <param name="id">Key: a scalar, or an object array with one value per key column for composite keys. Must not be null.</param>
        /// <returns>The node GUID.</returns>
        /// <exception cref="ArgumentNullException">Thrown when id is null.</exception>
        /// <exception cref="ArgumentException">Thrown when the key does not match the entity's key columns.</exception>
        public Guid GetNodeGuid<[DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] T>(object id) where T : class
        {
            return GetNodeGuid(typeof(T), id);
        }

        /// <summary>
        /// Returns the deterministic GUID of the edge a relationship maintains from a dependent node.
        /// </summary>
        /// <param name="relationship">Relationship. Must not be null.</param>
        /// <param name="dependentNodeGuid">Dependent node GUID.</param>
        /// <returns>The edge GUID (the edge exists only while the foreign key references an existing principal).</returns>
        /// <exception cref="ArgumentNullException">Thrown when relationship is null.</exception>
        public Guid GetEdgeGuid(LiteGraphRelationship relationship, Guid dependentNodeGuid)
        {
            ArgumentNullException.ThrowIfNull(relationship);
            return LiteGraphIdentity.Edge(GraphGuid, relationship.Id, dependentNodeGuid);
        }

        /// <summary>
        /// Returns the relationships maintained as edges in which an entity type is the dependent or the principal.
        /// Registers the type (and every type reachable from it) first.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <returns>The relationships ordered by identifier. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        public IReadOnlyList<LiteGraphRelationship> GetRelationships([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            _Registry.Register(EntityMetadata.For(entityType));
            return _Registry.ForDependent(entityType)
                .Concat(_Registry.ForPrincipal(entityType))
                .Distinct()
                .OrderBy(r => r.Id, StringComparer.Ordinal)
                .ToList();
        }

        /// <summary>
        /// Deletes every node and edge of the graph (Durable data and anything else stored in the graph), bypassing soft
        /// delete, query filters and transactions, and resets the auto-increment sequences. Must not be called while a
        /// transaction is open. Blocks the calling thread; prefer <see cref="ClearAsync(CancellationToken)"/>.
        /// </summary>
        /// <exception cref="ObjectDisposedException">Thrown when the backend is disposed.</exception>
        public void Clear()
        {
            Task.Run(() => ClearAsync(CancellationToken.None)).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Deletes every node and edge of the graph (Durable data and anything else stored in the graph), bypassing soft
        /// delete, query filters and transactions, and resets the auto-increment sequences. Must not be called while a
        /// transaction is open.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task.</returns>
        /// <exception cref="ObjectDisposedException">Thrown when the backend is disposed.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the token is canceled.</exception>
        public async Task ClearAsync(CancellationToken token = default)
        {
            ThrowIfDisposed();
            await _WriteGate.WaitAsync(token).ConfigureAwait(false);
            try
            {
                await _Client.Edge.DeleteAllInGraph(TenantGuid, GraphGuid, token).ConfigureAwait(false);
                await _Client.Node.DeleteAllInGraph(TenantGuid, GraphGuid, token).ConfigureAwait(false);
                lock (_Sequences) _Sequences.Clear();
            }
            finally
            {
                _WriteGate.Release();
            }
        }

        /// <summary>
        /// Deletes every node of an entity type (including soft-deleted rows) and their edges, bypassing soft delete,
        /// query filters and transactions, and resets its auto-increment sequence. Must not be called while a transaction
        /// is open. Blocks the calling thread; prefer <see cref="ClearAsync(Type, CancellationToken)"/>.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <returns>The number of nodes deleted.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend is disposed.</exception>
        public int Clear([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            return Task.Run(() => ClearAsync(entityType, CancellationToken.None)).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Deletes every node of an entity type (including soft-deleted rows) and their edges, bypassing soft delete,
        /// query filters and transactions, and resets its auto-increment sequence. Must not be called while a transaction
        /// is open.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The number of nodes deleted.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend is disposed.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the token is canceled.</exception>
        public async Task<int> ClearAsync([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            ThrowIfDisposed();
            EntityMetadata metadata = EntityMetadata.For(entityType);
            await _WriteGate.WaitAsync(token).ConfigureAwait(false);
            try
            {
                List<Guid> guids = new List<Guid>();
                await foreach (Node node in _Client.Node.ReadMany(TenantGuid, GraphGuid, null, new List<string> { metadata.TableName }, null, null, EnumerationOrderEnum.CreatedAscending, 0, false, false, token).ConfigureAwait(false))
                {
                    guids.Add(node.GUID);
                }

                for (int start = 0; start < guids.Count; start += _ReadChunkSize)
                {
                    token.ThrowIfCancellationRequested();
                    await _Client.Node.DeleteMany(TenantGuid, GraphGuid, guids.GetRange(start, Math.Min(_ReadChunkSize, guids.Count - start)), token).ConfigureAwait(false);
                }

                lock (_Sequences) _Sequences.Remove(metadata.EntityType);
                return guids.Count;
            }
            finally
            {
                _WriteGate.Release();
            }
        }

        /// <summary>
        /// Returns the stored rows of an entity (column name to stored value, as encoded in the node data), for diagnostics
        /// and tests that check the stored representation. Soft-deleted rows are included; transactions are ignored.
        /// Blocks the calling thread; prefer <see cref="GetStoredRowsAsync"/>.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <returns>The rows in creation order. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend is disposed.</exception>
        public IReadOnlyList<IReadOnlyDictionary<string, object?>> GetStoredRows([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            return Task.Run(() => GetStoredRowsAsync(entityType, CancellationToken.None)).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Returns the stored rows of an entity (column name to stored value, as encoded in the node data), for diagnostics
        /// and tests that check the stored representation. Soft-deleted rows are included; transactions are ignored.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The rows in creation order. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend is disposed.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the token is canceled.</exception>
        public async Task<IReadOnlyList<IReadOnlyDictionary<string, object?>>> GetStoredRowsAsync([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            ThrowIfDisposed();
            EntityMetadata metadata = EntityMetadata.For(entityType);
            LiteGraphTableSchema schema = LiteGraphTableSchema.For(metadata);
            List<IReadOnlyDictionary<string, object?>> rows = new List<IReadOnlyDictionary<string, object?>>();
            foreach (LiteGraphRow row in await ReadStoredAsync(LiteGraphReadRequest.All(metadata), token).ConfigureAwait(false))
            {
                Dictionary<string, object?> values = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                foreach (ColumnMetadata column in metadata.Columns)
                {
                    object? value = row.Values[schema.Ordinal(column)];
                    values[column.Name] = value is byte[] bytes ? bytes.Clone() : value;
                }

                rows.Add(values);
            }

            return rows;
        }

        /// <summary>
        /// Recomputes the edges of every registered relationship (see <see cref="RebuildEdgesAsync"/>). Blocks the calling
        /// thread; prefer <see cref="RebuildEdgesAsync"/>.
        /// </summary>
        /// <param name="entities">Entities to register before rebuilding (with every type reachable from them), for example <c>EntityMetadata.For&lt;Order&gt;()</c>; null for none.</param>
        /// <returns>The number of edges created or deleted.</returns>
        /// <exception cref="ObjectDisposedException">Thrown when the backend is disposed.</exception>
        public int RebuildEdges(IEnumerable<EntityMetadata>? entities = null)
        {
            return Task.Run(() => RebuildEdgesAsync(entities, CancellationToken.None)).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Recomputes the edges of every registered relationship from the stored foreign keys: creates missing edges and
        /// deletes Durable edges that no longer correspond to a foreign key (for example after nodes were changed outside
        /// Durable, after edges were disabled with <see cref="LiteGraphRepositorySettings.MaintainEdges"/>, or after a
        /// relationship became known later). Not atomic; applied in graph transactions of at most
        /// <see cref="LiteGraphRepositorySettings.MaxOperationsPerTransaction"/> operations.
        /// </summary>
        /// <param name="entities">Entities to register before rebuilding (with every type reachable from them), for example <c>EntityMetadata.For&lt;Order&gt;()</c>; null for none.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The number of edges created or deleted.</returns>
        /// <exception cref="ObjectDisposedException">Thrown when the backend is disposed.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the token is canceled.</exception>
        public async Task<int> RebuildEdgesAsync(IEnumerable<EntityMetadata>? entities = null, CancellationToken token = default)
        {
            ThrowIfDisposed();
            if (entities != null)
            {
                foreach (EntityMetadata metadata in entities) _Registry.Register(metadata ?? throw new ArgumentException("Entities cannot contain null.", nameof(entities)));
            }

            await _WriteGate.WaitAsync(token).ConfigureAwait(false);
            try
            {
                Dictionary<Guid, TransactionOperation> desired = new Dictionary<Guid, TransactionOperation>();
                Dictionary<Type, HashSet<Guid>> existing = new Dictionary<Type, HashSet<Guid>>();
                foreach (LiteGraphRelationship relationship in _Registry.All())
                {
                    if (!existing.TryGetValue(relationship.Principal.EntityType, out HashSet<Guid>? principals))
                    {
                        principals = new HashSet<Guid>((await ReadStoredAsync(LiteGraphReadRequest.All(relationship.Principal), token).ConfigureAwait(false)).Select(r => r.Guid));
                        existing[relationship.Principal.EntityType] = principals;
                    }

                    int ordinal = LiteGraphTableSchema.For(relationship.Dependent).Ordinal(relationship.ForeignKey);
                    foreach (LiteGraphRow dependent in await ReadStoredAsync(LiteGraphReadRequest.All(relationship.Dependent), token).ConfigureAwait(false))
                    {
                        object? foreignKey = dependent.Values[ordinal];
                        if (foreignKey == null) continue;
                        Guid principal = LiteGraphIdentity.Node(GraphGuid, relationship.Principal.TableName, new[] { foreignKey });
                        if (!principals.Contains(principal)) continue;
                        TransactionOperation operation = EdgeOperation(relationship, dependent.Guid, principal);
                        desired[operation.GUID!.Value] = operation;
                    }
                }

                List<TransactionOperation> operations = new List<TransactionOperation>();
                HashSet<Guid> present = new HashSet<Guid>();
                await foreach (Edge edge in _Client.Edge.ReadAllInGraph(TenantGuid, GraphGuid, EnumerationOrderEnum.CreatedAscending, 0, true, false, token).ConfigureAwait(false))
                {
                    if (!IsDurableEdge(edge)) continue;
                    if (desired.TryGetValue(edge.GUID, out TransactionOperation? wanted) && wanted.Payload is Edge target && target.From == edge.From && target.To == edge.To)
                    {
                        present.Add(edge.GUID);
                        continue;
                    }

                    if (!desired.ContainsKey(edge.GUID))
                        operations.Add(new TransactionOperation { OperationType = TransactionOperationTypeEnum.Delete, ObjectType = TransactionObjectTypeEnum.Edge, GUID = edge.GUID });
                }

                operations.AddRange(desired.Where(d => !present.Contains(d.Key)).Select(d => d.Value));
                await ApplyAsync(operations, false, token).ConfigureAwait(false);
                return operations.Count;
            }
            finally
            {
                _WriteGate.Release();
            }
        }

        /// <inheritdoc />
        public IAsyncEnumerable<object> QueryAsync(QueryModel model, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(model);
            ThrowIfDisposed();
            token.ThrowIfCancellationRequested();
            return QueryCoreAsync(model, token);
        }

        /// <inheritdoc />
        public async Task<long> CountAsync(QueryModel model, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(model);
            List<LiteGraphRow> rows = await ApplyModelAsync(model, "Count", token).ConfigureAwait(false);
            return rows.Count;
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Thrown when <paramref name="function"/> is Count or Any.</exception>
        public async Task<object?> AggregateAsync(QueryModel model, AggregateFunction function, QueryNode operand, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(model);
            ArgumentNullException.ThrowIfNull(operand);
            ThrowIfDisposed();
            token.ThrowIfCancellationRequested();

            LiteGraphTransaction? transaction = ResolveTransaction(model.Transaction);
            if (transaction != null) await transaction.Lock.WaitAsync(token).ConfigureAwait(false);
            object? result;
            try
            {
                transaction?.ThrowIfCompleted();
                LiteGraphSelection selection = await SelectAsync(model.Source, model.Filter, model.Orderings, new[] { operand }, transaction?.Scope, transaction != null, "Aggregate", token).ConfigureAwait(false);
                List<LiteGraphRow> rows = selection.Evaluator.Apply(model, selection.Candidates);
                result = selection.Evaluator.Aggregate(model.Source, rows, function, operand);
            }
            finally
            {
                transaction?.Lock.Release();
            }

            if (result != null && operand is ColumnNode column && (function == AggregateFunction.Min || function == AggregateFunction.Max))
                result = _Values.FromStored(column.Column, result);
            return result;
        }

        /// <inheritdoc />
        /// <exception cref="InvalidOperationException">Thrown when a row with the same primary key already exists.</exception>
        public async Task InsertAsync(EntityMetadata metadata, object entity, ITransaction? transaction, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(metadata);
            ArgumentNullException.ThrowIfNull(entity);
            ThrowIfDisposed();
            token.ThrowIfCancellationRequested();
            _Registry.Register(metadata);

            LiteGraphTableSchema schema = LiteGraphTableSchema.For(metadata);
            object?[] values = ToStoredValues(metadata, entity);
            ColumnMetadata? generated = metadata.AutoIncrementColumn;
            int generatedOrdinal = generated != null ? schema.Ordinal(generated) : -1;
            bool generate = generated != null && IsUnsetGenerated(values[generatedOrdinal]);

            await WriteAsync(transaction, async (scope, inTransaction) =>
            {
                Guid guid;
                if (generated != null)
                {
                    LiteGraphIdentitySequence sequence = await SequenceAsync(metadata, generated, token).ConfigureAwait(false);
                    if (generate)
                    {
                        guid = await NextFreeKeyAsync(metadata, generated, generatedOrdinal, sequence, values, scope, token).ConfigureAwait(false);
                        await AddInsertAsync(scope, new LiteGraphRow(guid, metadata, values, NextCreatedUtc(), null), inTransaction, token).ConfigureAwait(false);
                        return 1;
                    }

                    if (IsInteger(values[generatedOrdinal])) sequence.Observe(Convert.ToInt64(values[generatedOrdinal], CultureInfo.InvariantCulture));
                }

                object?[] key = RequireKey(metadata, schema, values);
                guid = LiteGraphIdentity.Node(GraphGuid, metadata.TableName, key);
                if (await ExistsAsync(guid, scope, token).ConfigureAwait(false))
                    throw new InvalidOperationException("Cannot insert " + metadata.EntityType.Name + ": a row with primary key " + FormatKey(key) + " already exists in '" + metadata.TableName + "'.");
                await AddInsertAsync(scope, new LiteGraphRow(guid, metadata, values, NextCreatedUtc(), null), inTransaction, token).ConfigureAwait(false);
                return 1;
            }, token).ConfigureAwait(false);

            if (generate) generated!.SetValue(entity, _Values.FromStored(generated, values[generatedOrdinal]));
        }

        /// <inheritdoc />
        public Task<int> ReplaceAsync(EntityMetadata metadata, object entity, QueryNode condition, QuerySource source, ITransaction? transaction, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(metadata);
            ArgumentNullException.ThrowIfNull(entity);
            ArgumentNullException.ThrowIfNull(condition);
            ArgumentNullException.ThrowIfNull(source);
            ThrowIfDisposed();
            token.ThrowIfCancellationRequested();

            object?[] replacement = ToStoredValues(metadata, entity);
            List<int> updatable = new List<int>();
            for (int i = 0; i < metadata.Columns.Count; i++)
            {
                if (!metadata.Columns[i].IsPrimaryKey && !metadata.Columns[i].IsAutoIncrement) updatable.Add(i);
            }

            return WriteAsync(transaction, async (scope, inTransaction) =>
            {
                LiteGraphSelection selection = await SelectAsync(source, condition, null, null, scope, inTransaction, "Replace", token).ConfigureAwait(false);
                List<LiteGraphRow> matches = selection.Evaluator.Filter(source, condition, selection.Candidates);
                foreach (LiteGraphRow row in matches)
                {
                    object?[] values = (object?[])row.Values.Clone();
                    foreach (int ordinal in updatable) values[ordinal] = replacement[ordinal] is byte[] bytes ? bytes.Clone() : replacement[ordinal];
                    await AddUpdateAsync(scope, row, values, inTransaction, token).ConfigureAwait(false);
                }

                return matches.Count;
            }, token);
        }

        /// <inheritdoc />
        /// <exception cref="InvalidOperationException">Thrown when an assignment changes a primary key to one that already exists.</exception>
        public Task<int> UpdateAsync(QueryModel model, IReadOnlyList<FieldAssignment> assignments, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(model);
            ArgumentNullException.ThrowIfNull(assignments);
            if (assignments.Count == 0) throw new ArgumentException("At least one assignment is required.", nameof(assignments));
            ThrowIfDisposed();
            token.ThrowIfCancellationRequested();

            EntityMetadata metadata = model.Metadata;
            LiteGraphTableSchema schema = LiteGraphTableSchema.For(metadata);
            return WriteAsync(model.Transaction, async (scope, inTransaction) =>
            {
                LiteGraphSelection selection = await SelectAsync(model.Source, model.Filter, null, assignments.Select(a => a.Value), scope, inTransaction, "Update", token).ConfigureAwait(false);
                LiteGraphQueryEvaluator evaluator = selection.Evaluator;
                List<LiteGraphRow> matches = evaluator.Filter(model.Source, model.Filter, selection.Candidates);
                List<object?[]> updated = new List<object?[]>(matches.Count);
                foreach (LiteGraphRow row in matches)
                {
                    evaluator.Bind(model.Source, row);
                    object?[] values = (object?[])row.Values.Clone();
                    foreach (FieldAssignment assignment in assignments)
                    {
                        values[schema.Ordinal(assignment.Column)] = assignment.Value is ValueNode constant
                            ? _Values.ToStored(assignment.Column, constant.Value)
                            : _Values.ComputedToStored(assignment.Column, evaluator.Visit(assignment.Value));
                    }

                    updated.Add(values);
                }

                Guid[] targets = new Guid[matches.Count];
                HashSet<Guid> vacated = new HashSet<Guid>();
                for (int i = 0; i < matches.Count; i++)
                {
                    targets[i] = LiteGraphIdentity.Node(GraphGuid, metadata.TableName, RequireKey(metadata, schema, updated[i]));
                    if (targets[i] != matches[i].Guid) vacated.Add(matches[i].Guid);
                }

                HashSet<Guid> claimed = new HashSet<Guid>();
                for (int i = 0; i < matches.Count; i++)
                {
                    if (targets[i] == matches[i].Guid) continue;
                    bool taken = !claimed.Add(targets[i]) || (!vacated.Contains(targets[i]) && await ExistsAsync(targets[i], scope, token).ConfigureAwait(false));
                    if (taken)
                        throw new InvalidOperationException("Cannot update " + metadata.EntityType.Name + ": a row with primary key " + FormatKey(schema.KeyOf(updated[i])) + " already exists in '" + metadata.TableName + "'.");
                }

                for (int i = 0; i < matches.Count; i++)
                {
                    if (targets[i] != matches[i].Guid) AddDelete(scope, matches[i]);
                }

                for (int i = 0; i < matches.Count; i++)
                {
                    if (targets[i] == matches[i].Guid) await AddUpdateAsync(scope, matches[i], updated[i], inTransaction, token).ConfigureAwait(false);
                    else await AddInsertAsync(scope, new LiteGraphRow(targets[i], metadata, updated[i], matches[i].CreatedUtc, null), inTransaction, token).ConfigureAwait(false);
                }

                return matches.Count;
            }, token);
        }

        /// <inheritdoc />
        public Task<int> DeleteAsync(QueryModel model, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(model);
            ThrowIfDisposed();
            token.ThrowIfCancellationRequested();
            return WriteAsync(model.Transaction, async (scope, inTransaction) =>
            {
                LiteGraphSelection selection = await SelectAsync(model.Source, model.Filter, null, null, scope, inTransaction, "Delete", token).ConfigureAwait(false);
                List<LiteGraphRow> matches = selection.Evaluator.Filter(model.Source, model.Filter, selection.Candidates);
                foreach (LiteGraphRow row in matches) AddDelete(scope, row);
                return matches.Count;
            }, token);
        }

        /// <summary>
        /// Begins an interactive transaction (see <see cref="LiteGraphTransaction"/>).
        /// </summary>
        /// <returns>The transaction. Never null. Dispose it; an uncommitted transaction rolls back on dispose.</returns>
        /// <exception cref="ObjectDisposedException">Thrown when the backend is disposed.</exception>
        public LiteGraphTransaction BeginTransaction()
        {
            ThrowIfDisposed();
            return new LiteGraphTransaction(this);
        }

        /// <summary>
        /// Begins an interactive transaction (see <see cref="LiteGraphTransaction"/>). Nothing is sent to LiteGraph until
        /// commit, so this completes synchronously.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The transaction. Never null. Dispose it; an uncommitted transaction rolls back on dispose.</returns>
        /// <exception cref="ObjectDisposedException">Thrown when the backend is disposed.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the token is canceled.</exception>
        public Task<LiteGraphTransaction> BeginTransactionAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            return Task.FromResult(BeginTransaction());
        }

        /// <inheritdoc />
        async Task<ITransaction> IRepositoryBackend.BeginTransactionAsync(CancellationToken token)
        {
            return await BeginTransactionAsync(token).ConfigureAwait(false);
        }

        /// <summary>
        /// Disposes the backend, and the client when the backend created it (which, in LiteGraph's in-memory mode, writes
        /// the database to its file). A client supplied in the settings is not disposed.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Disposes the backend asynchronously (see <see cref="Dispose()"/>).
        /// </summary>
        /// <returns>A task.</returns>
        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _Disposed, 1) == 1) return;
            if (OwnsClient) await _Client.DisposeAsync().ConfigureAwait(false);
            DeleteDirectory(_TemporaryDirectory);
            GC.SuppressFinalize(this);
        }

        #endregion

        #region Private-Methods

        internal void Register(EntityMetadata metadata)
        {
            ThrowIfDisposed();
            _Registry.Register(metadata);
        }

        internal async Task CommitAsync(LiteGraphTransaction transaction, CancellationToken token)
        {
            await transaction.Lock.WaitAsync(token).ConfigureAwait(false);
            try
            {
                if (!transaction.TryComplete()) throw new InvalidOperationException("The transaction has already completed.");
                try
                {
                    ThrowIfDisposed();
                    LiteGraphWriteScope scope = transaction.Scope;
                    if (scope.Operations.Count == 0) return;
                    if (scope.Operations.Count > _MaxOperations)
                        throw new InvalidOperationException("Transaction commit failed: it needs " + scope.Operations.Count + " LiteGraph operations, more than MaxOperationsPerTransaction (" + _MaxOperations + "), so it cannot be applied atomically. The transaction was rolled back.");

                    await _WriteGate.WaitAsync(token).ConfigureAwait(false);
                    try
                    {
                        await CheckConflictsAsync(scope, token).ConfigureAwait(false);
                        await ApplyAsync(scope.Operations, true, token).ConfigureAwait(false);
                    }
                    finally
                    {
                        _WriteGate.Release();
                    }
                }
                finally
                {
                    transaction.Scope.Clear();
                }
            }
            finally
            {
                transaction.Lock.Release();
            }
        }

        private void Dispose(bool disposing)
        {
            if (Interlocked.Exchange(ref _Disposed, 1) == 1) return;
            if (disposing && OwnsClient) _Client.Dispose();
            DeleteDirectory(_TemporaryDirectory);
        }

        private void ThrowIfDisposed()
        {
            if (Volatile.Read(ref _Disposed) == 1) throw new ObjectDisposedException(nameof(LiteGraphBackend));
        }

        private async IAsyncEnumerable<object> QueryCoreAsync(QueryModel model, [EnumeratorCancellation] CancellationToken token)
        {
            // The rows are read completely before the first one is returned, so callers that write while enumerating
            // (for example deleting each row read) cannot shift the scan.
            EntityMetadata metadata = model.Metadata;
            List<LiteGraphRow> rows = await ApplyModelAsync(model, "Query", token).ConfigureAwait(false);
            foreach (LiteGraphRow row in rows)
            {
                token.ThrowIfCancellationRequested();
                yield return Materialize(metadata, row);
            }
        }

        private async Task<List<LiteGraphRow>> ApplyModelAsync(QueryModel model, string operation, CancellationToken token)
        {
            ThrowIfDisposed();
            token.ThrowIfCancellationRequested();
            LiteGraphTransaction? transaction = ResolveTransaction(model.Transaction);
            if (transaction != null) await transaction.Lock.WaitAsync(token).ConfigureAwait(false);
            try
            {
                transaction?.ThrowIfCompleted();
                LiteGraphSelection selection = await SelectAsync(model.Source, model.Filter, model.Orderings, null, transaction?.Scope, transaction != null, operation, token).ConfigureAwait(false);
                return selection.Evaluator.Apply(model, selection.Candidates);
            }
            finally
            {
                transaction?.Lock.Release();
            }
        }

        private async Task<LiteGraphSelection> SelectAsync(
            QuerySource source,
            QueryNode? filter,
            IReadOnlyList<QueryOrdering>? orderings,
            IEnumerable<QueryNode>? extra,
            LiteGraphWriteScope? scope,
            bool inTransaction,
            string operation,
            CancellationToken token)
        {
            _Registry.Register(source.Metadata);
            LiteGraphReadRequest request = _Planner.Plan(source, filter);
            List<LiteGraphRow> candidates = await ReadRowsAsync(request, scope, inTransaction, operation, token).ConfigureAwait(false);
            LiteGraphRowSet related = await LoadRelatedAsync(filter, orderings, extra, scope, inTransaction, token).ConfigureAwait(false);
            return new LiteGraphSelection(candidates, new LiteGraphQueryEvaluator(_Values, related));
        }

        private async Task<LiteGraphRowSet> LoadRelatedAsync(
            QueryNode? filter,
            IReadOnlyList<QueryOrdering>? orderings,
            IEnumerable<QueryNode>? extra,
            LiteGraphWriteScope? scope,
            bool inTransaction,
            CancellationToken token)
        {
            Dictionary<Type, EntityMetadata> types = new Dictionary<Type, EntityMetadata>();
            LiteGraphRelatedTypes.Collect(filter, types);
            if (orderings != null)
            {
                foreach (QueryOrdering ordering in orderings) LiteGraphRelatedTypes.Collect(ordering.Key, types);
            }

            if (extra != null)
            {
                foreach (QueryNode node in extra) LiteGraphRelatedTypes.Collect(node, types);
            }

            LiteGraphRowSet related = new LiteGraphRowSet();
            foreach (EntityMetadata metadata in types.Values)
            {
                related.Add(metadata, await ReadRowsAsync(LiteGraphReadRequest.All(metadata), scope, inTransaction, "Related", token).ConfigureAwait(false));
            }

            return related;
        }

        private async Task<List<LiteGraphRow>> ReadRowsAsync(LiteGraphReadRequest request, LiteGraphWriteScope? scope, bool inTransaction, string operation, CancellationToken token)
        {
            RaisePlan(operation, request, inTransaction);
            List<LiteGraphRow> stored = await ReadStoredAsync(request, token).ConfigureAwait(false);
            if (scope == null || scope.Pending.Count == 0) return stored;

            Type entityType = request.Metadata.EntityType;
            List<LiteGraphRow> merged = new List<LiteGraphRow>(stored.Count);
            HashSet<Guid> seen = new HashSet<Guid>();
            foreach (LiteGraphRow row in stored)
            {
                seen.Add(row.Guid);
                if (scope.Pending.TryGetValue(row.Guid, out LiteGraphPendingNode? pending))
                {
                    if (pending.Current != null) merged.Add(pending.Current);
                }
                else
                {
                    merged.Add(row);
                }
            }

            HashSet<Guid>? requested = request.Guids != null ? new HashSet<Guid>(request.Guids) : null;
            foreach (KeyValuePair<Guid, LiteGraphPendingNode> pending in scope.Pending)
            {
                if (pending.Value.Current == null || pending.Value.Metadata.EntityType != entityType || seen.Contains(pending.Key)) continue;
                if (requested != null && !requested.Contains(pending.Key)) continue;
                merged.Add(pending.Value.Current);
            }

            SortRows(merged);
            return merged;
        }

        private async Task<List<LiteGraphRow>> ReadStoredAsync(LiteGraphReadRequest request, CancellationToken token)
        {
            List<LiteGraphRow> rows = new List<LiteGraphRow>();
            await foreach (LiteGraphRow row in StreamStoredAsync(request, token).ConfigureAwait(false)) rows.Add(row);
            if (request.Guids != null) SortRows(rows);
            return rows;
        }

        private async IAsyncEnumerable<LiteGraphRow> StreamStoredAsync(LiteGraphReadRequest request, [EnumeratorCancellation] CancellationToken token)
        {
            EntityMetadata metadata = request.Metadata;
            if (request.Guids != null)
            {
                List<Guid> guids = request.Guids.ToList();
                for (int start = 0; start < guids.Count; start += _ReadChunkSize)
                {
                    List<Guid> chunk = guids.GetRange(start, Math.Min(_ReadChunkSize, guids.Count - start));
                    await foreach (Node node in _Client.Node.ReadByGuids(TenantGuid, chunk, true, false, token).ConfigureAwait(false))
                    {
                        if (node.GraphGUID != GraphGuid) continue;
                        yield return Parse(metadata, node);
                    }
                }

                yield break;
            }

            await foreach (Node node in _Client.Node.ReadMany(TenantGuid, GraphGuid, null, new List<string> { metadata.TableName }, null, request.Filter, EnumerationOrderEnum.CreatedAscending, 0, true, false, token).ConfigureAwait(false))
            {
                yield return Parse(metadata, node);
            }
        }

        private async Task<int> WriteAsync(ITransaction? transaction, Func<LiteGraphWriteScope, bool, Task<int>> work, CancellationToken token)
        {
            ThrowIfDisposed();
            token.ThrowIfCancellationRequested();
            LiteGraphTransaction? owned = ResolveTransaction(transaction);
            if (owned == null)
            {
                await _WriteGate.WaitAsync(token).ConfigureAwait(false);
                try
                {
                    LiteGraphWriteScope scope = new LiteGraphWriteScope();
                    int count = await work(scope, false).ConfigureAwait(false);
                    await ApplyAsync(scope.Operations, false, token).ConfigureAwait(false);
                    return count;
                }
                finally
                {
                    _WriteGate.Release();
                }
            }

            await owned.Lock.WaitAsync(token).ConfigureAwait(false);
            try
            {
                owned.ThrowIfCompleted();
                owned.Scope.Checkpoint();
                try
                {
                    return await work(owned.Scope, true).ConfigureAwait(false);
                }
                catch
                {
                    owned.Scope.Restore();
                    throw;
                }
            }
            finally
            {
                owned.Lock.Release();
            }
        }

        private async Task ApplyAsync(List<TransactionOperation> operations, bool atomic, CancellationToken token)
        {
            if (operations.Count == 0) return;
            if (atomic && operations.Count > _MaxOperations)
                throw new InvalidOperationException("The write needs " + operations.Count + " LiteGraph operations, more than MaxOperationsPerTransaction (" + _MaxOperations + ").");

            for (int start = 0; start < operations.Count; start += _MaxOperations)
            {
                TransactionRequest request = new TransactionRequest
                {
                    MaxOperations = _MaxOperations,
                    TimeoutSeconds = _TimeoutSeconds,
                    Operations = operations.GetRange(start, Math.Min(_MaxOperations, operations.Count - start))
                };

                TransactionResult result = await _Client.Transaction.Execute(TenantGuid, GraphGuid, request, token).ConfigureAwait(false);
                if (!result.Success)
                {
                    throw new InvalidOperationException(
                        "LiteGraph rejected the write (graph transaction " + result.TransactionId + ", state " + result.State
                        + (result.FailedOperationIndex.HasValue ? ", operation " + (start + result.FailedOperationIndex.Value) : string.Empty)
                        + "): " + result.Error);
                }
            }
        }

        private async Task CheckConflictsAsync(LiteGraphWriteScope scope, CancellationToken token)
        {
            Dictionary<Guid, Node> stored = new Dictionary<Guid, Node>();
            List<Guid> guids = scope.Pending.Keys.ToList();
            for (int start = 0; start < guids.Count; start += _ReadChunkSize)
            {
                List<Guid> chunk = guids.GetRange(start, Math.Min(_ReadChunkSize, guids.Count - start));
                await foreach (Node node in _Client.Node.ReadByGuids(TenantGuid, chunk, true, false, token).ConfigureAwait(false))
                {
                    if (node.GraphGUID == GraphGuid) stored[node.GUID] = node;
                }
            }

            foreach (KeyValuePair<Guid, LiteGraphPendingNode> pending in scope.Pending)
            {
                bool exists = stored.TryGetValue(pending.Key, out Node? node);
                string entity = pending.Value.Metadata.EntityType.Name;
                if (pending.Value.ExistedBefore)
                {
                    if (!exists)
                        throw new InvalidOperationException("Transaction commit failed: the " + entity + " row (node " + pending.Key + ") was deleted by another writer after the transaction wrote it. The transaction was rolled back.");
                    if (!string.Equals(Signature(node!), pending.Value.BaseSignature, StringComparison.Ordinal))
                        throw new InvalidOperationException("Transaction commit failed: the " + entity + " row (node " + pending.Key + ") was changed by another writer after the transaction wrote it. The transaction was rolled back.");
                }
                else if (exists)
                {
                    throw new InvalidOperationException("Transaction commit failed: a " + entity + " row with the same primary key (node " + pending.Key + ") was created by another writer. The transaction was rolled back.");
                }
            }
        }

        private async Task AddInsertAsync(LiteGraphWriteScope scope, LiteGraphRow row, bool inTransaction, CancellationToken token)
        {
            EntityMetadata metadata = row.Metadata;
            scope.Write(metadata, row.Guid, row, null);
            scope.Operations.Add(NodeOperation(TransactionOperationTypeEnum.Create, row, row.Subordinates));
            if (!MaintainEdges) return;

            foreach (LiteGraphRelationship relationship in _Registry.ForDependent(metadata.EntityType))
            {
                object? foreignKey = row.Values[LiteGraphTableSchema.For(metadata).Ordinal(relationship.ForeignKey)];
                if (foreignKey == null) continue;
                Guid principal = LiteGraphIdentity.Node(GraphGuid, relationship.Principal.TableName, new[] { foreignKey });
                if (principal == row.Guid || await ExistsAsync(principal, scope, token).ConfigureAwait(false))
                    scope.Operations.Add(EdgeOperation(relationship, row.Guid, principal));
            }

            foreach (LiteGraphRelationship relationship in _Registry.ForPrincipal(metadata.EntityType))
            {
                object? key = row.Values[LiteGraphTableSchema.For(metadata).Ordinal(relationship.Principal.KeyColumns[0])];
                if (key == null) continue;
                foreach (LiteGraphRow dependent in await DependentsAsync(relationship, key, scope, inTransaction, token).ConfigureAwait(false))
                {
                    if (dependent.Guid == row.Guid && relationship.Dependent.EntityType == metadata.EntityType) continue;
                    scope.Operations.Add(EdgeOperation(relationship, dependent.Guid, row.Guid));
                }
            }
        }

        private async Task AddUpdateAsync(LiteGraphWriteScope scope, LiteGraphRow previous, object?[] values, bool inTransaction, CancellationToken token)
        {
            EntityMetadata metadata = previous.Metadata;
            Node? subordinates = previous.Subordinates;
            if (subordinates == null && _PreserveSubordinates && previous.Signature != null)
                subordinates = await _Client.Node.ReadByGuid(TenantGuid, GraphGuid, previous.Guid, false, true, token).ConfigureAwait(false);

            LiteGraphRow row = new LiteGraphRow(previous.Guid, metadata, values, previous.CreatedUtc, null) { Subordinates = subordinates };
            scope.Write(metadata, row.Guid, row, previous.Signature != null ? previous : null);
            scope.Operations.Add(NodeOperation(TransactionOperationTypeEnum.Update, row, subordinates));
            if (!MaintainEdges) return;

            LiteGraphTableSchema schema = LiteGraphTableSchema.For(metadata);
            foreach (LiteGraphRelationship relationship in _Registry.ForDependent(metadata.EntityType))
            {
                int ordinal = schema.Ordinal(relationship.ForeignKey);
                object? before = previous.Values[ordinal];
                object? after = values[ordinal];
                if (QueryValueComparer.Ordinal.Equals(before, after)) continue;

                scope.Operations.Add(new TransactionOperation
                {
                    OperationType = TransactionOperationTypeEnum.Delete,
                    ObjectType = TransactionObjectTypeEnum.Edge,
                    GUID = LiteGraphIdentity.Edge(GraphGuid, relationship.Id, row.Guid)
                });

                if (after == null) continue;
                Guid principal = LiteGraphIdentity.Node(GraphGuid, relationship.Principal.TableName, new[] { after });
                if (principal == row.Guid || await ExistsAsync(principal, scope, token).ConfigureAwait(false))
                    scope.Operations.Add(EdgeOperation(relationship, row.Guid, principal));
            }
        }

        private void AddDelete(LiteGraphWriteScope scope, LiteGraphRow row)
        {
            scope.Write(row.Metadata, row.Guid, null, row.Signature != null ? row : null);
            scope.Operations.Add(new TransactionOperation
            {
                OperationType = TransactionOperationTypeEnum.Delete,
                ObjectType = TransactionObjectTypeEnum.Node,
                GUID = row.Guid
            });
        }

        private async Task<List<LiteGraphRow>> DependentsAsync(LiteGraphRelationship relationship, object key, LiteGraphWriteScope scope, bool inTransaction, CancellationToken token)
        {
            Expr? filter = _Planner.Equality(relationship.ForeignKey, key);
            List<LiteGraphRow> rows = await ReadRowsAsync(new LiteGraphReadRequest(relationship.Dependent, null, filter), scope, inTransaction, "Dependents", token).ConfigureAwait(false);
            int ordinal = LiteGraphTableSchema.For(relationship.Dependent).Ordinal(relationship.ForeignKey);
            return rows.Where(r => r.Values[ordinal] != null && QueryValueComparer.Ordinal.Equals(r.Values[ordinal], key)).ToList();
        }

        private async Task<bool> ExistsAsync(Guid guid, LiteGraphWriteScope scope, CancellationToken token)
        {
            if (scope.Pending.TryGetValue(guid, out LiteGraphPendingNode? pending)) return pending.Current != null;
            if (scope.StoredExistence.TryGetValue(guid, out bool known)) return known;

            bool exists = false;
            await foreach (Node node in _Client.Node.ReadByGuids(TenantGuid, new List<Guid> { guid }, false, false, token).ConfigureAwait(false))
            {
                if (node.GraphGUID == GraphGuid) exists = true;
            }

            scope.StoredExistence[guid] = exists;
            return exists;
        }

        private async Task<LiteGraphIdentitySequence> SequenceAsync(EntityMetadata metadata, ColumnMetadata generated, CancellationToken token)
        {
            LiteGraphIdentitySequence sequence;
            lock (_Sequences)
            {
                if (!_Sequences.TryGetValue(metadata.EntityType, out LiteGraphIdentitySequence? found))
                {
                    found = new LiteGraphIdentitySequence();
                    _Sequences[metadata.EntityType] = found;
                }

                sequence = found;
            }

            if (sequence.Initialized) return sequence;
            await _SequenceGate.WaitAsync(token).ConfigureAwait(false);
            try
            {
                if (!sequence.Initialized)
                {
                    int ordinal = LiteGraphTableSchema.For(metadata).Ordinal(generated);
                    long largest = 0;
                    foreach (LiteGraphRow row in await ReadStoredAsync(LiteGraphReadRequest.All(metadata), token).ConfigureAwait(false))
                    {
                        if (IsInteger(row.Values[ordinal])) largest = Math.Max(largest, Convert.ToInt64(row.Values[ordinal], CultureInfo.InvariantCulture));
                    }

                    sequence.Initialize(largest);
                }
            }
            finally
            {
                _SequenceGate.Release();
            }

            return sequence;
        }

        private async Task<Guid> NextFreeKeyAsync(EntityMetadata metadata, ColumnMetadata generated, int ordinal, LiteGraphIdentitySequence sequence, object?[] values, LiteGraphWriteScope scope, CancellationToken token)
        {
            LiteGraphTableSchema schema = LiteGraphTableSchema.For(metadata);
            for (int attempt = 0; attempt < 10000; attempt++)
            {
                values[ordinal] = _Values.ToStored(generated, Convert.ChangeType(sequence.Next(), generated.ClrType, CultureInfo.InvariantCulture));
                Guid guid = LiteGraphIdentity.Node(GraphGuid, metadata.TableName, RequireKey(metadata, schema, values));
                if (!await ExistsAsync(guid, scope, token).ConfigureAwait(false)) return guid;
            }

            throw new InvalidOperationException("Could not generate a free " + generated.Name + " value for " + metadata.EntityType.Name + ".");
        }

        private TransactionOperation NodeOperation(TransactionOperationTypeEnum type, LiteGraphRow row, Node? subordinates)
        {
            string label = row.Metadata.TableName;
            List<string> labels = new List<string> { label };
            if (subordinates?.Labels != null)
            {
                foreach (string existing in subordinates.Labels)
                {
                    if (!string.IsNullOrEmpty(existing) && !labels.Contains(existing, StringComparer.Ordinal)) labels.Add(existing);
                }
            }

            Node node = new Node
            {
                GUID = row.Guid,
                TenantGUID = TenantGuid,
                GraphGUID = GraphGuid,
                Name = NodeName(row),
                Labels = labels,
                Tags = subordinates?.Tags != null && subordinates.Tags.Count > 0 ? subordinates.Tags : null,
                Vectors = subordinates?.Vectors != null && subordinates.Vectors.Count > 0 ? subordinates.Vectors : null,
                Data = BuildData(row),
                CreatedUtc = row.CreatedUtc,
                LastUpdateUtc = DateTime.UtcNow
            };

            return new TransactionOperation { OperationType = type, ObjectType = TransactionObjectTypeEnum.Node, GUID = row.Guid, Payload = node };
        }

        private TransactionOperation EdgeOperation(LiteGraphRelationship relationship, Guid dependent, Guid principal)
        {
            Guid guid = LiteGraphIdentity.Edge(GraphGuid, relationship.Id, dependent);
            ArrayBufferWriter<byte> buffer = new ArrayBufferWriter<byte>();
            using (Utf8JsonWriter writer = new Utf8JsonWriter(buffer))
            {
                writer.WriteStartObject();
                writer.WriteString("durable_relationship", relationship.Id);
                writer.WriteString("foreign_key", relationship.ForeignKey.Name);
                writer.WriteString("dependent", relationship.Dependent.TableName);
                writer.WriteString("principal", relationship.Principal.TableName);
                writer.WriteEndObject();
            }

            Edge edge = new Edge
            {
                GUID = guid,
                TenantGUID = TenantGuid,
                GraphGUID = GraphGuid,
                From = dependent,
                To = principal,
                Name = relationship.Label,
                Labels = new List<string> { relationship.Label },
                Data = ParseJson(buffer.WrittenMemory),
                CreatedUtc = DateTime.UtcNow,
                LastUpdateUtc = DateTime.UtcNow
            };

            return new TransactionOperation { OperationType = TransactionOperationTypeEnum.Upsert, ObjectType = TransactionObjectTypeEnum.Edge, GUID = guid, Payload = edge };
        }

        private static JsonElement BuildData(LiteGraphRow row)
        {
            ArrayBufferWriter<byte> buffer = new ArrayBufferWriter<byte>();
            using (Utf8JsonWriter writer = new Utf8JsonWriter(buffer))
            {
                writer.WriteStartObject();
                IReadOnlyList<ColumnMetadata> columns = row.Metadata.Columns;
                for (int i = 0; i < columns.Count; i++)
                {
                    writer.WritePropertyName(columns[i].Name);
                    LiteGraphJsonCodec.Write(writer, row.Values[i]);
                }

                writer.WriteEndObject();
            }

            return ParseJson(buffer.WrittenMemory);
        }

        private static JsonElement ParseJson(ReadOnlyMemory<byte> json)
        {
            using (JsonDocument document = JsonDocument.Parse(json))
            {
                return document.RootElement.Clone();
            }
        }

        private static string NodeName(LiteGraphRow row)
        {
            LiteGraphTableSchema schema = LiteGraphTableSchema.For(row.Metadata);
            string name = row.Metadata.TableName + ":" + string.Join(",", schema.KeyOf(row.Values).Select(v => Convert.ToString(v, CultureInfo.InvariantCulture)));
            return name.Length > 128 ? name.Substring(0, 128) : name;
        }

        private LiteGraphRow Parse(EntityMetadata metadata, Node node)
        {
            JsonElement data;
            if (node.Data is JsonElement element) data = element;
            else if (node.Data is string text) data = ParseJson(System.Text.Encoding.UTF8.GetBytes(text));
            else data = default;

            if (data.ValueKind != JsonValueKind.Object)
                throw new InvalidOperationException("Node " + node.GUID + " labelled '" + metadata.TableName + "' has no Durable data object; it was not written by Durable.");

            LiteGraphTableSchema schema = LiteGraphTableSchema.For(metadata);
            object?[] values = new object?[metadata.Columns.Count];
            for (int i = 0; i < values.Length; i++)
            {
                if (data.TryGetProperty(metadata.Columns[i].Name, out JsonElement value)) values[i] = LiteGraphJsonCodec.Read(value, schema.StoredTypes[i]);
            }

            return new LiteGraphRow(node.GUID, metadata, values, node.CreatedUtc, Signature(node));
        }

        private static string Signature(Node node)
        {
            string data = node.Data is JsonElement element ? element.GetRawText() : Convert.ToString(node.Data, CultureInfo.InvariantCulture) ?? string.Empty;
            return node.LastUpdateUtc.Ticks.ToString(CultureInfo.InvariantCulture) + "|" + data;
        }

        private static bool IsDurableEdge(Edge edge)
        {
            return edge.Data is JsonElement element
                && element.ValueKind == JsonValueKind.Object
                && element.TryGetProperty("durable_relationship", out JsonElement relationship)
                && relationship.ValueKind == JsonValueKind.String;
        }

        private object Materialize(EntityMetadata metadata, LiteGraphRow row)
        {
            object entity = metadata.CreateInstance();
            for (int i = 0; i < metadata.Columns.Count; i++)
            {
                ColumnMetadata column = metadata.Columns[i];
                column.SetValue(entity, _Values.FromStored(column, row.Values[i]));
            }

            return entity;
        }

        private object?[] ToStoredValues(EntityMetadata metadata, object entity)
        {
            object?[] values = new object?[metadata.Columns.Count];
            for (int i = 0; i < values.Length; i++)
            {
                ColumnMetadata column = metadata.Columns[i];
                values[i] = _Values.ToStored(column, column.GetValue(entity));
            }

            return values;
        }

        private LiteGraphTransaction? ResolveTransaction(ITransaction? transaction)
        {
            if (transaction == null) return null;
            if (transaction is not LiteGraphTransaction liteGraph || !ReferenceEquals(liteGraph.Backend, this))
                throw new ArgumentException("The transaction was not created by this LiteGraph backend.", nameof(transaction));
            liteGraph.ThrowIfCompleted();
            return liteGraph;
        }

        private void RaisePlan(string operation, LiteGraphReadRequest request, bool inTransaction)
        {
            LiteGraphQueryPlan plan = new LiteGraphQueryPlan(
                operation,
                request.Metadata.EntityType,
                request.Metadata.TableName,
                request.Strategy,
                request.Guids,
                request.Filter?.ToString(),
                inTransaction);
            _LastQueryPlan = plan;
            EventHandler<LiteGraphQueryPlan>? handlers = QueryPlanned;
            if (handlers != null)
            {
                try
                {
                    handlers(this, plan);
                }
                catch (Exception e)
                {
                    _Logger?.LogWarning(e, "A LiteGraph QueryPlanned handler threw: {Message}", e.Message);
                }
            }

            if (_Logger != null && _Logger.IsEnabled(LogLevel.Debug)) _Logger.LogDebug("LiteGraph read: {Plan}", plan.ToString());
        }

        private DateTime NextCreatedUtc()
        {
            long now = DateTime.UtcNow.Ticks;
            now -= now % 10;
            while (true)
            {
                long last = Interlocked.Read(ref _LastCreatedTicks);
                long next = Math.Max(now, last + 10);
                if (Interlocked.CompareExchange(ref _LastCreatedTicks, next, last) == last) return new DateTime(next, DateTimeKind.Utc);
            }
        }

        private static void SortRows(List<LiteGraphRow> rows)
        {
            rows.Sort((a, b) =>
            {
                int byTime = a.CreatedUtc.Ticks.CompareTo(b.CreatedUtc.Ticks);
                return byTime != 0 ? byTime : string.CompareOrdinal(a.Guid.ToString("D"), b.Guid.ToString("D"));
            });
        }

        private static object?[] RequireKey(EntityMetadata metadata, LiteGraphTableSchema schema, object?[] values)
        {
            object?[] key = schema.KeyOf(values);
            for (int i = 0; i < key.Length; i++)
            {
                if (key[i] == null)
                    throw new InvalidOperationException("Primary key column '" + metadata.KeyColumns[i].Name + "' of " + metadata.EntityType.Name + " is null.");
            }

            return key;
        }

        private static string FormatKey(object?[] key)
        {
            return key.Length == 1
                ? Convert.ToString(key[0], CultureInfo.InvariantCulture) ?? "null"
                : "(" + string.Join(", ", key.Select(k => Convert.ToString(k, CultureInfo.InvariantCulture))) + ")";
        }

        private static bool IsUnsetGenerated(object? value)
        {
            switch (value)
            {
                case null: return true;
                case int i: return i == 0;
                case long l: return l == 0;
                case short s: return s == 0;
                case byte b: return b == 0;
                default: return false;
            }
        }

        private static bool IsInteger(object? value)
        {
            return value is int || value is long || value is short || value is byte || value is sbyte || value is ushort || value is uint;
        }

        private static async Task<Guid> ResolveTenantAsync(LiteGraphClient client, LiteGraphRepositorySettings settings, CancellationToken token)
        {
            if (settings.TenantGuid.HasValue)
            {
                Guid guid = settings.TenantGuid.Value;
                if (!await client.Tenant.ExistsByGuid(guid, token).ConfigureAwait(false))
                    await client.Tenant.Create(new TenantMetadata { GUID = guid, Name = settings.TenantName }, token).ConfigureAwait(false);
                return guid;
            }

            await foreach (TenantMetadata tenant in client.Tenant.ReadMany(EnumerationOrderEnum.CreatedAscending, 0, token).ConfigureAwait(false))
            {
                if (string.Equals(tenant.Name, settings.TenantName, StringComparison.Ordinal)) return tenant.GUID;
            }

            TenantMetadata created = await client.Tenant.Create(new TenantMetadata { Name = settings.TenantName }, token).ConfigureAwait(false);
            return created.GUID;
        }

        private static async Task<Guid> ResolveGraphAsync(LiteGraphClient client, Guid tenantGuid, Guid? graphGuid, string graphName, CancellationToken token)
        {
            if (graphGuid.HasValue)
            {
                if (!await client.Graph.ExistsByGuid(tenantGuid, graphGuid.Value, token).ConfigureAwait(false))
                    await client.Graph.Create(new Graph { TenantGUID = tenantGuid, GUID = graphGuid.Value, Name = graphName }, token).ConfigureAwait(false);
                return graphGuid.Value;
            }

            await foreach (Graph graph in client.Graph.ReadMany(tenantGuid, graphName, null, null, null, EnumerationOrderEnum.CreatedAscending, 0, false, false, token).ConfigureAwait(false))
            {
                if (string.Equals(graph.Name, graphName, StringComparison.Ordinal)) return graph.GUID;
            }

            Graph createdGraph = await client.Graph.Create(new Graph { TenantGUID = tenantGuid, Name = graphName }, token).ConfigureAwait(false);
            return createdGraph.GUID;
        }

        private static void DeleteDirectory(string? directory)
        {
            if (string.IsNullOrEmpty(directory)) return;
            try
            {
                if (Directory.Exists(directory)) Directory.Delete(directory, true);
            }
            catch (IOException)
            {
                // Best effort: the driver may still hold the file; the temporary directory is left behind.
            }
            catch (UnauthorizedAccessException)
            {
                // Best effort, as above.
            }
        }

        #endregion
    }
}
