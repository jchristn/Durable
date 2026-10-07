namespace Durable.CosmosDb
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Runtime.CompilerServices;
    using System.Text;
    using System.Text.Json;
    using System.Text.Json.Nodes;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using Durable;
    using Durable.Query;
    using Microsoft.Azure.Cosmos;

    /// <summary>
    /// An Azure Cosmos DB for NoSQL <see cref="IRepositoryBackend"/>: stores entities as JSON documents in a Cosmos DB
    /// database. Use it through <see cref="CosmosDbRepository{T}"/> (or any <see cref="RepositoryBase{T}"/>); one backend
    /// wraps one <see cref="CosmosClient"/> and database and serves every entity type, so includes and navigation predicates
    /// work across repositories that share it. Create one backend per database and share it.
    /// <para>
    /// Storage: each entity type is a container named <see cref="EntityMetadata.TableName"/> (created on first use), each
    /// mapped column a document property named after the column, written by Durable from <see cref="EntityMetadata"/> with
    /// System.Text.Json through the SDK's stream APIs (the SDK's reflection serializer is never used for entities). The
    /// document <c>id</c> is the primary key as text (composite keys joined with <c>|</c>); a single primary key column named
    /// <c>id</c> with string values is the <c>id</c> itself, and a column named <c>id</c> that is not is stored as
    /// <c>durable_id</c>. Values are stored the way a database driver would (<see cref="IValueConverter"/> provider values,
    /// JSON text for JSON columns, enum names or numbers) and encoded losslessly and so that Cosmos DB orders them like C#
    /// (see <see cref="CosmosDbRepositorySettings"/> for partition keys): numbers as JSON numbers, DateTime as fixed-width
    /// ISO 8601 wall-clock text plus a kind suffix (<c>Z</c>, none, or the local offset), DateTimeOffset as its UTC instant
    /// (<c>...Z</c>), DateOnly as <c>yyyy-MM-dd</c>, TimeSpan and TimeOnly as ticks, Guid as lowercase text, byte arrays as
    /// base64. Because Cosmos DB may hold JSON numbers only as doubles, longs beyond plus or minus 2^53 and decimals a double
    /// cannot round-trip keep an exact copy under the document's <c>_durable</c> object, as does the offset of a
    /// DateTimeOffset; reads use it. Decimal scale (trailing zeros) is not preserved. Auto-increment values come from a
    /// counter document per container (see <see cref="CosmosDbRepositorySettings.SequenceContainerName"/>) incremented with
    /// an ETag-conditioned replace, so concurrent creates never share a key, also across processes. Inserting a duplicate
    /// primary key throws <see cref="InvalidOperationException"/>. Cosmos DB indexes every property;
    /// <see cref="IndexAttribute"/> needs no action, and <see cref="IndexAttribute.IsUnique"/> is not enforced (Cosmos DB
    /// unique keys apply only within a logical partition and only at container creation).
    /// </para>
    /// <para>
    /// Queries: the parts of a filter whose Cosmos DB semantics match C# (see <see cref="CosmosDbQueryPlan"/>) are pushed down
    /// as a parameterized Cosmos DB SQL query, with a single-key ORDER BY, OFFSET/LIMIT, COUNT, SUM, AVG, MIN and MAX when the
    /// whole query is exact; key lookups are point reads. Everything else (functions, navigations, multi-key ordering,
    /// grouping, residual filters) is evaluated client-side with C# semantics over the documents Cosmos DB returns, so
    /// push-down never changes results. Strings compare ordinally and case-sensitively, so
    /// <see cref="StringMatchMode.Database"/> behaves as <see cref="StringMatchMode.Ordinal"/>. Unordered queries return
    /// documents in primary key order.
    /// </para>
    /// <para>
    /// Writes: every single-document write is atomic. Updates and deletes read the matching documents and write each one
    /// with an ETag condition (<c>If-Match</c>); when another writer changed a document first, it is read again, re-checked
    /// against the condition (for example the expected version) and written again, so optimistic concurrency and set-based
    /// updates never lose updates. A write that matches several documents is not atomic as a whole, and neither is a change
    /// of a document's id or partition key (create the new document, then delete the old one). Transactions are not
    /// supported (<see cref="RepositoryCapabilities.Transactions"/> is absent): Cosmos DB transactional batches cover only one
    /// logical partition, so Durable does not pretend to offer atomicity across documents.
    /// </para>
    /// <para>
    /// Lifetime: create a backend with <see cref="CreateAsync"/> (or <see cref="Create"/>), share it across repositories, and
    /// dispose it when done. It disposes the client only when it created it (<see cref="OwnsClient"/>); a client passed in
    /// with <see cref="CosmosDbRepositorySettings.Client"/> is never disposed. Repositories never dispose the backend.
    /// </para>
    /// <para>
    /// Trimming and Native AOT: not supported. The Azure Cosmos DB SDK (Microsoft.Azure.Cosmos) uses Newtonsoft.Json and
    /// reflection internally, so <see cref="CreateAsync"/> and <see cref="Create"/> carry
    /// <c>[RequiresUnreferencedCode]</c> and <c>[RequiresDynamicCode]</c>. Durable's own code in
    /// this package is trim-annotated and warning-free.
    /// </para>
    /// Thread safety: safe for concurrent use by any number of repositories and threads.
    /// </summary>
    public sealed class CosmosDbBackend : IRepositoryBackend, IDisposable, IAsyncDisposable
    {
        #region Public-Members

        /// <summary>
        /// Gets the optional features every Cosmos DB backend supports: everything except
        /// <see cref="RepositoryCapabilities.Transactions"/>.
        /// </summary>
        public static RepositoryCapabilities SupportedCapabilities => RepositoryCapabilities.All & ~RepositoryCapabilities.Transactions;

        /// <summary>
        /// Gets the optional features the backend supports (see <see cref="SupportedCapabilities"/>).
        /// </summary>
        public RepositoryCapabilities Capabilities => SupportedCapabilities;

        /// <summary>
        /// Gets the Cosmos DB client. Never null.
        /// </summary>
        public CosmosClient Client { get; }

        /// <summary>
        /// Gets whether the backend disposes <see cref="Client"/> when it is disposed (true when the backend created it from
        /// settings, false when the client was passed in).
        /// </summary>
        public bool OwnsClient { get; }

        /// <summary>
        /// Gets the Cosmos DB database holding the containers. Never null.
        /// </summary>
        public Database Database { get; }

        /// <summary>
        /// Gets the JSON options used to store JSON columns. Never null.
        /// Default: camelCase property names, not indented (the same as the SQL providers).
        /// </summary>
        public JsonSerializerOptions JsonOptions => _Values.JsonOptions;

        /// <summary>
        /// Gets the plan of the most recent read or write executed by any thread, or null before the first one. Use it in
        /// single-threaded diagnostics and tests; use <see cref="QueryPlanned"/> to observe every plan.
        /// </summary>
        public CosmosDbQueryPlan? LastQueryPlan => _LastQueryPlan;

        /// <summary>
        /// Raised with the plan of every read and write, synchronously on the thread that executed it; the sender is the
        /// backend. Handlers must be thread-safe and fast and must not call the backend; an exception thrown by a handler is
        /// logged (Warning) and otherwise ignored.
        /// </summary>
        public event EventHandler<CosmosDbQueryPlan>? QueryPlanned;

        #endregion

        #region Private-Members

        private const string _NotAotCompatible =
            "The Azure Cosmos DB SDK (Microsoft.Azure.Cosmos) uses Newtonsoft.Json and reflection internally, which trimming and "
            + "Native AOT do not support. Durable.CosmosDb itself is trim-annotated and writes documents with System.Text.Json; "
            + "use the Cosmos DB backend on the JIT runtime.";

        private static readonly JsonSerializerOptions _DefaultJsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };

        private static readonly object _Unresolved = new object();
        private static readonly ItemRequestOptions _NoContent = new ItemRequestOptions { EnableContentResponseOnWrite = false };
        private readonly CosmosDbValueConverter _Values;
        private readonly ILogger? _Logger;
        private readonly Dictionary<Type, string> _PartitionKeys;
        private readonly bool _CreateIfNotExists;
        private readonly ThroughputProperties? _ContainerThroughput;
        private readonly string _SequenceContainerName;
        private readonly int _MaxConflictRetries;
        private readonly int _MaxConcurrentDeletes;
        private readonly ConcurrentDictionary<Type, CosmosDbContainerSchema> _Schemas = new ConcurrentDictionary<Type, CosmosDbContainerSchema>();
        private readonly ConcurrentDictionary<string, Lazy<Task<Container>>> _Containers = new ConcurrentDictionary<string, Lazy<Task<Container>>>(StringComparer.Ordinal);
        private volatile CosmosDbQueryPlan? _LastQueryPlan;
        private int _Disposed;

        #endregion

        #region Constructors-and-Factories

        private CosmosDbBackend(CosmosClient client, bool ownsClient, Database database, CosmosDbRepositorySettings settings)
        {
            Client = client;
            OwnsClient = ownsClient;
            Database = database;
            _Values = new CosmosDbValueConverter(settings.JsonOptions ?? _DefaultJsonOptions);
            _Logger = settings.Logger;
            _PartitionKeys = new Dictionary<Type, string>(settings.PartitionKeys);
            _CreateIfNotExists = settings.CreateIfNotExists;
            _ContainerThroughput = settings.ContainerAutoscaleMaxThroughput.HasValue
                ? ThroughputProperties.CreateAutoscaleThroughput(settings.ContainerAutoscaleMaxThroughput.Value)
                : settings.ContainerThroughput.HasValue ? ThroughputProperties.CreateManualThroughput(settings.ContainerThroughput.Value) : null;
            _SequenceContainerName = settings.SequenceContainerName;
            _MaxConflictRetries = settings.MaxConflictRetries;
            _MaxConcurrentDeletes = settings.MaxConcurrentDeletes;
        }

        /// <summary>
        /// Creates a backend: uses <see cref="CosmosDbRepositorySettings.Client"/> when set (not owned, never disposed),
        /// otherwise creates a client from the endpoint and key or the connection string (owned and disposed by the backend);
        /// then creates the database when missing (unless <see cref="CosmosDbRepositorySettings.CreateIfNotExists"/> is
        /// false, in which case it must exist). Containers are created on first use.
        /// </summary>
        /// <param name="settings">Settings; null uses <see cref="CosmosDbRepositorySettings.ForEmulator"/> with its defaults.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The backend. Never null. Dispose it when done.</returns>
        /// <exception cref="ArgumentException">Thrown when the settings are invalid (see <see cref="CosmosDbRepositorySettings.Validate"/>).</exception>
        /// <exception cref="InvalidOperationException">Thrown when the database does not exist and may not be created.</exception>
        /// <exception cref="CosmosException">Thrown when Cosmos DB rejects the request (for example a wrong key).</exception>
        /// <exception cref="OperationCanceledException">Thrown when the token is canceled.</exception>
        [RequiresUnreferencedCode(_NotAotCompatible)]
        [RequiresDynamicCode(_NotAotCompatible)]
        public static async Task<CosmosDbBackend> CreateAsync(CosmosDbRepositorySettings? settings = null, CancellationToken token = default)
        {
            settings ??= CosmosDbRepositorySettings.ForEmulator();
            settings.Validate();
            token.ThrowIfCancellationRequested();

            bool owns = settings.Client == null;
            CosmosClient client = settings.Client ?? CreateClient(settings);
            try
            {
                Database database;
                if (settings.CreateIfNotExists)
                {
                    ThroughputProperties? throughput = settings.DatabaseAutoscaleMaxThroughput.HasValue
                        ? ThroughputProperties.CreateAutoscaleThroughput(settings.DatabaseAutoscaleMaxThroughput.Value)
                        : settings.DatabaseThroughput.HasValue ? ThroughputProperties.CreateManualThroughput(settings.DatabaseThroughput.Value) : null;
                    DatabaseResponse response = await client.CreateDatabaseIfNotExistsAsync(settings.DatabaseName, throughput, null, token).ConfigureAwait(false);
                    database = response.Database;
                }
                else
                {
                    database = client.GetDatabase(settings.DatabaseName);
                    using ResponseMessage read = await database.ReadStreamAsync(null, token).ConfigureAwait(false);
                    if (read.StatusCode == HttpStatusCode.NotFound)
                        throw new InvalidOperationException("The Cosmos DB database '" + settings.DatabaseName + "' does not exist and CreateIfNotExists is false.");
                    read.EnsureSuccessStatusCode();
                }

                return new CosmosDbBackend(client, owns, database, settings);
            }
            catch (Exception)
            {
                if (owns) client.Dispose();
                throw;
            }
        }

        /// <summary>
        /// Creates a backend (see <see cref="CreateAsync"/>). Blocks the calling thread while the database is created or
        /// checked; prefer <see cref="CreateAsync"/> in asynchronous code.
        /// </summary>
        /// <param name="settings">Settings; null uses <see cref="CosmosDbRepositorySettings.ForEmulator"/> with its defaults.</param>
        /// <returns>The backend. Never null. Dispose it when done.</returns>
        /// <exception cref="ArgumentException">Thrown when the settings are invalid (see <see cref="CosmosDbRepositorySettings.Validate"/>).</exception>
        /// <exception cref="InvalidOperationException">Thrown when the database does not exist and may not be created.</exception>
        /// <exception cref="CosmosException">Thrown when Cosmos DB rejects the request.</exception>
        [RequiresUnreferencedCode(_NotAotCompatible)]
        [RequiresDynamicCode(_NotAotCompatible)]
        public static CosmosDbBackend Create(CosmosDbRepositorySettings? settings = null)
        {
            return Task.Run(() => CreateAsync(settings, CancellationToken.None)).GetAwaiter().GetResult();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Creates a repository over this backend. Does not touch the account (the container is created on first use).
        /// </summary>
        /// <typeparam name="T">Entity type.</typeparam>
        /// <param name="options">Options; null uses defaults.</param>
        /// <returns>A new repository.</returns>
        /// <exception cref="InvalidOperationException">Thrown when <typeparamref name="T"/> has no primary key or an invalid mapping.</exception>
        /// <exception cref="NotSupportedException">Thrown when the entity cannot be stored in Cosmos DB (see <see cref="CosmosDbRepository{T}"/>).</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        public CosmosDbRepository<T> CreateRepository<[DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] T>(RepositoryOptions? options = null) where T : class, new()
        {
            ThrowIfDisposed();
            return new CosmosDbRepository<T>(this, options);
        }

        /// <summary>
        /// Determines whether a transaction belongs to this backend. Always false: the Cosmos DB backend has no transactions.
        /// </summary>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>False.</returns>
        public bool Owns(ITransaction? transaction)
        {
            return false;
        }

        /// <summary>
        /// Always throws: Cosmos DB has no multi-document transactions across partitions (transactional batches cover one
        /// logical partition), and the backend does not fake atomicity. <see cref="Capabilities"/> lacks
        /// <see cref="RepositoryCapabilities.Transactions"/>, so repositories reject transactions before calling this.
        /// </summary>
        /// <returns>Never returns.</returns>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public ITransaction BeginTransaction()
        {
            throw TransactionsNotSupported();
        }

        /// <summary>
        /// Always throws (see <see cref="BeginTransaction"/>).
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Never returns.</returns>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public Task<ITransaction> BeginTransactionAsync(CancellationToken token = default)
        {
            throw TransactionsNotSupported();
        }

        /// <inheritdoc />
        Task<ITransaction> IRepositoryBackend.BeginTransactionAsync(CancellationToken token)
        {
            throw TransactionsNotSupported();
        }

        /// <summary>
        /// Returns the container of an entity type, creating it when missing (unless
        /// <see cref="CosmosDbRepositorySettings.CreateIfNotExists"/> is false). Use it for Cosmos DB features Durable does
        /// not expose (change feed, indexing policy, throughput).
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The container. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        /// <exception cref="NotSupportedException">Thrown when the entity cannot be stored in Cosmos DB.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the container is missing and may not be created, or exists with another partition key path.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        public Task<Container> GetContainerAsync([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            ThrowIfDisposed();
            return ContainerAsync(Schema(EntityMetadata.For(entityType)), token);
        }

        /// <summary>
        /// Returns the container of an entity type (see <see cref="GetContainerAsync"/>). Blocks the calling thread while the
        /// container is created or checked.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <returns>The container. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        /// <exception cref="NotSupportedException">Thrown when the entity cannot be stored in Cosmos DB.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the container is missing and may not be created, or exists with another partition key path.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        public Container GetContainer([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType)
        {
            return Task.Run(() => GetContainerAsync(entityType, CancellationToken.None)).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Returns the partition key path of an entity's container (<c>/id</c> unless configured in
        /// <see cref="CosmosDbRepositorySettings.PartitionKeys"/>).
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <returns>The path, for example <c>/id</c> or <c>/tenant_id</c>. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        /// <exception cref="NotSupportedException">Thrown when the entity cannot be stored in Cosmos DB.</exception>
        public string GetPartitionKeyPath([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            return Schema(EntityMetadata.For(entityType)).PartitionKeyPath;
        }

        /// <summary>
        /// Returns the stored documents of an entity, in primary key order, for diagnostics and tests that check the stored
        /// representation (including Cosmos DB system properties). Soft-deleted documents are included.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Copies of the documents. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        public async Task<IReadOnlyList<JsonObject>> GetStoredRowsAsync([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            ThrowIfDisposed();
            CosmosDbContainerSchema schema = Schema(EntityMetadata.For(entityType));
            Container container = await ContainerAsync(schema, token).ConfigureAwait(false);
            List<CosmosDbDocument> documents = await QueryDocumentsAsync(container, schema, "SELECT * FROM c", Array.Empty<KeyValuePair<string, object>>(), null, new CosmosDbRequestCharge(), token).ConfigureAwait(false);
            SortByKey(schema, documents);
            return documents.Select(d => d.Content).ToList();
        }

        /// <summary>
        /// Returns the stored documents of an entity (see <see cref="GetStoredRowsAsync"/>). Blocks the calling thread.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <returns>Copies of the documents in primary key order. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        public IReadOnlyList<JsonObject> GetStoredRows([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType)
        {
            return Task.Run(() => GetStoredRowsAsync(entityType, CancellationToken.None)).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Deletes every container of the database (all data and auto-increment counters, including containers Durable did
        /// not create), bypassing soft delete and query filters. The database itself is kept.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task.</returns>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        public async Task ClearAsync(CancellationToken token = default)
        {
            ThrowIfDisposed();
            List<string> names = new List<string>();
            using (FeedIterator iterator = Database.GetContainerQueryStreamIterator("SELECT c.id FROM c"))
            {
                while (iterator.HasMoreResults)
                {
                    using ResponseMessage response = await iterator.ReadNextAsync(token).ConfigureAwait(false);
                    response.EnsureSuccessStatusCode();
                    using JsonDocument page = await JsonDocument.ParseAsync(response.Content, default, token).ConfigureAwait(false);
                    foreach (JsonElement item in page.RootElement.GetProperty("DocumentCollections").EnumerateArray())
                        names.Add(item.GetProperty("id").GetString()!);
                }
            }

            foreach (string name in names)
            {
                using ResponseMessage deleted = await Database.GetContainer(name).DeleteContainerStreamAsync(null, token).ConfigureAwait(false);
                if (deleted.StatusCode != HttpStatusCode.NotFound) deleted.EnsureSuccessStatusCode();
            }

            _Containers.Clear();
        }

        /// <summary>
        /// Deletes every container of the database (see <see cref="ClearAsync(CancellationToken)"/>). Blocks the calling thread.
        /// </summary>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        public void Clear()
        {
            Task.Run(() => ClearAsync(CancellationToken.None)).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Removes every document of one entity type (including soft-deleted rows) and resets its auto-increment counter, so
        /// generated keys restart at 1. The container and its settings are kept. Bypasses soft delete and query filters; not
        /// atomic (documents are deleted one by one, <see cref="CosmosDbRepositorySettings.MaxConcurrentDeletes"/> at a time).
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The number of documents removed.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        /// <exception cref="NotSupportedException">Thrown when the entity cannot be stored in Cosmos DB.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        public async Task<int> ClearAsync([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            ThrowIfDisposed();
            CosmosDbContainerSchema schema = Schema(EntityMetadata.For(entityType));
            Container container = await ContainerAsync(schema, token).ConfigureAwait(false);
            List<CosmosDbDocument> documents = await QueryDocumentsAsync(container, schema, "SELECT * FROM c", Array.Empty<KeyValuePair<string, object>>(), null, new CosmosDbRequestCharge(), token).ConfigureAwait(false);

            int removed = 0;
            using (SemaphoreSlim gate = new SemaphoreSlim(_MaxConcurrentDeletes))
            {
                List<Task> deletes = new List<Task>(documents.Count);
                foreach (CosmosDbDocument document in documents)
                {
                    await gate.WaitAsync(token).ConfigureAwait(false);
                    deletes.Add(Task.Run(async () =>
                    {
                        try
                        {
                            using ResponseMessage response = await container.DeleteItemStreamAsync(document.Id, document.PartitionKey, null, token).ConfigureAwait(false);
                            if (response.StatusCode == HttpStatusCode.NotFound) return;
                            response.EnsureSuccessStatusCode();
                            Interlocked.Increment(ref removed);
                        }
                        finally
                        {
                            gate.Release();
                        }
                    }, token));
                }

                await Task.WhenAll(deletes).ConfigureAwait(false);
            }

            if (schema.Metadata.AutoIncrementColumn != null)
            {
                Container sequences = await SequenceContainerAsync(token).ConfigureAwait(false);
                using ResponseMessage reset = await sequences.DeleteItemStreamAsync(schema.ContainerName, new PartitionKey(schema.ContainerName), null, token).ConfigureAwait(false);
                if (reset.StatusCode != HttpStatusCode.NotFound) reset.EnsureSuccessStatusCode();
            }

            return removed;
        }

        /// <summary>
        /// Removes every document of one entity type (see <see cref="ClearAsync(Type, CancellationToken)"/>). Blocks the
        /// calling thread.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <returns>The number of documents removed.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        /// <exception cref="NotSupportedException">Thrown when the entity cannot be stored in Cosmos DB.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        public int Clear([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType)
        {
            return Task.Run(() => ClearAsync(entityType, CancellationToken.None)).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public IAsyncEnumerable<object> QueryAsync(QueryModel model, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(model);
            ThrowIfDisposed();
            token.ThrowIfCancellationRequested();
            return StreamAsync(model, token);
        }

        /// <inheritdoc />
        public async Task<long> CountAsync(QueryModel model, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(model);
            Prepare(model.Transaction, token);
            CosmosDbContainerSchema schema = Schema(model.Metadata);
            Container container = await ContainerAsync(schema, token).ConfigureAwait(false);
            CosmosDbRequestCharge charge = new CosmosDbRequestCharge();
            CosmosDbPushdown pushdown = await TranslateAsync(container, schema, model, charge, token).ConfigureAwait(false);
            if (!pushdown.Exact || CanPointRead(schema, pushdown)) return (await ReadAsync(model, true, "Count", token).ConfigureAwait(false)).Count;

            string text = "SELECT VALUE COUNT(1) FROM c" + WhereClause(pushdown);
            PartitionKey? partition = PartitionFor(schema, pushdown);
            JsonElement? result = await ScalarAsync(container, text, pushdown.Parameters, partition, charge, token).ConfigureAwait(false);
            long count = result.HasValue && result.Value.ValueKind == JsonValueKind.Number ? result.Value.GetInt64() : 0;
            if (model.Skip.HasValue) count = Math.Max(0, count - model.Skip.Value);
            if (model.Take.HasValue) count = Math.Min(count, model.Take.Value);
            Record(new CosmosDbQueryPlan("Count", schema.Metadata.EntityType, schema.ContainerName, text, pushdown.ParametersJson(), Describe(partition), false, true, null, false, true, -1, charge.Total));
            return count;
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Thrown when <paramref name="function"/> is Count or Any.</exception>
        public async Task<object?> AggregateAsync(QueryModel model, AggregateFunction function, QueryNode operand, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(model);
            ArgumentNullException.ThrowIfNull(operand);
            Prepare(model.Transaction, token);
            CosmosDbContainerSchema schema = Schema(model.Metadata);
            Container container = await ContainerAsync(schema, token).ConfigureAwait(false);

            if (!model.Skip.HasValue && !model.Take.HasValue && operand is ColumnNode column && ReferenceEquals(column.Source, model.Source) && Aggregatable(schema, column.Column, function))
            {
                CosmosDbRequestCharge charge = new CosmosDbRequestCharge();
                CosmosDbPushdown pushdown = await TranslateAsync(container, schema, model, charge, token).ConfigureAwait(false);
                if (pushdown.Exact)
                {
                    object? pushed = await PushedAggregateAsync(container, schema, pushdown, function, column.Column, charge, token).ConfigureAwait(false);
                    if (!ReferenceEquals(pushed, _Unresolved)) return pushed;
                }
            }

            List<CosmosDbDocument> rows = await ReadAsync(model, true, "Aggregate", token).ConfigureAwait(false);
            object? result = Evaluator().Aggregate(model.Source, rows, function, operand);
            if (result != null && operand is ColumnNode own && (function == AggregateFunction.Min || function == AggregateFunction.Max))
                result = _Values.FromStored(own.Column, result);
            return result;
        }

        /// <inheritdoc />
        public async Task InsertAsync(EntityMetadata metadata, object entity, ITransaction? transaction, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(metadata);
            ArgumentNullException.ThrowIfNull(entity);
            Prepare(transaction, token);

            CosmosDbContainerSchema schema = Schema(metadata);
            Container container = await ContainerAsync(schema, token).ConfigureAwait(false);
            ColumnMetadata? generated = metadata.AutoIncrementColumn;
            object? original = generated?.GetValue(entity);
            bool generate = generated != null && IsUnsetGenerated(_Values.ToStored(generated, original));
            if (generate)
            {
                long next = await NextSequenceAsync(schema, token).ConfigureAwait(false);
                generated!.SetValue(entity, _Values.FromStored(generated, ToGeneratedType(schema, generated, next)));
            }

            try
            {
                JsonObject document = ToDocument(schema, entity);
                string id = document[CosmosDbContainerSchema.IdField]!.GetValue<string>();
                if (!schema.PartitionKeyFromKey && await IdExistsAsync(container, id, token).ConfigureAwait(false))
                    throw DuplicateKey(metadata, id, null);

                using MemoryStream body = Serialize(document);
                using ResponseMessage response = await container.CreateItemStreamAsync(body, schema.PartitionKeyOf(document), _NoContent, token).ConfigureAwait(false);
                if (response.StatusCode == HttpStatusCode.Conflict) throw DuplicateKey(metadata, id, null);
                response.EnsureSuccessStatusCode();
            }
            catch (Exception)
            {
                if (generate) generated!.SetValue(entity, original);
                throw;
            }

            if (generated != null && !generate)
            {
                object? stored = _Values.ToStored(generated, generated.GetValue(entity));
                if (stored != null) await BumpSequenceAsync(schema, Convert.ToInt64(stored, CultureInfo.InvariantCulture), token).ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        public async Task<int> ReplaceAsync(EntityMetadata metadata, object entity, QueryNode condition, QuerySource source, ITransaction? transaction, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(metadata);
            ArgumentNullException.ThrowIfNull(entity);
            ArgumentNullException.ThrowIfNull(condition);
            ArgumentNullException.ThrowIfNull(source);
            Prepare(transaction, token);
            CosmosDbContainerSchema schema = Schema(metadata);
            Container container = await ContainerAsync(schema, token).ConfigureAwait(false);

            List<KeyValuePair<ColumnMetadata, object?>> replacement = new List<KeyValuePair<ColumnMetadata, object?>>();
            foreach (ColumnMetadata column in metadata.Columns)
            {
                if (column.IsPrimaryKey || column.IsAutoIncrement) continue;
                replacement.Add(new KeyValuePair<ColumnMetadata, object?>(column, _Values.ToStored(column, column.GetValue(entity))));
            }

            List<CosmosDbDocument> matches = await ReadAsync(new QueryModel(source) { Filter = condition }, false, "Replace", token).ConfigureAwait(false);
            int replaced = 0;
            foreach (CosmosDbDocument match in matches)
            {
                bool written = await RewriteAsync(container, schema, match, current =>
                {
                    JsonObject next = Copy(current.Content);
                    foreach (KeyValuePair<ColumnMetadata, object?> value in replacement) SetField(schema, next, value.Key, value.Value);
                    return next;
                }, source, condition, token).ConfigureAwait(false);
                if (written) replaced++;
            }

            return replaced;
        }

        /// <inheritdoc />
        /// <exception cref="InvalidOperationException">Thrown when an assignment changes a primary key to one that already exists.</exception>
        public async Task<int> UpdateAsync(QueryModel model, IReadOnlyList<FieldAssignment> assignments, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(model);
            ArgumentNullException.ThrowIfNull(assignments);
            if (assignments.Count == 0) throw new ArgumentException("At least one assignment is required.", nameof(assignments));
            Prepare(model.Transaction, token);
            CosmosDbContainerSchema schema = Schema(model.Metadata);
            Container container = await ContainerAsync(schema, token).ConfigureAwait(false);

            List<CosmosDbDocument> matches = await ReadAsync(model, false, "Update", token).ConfigureAwait(false);
            int updated = 0;
            foreach (CosmosDbDocument match in matches)
            {
                bool written = await RewriteAsync(container, schema, match, current => Assign(schema, model.Source, current, assignments), model.Source, model.Filter, token).ConfigureAwait(false);
                if (written) updated++;
            }

            return updated;
        }

        /// <inheritdoc />
        public async Task<int> DeleteAsync(QueryModel model, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(model);
            Prepare(model.Transaction, token);
            CosmosDbContainerSchema schema = Schema(model.Metadata);
            Container container = await ContainerAsync(schema, token).ConfigureAwait(false);

            List<CosmosDbDocument> matches = await ReadAsync(model, false, "Delete", token).ConfigureAwait(false);
            int deleted = 0;
            foreach (CosmosDbDocument match in matches)
            {
                bool written = await RewriteAsync(container, schema, match, current => null, model.Source, model.Filter, token).ConfigureAwait(false);
                if (written) deleted++;
            }

            return deleted;
        }

        /// <summary>
        /// Disposes the backend and, when <see cref="OwnsClient"/> is true, the client. Repositories over the backend can no
        /// longer be used; their operations throw <see cref="ObjectDisposedException"/>. Safe to call more than once.
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _Disposed, 1) == 1) return;
            if (OwnsClient) Client.Dispose();
        }

        /// <summary>
        /// Disposes the backend (see <see cref="Dispose"/>). The client closes synchronously, so this completes synchronously.
        /// </summary>
        /// <returns>A completed task.</returns>
        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }

        #endregion

        #region Private-Methods

        internal void ValidateMapping([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType)
        {
            ThrowIfDisposed();
            Schema(EntityMetadata.For(entityType));
        }

        [RequiresUnreferencedCode(_NotAotCompatible)]
        [RequiresDynamicCode(_NotAotCompatible)]
        private static CosmosClient CreateClient(CosmosDbRepositorySettings settings)
        {
            CosmosClientOptions options = new CosmosClientOptions
            {
                LimitToEndpoint = settings.LimitToEndpoint
            };
            if (settings.ConnectionMode.HasValue) options.ConnectionMode = settings.ConnectionMode.Value;
            if (settings.RequestTimeout.HasValue) options.RequestTimeout = settings.RequestTimeout.Value;
            if (settings.ApplicationName != null) options.ApplicationName = settings.ApplicationName;
            if (settings.AcceptAnyServerCertificate) options.ServerCertificateCustomValidationCallback = (certificate, chain, errors) => true;

            if (settings.ConnectionString != null) return new CosmosClient(settings.ConnectionString, options);
            return new CosmosClient(settings.Endpoint!, settings.AccountKey!, options);
        }

        private CosmosDbContainerSchema Schema(EntityMetadata metadata)
        {
            if (_Schemas.TryGetValue(metadata.EntityType, out CosmosDbContainerSchema? cached)) return cached;
            _PartitionKeys.TryGetValue(metadata.EntityType, out string? partitionKey);
            CosmosDbContainerSchema schema = new CosmosDbContainerSchema(metadata, partitionKey);
            return _Schemas.GetOrAdd(metadata.EntityType, schema);
        }

        private Task<Container> ContainerAsync(CosmosDbContainerSchema schema, CancellationToken token)
        {
            return ContainerAsync(schema.ContainerName, schema.PartitionKeyPath, token);
        }

        private Task<Container> SequenceContainerAsync(CancellationToken token)
        {
            return ContainerAsync(_SequenceContainerName, "/" + CosmosDbContainerSchema.IdField, token);
        }

        private async Task<Container> ContainerAsync(string name, string partitionKeyPath, CancellationToken token)
        {
            ThrowIfDisposed();
            Lazy<Task<Container>> lazy = _Containers.GetOrAdd(name, key => new Lazy<Task<Container>>(() => OpenContainerAsync(key, partitionKeyPath)));
            try
            {
                Container container = await lazy.Value.WaitAsync(token).ConfigureAwait(false);
                return container;
            }
            catch (Exception) when (lazy.Value.IsFaulted || lazy.Value.IsCanceled)
            {
                _Containers.TryRemove(new KeyValuePair<string, Lazy<Task<Container>>>(name, lazy));
                throw;
            }
        }

        private async Task<Container> OpenContainerAsync(string name, string partitionKeyPath)
        {
            string actualPath;
            if (_CreateIfNotExists)
            {
                try
                {
                    ContainerResponse response = await Database.CreateContainerIfNotExistsAsync(new ContainerProperties(name, partitionKeyPath), _ContainerThroughput, null, CancellationToken.None).ConfigureAwait(false);
                    actualPath = response.Resource.PartitionKeyPath;
                }
                catch (ArgumentException e)
                {
                    // The SDK rejects an existing container whose partition key path differs from the requested one.
                    ContainerResponse existing = await Database.GetContainer(name).ReadContainerAsync(null, CancellationToken.None).ConfigureAwait(false);
                    throw new InvalidOperationException("The Cosmos DB container '" + name + "' is partitioned by '" + existing.Resource.PartitionKeyPath + "', but Durable maps it to '" + partitionKeyPath + "'. Configure the matching partition key (CosmosDbRepositorySettings.PartitionKeys) or use another container.", e);
                }
            }
            else
            {
                try
                {
                    ContainerResponse response = await Database.GetContainer(name).ReadContainerAsync(null, CancellationToken.None).ConfigureAwait(false);
                    actualPath = response.Resource.PartitionKeyPath;
                }
                catch (CosmosException e) when (e.StatusCode == HttpStatusCode.NotFound)
                {
                    throw new InvalidOperationException("The Cosmos DB container '" + name + "' does not exist and CreateIfNotExists is false.", e);
                }
            }

            if (!string.Equals(actualPath, partitionKeyPath, StringComparison.Ordinal))
                throw new InvalidOperationException("The Cosmos DB container '" + name + "' is partitioned by '" + actualPath + "', but Durable maps it to '" + partitionKeyPath + "'. Configure the matching partition key (CosmosDbRepositorySettings.PartitionKeys) or use another container.");
            return Database.GetContainer(name);
        }

        private void Prepare(ITransaction? transaction, CancellationToken token)
        {
            ThrowIfDisposed();
            token.ThrowIfCancellationRequested();
            if (transaction != null) throw new ArgumentException("The transaction was not created by this Cosmos DB backend (the Cosmos DB backend has no transactions).", nameof(transaction));
        }

        private async IAsyncEnumerable<object> StreamAsync(QueryModel model, CancellationToken queryToken, [EnumeratorCancellation] CancellationToken enumerationToken = default)
        {
            Prepare(model.Transaction, queryToken);
            List<CosmosDbDocument> rows = await ReadAsync(model, true, "Query", queryToken).ConfigureAwait(false);
            CosmosDbContainerSchema schema = Schema(model.Metadata);
            foreach (CosmosDbDocument row in rows)
            {
                queryToken.ThrowIfCancellationRequested();
                enumerationToken.ThrowIfCancellationRequested();
                yield return Materialize(schema, row);
            }
        }

        private async Task<CosmosDbPushdown> TranslateAsync(Container container, CosmosDbContainerSchema schema, QueryModel model, CosmosDbRequestCharge charge, CancellationToken token)
        {
            CosmosDbPushdown pushdown = CosmosDbPushdown.Translate(model.Source, model.Filter, schema, _Values, false);
            if (pushdown.UncertainDecimals.Count == 0) return pushdown;

            // Decimal comparisons are exact only when the column holds no decimal a double cannot round-trip (those carry a
            // shadow); look for one, and widen the comparisons (re-checked client-side) when there is.
            string probe = "SELECT TOP 1 VALUE 1 FROM c WHERE " + string.Join(" OR ", pushdown.UncertainDecimals.Select(c => "IS_DEFINED(" + schema.ShadowPath(c) + ")"));
            JsonElement? found = await ScalarAsync(container, probe, Array.Empty<KeyValuePair<string, object>>(), null, charge, token).ConfigureAwait(false);
            if (!found.HasValue) return pushdown;
            return CosmosDbPushdown.Translate(model.Source, model.Filter, schema, _Values, true);
        }

        private async Task<List<CosmosDbDocument>> ReadAsync(QueryModel model, bool orderingAndPaging, string operation, CancellationToken token)
        {
            CosmosDbContainerSchema schema = Schema(model.Metadata);
            Container container = await ContainerAsync(schema, token).ConfigureAwait(false);
            CosmosDbRequestCharge charge = new CosmosDbRequestCharge();
            CosmosDbPushdown pushdown = await TranslateAsync(container, schema, model, charge, token).ConfigureAwait(false);
            bool paged = orderingAndPaging && (model.Skip.HasValue || model.Take.HasValue);

            List<CosmosDbDocument> documents;
            string? text = null;
            PartitionKey? partition = null;
            bool pointRead = false;
            bool orderPushed = false;
            bool pagePushed = false;
            if (CanPointRead(schema, pushdown))
            {
                pointRead = true;
                object?[] key = schema.Metadata.KeyColumns.Select(k => (object?)pushdown.Equalities[k]).ToArray();
                string id = schema.Id(key);
                partition = schema.PartitionKeyColumn == null ? new PartitionKey(id) : CosmosDbContainerSchema.PartitionKeyOfValue(pushdown.Equalities[schema.PartitionKeyColumn]);
                CosmosDbDocument? document = await PointReadAsync(container, schema, id, partition.Value, charge, token).ConfigureAwait(false);
                documents = document == null ? new List<CosmosDbDocument>() : new List<CosmosDbDocument> { document };
            }
            else
            {
                ColumnMetadata? orderColumn = null;
                bool descending = false;
                if (orderingAndPaging && model.Orderings.Count == 1 && model.Orderings[0].Key is ColumnNode key && ReferenceEquals(key.Source, model.Source) && Orderable(schema, key.Column))
                {
                    orderColumn = key.Column;
                    descending = model.Orderings[0].Descending;
                }
                else if (paged && model.Orderings.Count == 0 && pushdown.Exact && schema.SingleKey && Orderable(schema, schema.Metadata.KeyColumns[0]))
                {
                    orderColumn = schema.Metadata.KeyColumns[0];
                }

                orderPushed = orderColumn != null && (model.Orderings.Count > 0 || pushdown.Exact);
                pagePushed = paged && pushdown.Exact && orderPushed;
                if (pagePushed && await HasUncertainOrderingAsync(container, schema, orderColumn!, charge, token).ConfigureAwait(false))
                {
                    // Cosmos DB may order some values of this column differently from C# (doubles standing in for exact
                    // values, or non-finite numbers stored as strings), which a page cannot reveal: order and page client-side.
                    pagePushed = false;
                    orderPushed = false;
                }
                partition = PartitionFor(schema, pushdown);
                text = BuildQuery(schema, pushdown, orderPushed ? orderColumn : null, descending, pagePushed ? model.Skip : null, pagePushed ? model.Take : null);
                documents = await QueryDocumentsAsync(container, schema, text, pushdown.Parameters, partition, charge, token).ConfigureAwait(false);

                if (orderPushed && documents.Any(d => d.HasUncertainOrder(orderColumn!)))
                {
                    // Cosmos DB may have placed these documents differently from C# (see HasUncertainOrder): order client-side,
                    // reading every candidate when the page was cut inside Cosmos DB.
                    if (pagePushed)
                    {
                        pagePushed = false;
                        text = BuildQuery(schema, pushdown, null, false, null, null);
                        documents = await QueryDocumentsAsync(container, schema, text, pushdown.Parameters, partition, charge, token).ConfigureAwait(false);
                    }

                    orderPushed = false;
                }
            }

            int read = documents.Count;
            if (!orderPushed) SortByKey(schema, documents);

            // A point read applies only the key equalities, so the rest of the filter is always checked client-side.
            bool filtered = pushdown.Exact && !pointRead;
            string? clientSide = null;
            bool needsOrdering = orderingAndPaging && model.Orderings.Count > 0 && !orderPushed;
            if (!filtered || needsOrdering || (paged && !pagePushed))
            {
                QueryModel residual = new QueryModel(model.Source)
                {
                    Filter = filtered ? null : model.Filter,
                    Skip = paged && !pagePushed ? model.Skip : null,
                    Take = paged && !pagePushed ? model.Take : null
                };
                if (needsOrdering) residual.Orderings.AddRange(model.Orderings);
                documents = Evaluator().Apply(residual, documents);
                clientSide = DescribeClientSide(residual);
            }

            Record(new CosmosDbQueryPlan(operation, schema.Metadata.EntityType, schema.ContainerName, text, pushdown.ParametersJson(), Describe(partition), pointRead, pushdown.Exact, clientSide, orderPushed, pagePushed, read, charge.Total));
            return documents;
        }

        private async Task<bool> HasUncertainOrderingAsync(Container container, CosmosDbContainerSchema schema, ColumnMetadata column, CosmosDbRequestCharge charge, CancellationToken token)
        {
            CosmosDbValueKind kind = schema.Kind(column);
            string condition;
            if (kind == CosmosDbValueKind.Decimal || kind == CosmosDbValueKind.Int64) condition = "IS_DEFINED(" + schema.ShadowPath(column) + ")";
            else if (kind == CosmosDbValueKind.Single || kind == CosmosDbValueKind.Double) condition = "IS_STRING(" + schema.Path(column) + ")";
            else return false;
            JsonElement? found = await ScalarAsync(container, "SELECT TOP 1 VALUE 1 FROM c WHERE " + condition, Array.Empty<KeyValuePair<string, object>>(), null, charge, token).ConfigureAwait(false);
            return found.HasValue;
        }

        private static bool CanPointRead(CosmosDbContainerSchema schema, CosmosDbPushdown pushdown)
        {
            foreach (ColumnMetadata key in schema.Metadata.KeyColumns)
            {
                if (!pushdown.Equalities.ContainsKey(key)) return false;
            }

            return schema.PartitionKeyFromKey;
        }

        private static PartitionKey? PartitionFor(CosmosDbContainerSchema schema, CosmosDbPushdown pushdown)
        {
            if (schema.PartitionKeyColumn != null && pushdown.Equalities.TryGetValue(schema.PartitionKeyColumn, out object? value))
                return CosmosDbContainerSchema.PartitionKeyOfValue(value);
            return null;
        }

        private static bool Orderable(CosmosDbContainerSchema schema, ColumnMetadata column)
        {
            return schema.Kind(column) != CosmosDbValueKind.Bytes;
        }

        private static string BuildQuery(CosmosDbContainerSchema schema, CosmosDbPushdown pushdown, ColumnMetadata? orderColumn, bool descending, int? skip, int? take)
        {
            StringBuilder text = new StringBuilder("SELECT * FROM c");
            text.Append(WhereClause(pushdown));
            if (orderColumn != null) text.Append(" ORDER BY ").Append(schema.Path(orderColumn)).Append(descending ? " DESC" : " ASC");
            if (skip.HasValue || take.HasValue)
            {
                text.Append(" OFFSET ").Append((skip ?? 0).ToString(CultureInfo.InvariantCulture));
                text.Append(" LIMIT ").Append((take ?? int.MaxValue).ToString(CultureInfo.InvariantCulture));
            }

            return text.ToString();
        }

        private static string WhereClause(CosmosDbPushdown pushdown)
        {
            string? where = pushdown.Where();
            return where == null ? string.Empty : " WHERE " + where;
        }

        private static bool Aggregatable(CosmosDbContainerSchema schema, ColumnMetadata column, AggregateFunction function)
        {
            CosmosDbValueKind kind = schema.Kind(column);
            if (function == AggregateFunction.Sum || function == AggregateFunction.Average) return kind == CosmosDbValueKind.Integer && schema.StoredType(column) != typeof(TimeOnly);
            if (function != AggregateFunction.Min && function != AggregateFunction.Max) return false;
            return kind == CosmosDbValueKind.Integer || kind == CosmosDbValueKind.Int64 || kind == CosmosDbValueKind.String || kind == CosmosDbValueKind.Guid
                || kind == CosmosDbValueKind.DateTime || kind == CosmosDbValueKind.DateTimeOffset || kind == CosmosDbValueKind.DateOnly;
        }

        private async Task<object?> PushedAggregateAsync(Container container, CosmosDbContainerSchema schema, CosmosDbPushdown pushdown, AggregateFunction function, ColumnMetadata column, CosmosDbRequestCharge charge, CancellationToken token)
        {
            CosmosDbValueKind kind = schema.Kind(column);
            string path = schema.Path(column);
            string? where = pushdown.Where();
            string condition = " WHERE " + (where == null ? string.Empty : where + " AND ") + CosmosDbPushdown.Guard(kind, path);
            PartitionKey? partition = PartitionFor(schema, pushdown);

            if (function == AggregateFunction.Sum || function == AggregateFunction.Average)
            {
                string sumText = "SELECT VALUE SUM(" + path + ") FROM c" + condition;
                JsonElement? sum = await ScalarAsync(container, sumText, pushdown.Parameters, partition, charge, token).ConfigureAwait(false);
                string countText = "SELECT VALUE COUNT(1) FROM c" + condition;
                JsonElement? count = await ScalarAsync(container, countText, pushdown.Parameters, partition, charge, token).ConfigureAwait(false);
                long rows = count.HasValue && count.Value.ValueKind == JsonValueKind.Number ? count.Value.GetInt64() : 0;
                Record(new CosmosDbQueryPlan("Aggregate", schema.Metadata.EntityType, schema.ContainerName, sumText, pushdown.ParametersJson(), Describe(partition), false, true, null, false, true, -1, charge.Total));
                if (rows == 0) return null;
                if (!sum.HasValue || sum.Value.ValueKind != JsonValueKind.Number) return _Unresolved;
                double total = sum.Value.GetDouble();
                if (Math.Abs(total) > CosmosDbJsonCodec.MaxExactInteger) return _Unresolved;
                decimal exact = (decimal)total;
                return function == AggregateFunction.Sum ? exact : exact / rows;
            }

            string text = "SELECT VALUE " + (function == AggregateFunction.Min ? "MIN" : "MAX") + "(" + path + ") FROM c" + condition;
            JsonElement? extreme = await ScalarAsync(container, text, pushdown.Parameters, partition, charge, token).ConfigureAwait(false);
            Record(new CosmosDbQueryPlan("Aggregate", schema.Metadata.EntityType, schema.ContainerName, text, pushdown.ParametersJson(), Describe(partition), false, true, null, false, true, -1, charge.Total));
            if (!extreme.HasValue || extreme.Value.ValueKind == JsonValueKind.Null || extreme.Value.ValueKind == JsonValueKind.Undefined) return null;
            JsonElement value = extreme.Value;
            if (kind == CosmosDbValueKind.String && value.ValueKind == JsonValueKind.String && CosmosDbJsonCodec.HasHighCharacters(value.GetString()!)) return _Unresolved;
            if (kind == CosmosDbValueKind.Int64 && (value.ValueKind != JsonValueKind.Number || !value.TryGetInt64(out long integer) || Math.Abs(integer) > CosmosDbJsonCodec.MaxExactInteger)) return _Unresolved;
            object? stored = CosmosDbJsonCodec.Decode(value, null, schema.StoredType(column));
            return _Values.FromStored(column, stored);
        }

        private async Task<List<CosmosDbDocument>> QueryDocumentsAsync(Container container, CosmosDbContainerSchema schema, string text, IReadOnlyList<KeyValuePair<string, object>> parameters, PartitionKey? partition, CosmosDbRequestCharge charge, CancellationToken token)
        {
            List<CosmosDbDocument> documents = new List<CosmosDbDocument>();
            foreach (JsonElement item in await QueryElementsAsync(container, text, parameters, partition, charge, token).ConfigureAwait(false))
            {
                JsonObject? content = JsonObject.Create(item);
                if (content != null) documents.Add(new CosmosDbDocument(schema, content));
            }

            return documents;
        }

        private async Task<JsonElement?> ScalarAsync(Container container, string text, IReadOnlyList<KeyValuePair<string, object>> parameters, PartitionKey? partition, CosmosDbRequestCharge charge, CancellationToken token)
        {
            List<JsonElement> results = await QueryElementsAsync(container, text, parameters, partition, charge, token).ConfigureAwait(false);
            return results.Count == 0 ? null : results[0];
        }

        private async Task<List<JsonElement>> QueryElementsAsync(Container container, string text, IReadOnlyList<KeyValuePair<string, object>> parameters, PartitionKey? partition, CosmosDbRequestCharge charge, CancellationToken token)
        {
            QueryDefinition query = new QueryDefinition(text);
            foreach (KeyValuePair<string, object> parameter in parameters) query = query.WithParameter(parameter.Key, parameter.Value);
            QueryRequestOptions? options = partition.HasValue ? new QueryRequestOptions { PartitionKey = partition.Value } : null;

            List<JsonElement> results = new List<JsonElement>();
            using FeedIterator iterator = container.GetItemQueryStreamIterator(query, null, options);
            while (iterator.HasMoreResults)
            {
                using ResponseMessage response = await iterator.ReadNextAsync(token).ConfigureAwait(false);
                charge.Add(response.Headers.RequestCharge);
                response.EnsureSuccessStatusCode();
                if (response.Content == null) continue;
                using JsonDocument page = await JsonDocument.ParseAsync(response.Content, default, token).ConfigureAwait(false);
                if (!page.RootElement.TryGetProperty("Documents", out JsonElement items)) continue;
                foreach (JsonElement item in items.EnumerateArray()) results.Add(item.Clone());
            }

            return results;
        }

        private async Task<CosmosDbDocument?> PointReadAsync(Container container, CosmosDbContainerSchema schema, string id, PartitionKey partition, CosmosDbRequestCharge charge, CancellationToken token)
        {
            using ResponseMessage response = await container.ReadItemStreamAsync(id, partition, null, token).ConfigureAwait(false);
            charge.Add(response.Headers.RequestCharge);
            if (response.StatusCode == HttpStatusCode.NotFound) return null;
            response.EnsureSuccessStatusCode();
            JsonNode? node = await JsonNode.ParseAsync(response.Content, null, default, token).ConfigureAwait(false);
            return node is JsonObject content ? new CosmosDbDocument(schema, content) : null;
        }

        private async Task<bool> IdExistsAsync(Container container, string id, CancellationToken token)
        {
            KeyValuePair<string, object>[] parameters = { new KeyValuePair<string, object>("@id", id) };
            JsonElement? count = await ScalarAsync(container, "SELECT VALUE COUNT(1) FROM c WHERE c[\"id\"] = @id", parameters, null, new CosmosDbRequestCharge(), token).ConfigureAwait(false);
            return count.HasValue && count.Value.ValueKind == JsonValueKind.Number && count.Value.GetInt64() > 0;
        }

        private async Task<bool> RewriteAsync(Container container, CosmosDbContainerSchema schema, CosmosDbDocument original, Func<CosmosDbDocument, JsonObject?> transform, QuerySource source, QueryNode? condition, CancellationToken token)
        {
            CosmosDbDocument current = original;
            for (int attempt = 1; ; attempt++)
            {
                token.ThrowIfCancellationRequested();
                JsonObject? next = transform(current);
                HttpStatusCode status;
                if (next == null)
                {
                    using ResponseMessage response = await container.DeleteItemStreamAsync(current.Id, current.PartitionKey, IfMatch(current.ETag), token).ConfigureAwait(false);
                    status = response.StatusCode;
                    if (!response.IsSuccessStatusCode && status != HttpStatusCode.NotFound && status != HttpStatusCode.PreconditionFailed) response.EnsureSuccessStatusCode();
                }
                else
                {
                    string id = next[CosmosDbContainerSchema.IdField]!.GetValue<string>();
                    PartitionKey partition = schema.PartitionKeyOf(next);
                    if (id == current.Id && partition.Equals(current.PartitionKey))
                    {
                        using MemoryStream body = Serialize(next);
                        using ResponseMessage response = await container.ReplaceItemStreamAsync(body, current.Id, current.PartitionKey, IfMatch(current.ETag), token).ConfigureAwait(false);
                        status = response.StatusCode;
                        if (!response.IsSuccessStatusCode && status != HttpStatusCode.NotFound && status != HttpStatusCode.PreconditionFailed) response.EnsureSuccessStatusCode();
                    }
                    else
                    {
                        status = await RelocateAsync(container, schema, current, next, id, partition, token).ConfigureAwait(false);
                    }
                }

                if (status == HttpStatusCode.NotFound) return false;
                if (status != HttpStatusCode.PreconditionFailed) return true;

                // Another writer changed the document since it was read: read it again, re-check the condition, retry.
                if (attempt >= _MaxConflictRetries)
                    throw new InvalidOperationException("Cosmos DB document '" + current.Id + "' of '" + schema.ContainerName + "' kept changing; gave up after " + attempt.ToString(CultureInfo.InvariantCulture) + " conditional write attempts.");
                if (_Logger != null && _Logger.IsEnabled(LogLevel.Debug)) _Logger.LogDebug("Cosmos DB document {Id} of {Container} changed concurrently; retrying (attempt {Attempt})", current.Id, schema.ContainerName, attempt);
                CosmosDbDocument? reloaded = await PointReadAsync(container, schema, current.Id, current.PartitionKey, new CosmosDbRequestCharge(), token).ConfigureAwait(false);
                if (reloaded == null) return false;
                if (condition != null && !Evaluator().Bind(source, reloaded).Test(condition)) return false;
                current = reloaded;
            }
        }

        private async Task<HttpStatusCode> RelocateAsync(Container container, CosmosDbContainerSchema schema, CosmosDbDocument current, JsonObject next, string id, PartitionKey partition, CancellationToken token)
        {
            if (id != current.Id && !schema.PartitionKeyFromKey && await IdExistsAsync(container, id, token).ConfigureAwait(false))
                throw DuplicateKey(schema.Metadata, id, null);

            using (MemoryStream body = Serialize(next))
            {
                using ResponseMessage created = await container.CreateItemStreamAsync(body, partition, _NoContent, token).ConfigureAwait(false);
                if (created.StatusCode == HttpStatusCode.Conflict) throw DuplicateKey(schema.Metadata, id, null);
                created.EnsureSuccessStatusCode();
            }

            using ResponseMessage deleted = await container.DeleteItemStreamAsync(current.Id, current.PartitionKey, IfMatch(current.ETag), token).ConfigureAwait(false);
            if (deleted.IsSuccessStatusCode) return HttpStatusCode.OK;
            if (deleted.StatusCode != HttpStatusCode.PreconditionFailed && deleted.StatusCode != HttpStatusCode.NotFound) deleted.EnsureSuccessStatusCode();

            // The old document changed or vanished meanwhile: undo the copy and let the caller re-check.
            using ResponseMessage undo = await container.DeleteItemStreamAsync(id, partition, null, token).ConfigureAwait(false);
            if (!undo.IsSuccessStatusCode && undo.StatusCode != HttpStatusCode.NotFound) undo.EnsureSuccessStatusCode();
            return deleted.StatusCode;
        }

        private JsonObject Assign(CosmosDbContainerSchema schema, QuerySource source, CosmosDbDocument current, IReadOnlyList<FieldAssignment> assignments)
        {
            EntityMetadata metadata = schema.Metadata;
            CosmosDbQueryEvaluator evaluator = Evaluator();
            evaluator.Bind(source, current);
            List<KeyValuePair<ColumnMetadata, object?>> values = new List<KeyValuePair<ColumnMetadata, object?>>(assignments.Count);
            foreach (FieldAssignment assignment in assignments)
            {
                object? stored = assignment.Value is ValueNode constant
                    ? _Values.ToStored(assignment.Column, constant.Value)
                    : _Values.ComputedToStored(assignment.Column, evaluator.Visit(assignment.Value));
                values.Add(new KeyValuePair<ColumnMetadata, object?>(assignment.Column, stored));
            }

            JsonObject next = Copy(current.Content);
            bool keyChanges = false;
            foreach (KeyValuePair<ColumnMetadata, object?> value in values)
            {
                SetField(schema, next, value.Key, value.Value);
                if (value.Key.IsPrimaryKey) keyChanges = true;
            }

            if (keyChanges)
            {
                object?[] key = new object?[metadata.KeyColumns.Count];
                for (int i = 0; i < key.Length; i++)
                {
                    ColumnMetadata column = metadata.KeyColumns[i];
                    int assigned = values.FindIndex(v => ReferenceEquals(v.Key, column));
                    key[i] = assigned >= 0 ? values[assigned].Value : current.Get(column);
                    if (key[i] == null) throw new InvalidOperationException("Cannot update " + metadata.EntityType.Name + ": a primary key cannot be set to null.");
                }

                next[CosmosDbContainerSchema.IdField] = schema.Id(key);
            }

            return next;
        }

        private JsonObject ToDocument(CosmosDbContainerSchema schema, object entity)
        {
            EntityMetadata metadata = schema.Metadata;
            object?[] key = new object?[metadata.KeyColumns.Count];
            Dictionary<ColumnMetadata, object?> stored = new Dictionary<ColumnMetadata, object?>(ReferenceEqualityComparer.Instance);
            foreach (ColumnMetadata column in metadata.Columns)
            {
                object? value = _Values.ToStored(column, column.GetValue(entity));
                stored[column] = value;
                if (column.IsPrimaryKey) key[IndexOfKey(metadata, column)] = value;
            }

            JsonObject document = new JsonObject { [CosmosDbContainerSchema.IdField] = schema.Id(key) };
            foreach (ColumnMetadata column in metadata.Columns)
            {
                if (schema.Field(column) == CosmosDbContainerSchema.IdField) continue;
                SetField(schema, document, column, stored[column]);
            }

            return document;
        }

        private static void SetField(CosmosDbContainerSchema schema, JsonObject document, ColumnMetadata column, object? stored)
        {
            string field = schema.Field(column);
            JsonNode? node = CosmosDbJsonCodec.Encode(stored, schema.Kind(column), out string? shadow);
            if (field == CosmosDbContainerSchema.IdField)
            {
                if (node == null) throw new InvalidOperationException("Primary key column '" + column.Name + "' of " + schema.Metadata.EntityType.Name + " is null.");
                return;
            }

            document[field] = node;
            JsonObject? shadows = document[CosmosDbContainerSchema.ShadowField] as JsonObject;
            if (shadow != null)
            {
                if (shadows == null)
                {
                    shadows = new JsonObject();
                    document[CosmosDbContainerSchema.ShadowField] = shadows;
                }

                shadows[field] = shadow;
            }
            else if (shadows != null)
            {
                shadows.Remove(field);
                if (shadows.Count == 0) document.Remove(CosmosDbContainerSchema.ShadowField);
            }
        }

        private static JsonObject Copy(JsonObject content)
        {
            JsonObject copy = (JsonObject)content.DeepClone();
            foreach (string system in CosmosDbContainerSchema.SystemFields) copy.Remove(system);
            return copy;
        }

        private static int IndexOfKey(EntityMetadata metadata, ColumnMetadata column)
        {
            for (int i = 0; i < metadata.KeyColumns.Count; i++)
            {
                if (ReferenceEquals(metadata.KeyColumns[i], column)) return i;
            }

            throw new InvalidOperationException("Column '" + column.Name + "' is not a key column of " + metadata.EntityType.Name + ".");
        }

        private object Materialize(CosmosDbContainerSchema schema, CosmosDbDocument document)
        {
            object entity = schema.Metadata.CreateInstance();
            foreach (ColumnMetadata column in schema.Metadata.Columns) column.SetValue(entity, _Values.FromStored(column, document.Get(column)));
            return entity;
        }

        private static void SortByKey(CosmosDbContainerSchema schema, List<CosmosDbDocument> documents)
        {
            if (documents.Count < 2) return;
            IReadOnlyList<ColumnMetadata> keys = schema.Metadata.KeyColumns;
            List<CosmosDbDocument> sorted = documents.OrderBy(d => d.Get(keys[0]), QueryValueComparer.Ordinal).ToList();
            if (keys.Count > 1)
            {
                IOrderedEnumerable<CosmosDbDocument> ordered = documents.OrderBy(d => d.Get(keys[0]), QueryValueComparer.Ordinal);
                for (int i = 1; i < keys.Count; i++)
                {
                    ColumnMetadata key = keys[i];
                    ordered = ordered.ThenBy(d => d.Get(key), QueryValueComparer.Ordinal);
                }

                sorted = ordered.ToList();
            }

            documents.Clear();
            documents.AddRange(sorted);
        }

        private CosmosDbQueryEvaluator Evaluator()
        {
            return new CosmosDbQueryEvaluator(_Values, FindRelated);
        }

        private IReadOnlyList<CosmosDbDocument> FindRelated(EntityMetadata metadata, ColumnMetadata column, object key)
        {
            return Task.Run(() => FindRelatedAsync(metadata, column, key, CancellationToken.None)).GetAwaiter().GetResult();
        }

        private async Task<IReadOnlyList<CosmosDbDocument>> FindRelatedAsync(EntityMetadata metadata, ColumnMetadata column, object key, CancellationToken token)
        {
            CosmosDbContainerSchema schema = Schema(metadata);
            Container container = await ContainerAsync(schema, token).ConfigureAwait(false);
            CosmosDbRequestCharge charge = new CosmosDbRequestCharge();
            CosmosDbValueKind kind = schema.Kind(column);
            Type stored = schema.StoredType(column);
            bool exactKind = kind != CosmosDbValueKind.DateTime && kind != CosmosDbValueKind.Decimal && kind != CosmosDbValueKind.Single && kind != CosmosDbValueKind.Double;
            bool comparable = exactKind && (key.GetType() == stored || (kind == CosmosDbValueKind.Integer || kind == CosmosDbValueKind.Int64) && IsInteger(key) && IsInteger(stored));

            if (comparable && schema.SingleKey && ReferenceEquals(column, metadata.KeyColumns[0]) && schema.PartitionKeyColumn == null)
            {
                string id = schema.Id(new[] { (object?)ChangeIntegerType(key, stored) });
                CosmosDbDocument? document = await PointReadAsync(container, schema, id, new PartitionKey(id), charge, token).ConfigureAwait(false);
                Record(new CosmosDbQueryPlan("Lookup", metadata.EntityType, schema.ContainerName, null, null, null, true, true, null, false, false, document == null ? 0 : 1, charge.Total));
                return document == null ? Array.Empty<CosmosDbDocument>() : new[] { document };
            }

            string text;
            KeyValuePair<string, object>[] parameters;
            if (comparable)
            {
                string path = schema.Path(column);
                text = "SELECT * FROM c WHERE (" + CosmosDbPushdown.Guard(kind, path) + " AND " + path + " = @k)";
                parameters = new[] { new KeyValuePair<string, object>("@k", CosmosDbJsonCodec.Parameter(key)) };
            }
            else
            {
                text = "SELECT * FROM c";
                parameters = Array.Empty<KeyValuePair<string, object>>();
            }

            List<CosmosDbDocument> documents = await QueryDocumentsAsync(container, schema, text, parameters, null, charge, token).ConfigureAwait(false);
            Record(new CosmosDbQueryPlan("Lookup", metadata.EntityType, schema.ContainerName, text, null, null, false, comparable, comparable ? null : "related key", false, false, documents.Count, charge.Total));
            return documents;
        }

        private async Task<long> NextSequenceAsync(CosmosDbContainerSchema schema, CancellationToken token)
        {
            Container sequences = await SequenceContainerAsync(token).ConfigureAwait(false);
            string name = schema.ContainerName;
            PartitionKey partition = new PartitionKey(name);
            for (int attempt = 1; ; attempt++)
            {
                token.ThrowIfCancellationRequested();
                using (ResponseMessage read = await sequences.ReadItemStreamAsync(name, partition, null, token).ConfigureAwait(false))
                {
                    if (read.StatusCode == HttpStatusCode.NotFound)
                    {
                        using MemoryStream body = Serialize(new JsonObject { ["id"] = name, ["value"] = 1L });
                        using ResponseMessage created = await sequences.CreateItemStreamAsync(body, partition, _NoContent, token).ConfigureAwait(false);
                        if (created.IsSuccessStatusCode) return 1;
                        if (created.StatusCode != HttpStatusCode.Conflict) created.EnsureSuccessStatusCode();
                    }
                    else
                    {
                        read.EnsureSuccessStatusCode();
                        using JsonDocument counter = await JsonDocument.ParseAsync(read.Content, default, token).ConfigureAwait(false);
                        long next = counter.RootElement.GetProperty("value").GetInt64() + 1;
                        string etag = counter.RootElement.GetProperty(CosmosDbContainerSchema.ETagField).GetString()!;
                        using MemoryStream body = Serialize(new JsonObject { ["id"] = name, ["value"] = next });
                        using ResponseMessage replaced = await sequences.ReplaceItemStreamAsync(body, name, partition, IfMatch(etag), token).ConfigureAwait(false);
                        if (replaced.IsSuccessStatusCode) return next;
                        if (replaced.StatusCode != HttpStatusCode.PreconditionFailed) replaced.EnsureSuccessStatusCode();
                    }
                }

                if (attempt >= _MaxConflictRetries)
                    throw new InvalidOperationException("The auto-increment counter of '" + name + "' kept changing; gave up after " + attempt.ToString(CultureInfo.InvariantCulture) + " attempts.");
                await Task.Delay(Random.Shared.Next(1, 4 + Math.Min(attempt, 20) * 2), token).ConfigureAwait(false);
            }
        }

        private async Task BumpSequenceAsync(CosmosDbContainerSchema schema, long minimum, CancellationToken token)
        {
            Container sequences = await SequenceContainerAsync(token).ConfigureAwait(false);
            string name = schema.ContainerName;
            PartitionKey partition = new PartitionKey(name);
            for (int attempt = 1; ; attempt++)
            {
                token.ThrowIfCancellationRequested();
                using (ResponseMessage read = await sequences.ReadItemStreamAsync(name, partition, null, token).ConfigureAwait(false))
                {
                    if (read.StatusCode == HttpStatusCode.NotFound)
                    {
                        using MemoryStream body = Serialize(new JsonObject { ["id"] = name, ["value"] = minimum });
                        using ResponseMessage created = await sequences.CreateItemStreamAsync(body, partition, _NoContent, token).ConfigureAwait(false);
                        if (created.IsSuccessStatusCode) return;
                        if (created.StatusCode != HttpStatusCode.Conflict) created.EnsureSuccessStatusCode();
                    }
                    else
                    {
                        read.EnsureSuccessStatusCode();
                        using JsonDocument counter = await JsonDocument.ParseAsync(read.Content, default, token).ConfigureAwait(false);
                        if (counter.RootElement.GetProperty("value").GetInt64() >= minimum) return;
                        string etag = counter.RootElement.GetProperty(CosmosDbContainerSchema.ETagField).GetString()!;
                        using MemoryStream body = Serialize(new JsonObject { ["id"] = name, ["value"] = minimum });
                        using ResponseMessage replaced = await sequences.ReplaceItemStreamAsync(body, name, partition, IfMatch(etag), token).ConfigureAwait(false);
                        if (replaced.IsSuccessStatusCode) return;
                        if (replaced.StatusCode != HttpStatusCode.PreconditionFailed) replaced.EnsureSuccessStatusCode();
                    }
                }

                if (attempt >= _MaxConflictRetries)
                    throw new InvalidOperationException("The auto-increment counter of '" + name + "' kept changing; gave up after " + attempt.ToString(CultureInfo.InvariantCulture) + " attempts.");
                await Task.Delay(Random.Shared.Next(1, 4 + Math.Min(attempt, 20) * 2), token).ConfigureAwait(false);
            }
        }

        private static object ToGeneratedType(CosmosDbContainerSchema schema, ColumnMetadata column, long value)
        {
            Type stored = schema.StoredType(column);
            try
            {
                return Convert.ChangeType(value, stored, CultureInfo.InvariantCulture);
            }
            catch (OverflowException e)
            {
                throw new InvalidOperationException("The auto-increment counter of '" + schema.ContainerName + "' reached " + value.ToString(CultureInfo.InvariantCulture) + ", which does not fit " + schema.Metadata.EntityType.Name + "." + column.Property.Name + " (" + stored.Name + ").", e);
            }
        }

        private static object ChangeIntegerType(object key, Type stored)
        {
            if (key.GetType() == stored || !IsInteger(stored)) return key;
            return Convert.ChangeType(key, stored, CultureInfo.InvariantCulture);
        }

        private static bool IsInteger(object value)
        {
            return IsInteger(value.GetType());
        }

        private static bool IsInteger(Type type)
        {
            return type == typeof(int) || type == typeof(long) || type == typeof(short) || type == typeof(byte)
                || type == typeof(sbyte) || type == typeof(ushort) || type == typeof(uint) || type == typeof(ulong);
        }

        private static ItemRequestOptions IfMatch(string? etag)
        {
            return new ItemRequestOptions { IfMatchEtag = etag, EnableContentResponseOnWrite = false };
        }

        private static MemoryStream Serialize(JsonObject document)
        {
            MemoryStream buffer = new MemoryStream();
            using (Utf8JsonWriter writer = new Utf8JsonWriter(buffer))
            {
                document.WriteTo(writer);
            }

            buffer.Position = 0;
            return buffer;
        }

        private static string? Describe(PartitionKey? partition)
        {
            return partition.HasValue ? partition.Value.ToString() : null;
        }

        private void Record(CosmosDbQueryPlan plan)
        {
            _LastQueryPlan = plan;
            EventHandler<CosmosDbQueryPlan>? handlers = QueryPlanned;
            if (handlers != null)
            {
                try
                {
                    handlers(this, plan);
                }
                catch (Exception e)
                {
                    _Logger?.LogWarning(e, "A Cosmos DB QueryPlanned handler threw: {Message}", e.Message);
                }
            }

            if (_Logger != null && _Logger.IsEnabled(LogLevel.Debug)) _Logger.LogDebug("Cosmos DB {Plan}", plan.ToString());
        }

        private static string? DescribeClientSide(QueryModel residual)
        {
            StringBuilder text = new StringBuilder();
            if (residual.Filter != null) text.Append("WHERE ").Append(QueryModelFormatter.Describe(residual.Filter));
            if (residual.Orderings.Count > 0)
            {
                if (text.Length > 0) text.Append(' ');
                text.Append("ORDER BY ").Append(string.Join(", ", residual.Orderings.Select(o => QueryModelFormatter.Describe(o.Key) + (o.Descending ? " DESC" : " ASC"))));
            }

            if (residual.Skip.HasValue) text.Append(text.Length > 0 ? " " : string.Empty).Append("SKIP ").Append(residual.Skip.Value.ToString(CultureInfo.InvariantCulture));
            if (residual.Take.HasValue) text.Append(text.Length > 0 ? " " : string.Empty).Append("TAKE ").Append(residual.Take.Value.ToString(CultureInfo.InvariantCulture));
            return text.Length == 0 ? null : text.ToString();
        }

        private static InvalidOperationException DuplicateKey(EntityMetadata metadata, string id, Exception? inner)
        {
            return new InvalidOperationException("Cannot insert " + metadata.EntityType.Name + ": a row with primary key '" + id + "' already exists in '" + metadata.TableName + "'.", inner);
        }

        private static NotSupportedException TransactionsNotSupported()
        {
            return new NotSupportedException("The Cosmos DB backend does not support transactions: Cosmos DB transactional batches cover only one logical partition, and Durable does not fake atomicity across documents.");
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
                case uint ui: return ui == 0;
                case ulong ul: return ul == 0;
                case ushort us: return us == 0;
                case sbyte sb: return sb == 0;
                default: return false;
            }
        }

        private void ThrowIfDisposed()
        {
            if (Volatile.Read(ref _Disposed) == 1) throw new ObjectDisposedException(nameof(CosmosDbBackend));
        }

        #endregion
    }
}
