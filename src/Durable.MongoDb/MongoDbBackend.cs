namespace Durable.MongoDb
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.Linq;
    using System.Runtime.CompilerServices;
    using System.Text;
    using System.Text.Json;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using Durable;
    using Durable.Query;
    using MongoDB.Bson;
    using MongoDB.Driver;

    /// <summary>
    /// A MongoDB <see cref="IRepositoryBackend"/>: stores entities in the collections of one MongoDB database. Use it through
    /// <see cref="MongoDbRepository{T}"/> (or any <see cref="RepositoryBase{T}"/>); one backend wraps one client and database
    /// and serves every entity type, so includes, navigation predicates and transactions work across repositories that share
    /// it. Create one backend per database and share it.
    /// <para>
    /// Storage: each entity type is a collection named <see cref="EntityMetadata.TableName"/>; each mapped column is a
    /// document field named after the column, built from Durable's <see cref="EntityMetadata"/> (the driver's reflection
    /// class maps are never used). A single primary key is the document <c>_id</c>; a composite key is stored as its fields
    /// plus an <c>_id</c> sub-document of the key parts. Values are stored the way a database driver would
    /// (<see cref="IValueConverter"/> provider values, JSON text for JSON columns, enum names or numbers) and encoded
    /// losslessly and so that MongoDB orders values like C#: integers as Int32/Int64, decimal (full precision and scale) and
    /// ulong as Decimal128, float/double as Double, DateTime as the Decimal128 <c>Ticks + Kind / 4</c> (every tick and the
    /// <see cref="DateTimeKind"/> survive; the BSON date type keeps only milliseconds), DateTimeOffset as the Decimal128
    /// <c>UtcTicks + (offset minutes + 840) / 2000</c>, TimeSpan and TimeOnly as Int64 ticks, DateOnly as an Int32 day
    /// number, char as a one-character string, Guid as standard binary (subtype 4), bool, string and byte[] natively. A field
    /// missing from a document reads as null, which sets a non-nullable property to its default. Auto-increment keys (single
    /// integer primary key) come from a per-collection counter document in <see cref="MongoDbRepositorySettings.SequenceCollectionName"/>
    /// (an atomic <c>findOneAndUpdate</c>, taken outside transactions like a SQL sequence), so concurrent creates never share
    /// a key; inserting a duplicate primary key throws <see cref="InvalidOperationException"/>. Indexes are created for foreign
    /// keys, composite key parts, <see cref="IndexAttribute"/> and <see cref="CompositeIndexAttribute"/> on first insert (or by
    /// <see cref="EnsureIndexes"/>); unique indexes are enforced and, as in SQL, ignore rows whose indexed columns are null.
    /// </para>
    /// <para>
    /// Queries: the parts of a filter whose MongoDB semantics are exactly C#'s (comparisons, null checks, IN lists, string
    /// matches and case-insensitive equality between a column and a value, see <see cref="MongoDbFilterTranslator"/>) are
    /// pushed down as a query filter, where indexes apply; ordering by columns, Skip/Take, Count and Sum/Average/Min/Max of a
    /// column run on the server when the whole filter was pushed down; everything else (functions, arithmetic,
    /// navigations, and anything after an inexact filter) is evaluated client-side with C# semantics over the documents
    /// MongoDB returns, so push-down never changes results. Every command uses the simple (binary) collation, so a
    /// collection's default collation never applies: strings compare ordinally, <see cref="StringMatchMode.Database"/>
    /// behaves as <see cref="StringMatchMode.Ordinal"/>, and string keys are case-sensitive. MongoDB orders strings by UTF-8
    /// bytes (Unicode code points), which differs from C#'s UTF-16 ordinal order only between supplementary characters
    /// (for example emoji) and U+E000 to U+FFFF; server-side sorting follows MongoDB's order, filters never differ. Ties in a
    /// server-side sort are broken by <c>_id</c>; unordered queries return documents in <c>_id</c> order.
    /// </para>
    /// <para>
    /// Writes outside a transaction are atomic per document: conditional updates (optimistic concurrency) and deletes whose
    /// filter is pushed down exactly run as one <c>updateMany</c> / <c>deleteMany</c>; otherwise each document is updated or
    /// deleted with a filter on its complete previous content and re-checked when it changed concurrently, so updates are
    /// never lost. Transactions (<see cref="MongoDbTransaction"/>, client sessions) need a replica set or a sharded cluster;
    /// on a standalone server <see cref="Capabilities"/> omits <see cref="RepositoryCapabilities.Transactions"/> (see
    /// <see cref="SupportsTransactions"/>).
    /// </para>
    /// <para>
    /// Lifetime: create a backend with <see cref="CreateAsync"/> or <see cref="Create"/> (both contact the server once, to
    /// detect transaction support unless <see cref="MongoDbRepositorySettings.TransactionsEnabled"/> is set), share it across
    /// repositories, and dispose it when done. It disposes the client only when it created it (<see cref="OwnsClient"/>); a
    /// client passed in with <see cref="MongoDbRepositorySettings.Client"/> is never disposed. Repositories never dispose the
    /// backend.
    /// </para>
    /// <para>
    /// Trimming and Native AOT: not supported. The MongoDB .NET driver (MongoDB.Driver and MongoDB.Bson) produces trim and
    /// AOT analysis warnings (its serializer registry, conventions and LINQ provider use reflection and runtime code
    /// generation), so <see cref="CreateAsync"/> and <see cref="Create"/> carry <see cref="RequiresUnreferencedCodeAttribute"/>
    /// and <see cref="RequiresDynamicCodeAttribute"/>. Durable's own code in this package is trim-annotated and
    /// warning-free and never uses the driver's class maps.
    /// </para>
    /// Thread safety: safe for concurrent use by any number of repositories and threads.
    /// </summary>
    public sealed class MongoDbBackend : IRepositoryBackend, IDisposable, IAsyncDisposable
    {
        #region Public-Members

        /// <summary>
        /// Gets the optional features the backend supports: all of them, except <see cref="RepositoryCapabilities.Transactions"/>
        /// when <see cref="SupportsTransactions"/> is false.
        /// </summary>
        public RepositoryCapabilities Capabilities { get; }

        /// <summary>
        /// Gets whether the deployment supports multi-document transactions (a replica set or a sharded cluster, or as forced by
        /// <see cref="MongoDbRepositorySettings.TransactionsEnabled"/>).
        /// </summary>
        public bool SupportsTransactions { get; }

        /// <summary>
        /// Gets the MongoDB client. Never null.
        /// </summary>
        public IMongoClient Client { get; }

        /// <summary>
        /// Gets the MongoDB database holding the collections. Never null.
        /// </summary>
        public IMongoDatabase Database { get; }

        /// <summary>
        /// Gets the database name. Never null.
        /// </summary>
        public string DatabaseName { get; }

        /// <summary>
        /// Gets the collection holding the auto-increment sequences. Never null.
        /// </summary>
        public string SequenceCollectionName { get; }

        /// <summary>
        /// Gets whether the backend disposes <see cref="Client"/> when it is disposed (true when the backend created it from
        /// settings, false when the client was passed in).
        /// </summary>
        public bool OwnsClient { get; }

        /// <summary>
        /// Gets the JSON options used to store JSON columns. Never null.
        /// Default: camelCase property names, not indented (the same as the SQL providers).
        /// </summary>
        public JsonSerializerOptions JsonOptions => _Values.JsonOptions;

        /// <summary>
        /// Gets or sets whether each find outside a transaction also runs MongoDB's <c>explain</c> command, so the query plan
        /// carries the winning plan (<see cref="MongoDbQueryPlan.MongoDbExplain"/>); this costs an extra round trip per query.
        /// Default: false.
        /// </summary>
        public bool ExplainQueries { get; set; } = false;

        /// <summary>
        /// Gets the plan of the most recent read or write executed by any thread, or null before the first one. Use it in
        /// single-threaded diagnostics and tests; use <see cref="QueryPlanned"/> to observe every plan.
        /// </summary>
        public MongoDbQueryPlan? LastQueryPlan => _LastQueryPlan;

        /// <summary>
        /// Raised with the plan of every read and write, synchronously on the thread that executed it; the sender is the
        /// backend. Handlers must be thread-safe and fast and must not call the backend; an exception thrown by a handler is
        /// logged (Warning) and otherwise ignored.
        /// </summary>
        public event EventHandler<MongoDbQueryPlan>? QueryPlanned;

        #endregion

        #region Private-Members

        private const string _NotAotCompatible =
            "The MongoDB .NET driver (MongoDB.Driver, MongoDB.Bson) is not trim or Native AOT compatible: its serializer "
            + "registry, conventions and LINQ provider use reflection and runtime code generation. Durable.MongoDb itself is "
            + "trim-annotated and never uses the driver's class maps; use MongoDB on the JIT runtime.";

        private const int _MaxRetries = 16;

        private static readonly JsonSerializerOptions _DefaultJsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };

        private static readonly BsonDocument _SortById = new BsonDocument(MongoDbCollectionSchema.IdField, 1);
        private readonly MongoDbValueConverter _Values;
        private readonly ILogger? _Logger;
        private readonly ConcurrentDictionary<string, bool> _Ensured = new ConcurrentDictionary<string, bool>(StringComparer.Ordinal);
        private volatile MongoDbQueryPlan? _LastQueryPlan;
        private int _Disposed;

        #endregion

        #region Constructors-and-Factories

        private MongoDbBackend(IMongoClient client, bool ownsClient, bool supportsTransactions, MongoDbRepositorySettings settings)
        {
            Client = client;
            OwnsClient = ownsClient;
            DatabaseName = settings.DatabaseName;
            Database = client.GetDatabase(settings.DatabaseName);
            SequenceCollectionName = settings.SequenceCollectionName;
            SupportsTransactions = supportsTransactions;
            Capabilities = supportsTransactions ? RepositoryCapabilities.All : RepositoryCapabilities.All & ~RepositoryCapabilities.Transactions;
            _Values = new MongoDbValueConverter(settings.JsonOptions ?? _DefaultJsonOptions);
            _Logger = settings.Logger;
        }

        /// <summary>
        /// Creates a backend: uses <see cref="MongoDbRepositorySettings.Client"/> when set (not owned, never disposed),
        /// otherwise creates a client from the connection settings (owned and disposed by the backend). Contacts the server
        /// once to detect transaction support unless <see cref="MongoDbRepositorySettings.TransactionsEnabled"/> is set.
        /// Blocks the calling thread; prefer <see cref="CreateAsync"/> in asynchronous code.
        /// </summary>
        /// <param name="settings">Settings; null uses database "durable" on localhost:27017.</param>
        /// <returns>The backend. Never null. Dispose it when done.</returns>
        /// <exception cref="ArgumentException">Thrown when the settings are invalid (see <see cref="MongoDbRepositorySettings.Validate"/>).</exception>
        /// <exception cref="MongoException">Thrown when the server cannot be reached or rejects the connection (for example wrong credentials).</exception>
        /// <exception cref="TimeoutException">Thrown when no suitable server is found within the server selection timeout.</exception>
        [RequiresUnreferencedCode(_NotAotCompatible)]
        [RequiresDynamicCode(_NotAotCompatible)]
        public static MongoDbBackend Create(MongoDbRepositorySettings? settings = null)
        {
            settings ??= new MongoDbRepositorySettings();
            settings.Validate();
            bool owns = settings.Client == null;
            IMongoClient client = settings.Client ?? new MongoClient(settings.ToClientSettings());
            try
            {
                bool transactions = settings.TransactionsEnabled ?? DetectTransactions(client.GetDatabase("admin"));
                return new MongoDbBackend(client, owns, transactions, settings);
            }
            catch (Exception)
            {
                if (owns) client.Dispose();
                throw;
            }
        }

        /// <summary>
        /// Creates a backend (see <see cref="Create"/>), contacting the server asynchronously.
        /// </summary>
        /// <param name="settings">Settings; null uses database "durable" on localhost:27017.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The backend. Never null. Dispose it when done.</returns>
        /// <exception cref="ArgumentException">Thrown when the settings are invalid (see <see cref="MongoDbRepositorySettings.Validate"/>).</exception>
        /// <exception cref="MongoException">Thrown when the server cannot be reached or rejects the connection.</exception>
        /// <exception cref="TimeoutException">Thrown when no suitable server is found within the server selection timeout.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the token is canceled.</exception>
        [RequiresUnreferencedCode(_NotAotCompatible)]
        [RequiresDynamicCode(_NotAotCompatible)]
        public static async Task<MongoDbBackend> CreateAsync(MongoDbRepositorySettings? settings = null, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            settings ??= new MongoDbRepositorySettings();
            settings.Validate();
            bool owns = settings.Client == null;
            IMongoClient client = settings.Client ?? new MongoClient(settings.ToClientSettings());
            try
            {
                bool transactions = settings.TransactionsEnabled ?? await DetectTransactionsAsync(client.GetDatabase("admin"), token).ConfigureAwait(false);
                return new MongoDbBackend(client, owns, transactions, settings);
            }
            catch (Exception)
            {
                if (owns) client.Dispose();
                throw;
            }
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Creates a repository over this backend. Does not contact the server.
        /// </summary>
        /// <typeparam name="T">Entity type.</typeparam>
        /// <param name="options">Options; null uses defaults.</param>
        /// <returns>A new repository.</returns>
        /// <exception cref="InvalidOperationException">Thrown when <typeparamref name="T"/> has no primary key or an invalid mapping.</exception>
        /// <exception cref="NotSupportedException">Thrown when the entity cannot be stored in MongoDB (see <see cref="MongoDbRepository{T}"/>).</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        public MongoDbRepository<T> CreateRepository<[DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] T>(RepositoryOptions? options = null) where T : class, new()
        {
            ThrowIfDisposed();
            return new MongoDbRepository<T>(this, options);
        }

        /// <summary>
        /// Determines whether a transaction was created by this backend.
        /// </summary>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>True when the transaction belongs to this backend.</returns>
        public bool Owns(ITransaction? transaction)
        {
            return transaction is MongoDbTransaction mongo && ReferenceEquals(mongo.Backend, this);
        }

        /// <summary>
        /// Creates the indexes of an entity (foreign keys, composite key parts, <see cref="IndexAttribute"/> and
        /// <see cref="CompositeIndexAttribute"/>) if they do not exist yet, which also creates the collection. Inserts outside
        /// transactions call it the first time they write a collection; call it yourself to create the indexes up front.
        /// Failures (for example an index that exists with other options, or existing rows that violate a unique index) are
        /// logged (Warning), not thrown. Blocks the calling thread; prefer <see cref="EnsureIndexesAsync"/>.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        /// <exception cref="NotSupportedException">Thrown when the entity cannot be stored in MongoDB.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        public void EnsureIndexes([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            ThrowIfDisposed();
            EnsureIndexesCore(MongoDbCollectionSchema.For(EntityMetadata.For(entityType)), true, CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Creates the indexes of an entity if they do not exist yet (see <see cref="EnsureIndexes(Type)"/>).
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task that completes when the indexes exist.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        /// <exception cref="NotSupportedException">Thrown when the entity cannot be stored in MongoDB.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the token is canceled.</exception>
        public Task EnsureIndexesAsync([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            ThrowIfDisposed();
            token.ThrowIfCancellationRequested();
            return EnsureIndexesCore(MongoDbCollectionSchema.For(EntityMetadata.For(entityType)), false, token);
        }

        /// <summary>
        /// Returns the stored documents of an entity, in <c>_id</c> order, for diagnostics and tests that check the stored
        /// representation. Soft-deleted documents are included. Blocks the calling thread; prefer <see cref="GetStoredRowsAsync"/>.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <returns>The documents. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        public IReadOnlyList<BsonDocument> GetStoredRows([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            ThrowIfDisposed();
            MongoDbCollectionSchema schema = MongoDbCollectionSchema.For(EntityMetadata.For(entityType));
            List<BsonDocument> documents = Collection(schema).Find(new BsonDocument(), new FindOptions { Collation = Collation.Simple }).ToList();
            SortById(documents);
            return documents;
        }

        /// <summary>
        /// Returns the stored documents of an entity, in <c>_id</c> order (see <see cref="GetStoredRows"/>).
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The documents. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the token is canceled.</exception>
        public async Task<IReadOnlyList<BsonDocument>> GetStoredRowsAsync([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            ThrowIfDisposed();
            token.ThrowIfCancellationRequested();
            MongoDbCollectionSchema schema = MongoDbCollectionSchema.For(EntityMetadata.For(entityType));
            List<BsonDocument> documents = await (await Collection(schema).FindAsync(new BsonDocument(), new FindOptions<BsonDocument> { Collation = Collation.Simple }, token).ConfigureAwait(false))
                .ToListAsync(token).ConfigureAwait(false);
            SortById(documents);
            return documents;
        }

        /// <summary>
        /// Drops every collection of the database (all data, indexes and the auto-increment sequences), bypassing soft
        /// delete, query filters and transactions. Use a database dedicated to Durable: collections that Durable did not
        /// create are dropped too. Must not be called while a transaction is open. Blocks the calling thread; prefer
        /// <see cref="ClearAsync(CancellationToken)"/>.
        /// </summary>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        public void Clear()
        {
            ThrowIfDisposed();
            foreach (string name in Database.ListCollectionNames().ToList())
            {
                if (name.StartsWith("system.", StringComparison.Ordinal)) continue;
                Database.DropCollection(name);
            }

            _Ensured.Clear();
        }

        /// <summary>
        /// Drops every collection of the database (see <see cref="Clear()"/>).
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task that completes when the collections are dropped.</returns>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the token is canceled.</exception>
        public async Task ClearAsync(CancellationToken token = default)
        {
            ThrowIfDisposed();
            token.ThrowIfCancellationRequested();
            List<string> names = await (await Database.ListCollectionNamesAsync(null, token).ConfigureAwait(false)).ToListAsync(token).ConfigureAwait(false);
            foreach (string name in names)
            {
                if (name.StartsWith("system.", StringComparison.Ordinal)) continue;
                await Database.DropCollectionAsync(name, token).ConfigureAwait(false);
            }

            _Ensured.Clear();
        }

        /// <summary>
        /// Removes every document of one entity type (including soft-deleted rows) by dropping its collection, resets its
        /// auto-increment sequence, and recreates the (empty) collection with its indexes. Bypasses soft delete, query filters
        /// and transactions; must not be called while a transaction is open. Blocks the calling thread; prefer
        /// <see cref="ClearAsync(Type, CancellationToken)"/>.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <returns>The number of documents removed.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        /// <exception cref="NotSupportedException">Thrown when the entity cannot be stored in MongoDB.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        public int Clear([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            ThrowIfDisposed();
            MongoDbCollectionSchema schema = MongoDbCollectionSchema.For(EntityMetadata.For(entityType));
            long count = Collection(schema).CountDocuments(new BsonDocument());
            Database.DropCollection(schema.CollectionName);
            Database.GetCollection<BsonDocument>(SequenceCollectionName).DeleteOne(new BsonDocument(MongoDbCollectionSchema.IdField, schema.CollectionName));
            _Ensured.TryRemove(schema.CollectionName, out bool _);
            EnsureIndexesCore(schema, true, CancellationToken.None).GetAwaiter().GetResult();
            return (int)Math.Min(int.MaxValue, count);
        }

        /// <summary>
        /// Removes every document of one entity type (see <see cref="Clear(Type)"/>).
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The number of documents removed.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        /// <exception cref="NotSupportedException">Thrown when the entity cannot be stored in MongoDB.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the token is canceled.</exception>
        public async Task<int> ClearAsync([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            ThrowIfDisposed();
            token.ThrowIfCancellationRequested();
            MongoDbCollectionSchema schema = MongoDbCollectionSchema.For(EntityMetadata.For(entityType));
            long count = await Collection(schema).CountDocumentsAsync(new BsonDocument(), null, token).ConfigureAwait(false);
            await Database.DropCollectionAsync(schema.CollectionName, token).ConfigureAwait(false);
            await Database.GetCollection<BsonDocument>(SequenceCollectionName).DeleteOneAsync(new BsonDocument(MongoDbCollectionSchema.IdField, schema.CollectionName), token).ConfigureAwait(false);
            _Ensured.TryRemove(schema.CollectionName, out bool _);
            await EnsureIndexesCore(schema, false, token).ConfigureAwait(false);
            return (int)Math.Min(int.MaxValue, count);
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
        public Task<long> CountAsync(QueryModel model, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(model);
            return ExecuteAsync(model.Transaction, null, session => CountCoreAsync(model, session, token), token);
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Thrown when <paramref name="function"/> is Count or Any.</exception>
        public Task<object?> AggregateAsync(QueryModel model, AggregateFunction function, QueryNode operand, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(model);
            ArgumentNullException.ThrowIfNull(operand);
            if (function != AggregateFunction.Sum && function != AggregateFunction.Average && function != AggregateFunction.Min && function != AggregateFunction.Max)
                throw new NotSupportedException("Aggregate " + function + " is not a value aggregate.");
            return ExecuteAsync(model.Transaction, null, session => AggregateCoreAsync(model, function, operand, session, token), token);
        }

        /// <inheritdoc />
        public async Task InsertAsync(EntityMetadata metadata, object entity, ITransaction? transaction, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(metadata);
            ArgumentNullException.ThrowIfNull(entity);
            ThrowIfDisposed();
            token.ThrowIfCancellationRequested();

            MongoDbCollectionSchema schema = MongoDbCollectionSchema.For(metadata);
            ColumnMetadata? generated = metadata.AutoIncrementColumn;
            bool generate = generated != null && IsUnsetGenerated(_Values.ToStored(generated, generated.GetValue(entity)));
            BsonDocument document = ToDocument(schema, entity, generate);
            if (transaction == null) await EnsureIndexesCore(schema, false, token).ConfigureAwait(false);

            BsonValue id = await ExecuteAsync(transaction, metadata, async session =>
            {
                IMongoCollection<BsonDocument> collection = Collection(schema);
                if (generate) return await InsertGeneratedAsync(schema, collection, document, session, token).ConfigureAwait(false);

                BsonValue key = document[MongoDbCollectionSchema.IdField];
                BsonDocument byId = new BsonDocument(MongoDbCollectionSchema.IdField, key);
                long existing = session != null
                    ? await collection.CountDocumentsAsync(session, byId, new CountOptions { Limit = 1 }, token).ConfigureAwait(false)
                    : await collection.CountDocumentsAsync(byId, new CountOptions { Limit = 1 }, token).ConfigureAwait(false);
                if (existing > 0) throw DuplicateKey(metadata, key.ToString(), null);

                if (session != null) await collection.InsertOneAsync(session, document, null, token).ConfigureAwait(false);
                else await collection.InsertOneAsync(document, null, token).ConfigureAwait(false);
                if (generated != null) await BumpSequenceAsync(schema, key, token).ConfigureAwait(false);
                return key;
            }, token).ConfigureAwait(false);

            if (generate) generated!.SetValue(entity, _Values.FromStored(generated, MongoDbBsonCodec.Decode(id, schema.StoredType(generated))));
        }

        /// <inheritdoc />
        public Task<int> ReplaceAsync(EntityMetadata metadata, object entity, QueryNode condition, QuerySource source, ITransaction? transaction, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(metadata);
            ArgumentNullException.ThrowIfNull(entity);
            ArgumentNullException.ThrowIfNull(condition);
            ArgumentNullException.ThrowIfNull(source);
            MongoDbCollectionSchema schema = MongoDbCollectionSchema.For(metadata);

            BsonDocument replacement = new BsonDocument();
            foreach (ColumnMetadata column in metadata.Columns)
            {
                if (column.IsPrimaryKey || column.IsAutoIncrement) continue;
                replacement[schema.Field(column)] = MongoDbBsonCodec.Encode(_Values.ToStored(column, column.GetValue(entity)));
            }

            QueryModel model = new QueryModel(source) { Filter = condition, Transaction = transaction };
            return ExecuteAsync(transaction, metadata, async session =>
            {
                if (replacement.ElementCount == 0)
                {
                    List<BsonDocument> found = await ReadAsync(model, session, false, "Replace", null, token).ConfigureAwait(false);
                    return found.Count;
                }

                MongoDbPushdown pushdown = MongoDbPushdown.Translate(source, condition, schema, _Values);
                if (pushdown.Exact)
                {
                    UpdateResult result = await UpdateManyAsync(Collection(schema), session, pushdown.Combined(), new BsonDocument("$set", replacement), token).ConfigureAwait(false);
                    Record("Replace", schema, pushdown, null, null, null, true, -1, null);
                    return (int)result.MatchedCount;
                }

                return await ApplyPerDocumentAsync(schema, model, session, "Replace", document => new BsonDocument("$set", replacement), token).ConfigureAwait(false);
            }, token);
        }

        /// <inheritdoc />
        /// <exception cref="InvalidOperationException">Thrown when an assignment changes a primary key to one that already exists, or to null.</exception>
        public Task<int> UpdateAsync(QueryModel model, IReadOnlyList<FieldAssignment> assignments, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(model);
            ArgumentNullException.ThrowIfNull(assignments);
            if (assignments.Count == 0) throw new ArgumentException("At least one assignment is required.", nameof(assignments));
            MongoDbCollectionSchema schema = MongoDbCollectionSchema.For(model.Metadata);
            return ExecuteAsync(model.Transaction, model.Metadata, session => UpdateCoreAsync(schema, model, assignments, session, token), token);
        }

        /// <inheritdoc />
        public Task<int> DeleteAsync(QueryModel model, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(model);
            MongoDbCollectionSchema schema = MongoDbCollectionSchema.For(model.Metadata);
            return ExecuteAsync(model.Transaction, model.Metadata, async session =>
            {
                MongoDbPushdown pushdown = MongoDbPushdown.Translate(model.Source, model.Filter, schema, _Values);
                if (pushdown.Exact && !model.Skip.HasValue && !model.Take.HasValue)
                {
                    IMongoCollection<BsonDocument> collection = Collection(schema);
                    DeleteOptions options = new DeleteOptions { Collation = Collation.Simple };
                    DeleteResult result = session != null
                        ? await collection.DeleteManyAsync(session, pushdown.Combined(), options, token).ConfigureAwait(false)
                        : await collection.DeleteManyAsync(pushdown.Combined(), options, token).ConfigureAwait(false);
                    Record("Delete", schema, pushdown, null, null, null, true, -1, null);
                    return (int)result.DeletedCount;
                }

                return await ApplyPerDocumentAsync(schema, model, session, "Delete", null, token).ConfigureAwait(false);
            }, token);
        }

        /// <summary>
        /// Begins a transaction (see <see cref="MongoDbTransaction"/>). Blocks the calling thread while the session starts;
        /// prefer <see cref="BeginTransactionAsync"/> in asynchronous code.
        /// </summary>
        /// <returns>The transaction. Never null. Dispose it; an uncommitted transaction rolls back on dispose.</returns>
        /// <exception cref="NotSupportedException">Thrown when the deployment does not support transactions (see <see cref="SupportsTransactions"/>).</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        public MongoDbTransaction BeginTransaction()
        {
            ThrowIfDisposed();
            RequireTransactions();
            return MongoDbTransaction.Begin(this, Client, null);
        }

        /// <summary>
        /// Begins a transaction (see <see cref="MongoDbTransaction"/>).
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The transaction. Never null. Dispose it; an uncommitted transaction rolls back on dispose.</returns>
        /// <exception cref="NotSupportedException">Thrown when the deployment does not support transactions (see <see cref="SupportsTransactions"/>).</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the token is canceled.</exception>
        public Task<MongoDbTransaction> BeginTransactionAsync(CancellationToken token = default)
        {
            ThrowIfDisposed();
            RequireTransactions();
            return MongoDbTransaction.BeginAsync(this, Client, null, token);
        }

        /// <inheritdoc />
        async Task<ITransaction> IRepositoryBackend.BeginTransactionAsync(CancellationToken token)
        {
            return await BeginTransactionAsync(token).ConfigureAwait(false);
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
        /// Disposes the backend (see <see cref="Dispose"/>). The driver closes its connections synchronously, so this
        /// completes synchronously.
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
            MongoDbCollectionSchema.For(EntityMetadata.For(entityType));
        }

        private static bool DetectTransactions(IMongoDatabase admin)
        {
            BsonDocument hello;
            try
            {
                hello = admin.RunCommand<BsonDocument>(new BsonDocument("hello", 1));
            }
            catch (MongoCommandException)
            {
                hello = admin.RunCommand<BsonDocument>(new BsonDocument("isMaster", 1));
            }

            return SupportsTransactionsFrom(hello);
        }

        private static async Task<bool> DetectTransactionsAsync(IMongoDatabase admin, CancellationToken token)
        {
            BsonDocument hello;
            try
            {
                hello = await admin.RunCommandAsync<BsonDocument>(new BsonDocument("hello", 1), null, token).ConfigureAwait(false);
            }
            catch (MongoCommandException)
            {
                hello = await admin.RunCommandAsync<BsonDocument>(new BsonDocument("isMaster", 1), null, token).ConfigureAwait(false);
            }

            return SupportsTransactionsFrom(hello);
        }

        private static bool SupportsTransactionsFrom(BsonDocument hello)
        {
            if (!hello.Contains("logicalSessionTimeoutMinutes") || hello["logicalSessionTimeoutMinutes"].IsBsonNull) return false;
            if (hello.Contains("setName")) return true;
            return hello.Contains("msg") && hello["msg"].IsString && hello["msg"].AsString == "isdbgrid";
        }

        private void RequireTransactions()
        {
            if (!SupportsTransactions)
                throw new NotSupportedException("This MongoDB deployment does not support transactions (they need a replica set or a sharded cluster); the backend's capabilities omit Transactions.");
        }

        private async Task EnsureIndexesCore(MongoDbCollectionSchema schema, bool synchronous, CancellationToken token)
        {
            if (_Ensured.ContainsKey(schema.CollectionName)) return;
            IMongoCollection<BsonDocument> collection = Collection(schema);
            bool complete = true;
            try
            {
                if (synchronous) Database.CreateCollection(schema.CollectionName);
                else await Database.CreateCollectionAsync(schema.CollectionName, null, token).ConfigureAwait(false);
            }
            catch (MongoCommandException e) when (e.Code == 48 || e.CodeName == "NamespaceExists")
            {
            }

            foreach (MongoDbIndexDefinition index in schema.Indexes)
            {
                BsonDocument keys = new BsonDocument();
                BsonDocument partial = new BsonDocument();
                foreach (ColumnMetadata column in index.Columns)
                {
                    string field = schema.Field(column);
                    keys[field] = 1;
                    partial[field] = new BsonDocument("$type", MongoDbCollectionSchema.BsonTypeAlias(schema.StoredType(column)));
                }

                CreateIndexOptions<BsonDocument> options = new CreateIndexOptions<BsonDocument> { Name = index.Name };
                if (index.IsUnique)
                {
                    options.Unique = true;
                    options.PartialFilterExpression = partial;
                }

                CreateIndexModel<BsonDocument> model = new CreateIndexModel<BsonDocument>(keys, options);
                try
                {
                    if (synchronous) collection.Indexes.CreateOne(model);
                    else await collection.Indexes.CreateOneAsync(model, null, token).ConfigureAwait(false);
                }
                catch (MongoException e)
                {
                    complete = false;
                    _Logger?.LogWarning(e, "MongoDB index {Index} on {Collection} could not be created: {Message}", index.Name, schema.CollectionName, e.Message);
                }
            }

            if (complete) _Ensured[schema.CollectionName] = true;
        }

        private async IAsyncEnumerable<object> StreamAsync(QueryModel model, CancellationToken queryToken, [EnumeratorCancellation] CancellationToken enumerationToken = default)
        {
            List<object> entities = await ExecuteAsync(model.Transaction, null, async session =>
            {
                List<BsonDocument> rows = await ReadAsync(model, session, true, "Query", null, queryToken).ConfigureAwait(false);
                MongoDbCollectionSchema schema = MongoDbCollectionSchema.For(model.Metadata);
                List<object> materialized = new List<object>(rows.Count);
                foreach (BsonDocument row in rows) materialized.Add(Materialize(schema, row));
                return materialized;
            }, queryToken).ConfigureAwait(false);

            foreach (object entity in entities)
            {
                queryToken.ThrowIfCancellationRequested();
                enumerationToken.ThrowIfCancellationRequested();
                yield return entity;
            }
        }

        private async Task<TResult> ExecuteAsync<TResult>(ITransaction? transaction, EntityMetadata? metadata, Func<IClientSessionHandle?, Task<TResult>> work, CancellationToken token)
        {
            ThrowIfDisposed();
            token.ThrowIfCancellationRequested();
            MongoDbTransaction? mongo = ResolveTransaction(transaction);
            try
            {
                if (mongo != null) return await mongo.RunAsync(session => work(session), token).ConfigureAwait(false);
                return await work(null).ConfigureAwait(false);
            }
            catch (MongoWriteException e) when (e.WriteError != null && e.WriteError.Category == ServerErrorCategory.DuplicateKey)
            {
                throw DuplicateKey(metadata, null, e);
            }
            catch (MongoBulkWriteException e) when (e.WriteErrors.Any(w => w.Category == ServerErrorCategory.DuplicateKey))
            {
                throw DuplicateKey(metadata, null, e);
            }
        }

        private MongoDbTransaction? ResolveTransaction(ITransaction? transaction)
        {
            if (transaction == null) return null;
            if (transaction is not MongoDbTransaction mongo || !ReferenceEquals(mongo.Backend, this))
                throw new ArgumentException("The transaction was not created by this MongoDB backend.", nameof(transaction));
            if (mongo.IsCompleted) throw new InvalidOperationException("The transaction has already completed.");
            return mongo;
        }

        private async Task<long> CountCoreAsync(QueryModel model, IClientSessionHandle? session, CancellationToken token)
        {
            MongoDbCollectionSchema schema = MongoDbCollectionSchema.For(model.Metadata);
            MongoDbPushdown pushdown = MongoDbPushdown.Translate(model.Source, model.Filter, schema, _Values);
            if (!pushdown.Exact) return (await ReadAsync(model, session, true, "Count", pushdown, token).ConfigureAwait(false)).Count;

            long count;
            if (model.Take.HasValue && model.Take.Value == 0)
            {
                count = 0;
            }
            else
            {
                CountOptions options = new CountOptions { Collation = Collation.Simple };
                if (model.Skip.HasValue && model.Skip.Value > 0) options.Skip = model.Skip.Value;
                if (model.Take.HasValue) options.Limit = model.Take.Value;
                IMongoCollection<BsonDocument> collection = Collection(schema);
                BsonDocument filter = pushdown.Combined();
                count = session != null
                    ? await collection.CountDocumentsAsync(session, filter, options, token).ConfigureAwait(false)
                    : await collection.CountDocumentsAsync(filter, options, token).ConfigureAwait(false);
            }

            Record("Count", schema, pushdown, null, null, null, true, -1, null);
            return count;
        }

        private async Task<object?> AggregateCoreAsync(QueryModel model, AggregateFunction function, QueryNode operand, IClientSessionHandle? session, CancellationToken token)
        {
            MongoDbCollectionSchema schema = MongoDbCollectionSchema.For(model.Metadata);
            MongoDbPushdown pushdown = MongoDbPushdown.Translate(model.Source, model.Filter, schema, _Values);
            ColumnMetadata? column = operand is ColumnNode node && ReferenceEquals(node.Source, model.Source) ? node.Column : null;
            Type? stored = column != null ? schema.StoredType(column) : null;
            bool numeric = stored != null && MongoDbBsonCodec.IsExactNumber(stored);
            bool comparable = stored != null && (MongoDbBsonCodec.IsPlain(stored) || MongoDbBsonCodec.IsString(stored) || MongoDbBsonCodec.IsRanged(stored));
            bool supported = function == AggregateFunction.Sum || function == AggregateFunction.Average ? numeric : comparable;
            bool paged = model.Skip.HasValue || model.Take.HasValue;
            BsonDocument? sort = paged ? BuildSort(model, schema) : null;

            if (!pushdown.Exact || !supported || (paged && sort == null))
            {
                List<BsonDocument> rows = await ReadAsync(model, session, true, "Aggregate", pushdown, token).ConfigureAwait(false);
                object? computed = new MongoDbQueryEvaluator(Database, session, _Values).Aggregate(model.Source, rows, function, operand);
                if (computed != null && column != null && (function == AggregateFunction.Min || function == AggregateFunction.Max))
                    computed = _Values.FromStored(column, computed);
                return computed;
            }

            if (model.Take.HasValue && model.Take.Value == 0) return null;
            string field = schema.Field(column!);
            List<BsonDocument> stages = new List<BsonDocument>();
            if (pushdown.Conjuncts.Count > 0) stages.Add(new BsonDocument("$match", pushdown.Combined()));
            if (paged)
            {
                stages.Add(new BsonDocument("$sort", sort!));
                if (model.Skip.HasValue && model.Skip.Value > 0) stages.Add(new BsonDocument("$skip", model.Skip.Value));
                if (model.Take.HasValue) stages.Add(new BsonDocument("$limit", model.Take.Value));
            }

            stages.Add(new BsonDocument("$match", new BsonDocument(field, new BsonDocument("$ne", BsonNull.Value))));
            BsonDocument group = new BsonDocument(MongoDbCollectionSchema.IdField, BsonNull.Value);
            switch (function)
            {
                case AggregateFunction.Sum:
                case AggregateFunction.Average:
                    group["v"] = new BsonDocument("$sum", new BsonDocument("$toDecimal", "$" + field));
                    group["n"] = new BsonDocument("$sum", 1);
                    break;
                case AggregateFunction.Min:
                    group["v"] = new BsonDocument("$min", "$" + field);
                    break;
                default:
                    group["v"] = new BsonDocument("$max", "$" + field);
                    break;
            }

            stages.Add(new BsonDocument("$group", group));
            PipelineDefinition<BsonDocument, BsonDocument> pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(stages);
            AggregateOptions options = new AggregateOptions { Collation = Collation.Simple };
            IMongoCollection<BsonDocument> collection = Collection(schema);
            IAsyncCursor<BsonDocument> cursor = session != null
                ? await collection.AggregateAsync(session, pipeline, options, token).ConfigureAwait(false)
                : await collection.AggregateAsync(pipeline, options, token).ConfigureAwait(false);
            List<BsonDocument> results = await cursor.ToListAsync(token).ConfigureAwait(false);
            Record("Aggregate", schema, pushdown, sort != null ? MongoDbPushdown.ToJson(sort) : null, MongoDbPushdown.ToJson(new BsonDocument("pipeline", new BsonArray(stages))), null, true, -1, null);
            if (results.Count == 0) return null;

            BsonValue value = results[0]["v"];
            if (value.IsBsonNull) return null;
            switch (function)
            {
                case AggregateFunction.Sum:
                    return MongoDbBsonCodec.ToDecimal(value);
                case AggregateFunction.Average:
                    {
                        long count = results[0]["n"].ToInt64();
                        return count == 0 ? null : MongoDbBsonCodec.ToDecimal(value) / count;
                    }
                default:
                    return _Values.FromStored(column!, MongoDbBsonCodec.Decode(value, stored!));
            }
        }

        private async Task<List<BsonDocument>> ReadAsync(QueryModel model, IClientSessionHandle? session, bool orderingAndPaging, string operation, MongoDbPushdown? translated, CancellationToken token)
        {
            MongoDbCollectionSchema schema = MongoDbCollectionSchema.For(model.Metadata);
            MongoDbPushdown pushdown = translated ?? MongoDbPushdown.Translate(model.Source, model.Filter, schema, _Values);
            bool ordered = orderingAndPaging && model.Orderings.Count > 0;
            bool paged = orderingAndPaging && (model.Skip.HasValue || model.Take.HasValue);
            BsonDocument? sort = ordered ? BuildSort(model, schema) : null;
            bool sortPushed = sort != null;
            bool pagePushed = paged && pushdown.Exact && (sortPushed || !ordered);
            if (pagePushed && !sortPushed) sort = _SortById;

            BsonDocument filter = pushdown.Combined();
            List<BsonDocument> documents;
            string? explain = null;
            if (pagePushed && model.Take.HasValue && model.Take.Value == 0)
            {
                documents = new List<BsonDocument>();
            }
            else
            {
                FindOptions<BsonDocument> options = new FindOptions<BsonDocument> { Collation = Collation.Simple };
                if (sort != null) options.Sort = sort;
                if (pagePushed && model.Skip.HasValue && model.Skip.Value > 0) options.Skip = model.Skip.Value;
                if (pagePushed && model.Take.HasValue) options.Limit = model.Take.Value;
                IMongoCollection<BsonDocument> collection = Collection(schema);
                IAsyncCursor<BsonDocument> cursor = session != null
                    ? await collection.FindAsync(session, filter, options, token).ConfigureAwait(false)
                    : await collection.FindAsync(filter, options, token).ConfigureAwait(false);
                documents = await cursor.ToListAsync(token).ConfigureAwait(false);
                if (ExplainQueries && session == null) explain = await ExplainAsync(schema, filter, sort, options.Skip, options.Limit, token).ConfigureAwait(false);
            }

            int read = documents.Count;
            if (sort == null) SortById(documents);

            string? clientSide = null;
            if (!pushdown.Exact || (ordered && !sortPushed) || (paged && !pagePushed))
            {
                QueryModel residual = new QueryModel(model.Source)
                {
                    Filter = pushdown.Exact ? null : model.Filter,
                    Skip = paged && !pagePushed ? model.Skip : null,
                    Take = paged && !pagePushed ? model.Take : null
                };
                if (ordered && !sortPushed) residual.Orderings.AddRange(model.Orderings);
                documents = new MongoDbQueryEvaluator(Database, session, _Values).Apply(residual, documents);
                clientSide = DescribeClientSide(residual);
            }

            Record(operation, schema, pushdown, sort != null ? MongoDbPushdown.ToJson(sort) : null, null, clientSide, pagePushed, read, explain);
            return documents;
        }

        private static BsonDocument? BuildSort(QueryModel model, MongoDbCollectionSchema schema)
        {
            BsonDocument sort = new BsonDocument();
            foreach (QueryOrdering ordering in model.Orderings)
            {
                if (ordering.Key is not ColumnNode column || !ReferenceEquals(column.Source, model.Source)) return null;
                Type stored = schema.StoredType(column.Column);
                if (stored == typeof(byte[])) return null;
                string field = schema.Field(column.Column);
                if (sort.Contains(field)) continue;
                sort[field] = ordering.Descending ? -1 : 1;
            }

            if (!sort.Contains(MongoDbCollectionSchema.IdField)) sort[MongoDbCollectionSchema.IdField] = 1;
            return sort;
        }

        private async Task<string?> ExplainAsync(MongoDbCollectionSchema schema, BsonDocument filter, BsonDocument? sort, int? skip, int? limit, CancellationToken token)
        {
            BsonDocument find = new BsonDocument { { "find", schema.CollectionName }, { "filter", filter } };
            if (sort != null) find["sort"] = sort;
            if (skip.HasValue) find["skip"] = skip.Value;
            if (limit.HasValue) find["limit"] = limit.Value;
            find["collation"] = new BsonDocument("locale", "simple");
            try
            {
                BsonDocument result = await Database.RunCommandAsync<BsonDocument>(new BsonDocument { { "explain", find }, { "verbosity", "queryPlanner" } }, null, token).ConfigureAwait(false);
                BsonValue planner = result.GetValue("queryPlanner", BsonNull.Value);
                BsonValue winning = planner.IsBsonDocument ? planner.AsBsonDocument.GetValue("winningPlan", BsonNull.Value) : BsonNull.Value;
                return winning.IsBsonDocument ? MongoDbPushdown.ToJson(winning.AsBsonDocument) : null;
            }
            catch (MongoException e)
            {
                _Logger?.LogDebug(e, "MongoDB explain failed: {Message}", e.Message);
                return null;
            }
        }

        private async Task<int> UpdateCoreAsync(MongoDbCollectionSchema schema, QueryModel model, IReadOnlyList<FieldAssignment> assignments, IClientSessionHandle? session, CancellationToken token)
        {
            bool keyChanges = assignments.Any(a => a.Column.IsPrimaryKey);
            bool constants = assignments.All(a => a.Value is ValueNode);
            if (!keyChanges && constants && !model.Skip.HasValue && !model.Take.HasValue)
            {
                MongoDbPushdown pushdown = MongoDbPushdown.Translate(model.Source, model.Filter, schema, _Values);
                if (pushdown.Exact)
                {
                    BsonDocument set = new BsonDocument();
                    foreach (FieldAssignment assignment in assignments)
                        set[schema.Field(assignment.Column)] = MongoDbBsonCodec.Encode(_Values.ToStored(assignment.Column, ((ValueNode)assignment.Value).Value));
                    UpdateResult result = await UpdateManyAsync(Collection(schema), session, pushdown.Combined(), new BsonDocument("$set", set), token).ConfigureAwait(false);
                    Record("Update", schema, pushdown, null, null, null, true, -1, null);
                    return (int)result.MatchedCount;
                }
            }

            if (keyChanges) return await UpdateKeysAsync(schema, model, assignments, session, token).ConfigureAwait(false);
            return await ApplyPerDocumentAsync(schema, model, session, "Update", document => new BsonDocument("$set", ComputeAssignments(schema, model.Source, document, assignments, session)), token).ConfigureAwait(false);
        }

        private BsonDocument ComputeAssignments(MongoDbCollectionSchema schema, QuerySource source, BsonDocument document, IReadOnlyList<FieldAssignment> assignments, IClientSessionHandle? session)
        {
            MongoDbQueryEvaluator evaluator = new MongoDbQueryEvaluator(Database, session, _Values);
            evaluator.Bind(source, document);
            BsonDocument set = new BsonDocument();
            foreach (FieldAssignment assignment in assignments)
            {
                object? stored = assignment.Value is ValueNode constant
                    ? _Values.ToStored(assignment.Column, constant.Value)
                    : _Values.ComputedToStored(assignment.Column, evaluator.Visit(assignment.Value));
                set[schema.Field(assignment.Column)] = MongoDbBsonCodec.Encode(stored);
            }

            return set;
        }

        private async Task<int> ApplyPerDocumentAsync(MongoDbCollectionSchema schema, QueryModel model, IClientSessionHandle? session, string operation, Func<BsonDocument, BsonDocument>? update, CancellationToken token)
        {
            List<BsonDocument> matches = await ReadAsync(model, session, false, operation, null, token).ConfigureAwait(false);
            IMongoCollection<BsonDocument> collection = Collection(schema);
            int applied = 0;
            foreach (BsonDocument match in matches)
            {
                token.ThrowIfCancellationRequested();
                BsonDocument current = match;
                for (int attempt = 0; ; attempt++)
                {
                    BsonDocument filter = OriginalFilter(current);
                    long affected;
                    if (update != null)
                    {
                        UpdateOptions options = new UpdateOptions { Collation = Collation.Simple };
                        BsonDocument change = update(current);
                        UpdateResult result = session != null
                            ? await collection.UpdateOneAsync(session, filter, change, options, token).ConfigureAwait(false)
                            : await collection.UpdateOneAsync(filter, change, options, token).ConfigureAwait(false);
                        affected = result.MatchedCount;
                    }
                    else
                    {
                        DeleteOptions options = new DeleteOptions { Collation = Collation.Simple };
                        DeleteResult result = session != null
                            ? await collection.DeleteOneAsync(session, filter, options, token).ConfigureAwait(false)
                            : await collection.DeleteOneAsync(filter, options, token).ConfigureAwait(false);
                        affected = result.DeletedCount;
                    }

                    if (affected > 0)
                    {
                        applied++;
                        break;
                    }

                    // The document changed (or vanished) since it was read: re-read it and re-check the filter.
                    if (attempt >= _MaxRetries)
                        throw new InvalidOperationException(operation + " of " + schema.Metadata.EntityType.Name + " gave up after " + _MaxRetries.ToString(CultureInfo.InvariantCulture) + " concurrent modifications of document " + match[MongoDbCollectionSchema.IdField] + ".");
                    BsonDocument byId = new BsonDocument(MongoDbCollectionSchema.IdField, match[MongoDbCollectionSchema.IdField]);
                    FindOptions<BsonDocument> findOptions = new FindOptions<BsonDocument> { Collation = Collation.Simple, Limit = 1 };
                    IAsyncCursor<BsonDocument> cursor = session != null
                        ? await collection.FindAsync(session, byId, findOptions, token).ConfigureAwait(false)
                        : await collection.FindAsync(byId, findOptions, token).ConfigureAwait(false);
                    BsonDocument? reread = (await cursor.ToListAsync(token).ConfigureAwait(false)).FirstOrDefault();
                    if (reread == null) break;
                    if (model.Filter != null && !new MongoDbQueryEvaluator(Database, session, _Values).Bind(model.Source, reread).Test(model.Filter)) break;
                    current = reread;
                }
            }

            return applied;
        }

        private async Task<int> UpdateKeysAsync(MongoDbCollectionSchema schema, QueryModel model, IReadOnlyList<FieldAssignment> assignments, IClientSessionHandle? session, CancellationToken token)
        {
            EntityMetadata metadata = schema.Metadata;
            List<BsonDocument> matches = await ReadAsync(model, session, false, "Update", null, token).ConfigureAwait(false);
            List<BsonDocument> updated = new List<BsonDocument>(matches.Count);
            foreach (BsonDocument document in matches)
            {
                BsonDocument copy = (BsonDocument)document.DeepClone();
                foreach (BsonElement element in ComputeAssignments(schema, model.Source, document, assignments, session)) copy[element.Name] = element.Value;
                if (!schema.SingleKey)
                {
                    object?[] key = new object?[metadata.KeyColumns.Count];
                    for (int i = 0; i < key.Length; i++) key[i] = MongoDbBsonCodec.Decode(copy.GetValue(schema.Field(metadata.KeyColumns[i]), BsonNull.Value), schema.StoredType(metadata.KeyColumns[i]));
                    if (key.Any(k => k == null)) throw new InvalidOperationException("Cannot update " + metadata.EntityType.Name + ": a primary key cannot be set to null.");
                    copy[MongoDbCollectionSchema.IdField] = schema.Id(key);
                }

                if (copy[MongoDbCollectionSchema.IdField].IsBsonNull)
                    throw new InvalidOperationException("Cannot update " + metadata.EntityType.Name + ": a primary key cannot be set to null.");
                updated.Add(copy);
            }

            IMongoCollection<BsonDocument> collection = Collection(schema);
            HashSet<BsonValue> removed = new HashSet<BsonValue>();
            HashSet<BsonValue> added = new HashSet<BsonValue>();
            for (int i = 0; i < matches.Count; i++)
            {
                BsonValue oldId = matches[i][MongoDbCollectionSchema.IdField];
                if (!oldId.Equals(updated[i][MongoDbCollectionSchema.IdField])) removed.Add(oldId);
            }

            for (int i = 0; i < updated.Count; i++)
            {
                BsonValue newId = updated[i][MongoDbCollectionSchema.IdField];
                if (newId.Equals(matches[i][MongoDbCollectionSchema.IdField])) continue;
                bool exists = false;
                if (!removed.Contains(newId))
                {
                    BsonDocument byId = new BsonDocument(MongoDbCollectionSchema.IdField, newId);
                    exists = (session != null
                        ? await collection.CountDocumentsAsync(session, byId, new CountOptions { Limit = 1 }, token).ConfigureAwait(false)
                        : await collection.CountDocumentsAsync(byId, new CountOptions { Limit = 1 }, token).ConfigureAwait(false)) > 0;
                }

                if (!added.Add(newId) || exists)
                    throw new InvalidOperationException("Cannot update " + metadata.EntityType.Name + ": a row with primary key " + newId + " already exists in '" + metadata.TableName + "'.");
            }

            for (int i = 0; i < matches.Count; i++)
            {
                BsonValue oldId = matches[i][MongoDbCollectionSchema.IdField];
                if (removed.Contains(oldId))
                {
                    BsonDocument byId = new BsonDocument(MongoDbCollectionSchema.IdField, oldId);
                    if (session != null) await collection.DeleteOneAsync(session, byId, null, token).ConfigureAwait(false);
                    else await collection.DeleteOneAsync(byId, null, token).ConfigureAwait(false);
                }
            }

            for (int i = 0; i < updated.Count; i++)
            {
                BsonValue oldId = matches[i][MongoDbCollectionSchema.IdField];
                if (removed.Contains(oldId))
                {
                    if (session != null) await collection.InsertOneAsync(session, updated[i], null, token).ConfigureAwait(false);
                    else await collection.InsertOneAsync(updated[i], null, token).ConfigureAwait(false);
                }
                else
                {
                    BsonDocument byId = new BsonDocument(MongoDbCollectionSchema.IdField, oldId);
                    if (session != null) await collection.ReplaceOneAsync(session, byId, updated[i], (ReplaceOptions?)null, token).ConfigureAwait(false);
                    else await collection.ReplaceOneAsync(byId, updated[i], (ReplaceOptions?)null, token).ConfigureAwait(false);
                }
            }

            return matches.Count;
        }

        private static BsonDocument OriginalFilter(BsonDocument document)
        {
            BsonDocument filter = new BsonDocument();
            foreach (BsonElement element in document)
            {
                filter[element.Name] = element.Name == MongoDbCollectionSchema.IdField ? element.Value : new BsonDocument("$eq", element.Value);
            }

            return filter;
        }

        private static async Task<UpdateResult> UpdateManyAsync(IMongoCollection<BsonDocument> collection, IClientSessionHandle? session, BsonDocument filter, BsonDocument update, CancellationToken token)
        {
            UpdateOptions options = new UpdateOptions { Collation = Collation.Simple };
            return session != null
                ? await collection.UpdateManyAsync(session, filter, update, options, token).ConfigureAwait(false)
                : await collection.UpdateManyAsync(filter, update, options, token).ConfigureAwait(false);
        }

        private async Task<BsonValue> InsertGeneratedAsync(MongoDbCollectionSchema schema, IMongoCollection<BsonDocument> collection, BsonDocument document, IClientSessionHandle? session, CancellationToken token)
        {
            for (int attempt = 0; ; attempt++)
            {
                long next = await NextSequenceAsync(schema, token).ConfigureAwait(false);
                BsonValue key;
                if (schema.GeneratedKeyType == typeof(int))
                {
                    if (next > int.MaxValue) throw new InvalidOperationException("The auto-increment sequence of '" + schema.CollectionName + "' exceeded the range of a 32-bit key.");
                    key = new BsonInt32((int)next);
                }
                else
                {
                    key = new BsonInt64(next);
                }

                document[MongoDbCollectionSchema.IdField] = key;
                try
                {
                    if (session != null) await collection.InsertOneAsync(session, document, null, token).ConfigureAwait(false);
                    else await collection.InsertOneAsync(document, null, token).ConfigureAwait(false);
                    return key;
                }
                catch (MongoWriteException e) when (session == null && attempt < _MaxRetries && e.WriteError != null && e.WriteError.Category == ServerErrorCategory.DuplicateKey && e.WriteError.Message.Contains("_id_", StringComparison.Ordinal))
                {
                    // A row with this key already exists (for example data written without the sequence): move the
                    // sequence past the largest stored key and retry.
                    FindOptions<BsonDocument> options = new FindOptions<BsonDocument> { Sort = new BsonDocument(MongoDbCollectionSchema.IdField, -1), Limit = 1 };
                    BsonDocument? last = (await (await collection.FindAsync(new BsonDocument(), options, token).ConfigureAwait(false)).ToListAsync(token).ConfigureAwait(false)).FirstOrDefault();
                    if (last != null) await BumpSequenceAsync(schema, last[MongoDbCollectionSchema.IdField], token).ConfigureAwait(false);
                }
            }
        }

        private async Task<long> NextSequenceAsync(MongoDbCollectionSchema schema, CancellationToken token)
        {
            IMongoCollection<BsonDocument> sequences = Database.GetCollection<BsonDocument>(SequenceCollectionName);
            FindOneAndUpdateOptions<BsonDocument, BsonDocument> options = new FindOneAndUpdateOptions<BsonDocument, BsonDocument>
            {
                IsUpsert = true,
                ReturnDocument = ReturnDocument.After
            };
            BsonDocument? result = await sequences.FindOneAndUpdateAsync<BsonDocument>(
                new BsonDocument(MongoDbCollectionSchema.IdField, schema.CollectionName),
                new BsonDocument("$inc", new BsonDocument("value", 1L)),
                options,
                token).ConfigureAwait(false);
            if (result == null) throw new InvalidOperationException("The auto-increment sequence of '" + schema.CollectionName + "' could not be read.");
            return result["value"].ToInt64();
        }

        private async Task BumpSequenceAsync(MongoDbCollectionSchema schema, BsonValue key, CancellationToken token)
        {
            if (!key.IsNumeric) return;
            long value;
            try
            {
                value = key.ToInt64();
            }
            catch (OverflowException)
            {
                return;
            }

            if (value <= 0) return;
            IMongoCollection<BsonDocument> sequences = Database.GetCollection<BsonDocument>(SequenceCollectionName);
            await sequences.UpdateOneAsync(
                new BsonDocument(MongoDbCollectionSchema.IdField, schema.CollectionName),
                new BsonDocument("$max", new BsonDocument("value", value)),
                new UpdateOptions { IsUpsert = true },
                token).ConfigureAwait(false);
        }

        private BsonDocument ToDocument(MongoDbCollectionSchema schema, object entity, bool generate)
        {
            EntityMetadata metadata = schema.Metadata;
            BsonDocument document = new BsonDocument();
            object?[]? key = schema.SingleKey ? null : new object?[metadata.KeyColumns.Count];
            if (!schema.SingleKey || generate) document[MongoDbCollectionSchema.IdField] = BsonNull.Value;
            foreach (ColumnMetadata column in metadata.Columns)
            {
                object? stored = _Values.ToStored(column, column.GetValue(entity));
                string field = schema.Field(column);
                if (field == MongoDbCollectionSchema.IdField)
                {
                    if (generate) continue;
                    if (stored == null) throw new InvalidOperationException("Primary key column '" + column.Name + "' of " + metadata.EntityType.Name + " is null.");
                }

                document[field] = MongoDbBsonCodec.Encode(stored);
                if (key != null && column.IsPrimaryKey) key[IndexOfKey(metadata, column)] = stored;
            }

            if (key != null) document[MongoDbCollectionSchema.IdField] = schema.Id(key);
            return document;
        }

        private static int IndexOfKey(EntityMetadata metadata, ColumnMetadata column)
        {
            for (int i = 0; i < metadata.KeyColumns.Count; i++)
            {
                if (ReferenceEquals(metadata.KeyColumns[i], column)) return i;
            }

            throw new InvalidOperationException("Column '" + column.Name + "' is not a key column of " + metadata.EntityType.Name + ".");
        }

        private object Materialize(MongoDbCollectionSchema schema, BsonDocument document)
        {
            object entity = schema.Metadata.CreateInstance();
            foreach (ColumnMetadata column in schema.Metadata.Columns)
            {
                object? stored = MongoDbBsonCodec.Decode(document.GetValue(schema.Field(column), BsonNull.Value), schema.StoredType(column));
                column.SetValue(entity, _Values.FromStored(column, stored));
            }

            return entity;
        }

        private IMongoCollection<BsonDocument> Collection(MongoDbCollectionSchema schema)
        {
            return Database.GetCollection<BsonDocument>(schema.CollectionName);
        }

        private static void SortById(List<BsonDocument> documents)
        {
            if (documents.Count < 2) return;
            documents.Sort((a, b) => a.GetValue(MongoDbCollectionSchema.IdField, BsonNull.Value).CompareTo(b.GetValue(MongoDbCollectionSchema.IdField, BsonNull.Value)));
        }

        private void Record(string operation, MongoDbCollectionSchema schema, MongoDbPushdown pushdown, string? sort, string? pipeline, string? clientSide, bool pagingPushedDown, long read, string? explain)
        {
            MongoDbQueryPlan plan = new MongoDbQueryPlan(operation, schema.Metadata.EntityType, schema.CollectionName, pushdown.Describe(), sort, pipeline, pushdown.Exact, clientSide, pagingPushedDown, read, explain);
            _LastQueryPlan = plan;
            EventHandler<MongoDbQueryPlan>? handlers = QueryPlanned;
            if (handlers != null)
            {
                try
                {
                    handlers(this, plan);
                }
                catch (Exception e)
                {
                    _Logger?.LogWarning(e, "A MongoDB QueryPlanned handler threw: {Message}", e.Message);
                }
            }

            if (_Logger != null && _Logger.IsEnabled(LogLevel.Debug)) _Logger.LogDebug("MongoDB {Plan}", plan.ToString());
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

        private static InvalidOperationException DuplicateKey(EntityMetadata? metadata, string? key, Exception? inner)
        {
            string entity = metadata?.EntityType.Name ?? "entity";
            string table = metadata?.TableName ?? "?";
            string detail = inner?.Message ?? string.Empty;
            if (key != null)
                return new InvalidOperationException("Cannot insert " + entity + ": a row with primary key " + key + " already exists in '" + table + "'.", inner);
            return new InvalidOperationException("Cannot write " + entity + ": the write violates a unique key of '" + table + "' (" + detail + ").", inner);
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
                case ushort us: return us == 0;
                case sbyte sb: return sb == 0;
                default: return false;
            }
        }

        private void ThrowIfDisposed()
        {
            if (Volatile.Read(ref _Disposed) == 1) throw new ObjectDisposedException(nameof(MongoDbBackend));
        }

        #endregion
    }
}
