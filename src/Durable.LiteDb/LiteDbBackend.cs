namespace Durable.LiteDb
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
    using LiteDB;

    /// <summary>
    /// A LiteDB <see cref="IRepositoryBackend"/>: stores entities in an embedded single-file (or in-memory) LiteDB
    /// document database. Use it through <see cref="LiteDbRepository{T}"/> (or any <see cref="RepositoryBase{T}"/>); one
    /// backend wraps one <see cref="LiteDatabase"/> and serves every entity type, so includes, navigation predicates and
    /// transactions work across repositories that share it. Create one backend per data file and share it.
    /// <para>
    /// Storage: each entity type is a collection named <see cref="EntityMetadata.TableName"/>; each mapped column is a
    /// document field named after the column, built from Durable's <see cref="EntityMetadata"/> (LiteDB's reflection
    /// <c>BsonMapper</c> is never used for entities). A single primary key is the document <c>_id</c>; a composite key is
    /// stored as its fields plus an <c>_id</c> sub-document of the key parts. Values are stored the way a database driver
    /// would (<see cref="IValueConverter"/> provider values, JSON text for JSON columns, enum names or numbers) and encoded
    /// losslessly and so that LiteDB orders values like C#: integers as Int32/Int64, decimal (full precision and scale) and
    /// ulong as Decimal, float/double as Double, DateTime as the decimal <c>Ticks + Kind / 4</c> (every tick and the
    /// <see cref="DateTimeKind"/> survive; LiteDB's own date type keeps only milliseconds), DateTimeOffset as the decimal
    /// <c>UtcTicks + (offset minutes + 840) / 2000</c>, TimeSpan and TimeOnly as Int64 ticks, DateOnly as an Int32 day
    /// number, char as a one-character string, Guid, bool, string and byte[] natively. A field missing from a document
    /// (for example one written before a column was added) reads as null, which sets a non-nullable property to its default.
    /// Auto-increment keys (single integer primary key) come from LiteDB's per-collection sequence, which is atomic, so
    /// concurrent creates never share a key; inserting a duplicate primary key throws <see cref="InvalidOperationException"/>.
    /// Foreign key, composite key part and <see cref="IndexAttribute"/> fields get LiteDB indexes when a repository is
    /// created (strings only when their <see cref="ColumnMetadata.MaxLength"/> is at most 250, because LiteDB index keys
    /// are limited to 1023 bytes; indexes are never unique, so <see cref="IndexAttribute.IsUnique"/> is not enforced).
    /// LiteDB collection and field names are case-insensitive: two entities whose table names differ only in case share a
    /// collection.
    /// </para>
    /// <para>
    /// Queries: the parts of a filter whose LiteDB semantics are exactly C#'s (comparisons, null checks and IN lists between
    /// a column and a value, see <see cref="LiteDbQueryPlan"/>) are pushed down to LiteDB, where indexes apply; everything
    /// else (string matching, functions, navigations, ordering, grouping, aggregates) is evaluated client-side with C#
    /// semantics over the documents LiteDB returns, so push-down never changes results. Strings: new databases use an
    /// ordinal collation, so <see cref="StringMatchMode.Database"/> behaves as <see cref="StringMatchMode.Ordinal"/> (case-
    /// and accent-sensitive) and string keys are case-sensitive; for a database created with another collation (an
    /// existing file or a <see cref="LiteDatabase"/> you pass in), <see cref="HasOrdinalCollation"/> is false, only string
    /// equality is pushed down (re-checked client-side), and string primary keys follow that collation. Unordered queries
    /// return documents in primary key order.
    /// </para>
    /// <para>
    /// Writes outside a transaction are atomic: each runs in its own LiteDB transaction that takes the collection's write
    /// lock before reading, so conditional updates (optimistic concurrency) cannot lose updates. Transactions
    /// (<see cref="LiteDbTransaction"/>) run on a dedicated thread, so they work across awaits. A write outside a
    /// transaction waits (up to the database timeout) while an open transaction holds the collection's write lock; in
    /// <see cref="LiteDbConnectionType.Shared"/> mode an open transaction holds the data file's mutex, so every other
    /// operation on the file waits until it ends. LiteDB allows at most 100 concurrently open transactions per database,
    /// including the short read transactions of concurrent queries.
    /// </para>
    /// <para>
    /// Asynchrony: LiteDB is synchronous. Operations outside a transaction run synchronously on the calling thread and
    /// return completed tasks (like the in-memory backend); operations inside a transaction are queued to the transaction
    /// thread and complete asynchronously. Query results are read completely before the first entity is returned.
    /// </para>
    /// <para>
    /// Lifetime: create a backend with <see cref="Create"/> or <see cref="CreateAsync"/>, share it across repositories,
    /// and dispose it when done. It disposes the database only when it opened it (<see cref="OwnsDatabase"/>); a database
    /// passed in with <see cref="LiteDbRepositorySettings.Database"/> is never disposed. Repositories never dispose the
    /// backend.
    /// </para>
    /// Thread safety: safe for concurrent use by any number of repositories and threads; LiteDB serializes writers per
    /// collection and never blocks readers.
    /// </summary>
    public sealed class LiteDbBackend : IRepositoryBackend, IDisposable, IAsyncDisposable
    {
        #region Public-Members

        /// <summary>
        /// Gets the optional features the backend supports: all of them.
        /// </summary>
        public RepositoryCapabilities Capabilities => RepositoryCapabilities.All;

        /// <summary>
        /// Gets the LiteDB database. Never null.
        /// </summary>
        public LiteDatabase Database { get; }

        /// <summary>
        /// Gets whether the backend disposes <see cref="Database"/> when it is disposed (true when the backend opened it from
        /// settings, false when the database was passed in).
        /// </summary>
        public bool OwnsDatabase { get; }

        /// <summary>
        /// Gets whether the database compares strings ordinally (created by this backend, or otherwise with
        /// <see cref="Collation.Binary"/>). When false, only string equality is pushed down to LiteDB.
        /// </summary>
        public bool HasOrdinalCollation { get; }

        /// <summary>
        /// Gets the JSON options used to store JSON columns. Never null.
        /// Default: camelCase property names, not indented (the same as the SQL providers).
        /// </summary>
        public JsonSerializerOptions JsonOptions => _Values.JsonOptions;

        /// <summary>
        /// Gets or sets whether each query plan includes LiteDB's own plan (<see cref="LiteDbQueryPlan.LiteDbExplain"/>),
        /// which costs an extra optimizer pass per query.
        /// Default: false.
        /// </summary>
        public bool ExplainQueries { get; set; } = false;

        /// <summary>
        /// Gets the plan of the most recent read or write executed by any thread, or null before the first one. Use it
        /// in single-threaded diagnostics and tests; use <see cref="QueryPlanned"/> to observe every plan.
        /// </summary>
        public LiteDbQueryPlan? LastQueryPlan => _LastQueryPlan;

        /// <summary>
        /// Raised with the plan of every read and write, synchronously on the thread that executed it (inside a
        /// transaction, the transaction thread); the sender is the backend. Handlers must be thread-safe and fast and must
        /// not call the backend; an exception thrown by a handler is logged (Warning) and otherwise ignored.
        /// </summary>
        public event EventHandler<LiteDbQueryPlan>? QueryPlanned;

        #endregion

        #region Private-Members

        private static readonly JsonSerializerOptions _DefaultJsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };

        private static readonly BsonValue _LockProbe = new BsonValue(ObjectId.Empty);
        private readonly LiteDbValueConverter _Values;
        private readonly ILogger? _Logger;
        private readonly Collation _Collation;
        private readonly ConcurrentDictionary<string, bool> _Ensured = new ConcurrentDictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
        private volatile LiteDbQueryPlan? _LastQueryPlan;
        private int _Disposed;

        #endregion

        #region Constructors-and-Factories

        private LiteDbBackend(LiteDatabase database, bool ownsDatabase, LiteDbRepositorySettings settings)
        {
            Database = database;
            OwnsDatabase = ownsDatabase;
            _Values = new LiteDbValueConverter(settings.JsonOptions ?? _DefaultJsonOptions);
            _Logger = settings.Logger;
            _Collation = database.Collation;
            HasOrdinalCollation = _Collation.SortOptions == CompareOptions.Ordinal;
        }

        /// <summary>
        /// Creates a backend: wraps <see cref="LiteDbRepositorySettings.Database"/> when set (not owned, never disposed),
        /// otherwise opens (or creates) the database selected by <see cref="LiteDbRepositorySettings.Filename"/> (owned and
        /// disposed by the backend).
        /// </summary>
        /// <param name="settings">Settings; null uses <see cref="LiteDbRepositorySettings.ForInMemory"/>.</param>
        /// <returns>The backend. Never null. Dispose it when done.</returns>
        /// <exception cref="ArgumentException">Thrown when the settings are invalid (see <see cref="LiteDbRepositorySettings.Validate"/>).</exception>
        /// <exception cref="LiteException">Thrown when LiteDB cannot open the database (for example a wrong password or a file locked by another process).</exception>
        public static LiteDbBackend Create(LiteDbRepositorySettings? settings = null)
        {
            settings ??= LiteDbRepositorySettings.ForInMemory();
            settings.Validate();
            if (settings.Database != null) return new LiteDbBackend(settings.Database, false, settings);

            LiteDatabase database = new LiteDatabase(settings.ToConnectionString());
            try
            {
                if (!settings.ReadOnly || settings.IsInMemory)
                {
                    TimeSpan timeout = TimeSpan.FromSeconds(Math.Floor(settings.Timeout.TotalSeconds));
                    if (database.Timeout != timeout) database.Timeout = timeout;
                }

                return new LiteDbBackend(database, true, settings);
            }
            catch (Exception)
            {
                database.Dispose();
                throw;
            }
        }

        /// <summary>
        /// Creates a backend (see <see cref="Create"/>). LiteDB opens databases synchronously, so this completes
        /// synchronously; present so every backend has the same factories.
        /// </summary>
        /// <param name="settings">Settings; null uses <see cref="LiteDbRepositorySettings.ForInMemory"/>.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The backend. Never null. Dispose it when done.</returns>
        /// <exception cref="ArgumentException">Thrown when the settings are invalid (see <see cref="LiteDbRepositorySettings.Validate"/>).</exception>
        /// <exception cref="LiteException">Thrown when LiteDB cannot open the database.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the token is canceled.</exception>
        public static Task<LiteDbBackend> CreateAsync(LiteDbRepositorySettings? settings = null, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            return Task.FromResult(Create(settings));
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
        /// <exception cref="NotSupportedException">Thrown when the entity cannot be stored in LiteDB (see <see cref="LiteDbRepository{T}"/>).</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        public LiteDbRepository<T> CreateRepository<[DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] T>(RepositoryOptions? options = null) where T : class, new()
        {
            ThrowIfDisposed();
            return new LiteDbRepository<T>(this, options);
        }

        /// <summary>
        /// Determines whether a transaction was created over this backend's database.
        /// </summary>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>True when the transaction belongs to this backend's database.</returns>
        public bool Owns(ITransaction? transaction)
        {
            return transaction is LiteDbTransaction liteDb && ReferenceEquals(liteDb.Backend.Database, Database);
        }

        /// <summary>
        /// Creates the LiteDB indexes of an entity (foreign keys, composite key parts and <see cref="IndexAttribute"/>
        /// columns) if they do not exist yet. Repositories call it when they are created; failures are logged, not thrown,
        /// because indexes only affect performance.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        /// <exception cref="NotSupportedException">Thrown when the entity cannot be stored in LiteDB.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        public void EnsureIndexes([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            ThrowIfDisposed();
            EnsureIndexes(LiteDbCollectionSchema.For(EntityMetadata.For(entityType)));
        }

        /// <summary>
        /// Creates the LiteDB indexes of an entity if they do not exist yet (see <see cref="EnsureIndexes(Type)"/>). LiteDB is
        /// synchronous, so this completes synchronously.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A completed task.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        /// <exception cref="NotSupportedException">Thrown when the entity cannot be stored in LiteDB.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the token is canceled.</exception>
        public Task EnsureIndexesAsync([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            EnsureIndexes(entityType);
            return Task.CompletedTask;
        }

        /// <summary>
        /// Returns the stored documents of an entity, in primary key order, for diagnostics and tests that check the stored
        /// representation. Soft-deleted documents are included.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <returns>Copies of the documents. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        public IReadOnlyList<BsonDocument> GetStoredRows([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            ThrowIfDisposed();
            LiteDbCollectionSchema schema = LiteDbCollectionSchema.For(EntityMetadata.For(entityType));
            List<BsonDocument> documents = Database.GetCollection(schema.CollectionName).FindAll().ToList();
            SortById(documents);
            return documents;
        }

        /// <summary>
        /// Returns the stored documents of an entity (see <see cref="GetStoredRows"/>). LiteDB is synchronous, so this
        /// completes synchronously.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Copies of the documents in primary key order. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the token is canceled.</exception>
        public Task<IReadOnlyList<BsonDocument>> GetStoredRowsAsync([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            return Task.FromResult(GetStoredRows(entityType));
        }

        /// <summary>
        /// Drops every collection of the database (all data, indexes and auto-increment sequences), bypassing soft delete,
        /// query filters and transactions. Must not be called while a transaction is open.
        /// </summary>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        public void Clear()
        {
            ThrowIfDisposed();
            foreach (string name in Database.GetCollectionNames().ToList()) Database.DropCollection(name);
            _Ensured.Clear();
        }

        /// <summary>
        /// Drops every collection of the database (see <see cref="Clear()"/>). LiteDB is synchronous, so this completes
        /// synchronously.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A completed task.</returns>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the token is canceled.</exception>
        public Task ClearAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            Clear();
            return Task.CompletedTask;
        }

        /// <summary>
        /// Removes every document of one entity type (including soft-deleted rows) by dropping its collection, which also
        /// drops its indexes (recreated by the next repository or insert) and resets its auto-increment sequence. Bypasses
        /// soft delete, query filters and transactions; must not be called while a transaction is open.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <returns>The number of documents removed.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        /// <exception cref="NotSupportedException">Thrown when the entity cannot be stored in LiteDB.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        public int Clear([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            ThrowIfDisposed();
            LiteDbCollectionSchema schema = LiteDbCollectionSchema.For(EntityMetadata.For(entityType));
            if (!Database.CollectionExists(schema.CollectionName)) return 0;
            int count = Database.GetCollection(schema.CollectionName).Count();
            Database.DropCollection(schema.CollectionName);
            _Ensured.TryRemove(schema.CollectionName, out bool _);
            return count;
        }

        /// <summary>
        /// Removes every document of one entity type (see <see cref="Clear(Type)"/>). LiteDB is synchronous, so this
        /// completes synchronously.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The number of documents removed.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        /// <exception cref="NotSupportedException">Thrown when the entity cannot be stored in LiteDB.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the token is canceled.</exception>
        public Task<int> ClearAsync([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            return Task.FromResult(Clear(entityType));
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
            return ExecuteAsync(model.Transaction, null, () => Count(model), token);
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Thrown when <paramref name="function"/> is Count or Any.</exception>
        public Task<object?> AggregateAsync(QueryModel model, AggregateFunction function, QueryNode operand, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(model);
            ArgumentNullException.ThrowIfNull(operand);
            return ExecuteAsync(model.Transaction, null, () =>
            {
                List<BsonDocument> rows = Read(model, true, "Aggregate");
                object? result = new LiteDbQueryEvaluator(Database, _Values).Aggregate(model.Source, rows, function, operand);
                if (result != null && operand is ColumnNode column && (function == AggregateFunction.Min || function == AggregateFunction.Max))
                    result = _Values.FromStored(column.Column, result);
                return result;
            }, token);
        }

        /// <inheritdoc />
        public async Task InsertAsync(EntityMetadata metadata, object entity, ITransaction? transaction, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(metadata);
            ArgumentNullException.ThrowIfNull(entity);
            ThrowIfDisposed();
            token.ThrowIfCancellationRequested();

            LiteDbCollectionSchema schema = LiteDbCollectionSchema.For(metadata);
            ColumnMetadata? generated = metadata.AutoIncrementColumn;
            bool generate = generated != null && IsUnsetGenerated(_Values.ToStored(generated, generated.GetValue(entity)));
            BsonDocument document = ToDocument(schema, entity, generate);
            if (transaction == null) EnsureIndexes(schema);

            BsonValue id = await ExecuteAsync(transaction, schema.CollectionName, () =>
            {
                ILiteCollection<BsonDocument> collection = Database.GetCollection(schema.CollectionName, schema.AutoId ?? BsonAutoId.ObjectId);
                if (generate) return collection.Insert(document);

                BsonValue key = document[LiteDbCollectionSchema.IdField];
                if (collection.FindById(key) != null) throw DuplicateKey(metadata, key, null);
                try
                {
                    collection.Insert(document);
                }
                catch (LiteException e) when (e.ErrorCode == LiteException.INDEX_DUPLICATE_KEY)
                {
                    throw DuplicateKey(metadata, key, e);
                }

                return key;
            }, token).ConfigureAwait(false);

            if (generate) generated!.SetValue(entity, _Values.FromStored(generated, LiteDbBsonCodec.Decode(id, schema.StoredType(generated))));
        }

        /// <inheritdoc />
        public Task<int> ReplaceAsync(EntityMetadata metadata, object entity, QueryNode condition, QuerySource source, ITransaction? transaction, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(metadata);
            ArgumentNullException.ThrowIfNull(entity);
            ArgumentNullException.ThrowIfNull(condition);
            ArgumentNullException.ThrowIfNull(source);
            LiteDbCollectionSchema schema = LiteDbCollectionSchema.For(metadata);

            List<KeyValuePair<string, BsonValue>> replacement = new List<KeyValuePair<string, BsonValue>>();
            foreach (ColumnMetadata column in metadata.Columns)
            {
                if (column.IsPrimaryKey || column.IsAutoIncrement) continue;
                replacement.Add(new KeyValuePair<string, BsonValue>(schema.Field(column), LiteDbBsonCodec.Encode(_Values.ToStored(column, column.GetValue(entity)))));
            }

            return ExecuteAsync(transaction, schema.CollectionName, () =>
            {
                List<BsonDocument> matches = Read(new QueryModel(source) { Filter = condition }, false, "Replace");
                ILiteCollection<BsonDocument> collection = Database.GetCollection(schema.CollectionName);
                foreach (BsonDocument document in matches)
                {
                    foreach (KeyValuePair<string, BsonValue> field in replacement) document[field.Key] = field.Value;
                    collection.Update(document);
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
            LiteDbCollectionSchema schema = LiteDbCollectionSchema.For(model.Metadata);
            return ExecuteAsync(model.Transaction, schema.CollectionName, () => Update(schema, model, assignments), token);
        }

        /// <inheritdoc />
        public Task<int> DeleteAsync(QueryModel model, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(model);
            LiteDbCollectionSchema schema = LiteDbCollectionSchema.For(model.Metadata);
            return ExecuteAsync(model.Transaction, schema.CollectionName, () =>
            {
                ILiteCollection<BsonDocument> collection = Database.GetCollection(schema.CollectionName);
                LiteDbPushdown pushdown = LiteDbPushdown.Translate(model.Source, model.Filter, schema, _Values, HasOrdinalCollation);
                if (pushdown.Exact)
                {
                    BsonExpression? predicate = pushdown.Combined();
                    int deleted = predicate == null ? collection.DeleteAll() : collection.DeleteMany(predicate);
                    Record("Delete", schema, pushdown, null, true, -1, null);
                    return deleted;
                }

                List<BsonDocument> matches = Read(model, false, "Delete");
                foreach (BsonDocument document in matches) collection.Delete(document[LiteDbCollectionSchema.IdField]);
                return matches.Count;
            }, token);
        }

        /// <summary>
        /// Begins a transaction (see <see cref="LiteDbTransaction"/>). Blocks the calling thread until the transaction
        /// thread has begun the LiteDB transaction; prefer <see cref="BeginTransactionAsync"/> in asynchronous code.
        /// </summary>
        /// <returns>The transaction. Never null. Dispose it; an uncommitted transaction rolls back on dispose.</returns>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        /// <exception cref="LiteException">Thrown when LiteDB cannot begin a transaction (for example 100 are already open).</exception>
        public LiteDbTransaction BeginTransaction()
        {
            ThrowIfDisposed();
            return LiteDbTransaction.BeginAsync(this, Database, CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Begins a transaction (see <see cref="LiteDbTransaction"/>).
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The transaction. Never null. Dispose it; an uncommitted transaction rolls back on dispose.</returns>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        /// <exception cref="LiteException">Thrown when LiteDB cannot begin a transaction (for example 100 are already open).</exception>
        /// <exception cref="OperationCanceledException">Thrown when the token is canceled.</exception>
        public Task<LiteDbTransaction> BeginTransactionAsync(CancellationToken token = default)
        {
            ThrowIfDisposed();
            return LiteDbTransaction.BeginAsync(this, Database, token);
        }

        /// <inheritdoc />
        async Task<ITransaction> IRepositoryBackend.BeginTransactionAsync(CancellationToken token)
        {
            return await BeginTransactionAsync(token).ConfigureAwait(false);
        }

        /// <summary>
        /// Disposes the backend and, when <see cref="OwnsDatabase"/> is true, the database. Repositories over the backend can
        /// no longer be used; their operations throw <see cref="ObjectDisposedException"/>. Safe to call more than once.
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _Disposed, 1) == 1) return;
            if (OwnsDatabase) Database.Dispose();
        }

        /// <summary>
        /// Disposes the backend (see <see cref="Dispose"/>). LiteDB closes synchronously, so this completes synchronously.
        /// </summary>
        /// <returns>A completed task.</returns>
        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }

        #endregion

        #region Private-Methods

        internal void EnsureIndexes(LiteDbCollectionSchema schema)
        {
            if (_Ensured.ContainsKey(schema.CollectionName)) return;
            ILiteCollection<BsonDocument> collection = Database.GetCollection(schema.CollectionName);
            bool complete = true;
            foreach (KeyValuePair<ColumnMetadata, string> index in schema.IndexedColumns)
            {
                try
                {
                    collection.EnsureIndex(index.Value, BsonExpression.Create(schema.Path(index.Key)), false);
                }
                catch (LiteException e)
                {
                    complete = false;
                    _Logger?.LogWarning(e, "LiteDB index {Index} on {Collection}.{Column} could not be created: {Message}", index.Value, schema.CollectionName, index.Key.Name, e.Message);
                }
            }

            if (complete) _Ensured[schema.CollectionName] = true;
        }

        private async IAsyncEnumerable<object> StreamAsync(QueryModel model, CancellationToken queryToken, [EnumeratorCancellation] CancellationToken enumerationToken = default)
        {
            List<object> entities = await ExecuteAsync(model.Transaction, null, () =>
            {
                List<BsonDocument> rows = Read(model, true, "Query");
                LiteDbCollectionSchema schema = LiteDbCollectionSchema.For(model.Metadata);
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

        private Task<TResult> ExecuteAsync<TResult>(ITransaction? transaction, string? writeCollection, Func<TResult> work, CancellationToken token)
        {
            ThrowIfDisposed();
            token.ThrowIfCancellationRequested();
            LiteDbTransaction? liteDb = ResolveTransaction(transaction);
            if (liteDb != null)
            {
                return liteDb.RunAsync(() =>
                {
                    if (writeCollection != null) LockCollection(writeCollection);
                    return work();
                }, token);
            }

            if (writeCollection == null) return Task.FromResult(work());
            return Task.FromResult(WriteAtomically(writeCollection, work));
        }

        private TResult WriteAtomically<TResult>(string collection, Func<TResult> work)
        {
            bool began = Database.BeginTrans();
            try
            {
                LockCollection(collection);
                TResult result = work();
                if (began) Database.Commit();
                return result;
            }
            catch (Exception)
            {
                if (began)
                {
                    try
                    {
                        Database.Rollback();
                    }
                    catch (LiteException)
                    {
                    }
                }

                throw;
            }
        }

        private void LockCollection(string collection)
        {
            // Deleting an _id that cannot exist takes the collection's write lock (held until the transaction ends) without
            // changing anything, so the reads that follow see the latest committed data and no other writer can interleave.
            Database.GetCollection(collection).Delete(_LockProbe);
        }

        private LiteDbTransaction? ResolveTransaction(ITransaction? transaction)
        {
            if (transaction == null) return null;
            if (transaction is not LiteDbTransaction liteDb || !ReferenceEquals(liteDb.Backend.Database, Database))
                throw new ArgumentException("The transaction was not created over this LiteDB database.", nameof(transaction));
            if (liteDb.IsCompleted) throw new InvalidOperationException("The transaction has already completed.");
            return liteDb;
        }

        private long Count(QueryModel model)
        {
            LiteDbCollectionSchema schema = LiteDbCollectionSchema.For(model.Metadata);
            LiteDbPushdown pushdown = LiteDbPushdown.Translate(model.Source, model.Filter, schema, _Values, HasOrdinalCollation);
            if (!pushdown.Exact) return Read(model, true, "Count", pushdown).Count;

            ILiteCollection<BsonDocument> collection = Database.GetCollection(schema.CollectionName);
            BsonExpression? predicate = pushdown.Combined();
            long count = predicate == null ? collection.LongCount() : collection.LongCount(predicate);
            if (model.Skip.HasValue) count = Math.Max(0, count - model.Skip.Value);
            if (model.Take.HasValue) count = Math.Min(count, model.Take.Value);
            Record("Count", schema, pushdown, null, true, -1, null);
            return count;
        }

        private List<BsonDocument> Read(QueryModel model, bool orderingAndPaging, string operation, LiteDbPushdown? translated = null)
        {
            LiteDbCollectionSchema schema = LiteDbCollectionSchema.For(model.Metadata);
            LiteDbPushdown pushdown = translated ?? LiteDbPushdown.Translate(model.Source, model.Filter, schema, _Values, HasOrdinalCollation);
            bool paged = orderingAndPaging && (model.Skip.HasValue || model.Take.HasValue);
            bool pagePushed = paged && pushdown.Exact && model.Orderings.Count == 0;

            ILiteQueryable<BsonDocument> query = Database.GetCollection(schema.CollectionName).Query();
            BsonExpression? predicate = pushdown.Combined();
            if (predicate != null) query = query.Where(predicate);
            ILiteQueryableResult<BsonDocument> result = query;
            if (pagePushed)
            {
                result = query.OrderBy(BsonExpression.Create("$._id"));
                if (model.Skip.HasValue) result = result.Skip(model.Skip.Value);
                if (model.Take.HasValue) result = result.Limit(model.Take.Value);
            }

            string? explain = ExplainQueries ? LiteDB.JsonSerializer.Serialize(result.GetPlan()) : null;
            List<BsonDocument> documents = result.ToDocuments().ToList();
            int read = documents.Count;
            if (!pagePushed) SortById(documents);

            string? clientSide = null;
            bool needsOrdering = orderingAndPaging && model.Orderings.Count > 0;
            if (!pushdown.Exact || needsOrdering || (paged && !pagePushed))
            {
                QueryModel residual = new QueryModel(model.Source)
                {
                    Filter = pushdown.Exact ? null : model.Filter,
                    Skip = orderingAndPaging ? model.Skip : null,
                    Take = orderingAndPaging ? model.Take : null
                };
                if (orderingAndPaging) residual.Orderings.AddRange(model.Orderings);
                documents = new LiteDbQueryEvaluator(Database, _Values).Apply(residual, documents);
                clientSide = DescribeClientSide(residual);
            }

            Record(operation, schema, pushdown, clientSide, pagePushed, read, explain);
            return documents;
        }

        private int Update(LiteDbCollectionSchema schema, QueryModel model, IReadOnlyList<FieldAssignment> assignments)
        {
            EntityMetadata metadata = schema.Metadata;
            List<BsonDocument> matches = Read(model, false, "Update");
            LiteDbQueryEvaluator evaluator = new LiteDbQueryEvaluator(Database, _Values);
            bool keyChanges = assignments.Any(a => a.Column.IsPrimaryKey);
            List<BsonDocument> updated = new List<BsonDocument>(matches.Count);
            foreach (BsonDocument document in matches)
            {
                evaluator.Bind(model.Source, document);
                List<KeyValuePair<string, BsonValue>> values = new List<KeyValuePair<string, BsonValue>>(assignments.Count);
                foreach (FieldAssignment assignment in assignments)
                {
                    object? stored = assignment.Value is ValueNode constant
                        ? _Values.ToStored(assignment.Column, constant.Value)
                        : _Values.ComputedToStored(assignment.Column, evaluator.Visit(assignment.Value));
                    values.Add(new KeyValuePair<string, BsonValue>(schema.Field(assignment.Column), LiteDbBsonCodec.Encode(stored)));
                }

                BsonDocument copy = new BsonDocument();
                foreach (KeyValuePair<string, BsonValue> field in document) copy[field.Key] = field.Value;
                foreach (KeyValuePair<string, BsonValue> field in values) copy[field.Key] = field.Value;
                if (keyChanges && !schema.SingleKey)
                {
                    object?[] key = new object?[metadata.KeyColumns.Count];
                    for (int i = 0; i < key.Length; i++) key[i] = LiteDbBsonCodec.Decode(copy[schema.Field(metadata.KeyColumns[i])], schema.StoredType(metadata.KeyColumns[i]));
                    copy[LiteDbCollectionSchema.IdField] = schema.Id(key);
                }

                if (copy[LiteDbCollectionSchema.IdField].IsNull)
                    throw new InvalidOperationException("Cannot update " + metadata.EntityType.Name + ": a primary key cannot be set to null.");
                updated.Add(copy);
            }

            ILiteCollection<BsonDocument> collection = Database.GetCollection(schema.CollectionName);
            if (!keyChanges)
            {
                foreach (BsonDocument document in updated) collection.Update(document);
                return matches.Count;
            }

            HashSet<BsonValue> removed = new HashSet<BsonValue>();
            HashSet<BsonValue> added = new HashSet<BsonValue>();
            for (int i = 0; i < matches.Count; i++)
            {
                BsonValue oldId = matches[i][LiteDbCollectionSchema.IdField];
                if (!oldId.Equals(updated[i][LiteDbCollectionSchema.IdField])) removed.Add(oldId);
            }

            for (int i = 0; i < updated.Count; i++)
            {
                BsonValue newId = updated[i][LiteDbCollectionSchema.IdField];
                if (newId.Equals(matches[i][LiteDbCollectionSchema.IdField])) continue;
                if (!added.Add(newId) || (!removed.Contains(newId) && collection.FindById(newId) != null))
                    throw new InvalidOperationException("Cannot update " + metadata.EntityType.Name + ": a row with primary key " + newId + " already exists in '" + metadata.TableName + "'.");
            }

            foreach (BsonValue id in removed) collection.Delete(id);
            for (int i = 0; i < updated.Count; i++)
            {
                if (removed.Contains(matches[i][LiteDbCollectionSchema.IdField])) collection.Insert(updated[i]);
                else collection.Update(updated[i]);
            }

            return matches.Count;
        }

        private BsonDocument ToDocument(LiteDbCollectionSchema schema, object entity, bool generate)
        {
            EntityMetadata metadata = schema.Metadata;
            BsonDocument document = new BsonDocument();
            object?[]? key = schema.SingleKey ? null : new object?[metadata.KeyColumns.Count];
            foreach (ColumnMetadata column in metadata.Columns)
            {
                object? stored = _Values.ToStored(column, column.GetValue(entity));
                string field = schema.Field(column);
                if (field == LiteDbCollectionSchema.IdField)
                {
                    if (generate) continue;
                    if (stored == null) throw new InvalidOperationException("Primary key column '" + column.Name + "' of " + metadata.EntityType.Name + " is null.");
                }

                document[field] = LiteDbBsonCodec.Encode(stored);
                if (key != null && column.IsPrimaryKey) key[IndexOfKey(metadata, column)] = stored;
            }

            if (key != null) document[LiteDbCollectionSchema.IdField] = schema.Id(key);
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

        private object Materialize(LiteDbCollectionSchema schema, BsonDocument document)
        {
            object entity = schema.Metadata.CreateInstance();
            foreach (ColumnMetadata column in schema.Metadata.Columns)
            {
                object? stored = LiteDbBsonCodec.Decode(document[schema.Field(column)], schema.StoredType(column));
                column.SetValue(entity, _Values.FromStored(column, stored));
            }

            return entity;
        }

        private void SortById(List<BsonDocument> documents)
        {
            if (documents.Count < 2) return;
            Collation collation = _Collation;
            documents.Sort((a, b) => a[LiteDbCollectionSchema.IdField].CompareTo(b[LiteDbCollectionSchema.IdField], collation));
        }

        private void Record(string operation, LiteDbCollectionSchema schema, LiteDbPushdown pushdown, string? clientSide, bool pagingPushedDown, long read, string? explain)
        {
            LiteDbQueryPlan plan = new LiteDbQueryPlan(operation, schema.Metadata.EntityType, schema.CollectionName, pushdown.Predicates.ToList(), pushdown.ParametersJson(), pushdown.Exact, clientSide, pagingPushedDown, read, explain);
            _LastQueryPlan = plan;
            EventHandler<LiteDbQueryPlan>? handlers = QueryPlanned;
            if (handlers != null)
            {
                try
                {
                    handlers(this, plan);
                }
                catch (Exception e)
                {
                    _Logger?.LogWarning(e, "A LiteDB QueryPlanned handler threw: {Message}", e.Message);
                }
            }

            if (_Logger != null && _Logger.IsEnabled(LogLevel.Debug)) _Logger.LogDebug("LiteDB {Plan}", plan.ToString());
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

        private static InvalidOperationException DuplicateKey(EntityMetadata metadata, BsonValue key, Exception? inner)
        {
            return new InvalidOperationException("Cannot insert " + metadata.EntityType.Name + ": a row with primary key " + key + " already exists in '" + metadata.TableName + "'.", inner);
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
            if (Volatile.Read(ref _Disposed) == 1) throw new ObjectDisposedException(nameof(LiteDbBackend));
        }

        #endregion
    }
}
