namespace Durable.InMemory
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Text.Json;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Query;

    /// <summary>
    /// An in-memory <see cref="IRepositoryBackend"/>: the reference implementation of the backend contract and a fast,
    /// dependency-free store for tests and prototypes. Use it through <see cref="InMemoryRepository{T}"/> (or any
    /// <see cref="RepositoryBase{T}"/>); one backend holds the tables of every entity type, so includes and navigation
    /// predicates work across repositories that share it.
    /// <para>
    /// Storage: rows are copies of mapped column values, never caller instances and never navigation properties, so
    /// changing an entity after Create or after a read does not change stored data. Values are stored as a database driver
    /// would store them: <see cref="IValueConverter"/> columns store the provider value, JSON columns store serialized text
    /// (<see cref="JsonOptions"/>), enums store their name (or number with <see cref="Flags.Integer"/>), byte arrays are
    /// copied. Decimals keep full precision (no column scale is applied), and <see cref="DateTime"/> values keep their
    /// <see cref="DateTimeKind"/> (SQL providers may return Unspecified). Auto-increment keys come from a per-table sequence
    /// that is never reused (rolled-back inserts leave gaps); inserting a duplicate primary key throws
    /// <see cref="InvalidOperationException"/>. Primary keys compare ordinally (string keys are case-sensitive). Rows are
    /// read in insertion order unless the query orders them.
    /// </para>
    /// <para>
    /// Queries: <see cref="QueryNode"/> filters are evaluated with C# semantics: null equals only null, a comparison or
    /// string match involving null is false and its negation true, and arithmetic and functions on null yield null.
    /// <see cref="StringMatchMode.Database"/> behaves as <see cref="StringMatchMode.Ordinal"/> (case- and accent-sensitive),
    /// <see cref="StringMatchMode.IgnoreCase"/> as <see cref="StringComparison.OrdinalIgnoreCase"/>. Values are compared in
    /// their stored form (enum names, converter provider values, JSON text), as a database compares columns with converted
    /// parameters. Functions follow SQL where C# would throw: Substring clamps out-of-range arguments, Replace with an empty
    /// search string returns its input, and Math.Round rounds midpoints away from zero. Navigation members read the related
    /// row by key; collection Any/All/Count read related rows (through the junction for many-to-many) excluding
    /// soft-deleted ones. Ordering is ordinal for strings with nulls first, and stable (ties keep insertion order).
    /// </para>
    /// <para>
    /// Transactions use snapshot isolation (see <see cref="InMemoryTransaction"/>): a transaction reads the data committed
    /// when it began plus its own writes; operations outside it read committed data only and are never blocked by it; its
    /// writes are published atomically on commit; a commit fails with <see cref="InvalidOperationException"/> when another
    /// writer changed one of the rows it wrote after it began (first committer wins), and the transaction is rolled back.
    /// Each operation outside a transaction is atomic and immediately committed.
    /// </para>
    /// <para>
    /// Capabilities: the constructor accepts a <see cref="RepositoryCapabilities"/> mask (default
    /// <see cref="RepositoryCapabilities.All"/>) so tests can simulate a limited backend; repositories then reject the
    /// masked features with <see cref="NotSupportedException"/>.
    /// </para>
    /// Thread safety: safe for concurrent use by any number of repositories and threads. Reads never take locks; writes
    /// outside transactions are serialized by a short internal lock.
    /// </summary>
    public sealed class InMemoryBackend : IRepositoryBackend
    {
        #region Public-Members

        /// <inheritdoc />
        public RepositoryCapabilities Capabilities { get; }

        /// <summary>
        /// Gets the JSON options used to store JSON columns. Never null.
        /// Default: camelCase property names, not indented (the same as the SQL providers).
        /// </summary>
        public JsonSerializerOptions JsonOptions => _Values.JsonOptions;

        #endregion

        #region Private-Members

        private static readonly JsonSerializerOptions _DefaultJsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };

        private readonly object _CommitLock = new object();
        private readonly ConcurrentDictionary<Type, InMemoryIdentitySequence> _Identities = new ConcurrentDictionary<Type, InMemoryIdentitySequence>();
        private readonly InMemoryValueConverter _Values;
        private volatile InMemoryDatabaseState _Committed = InMemoryDatabaseState.Empty;
        private long _Sequence;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates an empty in-memory backend.
        /// </summary>
        /// <param name="capabilities">Capabilities to advertise. Default: <see cref="RepositoryCapabilities.All"/>.</param>
        /// <param name="jsonOptions">JSON options for JSON columns; null uses camelCase, non-indented output.</param>
        public InMemoryBackend(RepositoryCapabilities capabilities = RepositoryCapabilities.All, JsonSerializerOptions? jsonOptions = null)
        {
            Capabilities = capabilities;
            _Values = new InMemoryValueConverter(jsonOptions ?? _DefaultJsonOptions);
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
        /// <exception cref="NotSupportedException">Thrown when the entity needs a capability this backend does not advertise.</exception>
        public InMemoryRepository<T> CreateRepository<T>(RepositoryOptions? options = null) where T : class, new()
        {
            return new InMemoryRepository<T>(this, options);
        }

        /// <summary>
        /// Determines whether a transaction was created by this backend.
        /// </summary>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>True when the transaction belongs to this backend.</returns>
        public bool Owns(ITransaction? transaction)
        {
            return transaction is InMemoryTransaction inMemory && ReferenceEquals(inMemory.Backend, this);
        }

        /// <summary>
        /// Removes every row of every table and resets the auto-increment sequences. Open transactions keep their snapshots.
        /// </summary>
        public void Clear()
        {
            lock (_CommitLock)
            {
                _Committed = InMemoryDatabaseState.Empty;
                _Identities.Clear();
            }
        }

        /// <summary>
        /// Returns the committed rows of an entity as stored (column name to stored value), for diagnostics and tests that
        /// check the stored representation (converter provider values, JSON text, enum names). Soft-deleted rows are included.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <returns>The rows in insertion order. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        public IReadOnlyList<IReadOnlyDictionary<string, object?>> GetStoredRows(Type entityType)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            EntityMetadata metadata = EntityMetadata.For(entityType);
            List<IReadOnlyDictionary<string, object?>> rows = new List<IReadOnlyDictionary<string, object?>>();
            foreach (InMemoryRow row in _Committed.Table(metadata).Rows)
            {
                Dictionary<string, object?> values = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                for (int i = 0; i < metadata.Columns.Count; i++) values[metadata.Columns[i].Name] = row.Values[i] is byte[] bytes ? bytes.Clone() : row.Values[i];
                rows.Add(values);
            }

            return rows;
        }

        /// <inheritdoc />
        public IAsyncEnumerable<object> QueryAsync(QueryModel model, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(model);
            token.ThrowIfCancellationRequested();
            InMemoryDatabaseState state = ReadState(model.Transaction);
            List<InMemoryRow> rows = Select(state, model, true);
            EntityMetadata metadata = model.Metadata;
            return new InMemoryResultStream(rows, row => Materialize(metadata, row), token);
        }

        /// <inheritdoc />
        public Task<long> CountAsync(QueryModel model, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(model);
            token.ThrowIfCancellationRequested();
            return Task.FromResult((long)Select(ReadState(model.Transaction), model, true).Count);
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Thrown when <paramref name="function"/> is Count or Any.</exception>
        public Task<object?> AggregateAsync(QueryModel model, AggregateFunction function, QueryNode operand, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(model);
            ArgumentNullException.ThrowIfNull(operand);
            token.ThrowIfCancellationRequested();
            InMemoryDatabaseState state = ReadState(model.Transaction);
            List<InMemoryRow> rows = Select(state, model, true);
            InMemoryQueryEvaluator evaluator = new InMemoryQueryEvaluator(state, _Values);
            List<object?> values = new List<object?>(rows.Count);
            foreach (InMemoryRow row in rows) values.Add(evaluator.Bind(model.Source, row).Visit(operand));

            object? result = QueryAggregates.Compute(function, values, QueryValueComparer.Ordinal);
            if (result != null && operand is ColumnNode column && (function == AggregateFunction.Min || function == AggregateFunction.Max))
                result = _Values.FromStored(column.Column, result);
            return Task.FromResult(result);
        }

        /// <inheritdoc />
        public Task InsertAsync(EntityMetadata metadata, object entity, ITransaction? transaction, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(metadata);
            ArgumentNullException.ThrowIfNull(entity);
            token.ThrowIfCancellationRequested();

            object?[] values = ToStoredValues(metadata, entity);
            ColumnMetadata? generated = metadata.AutoIncrementColumn;
            int generatedOrdinal = -1;
            if (generated != null)
            {
                generatedOrdinal = InMemoryTableSchema.For(metadata).Ordinal(generated);
                InMemoryIdentitySequence sequence = _Identities.GetOrAdd(metadata.EntityType, _ => new InMemoryIdentitySequence());
                object? current = values[generatedOrdinal];
                if (IsUnsetGenerated(current))
                {
                    values[generatedOrdinal] = Convert.ChangeType(sequence.Next(), generated.ClrType, CultureInfo.InvariantCulture);
                }
                else
                {
                    sequence.Observe(Convert.ToInt64(current, CultureInfo.InvariantCulture));
                    generatedOrdinal = -1;
                }
            }

            InMemoryRowKey key = InMemoryTableSchema.For(metadata).KeyOf(values);
            Write(transaction, context =>
            {
                InMemoryTable table = context.State.Table(metadata);
                if (table.Find(key) != null)
                    throw new InvalidOperationException("Cannot insert " + metadata.EntityType.Name + ": a row with primary key " + key + " already exists in '" + metadata.TableName + "'.");
                context.State = context.State.With(table.With(new InMemoryRow(key, NextSequence(), values)));
                context.Touch(metadata, key);
                context.Count = 1;
            });

            if (generated != null && generatedOrdinal >= 0) generated.SetValue(entity, _Values.FromStored(generated, values[generatedOrdinal]));
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task<int> ReplaceAsync(EntityMetadata metadata, object entity, QueryNode condition, QuerySource source, ITransaction? transaction, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(metadata);
            ArgumentNullException.ThrowIfNull(entity);
            ArgumentNullException.ThrowIfNull(condition);
            ArgumentNullException.ThrowIfNull(source);
            token.ThrowIfCancellationRequested();

            object?[] replacement = ToStoredValues(metadata, entity);
            List<int> updatable = new List<int>();
            for (int i = 0; i < metadata.Columns.Count; i++)
            {
                if (!metadata.Columns[i].IsPrimaryKey && !metadata.Columns[i].IsAutoIncrement) updatable.Add(i);
            }

            int count = Write(transaction, context =>
            {
                InMemoryTable table = context.State.Table(metadata);
                InMemoryQueryEvaluator evaluator = new InMemoryQueryEvaluator(context.State, _Values);
                List<InMemoryRow> matches = table.Rows.Where(row => evaluator.Bind(source, row).Test(condition)).ToList();
                foreach (InMemoryRow row in matches)
                {
                    object?[] values = (object?[])row.Values.Clone();
                    foreach (int ordinal in updatable) values[ordinal] = replacement[ordinal] is byte[] bytes ? bytes.Clone() : replacement[ordinal];
                    table = table.With(new InMemoryRow(row.Key, row.Sequence, values));
                    context.Touch(metadata, row.Key);
                }

                context.State = context.State.With(table);
                context.Count = matches.Count;
            });
            return Task.FromResult(count);
        }

        /// <inheritdoc />
        /// <exception cref="InvalidOperationException">Thrown when an assignment changes a primary key to one that already exists.</exception>
        public Task<int> UpdateAsync(QueryModel model, IReadOnlyList<FieldAssignment> assignments, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(model);
            ArgumentNullException.ThrowIfNull(assignments);
            if (assignments.Count == 0) throw new ArgumentException("At least one assignment is required.", nameof(assignments));
            token.ThrowIfCancellationRequested();

            EntityMetadata metadata = model.Metadata;
            InMemoryTableSchema schema = InMemoryTableSchema.For(metadata);
            int count = Write(model.Transaction, context =>
            {
                InMemoryQueryEvaluator evaluator = new InMemoryQueryEvaluator(context.State, _Values);
                List<InMemoryRow> matches = Select(context.State, model, false);
                List<InMemoryRow> updated = new List<InMemoryRow>(matches.Count);
                foreach (InMemoryRow row in matches)
                {
                    evaluator.Bind(model.Source, row);
                    object?[] values = (object?[])row.Values.Clone();
                    foreach (FieldAssignment assignment in assignments)
                    {
                        values[schema.Ordinal(assignment.Column)] = assignment.Value is ValueNode constant
                            ? _Values.ToStored(assignment.Column, constant.Value)
                            : _Values.ComputedToStored(assignment.Column, evaluator.Visit(assignment.Value));
                    }

                    updated.Add(new InMemoryRow(schema.KeyOf(values), row.Sequence, values));
                }

                InMemoryTable table = context.State.Table(metadata);
                for (int i = 0; i < matches.Count; i++)
                {
                    if (!matches[i].Key.Equals(updated[i].Key)) table = table.Without(matches[i].Key);
                }

                for (int i = 0; i < updated.Count; i++)
                {
                    if (!matches[i].Key.Equals(updated[i].Key) && table.Find(updated[i].Key) != null)
                        throw new InvalidOperationException("Cannot update " + metadata.EntityType.Name + ": a row with primary key " + updated[i].Key + " already exists in '" + metadata.TableName + "'.");
                    table = table.With(updated[i]);
                    context.Touch(metadata, matches[i].Key);
                    context.Touch(metadata, updated[i].Key);
                }

                context.State = context.State.With(table);
                context.Count = matches.Count;
            });
            return Task.FromResult(count);
        }

        /// <inheritdoc />
        public Task<int> DeleteAsync(QueryModel model, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(model);
            token.ThrowIfCancellationRequested();
            int count = Write(model.Transaction, context =>
            {
                List<InMemoryRow> matches = Select(context.State, model, false);
                InMemoryTable table = context.State.Table(model.Metadata);
                foreach (InMemoryRow row in matches)
                {
                    table = table.Without(row.Key);
                    context.Touch(model.Metadata, row.Key);
                }

                context.State = context.State.With(table);
                context.Count = matches.Count;
            });
            return Task.FromResult(count);
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Thrown when <see cref="Capabilities"/> lacks <see cref="RepositoryCapabilities.Transactions"/>.</exception>
        public Task<ITransaction> BeginTransactionAsync(CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            return Task.FromResult<ITransaction>(BeginTransaction());
        }

        /// <summary>
        /// Begins a snapshot-isolation transaction.
        /// </summary>
        /// <returns>The transaction. Dispose it; an uncommitted transaction rolls back on dispose.</returns>
        /// <exception cref="NotSupportedException">Thrown when <see cref="Capabilities"/> lacks <see cref="RepositoryCapabilities.Transactions"/>.</exception>
        public InMemoryTransaction BeginTransaction()
        {
            QueryCapabilityValidator.Require(Capabilities, RepositoryCapabilities.Transactions, "BeginTransaction");
            return new InMemoryTransaction(this, _Committed);
        }

        #endregion

        #region Private-Methods

        internal void Commit(InMemoryTransaction transaction)
        {
            lock (transaction.SyncRoot)
            {
                transaction.ThrowIfCompleted();
                lock (_CommitLock)
                {
                    InMemoryDatabaseState committed = _Committed;
                    List<KeyValuePair<InMemoryRowKey, EntityMetadata>> written = transaction.WrittenRows.ToList();
                    foreach (KeyValuePair<InMemoryRowKey, EntityMetadata> row in written)
                    {
                        InMemoryRow? original = transaction.Snapshot.Find(row.Value.EntityType, row.Key);
                        InMemoryRow? current = committed.Find(row.Value.EntityType, row.Key);
                        if (!ReferenceEquals(original, current))
                        {
                            transaction.TryComplete();
                            transaction.Working = transaction.Snapshot;
                            throw new InvalidOperationException(
                                "Transaction commit failed: the " + row.Value.EntityType.Name + " row with key " + row.Key +
                                " was changed by another writer after the transaction started. The transaction was rolled back.");
                        }
                    }

                    InMemoryDatabaseState state = committed;
                    foreach (IGrouping<Type, KeyValuePair<InMemoryRowKey, EntityMetadata>> table in written.GroupBy(r => r.Value.EntityType))
                    {
                        InMemoryTable merged = state.Table(table.First().Value);
                        foreach (KeyValuePair<InMemoryRowKey, EntityMetadata> row in table)
                        {
                            InMemoryRow? final = transaction.Working.Find(table.Key, row.Key);
                            merged = final == null ? merged.Without(row.Key) : merged.With(final);
                        }

                        state = state.With(merged);
                    }

                    _Committed = state;
                    transaction.TryComplete();
                }
            }
        }

        private InMemoryTransaction? ResolveTransaction(ITransaction? transaction)
        {
            if (transaction == null) return null;
            if (transaction is not InMemoryTransaction inMemory || !ReferenceEquals(inMemory.Backend, this))
                throw new ArgumentException("The transaction was not created by this in-memory backend.", nameof(transaction));
            inMemory.ThrowIfCompleted();
            return inMemory;
        }

        private InMemoryDatabaseState ReadState(ITransaction? transaction)
        {
            InMemoryTransaction? inMemory = ResolveTransaction(transaction);
            if (inMemory == null) return _Committed;
            lock (inMemory.SyncRoot)
            {
                inMemory.ThrowIfCompleted();
                return inMemory.Working;
            }
        }

        private int Write(ITransaction? transaction, Action<InMemoryWriteContext> write)
        {
            InMemoryTransaction? inMemory = ResolveTransaction(transaction);
            if (inMemory == null)
            {
                lock (_CommitLock)
                {
                    InMemoryWriteContext context = new InMemoryWriteContext(_Committed);
                    write(context);
                    _Committed = context.State;
                    return context.Count;
                }
            }

            lock (inMemory.SyncRoot)
            {
                inMemory.ThrowIfCompleted();
                InMemoryWriteContext context = new InMemoryWriteContext(inMemory.Working);
                write(context);
                inMemory.Working = context.State;
                inMemory.RecordWrites(context.Touched);
                return context.Count;
            }
        }

        private List<InMemoryRow> Select(InMemoryDatabaseState state, QueryModel model, bool includeOrderingAndPaging)
        {
            InMemoryTable table = state.Table(model.Metadata);
            InMemoryQueryEvaluator evaluator = new InMemoryQueryEvaluator(state, _Values);
            List<InMemoryRow> rows = model.Filter == null
                ? table.Rows.ToList()
                : table.Rows.Where(row => evaluator.Bind(model.Source, row).Test(model.Filter)).ToList();
            if (!includeOrderingAndPaging) return rows;

            if (model.Orderings.Count > 0)
            {
                Dictionary<InMemoryRow, object?[]> keys = new Dictionary<InMemoryRow, object?[]>(ReferenceEqualityComparer.Instance);
                foreach (InMemoryRow row in rows)
                {
                    evaluator.Bind(model.Source, row);
                    object?[] values = new object?[model.Orderings.Count];
                    for (int i = 0; i < values.Length; i++) values[i] = evaluator.Visit(model.Orderings[i].Key);
                    keys[row] = values;
                }

                IOrderedEnumerable<InMemoryRow>? ordered = null;
                for (int i = 0; i < model.Orderings.Count; i++)
                {
                    int index = i;
                    bool descending = model.Orderings[i].Descending;
                    Func<InMemoryRow, object?> selector = row => keys[row][index];
                    if (ordered == null)
                        ordered = descending ? rows.OrderByDescending(selector, QueryValueComparer.Ordinal) : rows.OrderBy(selector, QueryValueComparer.Ordinal);
                    else
                        ordered = descending ? ordered.ThenByDescending(selector, QueryValueComparer.Ordinal) : ordered.ThenBy(selector, QueryValueComparer.Ordinal);
                }

                rows = ordered!.ToList();
            }

            IEnumerable<InMemoryRow> paged = rows;
            if (model.Skip.HasValue) paged = paged.Skip(model.Skip.Value);
            if (model.Take.HasValue) paged = paged.Take(model.Take.Value);
            return paged is List<InMemoryRow> list ? list : paged.ToList();
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

        private object Materialize(EntityMetadata metadata, InMemoryRow row)
        {
            object entity = metadata.CreateInstance();
            for (int i = 0; i < metadata.Columns.Count; i++)
            {
                ColumnMetadata column = metadata.Columns[i];
                column.SetValue(entity, _Values.FromStored(column, row.Values[i]));
            }

            return entity;
        }

        private long NextSequence()
        {
            return Interlocked.Increment(ref _Sequence);
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

        #endregion
    }
}
