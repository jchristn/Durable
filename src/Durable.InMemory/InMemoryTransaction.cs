namespace Durable.InMemory
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;

    /// <summary>
    /// A snapshot-isolation transaction of an <see cref="InMemoryBackend"/>. It starts from a snapshot of the committed
    /// data; reads inside it see that snapshot plus its own writes, and reads outside it see committed data only.
    /// <see cref="Commit"/> publishes its writes atomically unless another writer changed one of the rows it wrote after
    /// the snapshot was taken (first committer wins), in which case the commit fails with
    /// <see cref="InvalidOperationException"/> and the transaction is rolled back. <see cref="Rollback"/> and disposing an
    /// uncommitted transaction discard its writes. A transaction never blocks other readers or writers.
    /// Thread safety: like every <see cref="ITransaction"/>, do not use one transaction from several threads at once;
    /// individual operations are nevertheless serialized internally.
    /// </summary>
    public sealed class InMemoryTransaction : ITransaction
    {
        #region Public-Members

        /// <inheritdoc />
        public bool IsCompleted => Volatile.Read(ref _Completed) == 1;

        /// <summary>
        /// Gets the backend that created the transaction. Never null.
        /// </summary>
        public InMemoryBackend Backend { get; }

        #endregion

        #region Private-Members

        private readonly Dictionary<Type, Dictionary<InMemoryRowKey, EntityMetadata>> _Written = new Dictionary<Type, Dictionary<InMemoryRowKey, EntityMetadata>>();
        private int _Completed;

        #endregion

        #region Constructors-and-Factories

        internal InMemoryTransaction(InMemoryBackend backend, InMemoryDatabaseState snapshot)
        {
            Backend = backend;
            Snapshot = snapshot;
            Working = snapshot;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        /// <exception cref="InvalidOperationException">Thrown when the transaction has completed, or when another writer changed a row this transaction wrote.</exception>
        public void Commit()
        {
            Backend.Commit(this);
        }

        /// <inheritdoc />
        /// <exception cref="InvalidOperationException">Thrown when the transaction has already completed.</exception>
        public void Rollback()
        {
            lock (SyncRoot)
            {
                if (!TryComplete()) throw new InvalidOperationException("The transaction has already completed.");
                Working = Snapshot;
            }
        }

        /// <inheritdoc />
        /// <exception cref="InvalidOperationException">Thrown when the transaction has completed, or when another writer changed a row this transaction wrote.</exception>
        public Task CommitAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            Commit();
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        /// <exception cref="InvalidOperationException">Thrown when the transaction has already completed.</exception>
        public Task RollbackAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            Rollback();
            return Task.CompletedTask;
        }

        /// <summary>
        /// Disposes the transaction, rolling it back when it was not completed.
        /// </summary>
        public void Dispose()
        {
            lock (SyncRoot)
            {
                if (TryComplete()) Working = Snapshot;
            }
        }

        /// <summary>
        /// Disposes the transaction, rolling it back when it was not completed.
        /// </summary>
        /// <returns>A completed task.</returns>
        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }

        #endregion

        #region Private-Methods

        internal object SyncRoot { get; } = new object();

        internal InMemoryDatabaseState Snapshot { get; }

        internal InMemoryDatabaseState Working { get; set; }

        internal IEnumerable<KeyValuePair<InMemoryRowKey, EntityMetadata>> WrittenRows
        {
            get
            {
                foreach (Dictionary<InMemoryRowKey, EntityMetadata> table in _Written.Values)
                {
                    foreach (KeyValuePair<InMemoryRowKey, EntityMetadata> row in table) yield return row;
                }
            }
        }

        internal void RecordWrites(List<KeyValuePair<EntityMetadata, InMemoryRowKey>> touched)
        {
            foreach (KeyValuePair<EntityMetadata, InMemoryRowKey> row in touched)
            {
                if (!_Written.TryGetValue(row.Key.EntityType, out Dictionary<InMemoryRowKey, EntityMetadata>? keys))
                {
                    keys = new Dictionary<InMemoryRowKey, EntityMetadata>();
                    _Written[row.Key.EntityType] = keys;
                }

                keys[row.Value] = row.Key;
            }
        }

        internal bool TryComplete()
        {
            return Interlocked.Exchange(ref _Completed, 1) == 0;
        }

        internal void ThrowIfCompleted()
        {
            if (IsCompleted) throw new InvalidOperationException("The transaction has already completed.");
        }

        #endregion
    }
}
