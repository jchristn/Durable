namespace Durable.LiteGraph
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;

    /// <summary>
    /// An interactive transaction over a <see cref="LiteGraphBackend"/>. LiteGraph's own graph transactions execute a
    /// complete batch of operations atomically; this class bridges Durable's interactive model to it: writes made in the
    /// transaction are kept in the transaction (reads in the transaction see them merged over the stored graph), and
    /// <see cref="Commit"/> applies all of them as one LiteGraph graph transaction, so they become visible together or not
    /// at all. Rolling back (or disposing without committing) discards them; nothing was written to the graph.
    /// <para>
    /// Isolation: reads see committed data plus the transaction's own writes (read committed). Writes are checked at
    /// commit: if another writer changed or deleted a node this transaction wrote after the transaction first wrote it, or
    /// created a node with a key this transaction inserted, the commit fails with <see cref="InvalidOperationException"/>
    /// and the transaction is rolled back (first committer wins). The check and the commit are atomic among writers of the
    /// same backend instance; writers in other processes are only caught by LiteGraph's unique node GUIDs. A transaction
    /// whose writes need more than <see cref="LiteGraphRepositorySettings.MaxOperationsPerTransaction"/> graph operations
    /// fails at commit.
    /// </para>
    /// Thread safety: safe for concurrent use; operations on one transaction are serialized.
    /// </summary>
    public sealed class LiteGraphTransaction : ITransaction
    {
        #region Public-Members

        /// <inheritdoc />
        public bool IsCompleted => Volatile.Read(ref _Completed) == 1;

        /// <summary>
        /// Gets the backend that created the transaction. Never null.
        /// </summary>
        public LiteGraphBackend Backend { get; }

        /// <summary>
        /// Gets the number of LiteGraph operations (node and edge writes) the transaction will apply at commit.
        /// </summary>
        public int PendingOperationCount => Scope.Operations.Count;

        #endregion

        #region Private-Members

        private int _Completed;

        #endregion

        #region Constructors-and-Factories

        internal LiteGraphTransaction(LiteGraphBackend backend)
        {
            Backend = backend;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        /// <remarks>Blocks the calling thread until the transaction ends; prefer <see cref="CommitAsync"/> in asynchronous code.</remarks>
        /// <exception cref="InvalidOperationException">Thrown when the transaction already completed, when another writer changed a written node, or when LiteGraph rejects the writes; the transaction is then rolled back.</exception>
        public void Commit()
        {
            Task.Run(() => CommitAsync(CancellationToken.None)).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        /// <remarks>Blocks the calling thread until the transaction ends; prefer <see cref="RollbackAsync"/> in asynchronous code.</remarks>
        /// <exception cref="InvalidOperationException">Thrown when the transaction already completed.</exception>
        public void Rollback()
        {
            Lock.Wait();
            try
            {
                if (!TryComplete()) throw new InvalidOperationException("The transaction has already completed.");
                Scope.Clear();
            }
            finally
            {
                Lock.Release();
            }
        }

        /// <inheritdoc />
        /// <exception cref="InvalidOperationException">Thrown when the transaction already completed, when another writer changed a written node, or when LiteGraph rejects the writes; the transaction is then rolled back.</exception>
        public Task CommitAsync(CancellationToken token = default)
        {
            return Backend.CommitAsync(this, token);
        }

        /// <inheritdoc />
        /// <exception cref="InvalidOperationException">Thrown when the transaction already completed.</exception>
        public async Task RollbackAsync(CancellationToken token = default)
        {
            await Lock.WaitAsync(token).ConfigureAwait(false);
            try
            {
                if (!TryComplete()) throw new InvalidOperationException("The transaction has already completed.");
                Scope.Clear();
            }
            finally
            {
                Lock.Release();
            }
        }

        /// <summary>
        /// Rolls back the transaction when it has not completed. Blocks the calling thread while another operation of the
        /// transaction is running; prefer <see cref="DisposeAsync"/> in asynchronous code.
        /// </summary>
        public void Dispose()
        {
            Lock.Wait();
            try
            {
                if (TryComplete()) Scope.Clear();
            }
            finally
            {
                Lock.Release();
            }
        }

        /// <summary>
        /// Rolls back the transaction when it has not completed.
        /// </summary>
        /// <returns>A task.</returns>
        public async ValueTask DisposeAsync()
        {
            await Lock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (TryComplete()) Scope.Clear();
            }
            finally
            {
                Lock.Release();
            }
        }

        #endregion

        #region Private-Methods

        internal SemaphoreSlim Lock { get; } = new SemaphoreSlim(1, 1);

        internal LiteGraphWriteScope Scope { get; } = new LiteGraphWriteScope();

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
