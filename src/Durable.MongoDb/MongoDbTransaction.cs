namespace Durable.MongoDb
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using MongoDB.Driver;

    /// <summary>
    /// A MongoDB multi-document transaction: a client session (<see cref="Session"/>) with a started transaction, used by
    /// every operation passed this transaction. Requires a replica set or a sharded cluster (see
    /// <see cref="MongoDbBackend.SupportsTransactions"/>).
    /// <para>
    /// Semantics are MongoDB's: reads inside the transaction see a snapshot plus its own writes; other sessions see the
    /// writes only after commit. Writers do not wait for each other: when two transactions write the same document, the
    /// second write fails immediately with a write conflict (a <see cref="MongoException"/> labeled
    /// <c>TransientTransactionError</c>), and the transaction must be retried by the caller. If any server operation inside
    /// the transaction fails, MongoDB aborts the transaction; this transaction then rejects further operations and
    /// <see cref="Commit"/> with <see cref="InvalidOperationException"/> (rollback and dispose still succeed). Errors Durable
    /// detects before writing (for example a duplicate primary key on insert) do not abort the transaction. MongoDB limits a
    /// transaction's lifetime (60 seconds by default, server parameter <c>transactionLifetimeLimitSeconds</c>).
    /// Disposing an uncommitted transaction rolls it back. Generated keys are taken outside the transaction (like SQL
    /// sequences), so a rolled-back insert leaves a gap.
    /// </para>
    /// Thread safety: like every <see cref="ITransaction"/>, do not use one transaction from several threads at once;
    /// operations are nevertheless serialized.
    /// </summary>
    public sealed class MongoDbTransaction : ITransaction
    {
        #region Public-Members

        /// <inheritdoc />
        public bool IsCompleted => Volatile.Read(ref _Completed) == 1;

        /// <summary>
        /// Gets the backend that created the transaction. Never null.
        /// </summary>
        public MongoDbBackend Backend { get; }

        /// <summary>
        /// Gets the client session that carries the transaction. Never null. Pass it to your own driver calls to run them in
        /// the same transaction; do not commit, abort or dispose it directly.
        /// </summary>
        public IClientSessionHandle Session { get; }

        #endregion

        #region Private-Members

        private readonly SemaphoreSlim _Gate = new SemaphoreSlim(1, 1);
        private int _Completed;
        private int _Disposed;
        private volatile Exception? _Failure;

        #endregion

        #region Constructors-and-Factories

        private MongoDbTransaction(MongoDbBackend backend, IClientSessionHandle session)
        {
            Backend = backend;
            Session = session;
        }

        internal static MongoDbTransaction Begin(MongoDbBackend backend, IMongoClient client, TransactionOptions? options)
        {
            IClientSessionHandle session = client.StartSession();
            try
            {
                session.StartTransaction(options);
                return new MongoDbTransaction(backend, session);
            }
            catch (Exception)
            {
                session.Dispose();
                throw;
            }
        }

        internal static async Task<MongoDbTransaction> BeginAsync(MongoDbBackend backend, IMongoClient client, TransactionOptions? options, CancellationToken token)
        {
            IClientSessionHandle session = await client.StartSessionAsync(null, token).ConfigureAwait(false);
            try
            {
                session.StartTransaction(options);
                return new MongoDbTransaction(backend, session);
            }
            catch (Exception)
            {
                session.Dispose();
                throw;
            }
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        /// <exception cref="InvalidOperationException">Thrown when the transaction has completed, or was aborted because an operation inside it failed.</exception>
        /// <exception cref="MongoException">Thrown when MongoDB rejects the commit (for example a write conflict).</exception>
        public void Commit()
        {
            BeginFinish();
            _Gate.Wait();
            try
            {
                if (_Failure != null)
                {
                    TryAbort();
                    throw RolledBack();
                }

                Session.CommitTransaction();
            }
            finally
            {
                _Gate.Release();
            }
        }

        /// <inheritdoc />
        /// <exception cref="InvalidOperationException">Thrown when the transaction has already completed.</exception>
        public void Rollback()
        {
            BeginFinish();
            _Gate.Wait();
            try
            {
                TryAbort();
            }
            finally
            {
                _Gate.Release();
            }
        }

        /// <inheritdoc />
        /// <exception cref="InvalidOperationException">Thrown when the transaction has completed, or was aborted because an operation inside it failed.</exception>
        /// <exception cref="MongoException">Thrown when MongoDB rejects the commit (for example a write conflict).</exception>
        public async Task CommitAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            BeginFinish();
            await _Gate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            try
            {
                if (_Failure != null)
                {
                    await TryAbortAsync().ConfigureAwait(false);
                    throw RolledBack();
                }

                await Session.CommitTransactionAsync(token).ConfigureAwait(false);
            }
            finally
            {
                _Gate.Release();
            }
        }

        /// <inheritdoc />
        /// <exception cref="InvalidOperationException">Thrown when the transaction has already completed.</exception>
        public async Task RollbackAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            BeginFinish();
            await _Gate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            try
            {
                await TryAbortAsync().ConfigureAwait(false);
            }
            finally
            {
                _Gate.Release();
            }
        }

        /// <summary>
        /// Disposes the transaction, rolling it back when it was not completed, and ends the session. Safe to call more
        /// than once.
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _Disposed, 1) == 1) return;
            if (Interlocked.Exchange(ref _Completed, 1) == 0)
            {
                _Gate.Wait();
                try
                {
                    TryAbort();
                }
                finally
                {
                    _Gate.Release();
                }
            }

            Session.Dispose();
        }

        /// <summary>
        /// Disposes the transaction, rolling it back when it was not completed, and ends the session. Safe to call more
        /// than once.
        /// </summary>
        /// <returns>A task that completes when the transaction was rolled back and the session ended.</returns>
        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _Disposed, 1) == 1) return;
            if (Interlocked.Exchange(ref _Completed, 1) == 0)
            {
                await _Gate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
                try
                {
                    await TryAbortAsync().ConfigureAwait(false);
                }
                finally
                {
                    _Gate.Release();
                }
            }

            Session.Dispose();
        }

        #endregion

        #region Private-Methods

        internal async Task<TResult> RunAsync<TResult>(Func<IClientSessionHandle, Task<TResult>> work, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(work);
            token.ThrowIfCancellationRequested();
            ThrowIfUnusable();
            await _Gate.WaitAsync(token).ConfigureAwait(false);
            try
            {
                ThrowIfUnusable();
                try
                {
                    return await work(Session).ConfigureAwait(false);
                }
                catch (MongoException e)
                {
                    // MongoDB aborts a transaction when a server operation inside it fails; make that visible instead of
                    // letting later operations fail with NoSuchTransaction.
                    _Failure ??= e;
                    throw;
                }
                catch (OperationCanceledException e)
                {
                    _Failure ??= e;
                    throw;
                }
            }
            finally
            {
                _Gate.Release();
            }
        }

        private void BeginFinish()
        {
            if (Interlocked.Exchange(ref _Completed, 1) == 1) throw new InvalidOperationException("The transaction has already completed.");
        }

        private void TryAbort()
        {
            try
            {
                if (Session.IsInTransaction) Session.AbortTransaction();
            }
            catch (MongoException)
            {
            }
        }

        private async Task TryAbortAsync()
        {
            try
            {
                if (Session.IsInTransaction) await Session.AbortTransactionAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (MongoException)
            {
            }
        }

        private void ThrowIfUnusable()
        {
            if (IsCompleted) throw new InvalidOperationException("The transaction has already completed.");
            if (_Failure != null) throw RolledBack();
        }

        private InvalidOperationException RolledBack()
        {
            return new InvalidOperationException("The MongoDB transaction was aborted because an operation inside it failed: " + (_Failure?.Message ?? "unknown error") + ".", _Failure);
        }

        #endregion
    }
}
