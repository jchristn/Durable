namespace Durable.LiteDb
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using LiteDB;

    /// <summary>
    /// A LiteDB transaction usable from asynchronous code. LiteDB binds a transaction (and its locks) to the thread that
    /// began it, which breaks when an <c>await</c> resumes on another thread; this transaction therefore owns a dedicated
    /// thread that begins the LiteDB transaction and runs every operation performed with it, in order, until it is
    /// committed, rolled back or disposed. Callers on any thread (and any number of awaits) can use it; their operations
    /// are queued to that thread and complete asynchronously.
    /// <para>
    /// Semantics are LiteDB's: reads inside the transaction see its own writes; other readers see committed data only and
    /// are never blocked; the first write to a collection takes that collection's write lock until the transaction ends, so
    /// other writers to the same collection wait (up to the database timeout, then fail). In
    /// <see cref="LiteDbConnectionType.Shared"/> mode the transaction holds the data file's mutex until it ends, so every
    /// other operation on the file (from any thread or process) waits for it; do not await non-transactional operations on
    /// the same file while it is open. If an operation inside the
    /// transaction fails inside LiteDB, LiteDB rolls the whole transaction back; this transaction then rejects further
    /// operations and <see cref="Commit"/> with <see cref="InvalidOperationException"/> (rollback and dispose still succeed).
    /// Disposing an uncommitted transaction rolls it back. Always dispose transactions: an abandoned transaction keeps its
    /// thread and its locks. LiteDB allows at most 100 open transactions per database.
    /// </para>
    /// Thread safety: like every <see cref="ITransaction"/>, do not use one transaction from several threads at once;
    /// operations are nevertheless serialized on the transaction thread.
    /// </summary>
    public sealed class LiteDbTransaction : ITransaction
    {
        #region Public-Members

        /// <inheritdoc />
        public bool IsCompleted => Volatile.Read(ref _Completed) == 1;

        /// <summary>
        /// Gets the backend that created the transaction. Never null.
        /// </summary>
        public LiteDbBackend Backend { get; }

        #endregion

        #region Private-Members

        private readonly LiteDatabase _Database;
        private readonly BlockingCollection<Action> _Queue = new BlockingCollection<Action>();
        private readonly Thread _Thread;
        private int _Completed;
        private volatile bool _Ended;
        private volatile Exception? _Failure;
        private int? _TransactionId;

        #endregion

        #region Constructors-and-Factories

        private LiteDbTransaction(LiteDbBackend backend, LiteDatabase database)
        {
            Backend = backend;
            _Database = database;
            _Thread = new Thread(Run)
            {
                IsBackground = true,
                Name = "Durable.LiteDb transaction"
            };
        }

        internal static Task<LiteDbTransaction> BeginAsync(LiteDbBackend backend, LiteDatabase database, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            LiteDbTransaction transaction = new LiteDbTransaction(backend, database);
            transaction._Thread.Start();
            TaskCompletionSource<LiteDbTransaction> started = new TaskCompletionSource<LiteDbTransaction>(TaskCreationOptions.RunContinuationsAsynchronously);
            transaction._Queue.Add(() =>
            {
                try
                {
                    database.BeginTrans();
                    transaction._TransactionId = transaction.FindTransactionId(Environment.CurrentManagedThreadId);
                    started.TrySetResult(transaction);
                }
                catch (Exception e)
                {
                    transaction._Ended = true;
                    Interlocked.Exchange(ref transaction._Completed, 1);
                    transaction._Queue.CompleteAdding();
                    started.TrySetException(e);
                }
            });
            return started.Task;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        /// <remarks>Blocks the calling thread until the transaction ends; prefer <see cref="CommitAsync"/> in asynchronous code.</remarks>
        /// <exception cref="InvalidOperationException">Thrown when the transaction has completed, or was rolled back because an operation inside it failed.</exception>
        public void Commit()
        {
            Finish(true).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        /// <remarks>Blocks the calling thread until the transaction ends; prefer <see cref="RollbackAsync"/> in asynchronous code.</remarks>
        /// <exception cref="InvalidOperationException">Thrown when the transaction has already completed.</exception>
        public void Rollback()
        {
            Finish(false).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        /// <exception cref="InvalidOperationException">Thrown when the transaction has completed, or was rolled back because an operation inside it failed.</exception>
        public Task CommitAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            return Finish(true);
        }

        /// <inheritdoc />
        /// <exception cref="InvalidOperationException">Thrown when the transaction has already completed.</exception>
        public Task RollbackAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            return Finish(false);
        }

        /// <summary>
        /// Disposes the transaction, rolling it back when it was not completed, and ends its thread. Blocks the calling
        /// thread until the rollback completes; prefer <see cref="DisposeAsync"/> in asynchronous code.
        /// </summary>
        public void Dispose()
        {
            if (!IsCompleted)
            {
                try
                {
                    Finish(false).GetAwaiter().GetResult();
                }
                catch (InvalidOperationException)
                {
                }
            }

            _Queue.CompleteAdding();
        }

        /// <summary>
        /// Disposes the transaction, rolling it back when it was not completed, and ends its thread.
        /// </summary>
        /// <returns>A task that completes when the transaction was rolled back.</returns>
        public async ValueTask DisposeAsync()
        {
            if (!IsCompleted)
            {
                try
                {
                    await Finish(false).ConfigureAwait(false);
                }
                catch (InvalidOperationException)
                {
                }
            }

            _Queue.CompleteAdding();
        }

        #endregion

        #region Private-Methods

        internal Task<TResult> RunAsync<TResult>(Func<TResult> work, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(work);
            token.ThrowIfCancellationRequested();
            if (Thread.CurrentThread == _Thread)
            {
                ThrowIfUnusable();
                return Task.FromResult(work());
            }

            ThrowIfUnusable();
            TaskCompletionSource<TResult> completion = new TaskCompletionSource<TResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            Enqueue(() =>
            {
                if (token.IsCancellationRequested)
                {
                    completion.TrySetCanceled(token);
                    return;
                }

                try
                {
                    ThrowIfUnusable();
                    completion.TrySetResult(work());
                }
                catch (OperationCanceledException e)
                {
                    OnFailure(e);
                    completion.TrySetCanceled(e.CancellationToken);
                }
                catch (Exception e)
                {
                    OnFailure(e);
                    completion.TrySetException(e);
                }
            });
            return completion.Task;
        }

        private Task Finish(bool commit)
        {
            if (Interlocked.Exchange(ref _Completed, 1) == 1)
                return Task.FromException(new InvalidOperationException("The transaction has already completed."));

            TaskCompletionSource<bool> completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            Action finish = () =>
            {
                try
                {
                    if (_Ended)
                    {
                        if (commit) throw RolledBack();
                    }
                    else if (_Failure != null)
                    {
                        _Ended = true;
                        if (commit) throw RolledBack();
                    }
                    else if (commit)
                    {
                        _Ended = true;
                        _Database.Commit();
                    }
                    else
                    {
                        _Ended = true;
                        _Database.Rollback();
                    }

                    completion.TrySetResult(true);
                }
                catch (Exception e)
                {
                    TryRollbackOnThread();
                    completion.TrySetException(e);
                }
                finally
                {
                    _Queue.CompleteAdding();
                }
            };

            try
            {
                _Queue.Add(finish);
            }
            catch (InvalidOperationException)
            {
                if (commit && (_Failure != null || _Ended)) return Task.FromException(RolledBack());
                return Task.CompletedTask;
            }

            return completion.Task;
        }

        private void Enqueue(Action action)
        {
            try
            {
                _Queue.Add(action);
            }
            catch (InvalidOperationException)
            {
                throw new InvalidOperationException("The transaction has already completed.");
            }
        }

        private void Run()
        {
            foreach (Action action in _Queue.GetConsumingEnumerable()) action();
            TryRollbackOnThread();
        }

        private void OnFailure(Exception failure)
        {
            if (_Ended || _Failure != null) return;
            try
            {
                // LiteDB rolls an explicit transaction back when an operation inside it fails. The transaction is still alive
                // only if LiteDB still lists it (probing with BeginTrans would disturb LiteDB's shared-mode engine). When in
                // doubt, roll back: losing the transaction silently would let later operations auto-commit.
                if (_TransactionId.HasValue && FindTransactionId(null) == _TransactionId) return;
                _Database.Rollback();
                _Failure = failure;
            }
            catch (Exception)
            {
                _Failure = failure;
            }
        }

        private int? FindTransactionId(int? threadId)
        {
            foreach (BsonDocument transaction in _Database.GetCollection("$transactions").FindAll())
            {
                int id = transaction["transactionID"].AsInt32;
                if (threadId.HasValue ? transaction["threadID"].AsInt32 == threadId.Value : id == _TransactionId) return id;
            }

            return null;
        }

        private void TryRollbackOnThread()
        {
            _Ended = true;
            try
            {
                _Database.Rollback();
            }
            catch (Exception)
            {
            }
        }

        private void ThrowIfUnusable()
        {
            if (IsCompleted || _Ended) throw new InvalidOperationException("The transaction has already completed.");
            if (_Failure != null) throw RolledBack();
        }

        private InvalidOperationException RolledBack()
        {
            return new InvalidOperationException("The LiteDB transaction was rolled back because an operation inside it failed: " + (_Failure?.Message ?? "unknown error") + ".", _Failure);
        }

        #endregion
    }
}
