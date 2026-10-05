namespace Durable
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// An ambient transaction scope. While a scope is current, repository calls made without an explicit transaction
    /// on the same async flow use <see cref="Transaction"/>. Disposing a scope that was not completed rolls back.
    /// Nested scopes created for the same transaction share it and only the outermost scope commits.
    /// Thread safety: a scope flows with the async execution context and must not be shared across concurrent flows.
    /// </summary>
    public class TransactionScope : ITransactionScope
    {
        #region Public-Members

        /// <summary>
        /// Gets the scope current on this async flow, or null. Completed or disposed scopes are skipped.
        /// </summary>
        public static TransactionScope? Current
        {
            get
            {
                TransactionScope? scope = _Current.Value;
                while (scope != null && (scope._Disposed || scope._Faulted)) scope = scope._Parent;
                return scope;
            }
        }

        /// <inheritdoc />
        /// <exception cref="InvalidOperationException">Thrown when accessed before an asynchronous scope finished starting its transaction.</exception>
        public ITransaction Transaction => _Transaction ?? throw new InvalidOperationException("The transaction scope has not finished starting its transaction.");

        /// <inheritdoc />
        public bool IsCompleted => _Completed;

        #endregion

        #region Private-Members

        private static readonly AsyncLocal<TransactionScope?> _Current = new AsyncLocal<TransactionScope?>();
        private readonly TransactionScope? _Parent;
        private ITransaction? _Transaction;
        private readonly bool _OwnsTransaction;
        private bool _Completed;
        private bool _Disposed;
        private bool _Faulted;

        #endregion

        #region Constructors-and-Factories

        private TransactionScope(ITransaction? transaction, bool ownsTransaction, TransactionScope? parent)
        {
            _Transaction = transaction;
            _OwnsTransaction = ownsTransaction;
            _Parent = parent;
            _Current.Value = this;
        }

        /// <summary>
        /// Creates a scope around an existing transaction. If the current scope already uses the same transaction,
        /// a nested, non-owning scope is created.
        /// </summary>
        /// <param name="transaction">Transaction. Must not be null.</param>
        /// <returns>The new scope, which is now current.</returns>
        /// <exception cref="ArgumentNullException">Thrown when transaction is null.</exception>
        public static TransactionScope Create(ITransaction transaction)
        {
            ArgumentNullException.ThrowIfNull(transaction);
            TransactionScope? current = Current;
            if (current != null && ReferenceEquals(current._Transaction, transaction))
                return new TransactionScope(transaction, false, current);
            return new TransactionScope(transaction, true, current);
        }

        /// <summary>
        /// Begins a transaction on the repository and makes a new owning scope current.
        /// </summary>
        /// <typeparam name="T">Entity type.</typeparam>
        /// <param name="repository">Repository. Must not be null.</param>
        /// <returns>The new scope.</returns>
        /// <exception cref="ArgumentNullException">Thrown when repository is null.</exception>
        public static TransactionScope Create<T>(IRepository<T> repository) where T : class, new()
        {
            ArgumentNullException.ThrowIfNull(repository);
            ITransaction transaction = repository.BeginTransaction();
            return new TransactionScope(transaction, true, Current);
        }

        /// <summary>
        /// Begins a transaction asynchronously and makes a new owning scope current on the caller's async flow.
        /// The scope becomes current immediately (before the returned task completes), so code that awaits this method
        /// observes it via <see cref="Current"/>.
        /// </summary>
        /// <typeparam name="T">Entity type.</typeparam>
        /// <param name="repository">Repository. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task returning the new scope.</returns>
        /// <exception cref="ArgumentNullException">Thrown when repository is null.</exception>
        public static Task<TransactionScope> CreateAsync<T>(IRepository<T> repository, CancellationToken token = default) where T : class, new()
        {
            ArgumentNullException.ThrowIfNull(repository);

            // Not an async method: assigning the AsyncLocal here mutates the caller's execution context.
            // Inside an async method the assignment would be discarded when the method returns.
            TransactionScope scope = new TransactionScope(null, true, Current);
            return scope.StartAsync(repository, token);
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        /// <exception cref="ObjectDisposedException">Thrown when the scope is disposed.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the scope is already completed.</exception>
        public void Complete()
        {
            ThrowIfUnusable();
            _Completed = true;
            if (_OwnsTransaction) Transaction.Commit();
        }

        /// <inheritdoc />
        /// <exception cref="ObjectDisposedException">Thrown when the scope is disposed.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the scope is already completed.</exception>
        public async Task CompleteAsync(CancellationToken token = default)
        {
            ThrowIfUnusable();
            _Completed = true;
            if (_OwnsTransaction) await Transaction.CommitAsync(token).ConfigureAwait(false);
        }

        /// <summary>
        /// Disposes the scope, rolling back an owned transaction that was not completed, and restores the parent scope.
        /// </summary>
        public void Dispose()
        {
            if (_Disposed) return;
            _Disposed = true;
            if (ReferenceEquals(_Current.Value, this)) _Current.Value = _Parent;

            if (_OwnsTransaction && _Transaction != null)
            {
                if (!_Completed && !_Transaction.IsCompleted)
                {
                    try
                    {
                        _Transaction.Rollback();
                    }
                    catch (InvalidOperationException)
                    {
                    }
                }

                _Transaction.Dispose();
            }

            GC.SuppressFinalize(this);
        }

        #endregion

        #region Private-Methods

        private async Task<TransactionScope> StartAsync<T>(IRepository<T> repository, CancellationToken token) where T : class, new()
        {
            try
            {
                _Transaction = await repository.BeginTransactionAsync(token).ConfigureAwait(false);
                return this;
            }
            catch
            {
                _Faulted = true;
                throw;
            }
        }

        private void ThrowIfUnusable()
        {
            if (_Disposed) throw new ObjectDisposedException(nameof(TransactionScope));
            if (_Completed) throw new InvalidOperationException("Transaction scope has already been completed.");
        }

        #endregion
    }
}
