namespace Durable
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Durable's ambient transaction scope. While a scope is current, repository calls made without an explicit
    /// transaction on the same async flow use <see cref="Transaction"/> (when the repository can use that transaction,
    /// for example a SQL repository of the same provider). Disposing a scope that was not completed rolls back; prefer
    /// <c>await using</c> so the rollback runs asynchronously. Nested scopes created for the same transaction share it and
    /// only the outermost scope commits.
    /// <para>
    /// This is not <c>System.Transactions.TransactionScope</c> and Durable does not participate in System.Transactions:
    /// it never reads <c>Transaction.Current</c> or enlists connections. (An ADO.NET driver configured to auto-enlist may
    /// still enlist connections Durable opens inside a System.Transactions scope; that is driver behavior.)
    /// </para>
    /// Thread safety: a scope flows with the async execution context and must not be shared across concurrent flows.
    /// </summary>
    public class AmbientTransactionScope : ITransactionScope
    {
        #region Public-Members

        /// <summary>
        /// Gets the scope current on this async flow, or null. Completed or disposed scopes are skipped.
        /// </summary>
        public static AmbientTransactionScope? Current
        {
            get
            {
                AmbientTransactionScope? scope = _Current.Value;
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

        private static readonly AsyncLocal<AmbientTransactionScope?> _Current = new AsyncLocal<AmbientTransactionScope?>();
        private readonly AmbientTransactionScope? _Parent;
        private ITransaction? _Transaction;
        private readonly bool _OwnsTransaction;
        private bool _Completed;
        private bool _Disposed;
        private bool _Faulted;

        #endregion

        #region Constructors-and-Factories

        private AmbientTransactionScope(ITransaction? transaction, bool ownsTransaction, AmbientTransactionScope? parent)
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
        public static AmbientTransactionScope Create(ITransaction transaction)
        {
            ArgumentNullException.ThrowIfNull(transaction);
            AmbientTransactionScope? current = Current;
            if (current != null && ReferenceEquals(current._Transaction, transaction))
                return new AmbientTransactionScope(transaction, false, current);
            return new AmbientTransactionScope(transaction, true, current);
        }

        /// <summary>
        /// Begins a transaction on the repository and makes a new owning scope current.
        /// </summary>
        /// <typeparam name="T">Entity type.</typeparam>
        /// <param name="repository">Repository. Must not be null.</param>
        /// <returns>The new scope.</returns>
        /// <exception cref="ArgumentNullException">Thrown when repository is null.</exception>
        public static AmbientTransactionScope Create<[DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] T>(IRepository<T> repository) where T : class, new()
        {
            ArgumentNullException.ThrowIfNull(repository);
            ITransaction transaction = repository.BeginTransaction();
            return new AmbientTransactionScope(transaction, true, Current);
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
        public static Task<AmbientTransactionScope> CreateAsync<[DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] T>(IRepository<T> repository, CancellationToken token = default) where T : class, new()
        {
            ArgumentNullException.ThrowIfNull(repository);

            // Not an async method: assigning the AsyncLocal here mutates the caller's execution context.
            // Inside an async method the assignment would be discarded when the method returns.
            AmbientTransactionScope scope = new AmbientTransactionScope(null, true, Current);
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
        /// Rollback failures caused by a transaction that already ended are ignored. Prefer <see cref="DisposeAsync"/>.
        /// </summary>
        public void Dispose()
        {
            if (!BeginDispose()) return;

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

        /// <summary>
        /// Disposes the scope asynchronously, rolling back an owned transaction that was not completed with
        /// <see cref="ITransaction.RollbackAsync"/>, and restores the parent scope. The scope stops being
        /// <see cref="Current"/> before the returned task completes.
        /// </summary>
        /// <returns>A task that completes when the transaction has been rolled back (if needed) and disposed.</returns>
        public ValueTask DisposeAsync()
        {
            // Not an async method: restoring the AsyncLocal must happen on the caller's execution context.
            if (!BeginDispose()) return ValueTask.CompletedTask;
            GC.SuppressFinalize(this);
            if (!_OwnsTransaction || _Transaction == null) return ValueTask.CompletedTask;
            return EndDisposeAsync(_Transaction);
        }

        #endregion

        #region Private-Methods

        private async Task<AmbientTransactionScope> StartAsync<[DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] T>(IRepository<T> repository, CancellationToken token) where T : class, new()
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

        private bool BeginDispose()
        {
            if (_Disposed) return false;
            _Disposed = true;
            if (ReferenceEquals(_Current.Value, this)) _Current.Value = _Parent;
            return true;
        }

        private async ValueTask EndDisposeAsync(ITransaction transaction)
        {
            if (!_Completed && !transaction.IsCompleted)
            {
                try
                {
                    await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                }
                catch (InvalidOperationException)
                {
                }
            }

            await transaction.DisposeAsync().ConfigureAwait(false);
        }

        private void ThrowIfUnusable()
        {
            if (_Disposed) throw new ObjectDisposedException(nameof(AmbientTransactionScope));
            if (_Completed) throw new InvalidOperationException("Transaction scope has already been completed.");
        }

        #endregion
    }
}
