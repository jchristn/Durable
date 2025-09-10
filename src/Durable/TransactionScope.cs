namespace Durable
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Provides a convenient way to manage database transactions with automatic rollback on disposal if not completed.
    /// Supports nested transaction scopes by reusing existing transactions on the same connection.
    /// </summary>
    public class TransactionScope : ITransactionScope
    {
        #region Public-Members

        /// <summary>
        /// Gets the current transaction scope for the current async context.
        /// </summary>
        public static TransactionScope? Current => _Current.Value;
        
        /// <summary>
        /// Gets the transaction associated with this scope.
        /// </summary>
        public ITransaction Transaction => _Transaction;
        
        /// <summary>
        /// Gets a value indicating whether this transaction scope has been completed.
        /// </summary>
        public bool IsCompleted => _Completed;

        #endregion

        #region Private-Members

        private static readonly AsyncLocal<TransactionScope> _Current = new AsyncLocal<TransactionScope>();
        private readonly TransactionScope? _Parent;
        private readonly ITransaction _Transaction;
        private readonly bool _OwnsTransaction;
        private bool _Completed;
        private bool _Disposed;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the TransactionScope class.
        /// </summary>
        /// <param name="transaction">The transaction to manage.</param>
        /// <param name="ownsTransaction">Whether this scope owns the transaction and should commit/rollback it.</param>
        /// <param name="parent">The parent transaction scope, if any.</param>
        private TransactionScope(ITransaction transaction, bool ownsTransaction, TransactionScope? parent)
        {
            _Transaction = transaction ?? throw new ArgumentNullException(nameof(transaction));
            _OwnsTransaction = ownsTransaction;
            _Parent = parent;
            _Current.Value = this;
        }

        /// <summary>
        /// Creates a new transaction scope with the specified transaction.
        /// If there's an existing scope with the same connection, creates a nested scope.
        /// </summary>
        /// <param name="transaction">The transaction to use for the scope.</param>
        /// <returns>A new transaction scope.</returns>
        public static TransactionScope Create(ITransaction transaction)
        {
            if (transaction == null)
                throw new ArgumentNullException(nameof(transaction));

            TransactionScope? current = _Current.Value;
            
            // If we already have a transaction and it's the same connection, create a nested scope
            if (current != null && 
                current._Transaction.Connection == transaction.Connection)
            {
                return new TransactionScope(current._Transaction, false, current);
            }

            return new TransactionScope(transaction, true, current);
        }

        /// <summary>
        /// Asynchronously creates a new transaction scope by beginning a transaction on the specified repository.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="repository">The repository to begin the transaction on.</param>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task that resolves to a new transaction scope.</returns>
        public static async Task<TransactionScope> CreateAsync<T>(IRepository<T> repository, CancellationToken token = default) where T : class, new()
        {
            ITransaction transaction = await repository.BeginTransactionAsync(token);
            return new TransactionScope(transaction, true, _Current.Value);
        }

        /// <summary>
        /// Creates a new transaction scope by beginning a transaction on the specified repository.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="repository">The repository to begin the transaction on.</param>
        /// <returns>A new transaction scope.</returns>
        public static TransactionScope Create<T>(IRepository<T> repository) where T : class, new()
        {
            ITransaction transaction = repository.BeginTransaction();
            return new TransactionScope(transaction, true, _Current.Value);
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Marks the transaction scope as completed, committing the transaction if this scope owns it.
        /// </summary>
        public void Complete()
        {
            if (_Disposed)
                throw new ObjectDisposedException(nameof(TransactionScope));
            if (_Completed)
                throw new InvalidOperationException("Transaction scope has already been completed");

            _Completed = true;

            if (_OwnsTransaction)
            {
                _Transaction.Commit();
            }
        }

        /// <summary>
        /// Asynchronously marks the transaction scope as completed, committing the transaction if this scope owns it.
        /// </summary>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public async Task CompleteAsync(CancellationToken token = default)
        {
            if (_Disposed)
                throw new ObjectDisposedException(nameof(TransactionScope));
            if (_Completed)
                throw new InvalidOperationException("Transaction scope has already been completed");

            _Completed = true;

            if (_OwnsTransaction)
            {
                await _Transaction.CommitAsync(token);
            }
        }

        /// <summary>
        /// Disposes the transaction scope, rolling back the transaction if not completed and this scope owns it.
        /// </summary>
        public void Dispose()
        {
            if (!_Disposed)
            {
                _Current.Value = _Parent!;

                if (!_Completed && _OwnsTransaction)
                {
                    try
                    {
                        _Transaction.Rollback();
                    }
                    catch
                    {
                        // Swallow rollback exceptions during dispose
                    }
                }

                if (_OwnsTransaction)
                {
                    _Transaction?.Dispose();
                }

                _Disposed = true;
            }
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}