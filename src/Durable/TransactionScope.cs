namespace Durable
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    public class TransactionScope : ITransactionScope
    {
        #region Public-Members

        public static TransactionScope? Current => _current.Value;
        public ITransaction Transaction => _transaction;
        public bool IsCompleted => _completed;

        #endregion

        #region Private-Members

        private static readonly AsyncLocal<TransactionScope> _current = new AsyncLocal<TransactionScope>();
        private readonly TransactionScope? _parent;
        private readonly ITransaction _transaction;
        private readonly bool _ownsTransaction;
        private bool _completed;
        private bool _disposed;

        #endregion

        #region Constructors-and-Factories

        private TransactionScope(ITransaction transaction, bool ownsTransaction, TransactionScope? parent)
        {
            _transaction = transaction ?? throw new ArgumentNullException(nameof(transaction));
            _ownsTransaction = ownsTransaction;
            _parent = parent;
            _current.Value = this;
        }

        public static TransactionScope Create(ITransaction transaction)
        {
            if (transaction == null)
                throw new ArgumentNullException(nameof(transaction));

            var current = _current.Value;
            
            // If we already have a transaction and it's the same connection, create a nested scope
            if (current != null && 
                current._transaction.Connection == transaction.Connection)
            {
                return new TransactionScope(current._transaction, false, current);
            }

            return new TransactionScope(transaction, true, current);
        }

        public static async Task<TransactionScope> CreateAsync<T>(IRepository<T> repository, CancellationToken token = default) where T : class, new()
        {
            var transaction = await repository.BeginTransactionAsync(token);
            return new TransactionScope(transaction, true, _current.Value);
        }

        public static TransactionScope Create<T>(IRepository<T> repository) where T : class, new()
        {
            var transaction = repository.BeginTransaction();
            return new TransactionScope(transaction, true, _current.Value);
        }

        #endregion

        #region Public-Methods

        public void Complete()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(TransactionScope));
            if (_completed)
                throw new InvalidOperationException("Transaction scope has already been completed");

            _completed = true;

            if (_ownsTransaction)
            {
                _transaction.Commit();
            }
        }

        public async Task CompleteAsync(CancellationToken token = default)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(TransactionScope));
            if (_completed)
                throw new InvalidOperationException("Transaction scope has already been completed");

            _completed = true;

            if (_ownsTransaction)
            {
                await _transaction.CommitAsync(token);
            }
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _current.Value = _parent;

                if (!_completed && _ownsTransaction)
                {
                    try
                    {
                        _transaction.Rollback();
                    }
                    catch
                    {
                        // Swallow rollback exceptions during dispose
                    }
                }

                if (_ownsTransaction)
                {
                    _transaction?.Dispose();
                }

                _disposed = true;
            }
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}