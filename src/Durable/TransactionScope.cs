namespace Durable
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    public class TransactionScope : ITransactionScope
    {
        #region Public-Members

        public static TransactionScope? Current => _Current.Value;
        public ITransaction Transaction => _Transaction;
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

        private TransactionScope(ITransaction transaction, bool ownsTransaction, TransactionScope? parent)
        {
            _Transaction = transaction ?? throw new ArgumentNullException(nameof(transaction));
            _OwnsTransaction = ownsTransaction;
            _Parent = parent;
            _Current.Value = this;
        }

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

        public static async Task<TransactionScope> CreateAsync<T>(IRepository<T> repository, CancellationToken token = default) where T : class, new()
        {
            ITransaction transaction = await repository.BeginTransactionAsync(token);
            return new TransactionScope(transaction, true, _Current.Value);
        }

        public static TransactionScope Create<T>(IRepository<T> repository) where T : class, new()
        {
            ITransaction transaction = repository.BeginTransaction();
            return new TransactionScope(transaction, true, _Current.Value);
        }

        #endregion

        #region Public-Methods

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