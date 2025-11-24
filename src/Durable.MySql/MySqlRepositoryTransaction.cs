#nullable enable
namespace Durable.MySql
{
    using System;
    using System.Data;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;
    using MySqlConnector;

    /// <summary>
    /// MySQL-specific implementation of ITransaction for managing database transactions with savepoint support.
    /// </summary>
    internal class MySqlRepositoryTransaction : ITransaction
    {
        #region Public-Members

        /// <summary>
        /// Gets the database connection associated with this transaction.
        /// </summary>
        public DbConnection Connection => _Connection;

        /// <summary>
        /// Gets the underlying database transaction.
        /// </summary>
        public DbTransaction Transaction => _Transaction;

        #endregion

        #region Private-Members

        private readonly DbConnection _Connection;
        private readonly DbTransaction _Transaction;
        private readonly IConnectionFactory _ConnectionFactory;
        private bool _Disposed;
        private int _SavepointCounter;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the MySqlRepositoryTransaction class.
        /// </summary>
        /// <param name="connection">The MySQL connection for this transaction</param>
        /// <param name="transaction">The MySQL transaction instance</param>
        /// <param name="connectionFactory">The connection factory for returning connections</param>
        /// <exception cref="ArgumentNullException">Thrown when any parameter is null</exception>
        public MySqlRepositoryTransaction(DbConnection connection, DbTransaction transaction, IConnectionFactory connectionFactory)
        {
            _Connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _Transaction = transaction ?? throw new ArgumentNullException(nameof(transaction));
            _ConnectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Commits the transaction, making all changes permanent.
        /// </summary>
        /// <exception cref="ObjectDisposedException">Thrown when the transaction has been disposed</exception>
        public void Commit()
        {
            ThrowIfDisposed();
            _Transaction.Commit();
        }

        /// <summary>
        /// Asynchronously commits the transaction, making all changes permanent.
        /// </summary>
        /// <param name="token">A cancellation token that can be used to cancel the operation</param>
        /// <returns>A task that represents the asynchronous commit operation</returns>
        /// <exception cref="ObjectDisposedException">Thrown when the transaction has been disposed</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled via the cancellation token</exception>
        public async Task CommitAsync(CancellationToken token = default)
        {
            ThrowIfDisposed();
            await _Transaction.CommitAsync(token).ConfigureAwait(false);
        }

        /// <summary>
        /// Rolls back the transaction, undoing all changes made within the transaction.
        /// </summary>
        /// <exception cref="ObjectDisposedException">Thrown when the transaction has been disposed</exception>
        public void Rollback()
        {
            ThrowIfDisposed();
            _Transaction.Rollback();
        }

        /// <summary>
        /// Asynchronously rolls back the transaction, undoing all changes made within the transaction.
        /// </summary>
        /// <param name="token">A cancellation token that can be used to cancel the operation</param>
        /// <returns>A task that represents the asynchronous rollback operation</returns>
        /// <exception cref="ObjectDisposedException">Thrown when the transaction has been disposed</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled via the cancellation token</exception>
        public async Task RollbackAsync(CancellationToken token = default)
        {
            ThrowIfDisposed();
            await _Transaction.RollbackAsync(token).ConfigureAwait(false);
        }

        /// <summary>
        /// Creates a savepoint within the transaction.
        /// </summary>
        /// <param name="name">The optional name for the savepoint. If null, a name will be generated.</param>
        /// <returns>An ISavepoint representing the created savepoint</returns>
        /// <exception cref="ObjectDisposedException">Thrown when the transaction has been disposed</exception>
        /// <exception cref="InvalidOperationException">Thrown when the connection is not open</exception>
        public ISavepoint CreateSavepoint(string? name = null)
        {
            ThrowIfDisposed();
            ValidateConnectionState();

            name = name ?? $"sp_{Interlocked.Increment(ref _SavepointCounter)}";
            return new MySqlSavepoint((MySqlConnection)_Connection, (MySqlTransaction)_Transaction, name);
        }

        /// <summary>
        /// Asynchronously creates a savepoint within the transaction.
        /// </summary>
        /// <param name="name">The optional name for the savepoint. If null, a name will be generated.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation</param>
        /// <returns>A task that returns an ISavepoint representing the created savepoint</returns>
        /// <exception cref="ObjectDisposedException">Thrown when the transaction has been disposed</exception>
        /// <exception cref="InvalidOperationException">Thrown when the connection is not open</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled via the cancellation token</exception>
        public async Task<ISavepoint> CreateSavepointAsync(string? name = null, CancellationToken token = default)
        {
            ThrowIfDisposed();
            ValidateConnectionState();

            name = name ?? $"sp_{Interlocked.Increment(ref _SavepointCounter)}";

            // Create savepoint asynchronously
            using MySqlCommand command = new MySqlCommand($"SAVEPOINT `{name}`", (MySqlConnection)_Connection, (MySqlTransaction)_Transaction);
            await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);

            return new MySqlSavepoint((MySqlConnection)_Connection, (MySqlTransaction)_Transaction, name, false);
        }

        /// <summary>
        /// Disposes of the transaction and returns the connection to the connection factory.
        /// </summary>
        public void Dispose()
        {
            if (!_Disposed)
            {
                _Transaction?.Dispose();
                if (_Connection != null)
                {
                    _ConnectionFactory.ReturnConnection(_Connection);
                }
                _Disposed = true;
            }
        }

        #endregion

        #region Private-Methods

        private void ThrowIfDisposed()
        {
            if (_Disposed)
                throw new ObjectDisposedException(nameof(MySqlRepositoryTransaction));
        }

        private void ValidateConnectionState()
        {
            if (_Connection?.State != ConnectionState.Open)
                throw new InvalidOperationException("Connection must be open to create savepoints");
        }

        #endregion
    }
}