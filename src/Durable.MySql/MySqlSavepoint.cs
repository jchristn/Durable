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
    /// MySQL-specific implementation of ISavepoint for managing transaction savepoints.
    /// </summary>
    internal class MySqlSavepoint : ISavepoint
    {
        #region Public-Members

        /// <summary>
        /// Gets the name of the savepoint.
        /// </summary>
        public string Name { get; }

        #endregion

        #region Private-Members

        private readonly MySqlConnection _Connection;
        private readonly MySqlTransaction _Transaction;
        private bool _Disposed;
        private readonly bool _CreatedViaSql;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the MySqlSavepoint class.
        /// Creates the savepoint immediately using MySQL SAVEPOINT syntax.
        /// </summary>
        /// <param name="connection">The MySQL connection associated with the savepoint</param>
        /// <param name="transaction">The MySQL transaction associated with the savepoint</param>
        /// <param name="name">The name of the savepoint</param>
        /// <param name="createImmediately">Whether to create the savepoint immediately via SQL</param>
        /// <exception cref="ArgumentNullException">Thrown when any parameter is null</exception>
        /// <exception cref="ArgumentException">Thrown when name is null or empty</exception>
        public MySqlSavepoint(MySqlConnection connection, MySqlTransaction transaction, string name, bool createImmediately = true)
        {
            _Connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _Transaction = transaction ?? throw new ArgumentNullException(nameof(transaction));
            Name = !string.IsNullOrEmpty(name) ? name : throw new ArgumentException("Savepoint name cannot be null or empty", nameof(name));

            if (createImmediately)
            {
                using MySqlCommand command = new MySqlCommand($"SAVEPOINT `{Name}`", _Connection, _Transaction);
                command.ExecuteNonQuery();
                _CreatedViaSql = true;
            }
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Rolls back the transaction to this savepoint.
        /// </summary>
        /// <exception cref="ObjectDisposedException">Thrown when the savepoint has been disposed</exception>
        /// <exception cref="InvalidOperationException">Thrown when the connection is not open</exception>
        public void Rollback()
        {
            ThrowIfDisposed();
            ValidateConnectionState();

            using MySqlCommand command = new MySqlCommand($"ROLLBACK TO SAVEPOINT `{Name}`", _Connection, _Transaction);
            command.ExecuteNonQuery();
        }

        /// <summary>
        /// Asynchronously rolls back the transaction to this savepoint.
        /// </summary>
        /// <param name="token">A cancellation token that can be used to cancel the operation</param>
        /// <returns>A task that represents the asynchronous rollback operation</returns>
        /// <exception cref="ObjectDisposedException">Thrown when the savepoint has been disposed</exception>
        /// <exception cref="InvalidOperationException">Thrown when the connection is not open</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled via the cancellation token</exception>
        public async Task RollbackAsync(CancellationToken token = default)
        {
            ThrowIfDisposed();
            ValidateConnectionState();

            using MySqlCommand command = new MySqlCommand($"ROLLBACK TO SAVEPOINT `{Name}`", _Connection, _Transaction);
            await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        /// <summary>
        /// Releases the savepoint, removing it from the transaction.
        /// Once released, the savepoint can no longer be used for rollback.
        /// </summary>
        /// <exception cref="ObjectDisposedException">Thrown when the savepoint has been disposed</exception>
        /// <exception cref="InvalidOperationException">Thrown when the connection is not open</exception>
        public void Release()
        {
            ThrowIfDisposed();
            ValidateConnectionState();

            using MySqlCommand command = new MySqlCommand($"RELEASE SAVEPOINT `{Name}`", _Connection, _Transaction);
            command.ExecuteNonQuery();
        }

        /// <summary>
        /// Asynchronously releases the savepoint, removing it from the transaction.
        /// Once released, the savepoint can no longer be used for rollback.
        /// </summary>
        /// <param name="token">A cancellation token that can be used to cancel the operation</param>
        /// <returns>A task that represents the asynchronous release operation</returns>
        /// <exception cref="ObjectDisposedException">Thrown when the savepoint has been disposed</exception>
        /// <exception cref="InvalidOperationException">Thrown when the connection is not open</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled via the cancellation token</exception>
        public async Task ReleaseAsync(CancellationToken token = default)
        {
            ThrowIfDisposed();
            ValidateConnectionState();

            using MySqlCommand command = new MySqlCommand($"RELEASE SAVEPOINT `{Name}`", _Connection, _Transaction);
            await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        /// <summary>
        /// Disposes of the savepoint, automatically releasing it if it hasn't been released already.
        /// </summary>
        public void Dispose()
        {
            if (!_Disposed)
            {
                try
                {
                    if (_CreatedViaSql && _Connection?.State == ConnectionState.Open)
                    {
                        Release();
                    }
                }
                catch
                {
                    // Ignore exceptions during dispose - the transaction may have been rolled back or committed
                }
                finally
                {
                    _Disposed = true;
                }
            }
        }

        #endregion

        #region Private-Methods

        private void ThrowIfDisposed()
        {
            if (_Disposed)
                throw new ObjectDisposedException(nameof(MySqlSavepoint));
        }

        private void ValidateConnectionState()
        {
            if (_Connection?.State != ConnectionState.Open)
                throw new InvalidOperationException("Connection must be open to use savepoints");
        }

        #endregion
    }
}