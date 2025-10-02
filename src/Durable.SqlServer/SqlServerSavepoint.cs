#nullable enable
namespace Durable.SqlServer
{
    using System;
    using System.Data;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;

    /// <summary>
    /// SQL Server-specific implementation of ISavepoint for managing transaction savepoints.
    /// SQL Server uses SAVE TRANSACTION and ROLLBACK TRANSACTION TO syntax for savepoints.
    /// Note: SQL Server does not support RELEASE SAVEPOINT - savepoints are automatically released on commit.
    /// </summary>
    internal class SqlServerSavepoint : ISavepoint
    {

        #region Public-Members

        /// <summary>
        /// Gets the name of the savepoint.
        /// </summary>
        public string Name { get; }

        #endregion

        #region Private-Members

        private readonly SqlConnection _Connection;
        private readonly SqlTransaction _Transaction;
        private bool _Disposed;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the SqlServerSavepoint class.
        /// Creates the savepoint immediately using SQL Server SAVE TRANSACTION syntax.
        /// </summary>
        /// <param name="connection">The SQL Server connection associated with the savepoint</param>
        /// <param name="transaction">The SQL Server transaction associated with the savepoint</param>
        /// <param name="name">The name of the savepoint</param>
        /// <param name="createImmediately">Whether to create the savepoint immediately via SQL</param>
        /// <exception cref="ArgumentNullException">Thrown when any parameter is null</exception>
        /// <exception cref="ArgumentException">Thrown when name is null or empty</exception>
        public SqlServerSavepoint(SqlConnection connection, SqlTransaction transaction, string name, bool createImmediately = true)
        {
            _Connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _Transaction = transaction ?? throw new ArgumentNullException(nameof(transaction));
            Name = !string.IsNullOrEmpty(name) ? name : throw new ArgumentException("Savepoint name cannot be null or empty", nameof(name));

            if (createImmediately)
            {
                using SqlCommand command = new SqlCommand($"SAVE TRANSACTION {Name}", _Connection, _Transaction);
                command.ExecuteNonQuery();
            }
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Rolls back the transaction to this savepoint.
        /// SQL Server uses ROLLBACK TRANSACTION TO savepoint_name syntax.
        /// </summary>
        /// <exception cref="ObjectDisposedException">Thrown when the savepoint has been disposed</exception>
        /// <exception cref="InvalidOperationException">Thrown when the connection is not open</exception>
        public void Rollback()
        {
            ThrowIfDisposed();
            ValidateConnectionState();

            using SqlCommand command = new SqlCommand($"ROLLBACK TRANSACTION {Name}", _Connection, _Transaction);
            command.ExecuteNonQuery();
        }

        /// <summary>
        /// Asynchronously rolls back the transaction to this savepoint.
        /// SQL Server uses ROLLBACK TRANSACTION TO savepoint_name syntax.
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

            using SqlCommand command = new SqlCommand($"ROLLBACK TRANSACTION {Name}", _Connection, _Transaction);
            await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        /// <summary>
        /// Releases the savepoint (no-op for SQL Server).
        /// SQL Server does not support explicit RELEASE SAVEPOINT - savepoints are automatically released
        /// when the transaction commits or when an outer savepoint is rolled back.
        /// This method exists for interface compatibility but performs no operation.
        /// </summary>
        /// <exception cref="ObjectDisposedException">Thrown when the savepoint has been disposed</exception>
        public void Release()
        {
            ThrowIfDisposed();
            // SQL Server doesn't support RELEASE SAVEPOINT - this is a no-op for compatibility
        }

        /// <summary>
        /// Asynchronously releases the savepoint (no-op for SQL Server).
        /// SQL Server does not support explicit RELEASE SAVEPOINT - savepoints are automatically released
        /// when the transaction commits or when an outer savepoint is rolled back.
        /// This method exists for interface compatibility but performs no operation.
        /// </summary>
        /// <param name="token">A cancellation token that can be used to cancel the operation</param>
        /// <returns>A completed task</returns>
        /// <exception cref="ObjectDisposedException">Thrown when the savepoint has been disposed</exception>
        public Task ReleaseAsync(CancellationToken token = default)
        {
            ThrowIfDisposed();
            // SQL Server doesn't support RELEASE SAVEPOINT - this is a no-op for compatibility
            return Task.CompletedTask;
        }

        /// <summary>
        /// Disposes of the savepoint.
        /// Note: SQL Server automatically releases savepoints on transaction commit,
        /// so no explicit cleanup is needed.
        /// </summary>
        public void Dispose()
        {
            if (!_Disposed)
            {
                // SQL Server doesn't require explicit savepoint release
                _Disposed = true;
            }
        }

        #endregion

        #region Private-Methods

        private void ThrowIfDisposed()
        {
            if (_Disposed)
                throw new ObjectDisposedException(nameof(SqlServerSavepoint));
        }

        private void ValidateConnectionState()
        {
            if (_Connection?.State != ConnectionState.Open)
                throw new InvalidOperationException("Connection must be open to use savepoints");
        }

        #endregion
    }
}
