namespace Durable.Sqlite
{
    using System;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;

    /// <summary>
    /// Provides a factory for creating and managing SQLite database connections with connection pooling support.
    /// Implements connection pooling to improve performance and resource management for SQLite databases.
    /// </summary>
    public class SqliteConnectionFactory : IConnectionFactory
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private readonly ConnectionPool _ConnectionPool;
        private readonly string _ConnectionString;
        private volatile bool _Disposed;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the SqliteConnectionFactory with the specified connection string and pooling options.
        /// </summary>
        /// <param name="connectionString">The SQLite connection string used to create database connections.</param>
        /// <param name="options">Optional connection pool configuration settings. Uses default settings if null.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null.</exception>
        public SqliteConnectionFactory(string connectionString, ConnectionPoolOptions options = null)
        {
            _ConnectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _ConnectionPool = new ConnectionPool(() => new SqliteConnection(_ConnectionString), options);
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Retrieves a database connection from the connection pool synchronously.
        /// </summary>
        /// <returns>A ready-to-use SQLite database connection from the pool.</returns>
        /// <exception cref="ObjectDisposedException">Thrown when the factory has been disposed.</exception>
        public DbConnection GetConnection()
        {
            ThrowIfDisposed();
            return _ConnectionPool.GetConnection();
        }

        /// <summary>
        /// Retrieves a database connection from the connection pool asynchronously.
        /// </summary>
        /// <param name="cancellationToken">A cancellation token to cancel the operation if needed.</param>
        /// <returns>A task representing the asynchronous operation that returns a ready-to-use SQLite database connection from the pool.</returns>
        /// <exception cref="ObjectDisposedException">Thrown when the factory has been disposed.</exception>
        /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled via the cancellation token.</exception>
        public Task<DbConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            return _ConnectionPool.GetConnectionAsync(cancellationToken);
        }

        /// <summary>
        /// Returns a database connection to the connection pool for reuse.
        /// </summary>
        /// <param name="connection">The database connection to return to the pool. Null connections are safely ignored.</param>
        public void ReturnConnection(DbConnection connection)
        {
            if (!_Disposed && connection != null)
            {
                _ConnectionPool.ReturnConnection(connection);
            }
        }

        /// <summary>
        /// Returns a database connection to the connection pool for reuse asynchronously.
        /// </summary>
        /// <param name="connection">The database connection to return to the pool. Null connections are safely ignored.</param>
        /// <returns>A task representing the asynchronous return operation.</returns>
        public Task ReturnConnectionAsync(DbConnection connection)
        {
            if (_Disposed || connection == null)
                return Task.CompletedTask;

            return _ConnectionPool.ReturnConnectionAsync(connection);
        }

        /// <summary>
        /// Disposes of the connection factory and releases all managed resources including the connection pool.
        /// All connections in the pool will be closed and disposed, and SQLite's internal connection pool will be cleared.
        /// </summary>
        public void Dispose()
        {
            if (_Disposed)
                return;

            _Disposed = true;
            _ConnectionPool?.Dispose();

            // Clear SQLite's internal connection pool to ensure all file locks are released
            // This is necessary because SQLite maintains its own ADO.NET connection pool
            // independent of our custom ConnectionPool implementation
            SqliteConnection.ClearAllPools();
        }

        #endregion

        #region Private-Methods

        private void ThrowIfDisposed()
        {
            if (_Disposed)
                throw new ObjectDisposedException(nameof(SqliteConnectionFactory));
        }

        #endregion
    }
}