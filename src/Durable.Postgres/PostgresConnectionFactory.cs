namespace Durable.Postgres
{
    using System;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;

    /// <summary>
    /// Provides a factory for creating and managing PostgreSQL database connections with connection pooling support.
    /// Implements connection pooling to improve performance and resource management for PostgreSQL databases.
    /// </summary>
    public class PostgresConnectionFactory : IConnectionFactory
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
        /// Initializes a new instance of the PostgresConnectionFactory with the specified connection string and pooling options.
        /// </summary>
        /// <param name="connectionString">The PostgreSQL connection string used to create database connections.</param>
        /// <param name="options">Optional connection pool configuration settings. Uses default settings if null.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null.</exception>
        public PostgresConnectionFactory(string connectionString, ConnectionPoolOptions? options = null)
        {
            _ConnectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _ConnectionPool = new ConnectionPool(() => new NpgsqlConnection(_ConnectionString), options);
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Retrieves a database connection from the connection pool synchronously.
        /// </summary>
        /// <returns>A ready-to-use PostgreSQL database connection from the pool.</returns>
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
        /// <returns>A task representing the asynchronous operation that returns a ready-to-use PostgreSQL database connection from the pool.</returns>
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
        /// Asynchronously returns a database connection to the connection pool for reuse.
        /// </summary>
        /// <param name="connection">The database connection to return to the pool. Null connections are safely ignored.</param>
        /// <returns>A task representing the asynchronous return operation.</returns>
        public Task ReturnConnectionAsync(DbConnection connection)
        {
            if (!_Disposed && connection != null)
            {
                return _ConnectionPool.ReturnConnectionAsync(connection);
            }
            return Task.CompletedTask;
        }

        /// <summary>
        /// Releases all resources used by the PostgresConnectionFactory.
        /// Closes all pooled connections and disposes of the connection pool.
        /// </summary>
        public void Dispose()
        {
            if (!_Disposed)
            {
                _ConnectionPool?.Dispose();
                _Disposed = true;
            }
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Throws an ObjectDisposedException if this factory instance has been disposed.
        /// </summary>
        /// <exception cref="ObjectDisposedException">Thrown when the factory has been disposed.</exception>
        private void ThrowIfDisposed()
        {
            if (_Disposed)
            {
                throw new ObjectDisposedException(nameof(PostgresConnectionFactory), "Connection factory has been disposed.");
            }
        }

        #endregion
    }
}