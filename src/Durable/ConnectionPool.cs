namespace Durable
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Timers;

    /// <summary>
    /// Provides a thread-safe connection pool for database connections with automatic cleanup and connection validation.
    /// </summary>
    public class ConnectionPool : IDisposable
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private readonly Func<DbConnection> _ConnectionFactory;
        private readonly ConnectionPoolOptions _Options;
        private readonly ConcurrentQueue<PooledConnection> _AvailableConnections;
        private readonly ConcurrentBag<PooledConnection> _AllConnections;
        private readonly SemaphoreSlim _Semaphore;
        private readonly System.Timers.Timer _CleanupTimer;
        private volatile bool _Disposed;
        private int _ConnectionCount;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the ConnectionPool class.
        /// </summary>
        /// <param name="connectionFactory">Factory function to create new database connections.</param>
        /// <param name="options">Configuration options for the connection pool. If null, default options are used.</param>
        public ConnectionPool(Func<DbConnection> connectionFactory, ConnectionPoolOptions? options = null)
        {
            _ConnectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
            _Options = options ?? new ConnectionPoolOptions();
            _AvailableConnections = new ConcurrentQueue<PooledConnection>();
            _AllConnections = new ConcurrentBag<PooledConnection>();
            _Semaphore = new SemaphoreSlim(_Options.MaxPoolSize, _Options.MaxPoolSize);

            InitializeMinConnections();

            _CleanupTimer = new System.Timers.Timer(TimeSpan.FromMinutes(1).TotalMilliseconds);
            _CleanupTimer.Elapsed += CleanupIdleConnections;
            _CleanupTimer.Start();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Asynchronously retrieves a database connection from the pool.
        /// </summary>
        /// <param name="cancellationToken">Token to cancel the operation.</param>
        /// <returns>A database connection from the pool.</returns>
        /// <exception cref="TimeoutException">Thrown when no connection becomes available within the timeout period.</exception>
        public async Task<DbConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();

            if (!await _Semaphore.WaitAsync(_Options.ConnectionTimeout, cancellationToken))
            {
                throw new TimeoutException("Timeout waiting for available connection from pool");
            }

            try
            {
                if (_AvailableConnections.TryDequeue(out PooledConnection? pooledConnection) && pooledConnection != null)
                {
                    if (IsConnectionValid(pooledConnection))
                    {
                        pooledConnection.LastUsed = DateTime.UtcNow;
                        pooledConnection.IsInUse = true;
                        return pooledConnection.Connection;
                    }
                    else
                    {
                        await DisposePooledConnectionAsync(pooledConnection);
                    }
                }

                DbConnection newConnection = await CreateNewConnectionAsync(cancellationToken);
                return newConnection;
            }
            catch
            {
                _Semaphore.Release();
                throw;
            }
        }

        /// <summary>
        /// Synchronously retrieves a database connection from the pool.
        /// </summary>
        /// <returns>A database connection from the pool.</returns>
        /// <exception cref="TimeoutException">Thrown when no connection becomes available within the timeout period.</exception>
        public DbConnection GetConnection()
        {
            ThrowIfDisposed();

            if (!_Semaphore.Wait(_Options.ConnectionTimeout))
            {
                throw new TimeoutException("Timeout waiting for available connection from pool");
            }

            try
            {
                if (_AvailableConnections.TryDequeue(out PooledConnection? pooledConnection) && pooledConnection != null)
                {
                    if (IsConnectionValid(pooledConnection))
                    {
                        pooledConnection.LastUsed = DateTime.UtcNow;
                        pooledConnection.IsInUse = true;
                        return pooledConnection.Connection;
                    }
                    else
                    {
                        DisposePooledConnection(pooledConnection);
                    }
                }

                DbConnection newConnection = CreateNewConnection();
                return newConnection;
            }
            catch
            {
                _Semaphore.Release();
                throw;
            }
        }

        /// <summary>
        /// Asynchronously returns a database connection to the pool.
        /// </summary>
        /// <param name="connection">The connection to return to the pool.</param>
        public async Task ReturnConnectionAsync(DbConnection connection)
        {
            if (connection == null || _Disposed)
                return;

            PooledConnection? pooledConnection = FindPooledConnection(connection);
            if (pooledConnection != null)
            {
                pooledConnection.IsInUse = false;
                pooledConnection.LastUsed = DateTime.UtcNow;

                if (IsConnectionValid(pooledConnection))
                {
                    _AvailableConnections.Enqueue(pooledConnection);
                }
                else
                {
                    await DisposePooledConnectionAsync(pooledConnection);
                }
            }

            _Semaphore.Release();
        }

        /// <summary>
        /// Synchronously returns a database connection to the pool.
        /// </summary>
        /// <param name="connection">The connection to return to the pool.</param>
        public void ReturnConnection(DbConnection connection)
        {
            if (connection == null || _Disposed)
                return;

            PooledConnection? pooledConnection = FindPooledConnection(connection);
            if (pooledConnection != null)
            {
                pooledConnection.IsInUse = false;
                pooledConnection.LastUsed = DateTime.UtcNow;

                if (IsConnectionValid(pooledConnection))
                {
                    _AvailableConnections.Enqueue(pooledConnection);
                }
                else
                {
                    DisposePooledConnection(pooledConnection);
                }
            }

            _Semaphore.Release();
        }

        /// <summary>
        /// Releases all resources used by the ConnectionPool.
        /// </summary>
        public void Dispose()
        {
            if (_Disposed)
                return;

            _Disposed = true;
            _CleanupTimer?.Stop();
            _CleanupTimer?.Dispose();

            // Close and dispose all connections
            foreach (PooledConnection pooledConnection in _AllConnections)
            {
                try
                {
                    if (pooledConnection.Connection.State != System.Data.ConnectionState.Closed)
                    {
                        pooledConnection.Connection.Close();
                    }
                    pooledConnection.Connection.Dispose();
                }
                catch { }
            }

            _Semaphore?.Dispose();
        }

        #endregion

        #region Private-Methods

        private void InitializeMinConnections()
        {
            for (int i = 0; i < _Options.MinPoolSize; i++)
            {
                try
                {
                    DbConnection connection = _ConnectionFactory();
                    connection.Open(); // Ensure connections are opened during initialization
                    PooledConnection pooledConnection = new PooledConnection(connection);
                    _AllConnections.Add(pooledConnection);
                    _AvailableConnections.Enqueue(pooledConnection);
                    Interlocked.Increment(ref _ConnectionCount);
                }
                catch
                {
                    break;
                }
            }
        }

        private async Task<DbConnection> CreateNewConnectionAsync(CancellationToken cancellationToken)
        {
            DbConnection connection = _ConnectionFactory();
            await connection.OpenAsync(cancellationToken);
            
            PooledConnection pooledConnection = new PooledConnection(connection) { IsInUse = true };
            _AllConnections.Add(pooledConnection);
            Interlocked.Increment(ref _ConnectionCount);
            
            return connection;
        }

        private DbConnection CreateNewConnection()
        {
            DbConnection connection = _ConnectionFactory();
            connection.Open();
            
            PooledConnection pooledConnection = new PooledConnection(connection) { IsInUse = true };
            _AllConnections.Add(pooledConnection);
            Interlocked.Increment(ref _ConnectionCount);
            
            return connection;
        }

        private bool IsConnectionValid(PooledConnection pooledConnection)
        {
            if (!_Options.ValidateConnections)
                return true;

            try
            {
                DbConnection connection = pooledConnection.Connection;
                return connection.State == System.Data.ConnectionState.Open &&
                       DateTime.UtcNow - pooledConnection.LastUsed < _Options.IdleTimeout;
            }
            catch
            {
                return false;
            }
        }

        private PooledConnection? FindPooledConnection(DbConnection connection)
        {
            foreach (PooledConnection pooledConnection in _AllConnections)
            {
                if (ReferenceEquals(pooledConnection.Connection, connection))
                {
                    return pooledConnection;
                }
            }
            return null;
        }

        private async Task DisposePooledConnectionAsync(PooledConnection pooledConnection)
        {
            try
            {
                await pooledConnection.Connection.DisposeAsync();
            }
            catch { }
            finally
            {
                Interlocked.Decrement(ref _ConnectionCount);
            }
        }

        private void DisposePooledConnection(PooledConnection pooledConnection)
        {
            try
            {
                pooledConnection.Connection.Dispose();
            }
            catch { }
            finally
            {
                Interlocked.Decrement(ref _ConnectionCount);
            }
        }

        private void CleanupIdleConnections(object? sender, ElapsedEventArgs e)
        {
            if (_Disposed)
                return;

            DateTime cutoffTime = DateTime.UtcNow - _Options.IdleTimeout;
            List<PooledConnection> connectionsToRemove = new List<PooledConnection>();

            // Collect idle connections while preserving minimum pool size
            while (_AvailableConnections.TryDequeue(out PooledConnection? pooledConnection) && pooledConnection != null)
            {
                if (pooledConnection.LastUsed < cutoffTime && _ConnectionCount > _Options.MinPoolSize)
                {
                    connectionsToRemove.Add(pooledConnection);
                }
                else
                {
                    _AvailableConnections.Enqueue(pooledConnection);
                    break;
                }
            }

            // Dispose idle connections
            foreach (PooledConnection connection in connectionsToRemove)
            {
                _ = Task.Run(async () => await DisposePooledConnectionAsync(connection));
            }
        }

        private void ThrowIfDisposed()
        {
            if (_Disposed)
                throw new ObjectDisposedException(nameof(ConnectionPool));
        }

        #endregion

    }
}