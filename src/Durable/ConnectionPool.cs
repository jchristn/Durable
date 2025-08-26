namespace Durable
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Timers;

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

        public async Task<DbConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();

            if (!await _Semaphore.WaitAsync(_Options.ConnectionTimeout, cancellationToken))
            {
                throw new TimeoutException("Timeout waiting for available connection from pool");
            }

            try
            {
                if (_AvailableConnections.TryDequeue(out PooledConnection pooledConnection))
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

        public DbConnection GetConnection()
        {
            ThrowIfDisposed();

            if (!_Semaphore.Wait(_Options.ConnectionTimeout))
            {
                throw new TimeoutException("Timeout waiting for available connection from pool");
            }

            try
            {
                if (_AvailableConnections.TryDequeue(out PooledConnection pooledConnection))
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

        public async Task ReturnConnectionAsync(DbConnection connection)
        {
            if (connection == null || _Disposed)
                return;

            PooledConnection pooledConnection = FindPooledConnection(connection);
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

        public void ReturnConnection(DbConnection connection)
        {
            if (connection == null || _Disposed)
                return;

            PooledConnection pooledConnection = FindPooledConnection(connection);
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

        public void Dispose()
        {
            if (_Disposed)
                return;

            _Disposed = true;
            _CleanupTimer?.Stop();
            _CleanupTimer?.Dispose();

            // Dispose all connections
            foreach (PooledConnection pooledConnection in _AllConnections)
            {
                try
                {
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
            while (_AvailableConnections.TryDequeue(out PooledConnection pooledConnection))
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

        private class PooledConnection
        {
            public DbConnection Connection { get; }
            public DateTime Created { get; }
            public DateTime LastUsed { get; set; }
            public bool IsInUse { get; set; }

            public PooledConnection(DbConnection connection)
            {
                Connection = connection;
                Created = DateTime.UtcNow;
                LastUsed = DateTime.UtcNow;
                IsInUse = false;
            }
        }
    }
}