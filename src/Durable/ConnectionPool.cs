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
        private readonly Func<DbConnection> _connectionFactory;
        private readonly ConnectionPoolOptions _options;
        private readonly ConcurrentQueue<PooledConnection> _availableConnections;
        private readonly ConcurrentBag<PooledConnection> _allConnections;
        private readonly SemaphoreSlim _semaphore;
        private readonly System.Timers.Timer _cleanupTimer;
        private volatile bool _disposed;
        private int _connectionCount;

        public ConnectionPool(Func<DbConnection> connectionFactory, ConnectionPoolOptions options = null)
        {
            _connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
            _options = options ?? new ConnectionPoolOptions();
            _availableConnections = new ConcurrentQueue<PooledConnection>();
            _allConnections = new ConcurrentBag<PooledConnection>();
            _semaphore = new SemaphoreSlim(_options.MaxPoolSize, _options.MaxPoolSize);

            InitializeMinConnections();

            _cleanupTimer = new System.Timers.Timer(TimeSpan.FromMinutes(1).TotalMilliseconds);
            _cleanupTimer.Elapsed += CleanupIdleConnections;
            _cleanupTimer.Start();
        }

        public async Task<DbConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();

            if (!await _semaphore.WaitAsync(_options.ConnectionTimeout, cancellationToken))
            {
                throw new TimeoutException("Timeout waiting for available connection from pool");
            }

            try
            {
                if (_availableConnections.TryDequeue(out var pooledConnection))
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

                var newConnection = await CreateNewConnectionAsync(cancellationToken);
                return newConnection;
            }
            catch
            {
                _semaphore.Release();
                throw;
            }
        }

        public DbConnection GetConnection()
        {
            ThrowIfDisposed();

            if (!_semaphore.Wait(_options.ConnectionTimeout))
            {
                throw new TimeoutException("Timeout waiting for available connection from pool");
            }

            try
            {
                if (_availableConnections.TryDequeue(out var pooledConnection))
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

                var newConnection = CreateNewConnection();
                return newConnection;
            }
            catch
            {
                _semaphore.Release();
                throw;
            }
        }

        public async Task ReturnConnectionAsync(DbConnection connection)
        {
            if (connection == null || _disposed)
                return;

            var pooledConnection = FindPooledConnection(connection);
            if (pooledConnection != null)
            {
                pooledConnection.IsInUse = false;
                pooledConnection.LastUsed = DateTime.UtcNow;

                if (IsConnectionValid(pooledConnection))
                {
                    _availableConnections.Enqueue(pooledConnection);
                }
                else
                {
                    await DisposePooledConnectionAsync(pooledConnection);
                }
            }

            _semaphore.Release();
        }

        public void ReturnConnection(DbConnection connection)
        {
            if (connection == null || _disposed)
                return;

            var pooledConnection = FindPooledConnection(connection);
            if (pooledConnection != null)
            {
                pooledConnection.IsInUse = false;
                pooledConnection.LastUsed = DateTime.UtcNow;

                if (IsConnectionValid(pooledConnection))
                {
                    _availableConnections.Enqueue(pooledConnection);
                }
                else
                {
                    DisposePooledConnection(pooledConnection);
                }
            }

            _semaphore.Release();
        }

        private void InitializeMinConnections()
        {
            for (int i = 0; i < _options.MinPoolSize; i++)
            {
                try
                {
                    var connection = _connectionFactory();
                    connection.Open(); // Ensure connections are opened during initialization
                    var pooledConnection = new PooledConnection(connection);
                    _allConnections.Add(pooledConnection);
                    _availableConnections.Enqueue(pooledConnection);
                    Interlocked.Increment(ref _connectionCount);
                }
                catch
                {
                    break;
                }
            }
        }

        private async Task<DbConnection> CreateNewConnectionAsync(CancellationToken cancellationToken)
        {
            var connection = _connectionFactory();
            await connection.OpenAsync(cancellationToken);
            
            var pooledConnection = new PooledConnection(connection) { IsInUse = true };
            _allConnections.Add(pooledConnection);
            Interlocked.Increment(ref _connectionCount);
            
            return connection;
        }

        private DbConnection CreateNewConnection()
        {
            var connection = _connectionFactory();
            connection.Open();
            
            var pooledConnection = new PooledConnection(connection) { IsInUse = true };
            _allConnections.Add(pooledConnection);
            Interlocked.Increment(ref _connectionCount);
            
            return connection;
        }

        private bool IsConnectionValid(PooledConnection pooledConnection)
        {
            if (!_options.ValidateConnections)
                return true;

            try
            {
                var connection = pooledConnection.Connection;
                return connection.State == System.Data.ConnectionState.Open &&
                       DateTime.UtcNow - pooledConnection.LastUsed < _options.IdleTimeout;
            }
            catch
            {
                return false;
            }
        }

        private PooledConnection FindPooledConnection(DbConnection connection)
        {
            foreach (var pooledConnection in _allConnections)
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
                Interlocked.Decrement(ref _connectionCount);
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
                Interlocked.Decrement(ref _connectionCount);
            }
        }

        private void CleanupIdleConnections(object sender, ElapsedEventArgs e)
        {
            if (_disposed)
                return;

            var cutoffTime = DateTime.UtcNow - _options.IdleTimeout;
            var connectionsToRemove = new List<PooledConnection>();

            // Collect idle connections while preserving minimum pool size
            while (_availableConnections.TryDequeue(out var pooledConnection))
            {
                if (pooledConnection.LastUsed < cutoffTime && _connectionCount > _options.MinPoolSize)
                {
                    connectionsToRemove.Add(pooledConnection);
                }
                else
                {
                    _availableConnections.Enqueue(pooledConnection);
                    break;
                }
            }

            // Dispose idle connections
            foreach (var connection in connectionsToRemove)
            {
                _ = Task.Run(async () => await DisposePooledConnectionAsync(connection));
            }
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(ConnectionPool));
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            _cleanupTimer?.Stop();
            _cleanupTimer?.Dispose();

            // Dispose all connections
            foreach (var pooledConnection in _allConnections)
            {
                try
                {
                    pooledConnection.Connection.Dispose();
                }
                catch { }
            }

            _semaphore?.Dispose();
        }

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