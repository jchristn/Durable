namespace Durable.Sqlite
{
    using System;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;

    public class SqliteConnectionFactory : IConnectionFactory
    {
        private readonly ConnectionPool _connectionPool;
        private readonly string _connectionString;
        private volatile bool _disposed;

        public SqliteConnectionFactory(string connectionString, ConnectionPoolOptions options = null)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _connectionPool = new ConnectionPool(() => new SqliteConnection(_connectionString), options);
        }

        public DbConnection GetConnection()
        {
            ThrowIfDisposed();
            return _connectionPool.GetConnection();
        }

        public Task<DbConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            return _connectionPool.GetConnectionAsync(cancellationToken);
        }

        public void ReturnConnection(DbConnection connection)
        {
            if (!_disposed && connection != null)
            {
                _connectionPool.ReturnConnection(connection);
            }
        }

        public Task ReturnConnectionAsync(DbConnection connection)
        {
            if (_disposed || connection == null)
                return Task.CompletedTask;

            return _connectionPool.ReturnConnectionAsync(connection);
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(SqliteConnectionFactory));
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            _connectionPool?.Dispose();
        }
    }

    public static class SqliteConnectionFactoryExtensions
    {
        public static SqliteConnectionFactory CreateFactory(this string connectionString, Action<ConnectionPoolOptions> configureOptions = null)
        {
            var options = new ConnectionPoolOptions();
            configureOptions?.Invoke(options);
            return new SqliteConnectionFactory(connectionString, options);
        }
    }
}