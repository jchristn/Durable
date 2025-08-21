namespace Durable.Sqlite
{
    using System;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;

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

        public SqliteConnectionFactory(string connectionString, ConnectionPoolOptions options = null)
        {
            _ConnectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _ConnectionPool = new ConnectionPool(() => new SqliteConnection(_ConnectionString), options);
        }

        #endregion

        #region Public-Methods

        public DbConnection GetConnection()
        {
            ThrowIfDisposed();
            return _ConnectionPool.GetConnection();
        }

        public Task<DbConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            return _ConnectionPool.GetConnectionAsync(cancellationToken);
        }

        public void ReturnConnection(DbConnection connection)
        {
            if (!_Disposed && connection != null)
            {
                _ConnectionPool.ReturnConnection(connection);
            }
        }

        public Task ReturnConnectionAsync(DbConnection connection)
        {
            if (_Disposed || connection == null)
                return Task.CompletedTask;

            return _ConnectionPool.ReturnConnectionAsync(connection);
        }

        public void Dispose()
        {
            if (_Disposed)
                return;

            _Disposed = true;
            _ConnectionPool?.Dispose();
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