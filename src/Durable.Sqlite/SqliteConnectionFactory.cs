namespace Durable.Sqlite
{
    using System;
    using System.Data.Common;
    using Microsoft.Data.Sqlite;
    using Durable.Sql;

    /// <summary>
    /// Opens SQLite connections, relying on Microsoft.Data.Sqlite's built-in pooling.
    /// In-memory databases stay alive for the factory's lifetime: a private ":memory:" data source is converted to a
    /// uniquely named shared-cache database so every connection from this factory sees the same data, and one connection
    /// is kept open until the factory is disposed.
    /// Thread safety: safe for concurrent use.
    /// </summary>
    public sealed class SqliteConnectionFactory : ConnectionFactory
    {
        #region Public-Members

        /// <summary>
        /// Gets the effective connection string. Never null.
        /// </summary>
        public string ConnectionString { get; }

        /// <summary>
        /// Gets whether the database is in memory.
        /// </summary>
        public bool IsInMemory { get; }

        #endregion

        #region Private-Members

        private readonly object _KeepAliveLock = new object();
        private SqliteConnection? _KeepAlive;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the factory.
        /// </summary>
        /// <param name="connectionString">SQLite connection string. Must not be null.</param>
        /// <param name="maxConcurrentConnections">Optional cap on concurrently open connections; null for none.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null.</exception>
        public SqliteConnectionFactory(string connectionString, int? maxConcurrentConnections = null) : base(maxConcurrentConnections)
        {
            ArgumentNullException.ThrowIfNull(connectionString);
            SqliteConnectionStringBuilder builder = new SqliteConnectionStringBuilder(connectionString);
            if (string.Equals(builder.DataSource, ":memory:", StringComparison.OrdinalIgnoreCase))
            {
                builder.DataSource = "durable-" + Guid.NewGuid().ToString("N");
                builder.Mode = SqliteOpenMode.Memory;
                builder.Cache = SqliteCacheMode.Shared;
            }

            IsInMemory = builder.Mode == SqliteOpenMode.Memory;
            ConnectionString = builder.ToString();
        }

        #endregion

        #region Private-Methods

        /// <inheritdoc />
        protected override DbConnection CreateConnection()
        {
            if (IsInMemory && _KeepAlive == null)
            {
                lock (_KeepAliveLock)
                {
                    if (_KeepAlive == null)
                    {
                        SqliteConnection keepAlive = new SqliteConnection(ConnectionString);
                        keepAlive.Open();
                        _KeepAlive = keepAlive;
                    }
                }
            }

            return new SqliteConnection(ConnectionString);
        }

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                lock (_KeepAliveLock)
                {
                    _KeepAlive?.Dispose();
                    _KeepAlive = null;
                }

                using (SqliteConnection connection = new SqliteConnection(ConnectionString))
                {
                    SqliteConnection.ClearPool(connection);
                }
            }

            base.Dispose(disposing);
        }

        #endregion
    }
}
