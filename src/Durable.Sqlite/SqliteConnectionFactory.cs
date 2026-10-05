namespace Durable.Sqlite
{
    using System;
    using System.Data.Common;
    using System.Globalization;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    using Durable.Sql;

    /// <summary>
    /// Opens SQLite connections, relying on Microsoft.Data.Sqlite's built-in pooling.
    /// In-memory databases stay alive for the factory's lifetime: a private ":memory:" data source becomes a uniquely
    /// named in-memory database so every connection from this factory sees the same data, and one connection is kept open
    /// until the factory is disposed. It uses SQLite's "memdb" VFS (<c>file:/name?vfs=memdb</c>) rather than
    /// shared-cache mode: memdb uses normal database locking, so concurrent connections wait for each other (see
    /// <see cref="BusyTimeoutMilliseconds"/>) instead of hitting shared-cache table locks, which Microsoft.Data.Sqlite
    /// surfaces as an <see cref="ArgumentOutOfRangeException"/>. Other connection strings are used as given, so raw
    /// connections opened with the same string see the same database; to share a named in-memory database safely under
    /// concurrency, use <c>Data Source=file:/name?vfs=memdb</c> rather than <c>Mode=Memory;Cache=Shared</c>.
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

        /// <summary>
        /// Gets or sets how long SQLite waits for another connection's lock before failing with "database is locked",
        /// applied with <c>PRAGMA busy_timeout</c> to every connection the factory opens. Without it, concurrent writers
        /// fail when a commit cannot get the write lock immediately. 0 leaves SQLite's default (no waiting).
        /// Default: 30000 (30 seconds). Minimum: 0.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is negative.</exception>
        public int BusyTimeoutMilliseconds
        {
            get => _BusyTimeoutMilliseconds;
            set
            {
                if (value < 0) throw new ArgumentOutOfRangeException(nameof(value), "BusyTimeoutMilliseconds cannot be negative.");
                _BusyTimeoutMilliseconds = value;
            }
        }

        #endregion

        #region Private-Members

        private readonly object _KeepAliveLock = new object();
        private SqliteConnection? _KeepAlive;
        private int _BusyTimeoutMilliseconds = 30000;

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
                builder.DataSource = MemoryDatabaseUri("durable-" + Guid.NewGuid().ToString("N"));
                builder.Mode = SqliteOpenMode.ReadWriteCreate;
                builder.Cache = SqliteCacheMode.Default;
            }

            IsInMemory = builder.Mode == SqliteOpenMode.Memory || builder.DataSource.Contains("vfs=memdb", StringComparison.OrdinalIgnoreCase);
            ConnectionString = builder.ToString();
        }

        #endregion

        #region Private-Methods

        private string BusyTimeoutSql()
        {
            return "PRAGMA busy_timeout = " + _BusyTimeoutMilliseconds.ToString(CultureInfo.InvariantCulture);
        }

        private static string MemoryDatabaseUri(string name)
        {
            return "file:/" + Uri.EscapeDataString(name) + "?vfs=memdb";
        }

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
        protected override void OnConnectionOpened(DbConnection connection)
        {
            ArgumentNullException.ThrowIfNull(connection);
            if (_BusyTimeoutMilliseconds == 0) return;
            using DbCommand command = connection.CreateCommand();
            command.CommandText = BusyTimeoutSql();
            command.ExecuteNonQuery();
        }

        /// <inheritdoc />
        protected override async Task OnConnectionOpenedAsync(DbConnection connection, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(connection);
            if (_BusyTimeoutMilliseconds == 0) return;
            DbCommand command = connection.CreateCommand();
            await using (command.ConfigureAwait(false))
            {
                command.CommandText = BusyTimeoutSql();
                await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            }
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
