namespace Durable.Sqlite
{
    using System;
    using System.Data.Common;
    using System.Globalization;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    using SQLitePCL;
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
    /// Disposing the factory releases the private ":memory:" database. It does not clear the driver's connection pool for
    /// other connection strings, because other factories may be using that pool concurrently; to release a database file or
    /// a named in-memory database once nothing uses it, call <c>SqliteConnection.ClearPool</c> or
    /// <c>SqliteConnection.ClearAllPools</c>.
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
        private bool _OwnsPrivateDatabase;
        private int _BusyTimeoutMilliseconds = 30000;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the factory from strongly-typed settings (<see cref="SqliteRepositorySettings.BuildConnectionString"/>).
        /// </summary>
        /// <param name="settings">Settings. Must not be null.</param>
        /// <param name="maxConcurrentConnections">Optional cap on concurrently open connections; null for none.</param>
        /// <exception cref="ArgumentNullException">Thrown when settings is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the settings lack required values (see <see cref="SqliteRepositorySettings.BuildConnectionString"/>).</exception>
        public SqliteConnectionFactory(SqliteRepositorySettings settings, int? maxConcurrentConnections = null)
            : this(BuildConnectionString(settings), maxConcurrentConnections)
        {
        }

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
                _OwnsPrivateDatabase = true;
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

        private static bool InStaleTransaction(DbConnection connection)
        {
            // A freshly leased connection has no Microsoft.Data.Sqlite transaction; autocommit off means the native
            // connection is still inside a transaction from its previous lease.
            if (connection is not SqliteConnection sqlite || sqlite.Handle == null) return false;
            return raw.sqlite3_get_autocommit(sqlite.Handle) == 0;
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

        /// <summary>
        /// Prepares a newly opened connection: rolls back a transaction the driver's pool left open on it (see below) and
        /// applies <see cref="BusyTimeoutMilliseconds"/>.
        /// Microsoft.Data.Sqlite returns a native connection to its pool without checking that its transaction ended, for
        /// example after a transaction started with raw SQL (<c>BEGIN</c>, <c>SAVEPOINT</c>) or one whose <c>ROLLBACK</c>
        /// failed. The next lease of that native connection would then run inside the stale transaction, and its
        /// <c>BeginTransaction</c> would fail with "cannot start a transaction within a transaction". A connection leased
        /// from this factory never starts inside a transaction.
        /// </summary>
        /// <param name="connection">The opened connection. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="connection"/> is null.</exception>
        protected override void OnConnectionOpened(DbConnection connection)
        {
            ArgumentNullException.ThrowIfNull(connection);
            if (InStaleTransaction(connection))
            {
                using DbCommand rollback = connection.CreateCommand();
                rollback.CommandText = "ROLLBACK";
                rollback.ExecuteNonQuery();
            }

            if (_BusyTimeoutMilliseconds == 0) return;
            using DbCommand command = connection.CreateCommand();
            command.CommandText = BusyTimeoutSql();
            command.ExecuteNonQuery();
        }

        /// <summary>
        /// Asynchronous <see cref="OnConnectionOpened"/>: rolls back a transaction the driver's pool left open on the
        /// connection and applies <see cref="BusyTimeoutMilliseconds"/>.
        /// </summary>
        /// <param name="connection">The opened connection. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task that completes when the connection is ready.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="connection"/> is null.</exception>
        protected override async Task OnConnectionOpenedAsync(DbConnection connection, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(connection);
            if (InStaleTransaction(connection))
            {
                DbCommand rollback = connection.CreateCommand();
                await using (rollback.ConfigureAwait(false))
                {
                    rollback.CommandText = "ROLLBACK";
                    await rollback.ExecuteNonQueryAsync(token).ConfigureAwait(false);
                }
            }

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

                // Only the private database generated for ":memory:" is cleared: its connection string is unique to this
                // factory. Clearing the pool of a shared connection string races with other factories still using it.
                if (_OwnsPrivateDatabase)
                {
                    using (SqliteConnection connection = new SqliteConnection(ConnectionString))
                    {
                        SqliteConnection.ClearPool(connection);
                    }
                }
            }

            base.Dispose(disposing);
        }

        private static string BuildConnectionString(SqliteRepositorySettings settings)
        {
            ArgumentNullException.ThrowIfNull(settings);
            return settings.BuildConnectionString();
        }

        #endregion
    }
}
