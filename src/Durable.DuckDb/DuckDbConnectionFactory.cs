namespace Durable.DuckDb
{
    using System;
    using System.Data.Common;
    using DuckDB.NET.Data;
    using Durable.Sql;

    /// <summary>
    /// Opens DuckDB connections. DuckDB runs in-process and has no connection pool; instead, a database instance lives as
    /// long as at least one connection to it is open, and DuckDB.NET shares one instance between connections opened with
    /// the same data source. This factory therefore opens one root connection on first use and keeps it open until it is
    /// disposed. Every connection it hands out shares the root's database instance (in-memory connections are
    /// <see cref="DuckDBConnection.Duplicate"/>s of the root; file connections are opened with the same data source):
    /// <list type="bullet">
    /// <item><description><c>:memory:</c> (or an empty data source): an in-memory database private to this factory. All
    /// repositories sharing the factory see the same data; raw connections opened with the same connection string do not
    /// (each would get its own empty database). The database is released when the factory is disposed.</description></item>
    /// <item><description><c>:memory:?cache=shared</c>: the process-wide shared in-memory database. Raw connections with the
    /// same string share it; it lives while any connection to it (including this factory's root) is open.</description></item>
    /// <item><description>A file path: the database file. The root connection keeps the database open (and DuckDB's file
    /// lock held: a read-write file can be opened by one process at a time) until the factory is disposed, which avoids
    /// re-opening and checkpointing the file for every operation.</description></item>
    /// </list>
    /// Concurrency: DuckDB uses optimistic multi-version concurrency control. Concurrent transactions do not block each
    /// other; a transaction that updates or deletes a row changed by another concurrent transaction fails with a
    /// "Conflict on update" error (a <see cref="DuckDBException"/>) instead of waiting, so callers that update the same rows
    /// from several threads should retry such failures.
    /// Thread safety: safe for concurrent use.
    /// </summary>
    public sealed class DuckDbConnectionFactory : ConnectionFactory
    {
        #region Public-Members

        /// <summary>
        /// Gets the effective connection string. Never null.
        /// </summary>
        public string ConnectionString { get; }

        /// <summary>
        /// Gets whether the database is in memory (<c>:memory:</c>, <c>:memory:?cache=shared</c> or an empty data source).
        /// </summary>
        public bool IsInMemory { get; }

        /// <summary>
        /// Gets whether the database is an in-memory database private to this factory (a <c>:memory:</c> data source without options).
        /// </summary>
        public bool IsPrivateInMemory { get; }

        #endregion

        #region Private-Members

        private readonly object _RootLock = new object();
        private DuckDBConnection? _Root;
        private bool _RootDisposed;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the factory from strongly-typed settings (<see cref="DuckDbRepositorySettings.BuildConnectionString"/>).
        /// </summary>
        /// <param name="settings">Settings. Must not be null.</param>
        /// <param name="maxConcurrentConnections">Optional cap on concurrently open connections; null for none.</param>
        /// <exception cref="ArgumentNullException">Thrown when settings is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the settings lack a data source.</exception>
        public DuckDbConnectionFactory(DuckDbRepositorySettings settings, int? maxConcurrentConnections = null)
            : this(BuildConnectionString(settings), maxConcurrentConnections)
        {
        }

        /// <summary>
        /// Instantiates the factory. No connection is opened until the first connection is requested.
        /// </summary>
        /// <param name="connectionString">DuckDB.NET connection string (for example <c>Data Source=:memory:</c>). Must not be null.</param>
        /// <param name="maxConcurrentConnections">Optional cap on concurrently open connections; null for none.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null.</exception>
        /// <exception cref="ArgumentException">Thrown when the connection string is invalid.</exception>
        public DuckDbConnectionFactory(string connectionString, int? maxConcurrentConnections = null) : base(maxConcurrentConnections)
        {
            ArgumentNullException.ThrowIfNull(connectionString);
            DuckDBConnectionStringBuilder builder;
            try
            {
                builder = new DuckDBConnectionStringBuilder { ConnectionString = connectionString };
            }
            catch (InvalidOperationException e)
            {
                throw new ArgumentException("Invalid DuckDB connection string: " + e.Message, nameof(connectionString), e);
            }

            string dataSource = builder.DataSource ?? string.Empty;
            IsInMemory = DuckDbRepositorySettings.IsInMemoryDataSource(dataSource);
            IsPrivateInMemory = IsInMemory && dataSource.IndexOf('?') < 0;
            ConnectionString = connectionString;
        }

        #endregion

        #region Private-Methods

        /// <inheritdoc />
        protected override DbConnection CreateConnection()
        {
            lock (_RootLock)
            {
                if (_RootDisposed) throw new ObjectDisposedException(nameof(DuckDbConnectionFactory));
                if (_Root == null)
                {
                    DuckDBConnection root = new DuckDBConnection(ConnectionString);
                    root.Open();
                    _Root = root;
                }

                // DuckDB.NET duplicates in-memory connections only; a file connection opened with the same data source joins
                // the database instance the root keeps open.
                return IsInMemory ? _Root.Duplicate() : new DuckDBConnection(ConnectionString);
            }
        }

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                lock (_RootLock)
                {
                    _RootDisposed = true;
                    _Root?.Dispose();
                    _Root = null;
                }
            }

            base.Dispose(disposing);
        }

        private static string BuildConnectionString(DuckDbRepositorySettings settings)
        {
            ArgumentNullException.ThrowIfNull(settings);
            return settings.BuildConnectionString();
        }

        #endregion
    }
}
