namespace Durable
{
    using System;
    using System.Data;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Wraps a pooled database connection and automatically returns it to the pool when disposed.
    /// This class forwards all operations to the underlying connection while intercepting disposal
    /// to ensure proper connection pool management.
    /// </summary>
    public sealed class PooledConnectionHandle : DbConnection
    {
#pragma warning disable CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).

        #region Private-Members

        private DbConnection? _InnerConnection;
        private readonly ConnectionPool _Pool;
        private bool _Returned = false;
        private readonly object _Lock = new object();

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the PooledConnectionHandle class.
        /// </summary>
        /// <param name="innerConnection">The actual database connection to wrap.</param>
        /// <param name="pool">The connection pool that owns this connection.</param>
        /// <exception cref="ArgumentNullException">Thrown when innerConnection or pool is null.</exception>
        public PooledConnectionHandle(DbConnection innerConnection, ConnectionPool pool)
        {
            _InnerConnection = innerConnection ?? throw new ArgumentNullException(nameof(innerConnection));
            _Pool = pool ?? throw new ArgumentNullException(nameof(pool));
        }

        #endregion

        #region Public-Members

        /// <summary>
        /// Gets the underlying database connection that this handle wraps.
        /// Used internally by the connection pool to unwrap connections.
        /// </summary>
        internal DbConnection InnerConnection
        {
            get
            {
                if (_InnerConnection == null)
                    throw new ObjectDisposedException(nameof(PooledConnectionHandle));
                return _InnerConnection;
            }
        }

        /// <summary>
        /// Gets or sets the connection string for the database connection.
        /// </summary>
        public override string ConnectionString
        {
            get => InnerConnection.ConnectionString;
            set => InnerConnection.ConnectionString = value;
        }

        /// <summary>
        /// Gets the name of the current database.
        /// </summary>
        public override string Database => InnerConnection.Database;

        /// <summary>
        /// Gets the name of the database server.
        /// </summary>
        public override string DataSource => InnerConnection.DataSource;

        /// <summary>
        /// Gets the version of the database server.
        /// </summary>
        public override string ServerVersion => InnerConnection.ServerVersion;

        /// <summary>
        /// Gets the current state of the connection.
        /// </summary>
        public override ConnectionState State => InnerConnection.State;

        #endregion

        #region Public-Methods

        /// <summary>
        /// Unwraps a database connection if it's a PooledConnectionHandle, returning the inner connection.
        /// If the connection is not wrapped, returns it as-is.
        /// </summary>
        /// <param name="connection">The connection to unwrap.</param>
        /// <returns>The unwrapped database connection.</returns>
        public static DbConnection Unwrap(DbConnection connection)
        {
            if (connection is PooledConnectionHandle handle)
            {
                return handle.InnerConnection;
            }
            return connection;
        }

        /// <summary>
        /// Changes the current database for the connection.
        /// </summary>
        /// <param name="databaseName">The name of the database to use.</param>
        public override void ChangeDatabase(string databaseName)
        {
            InnerConnection.ChangeDatabase(databaseName);
        }

        /// <summary>
        /// Opens the database connection.
        /// </summary>
        public override void Open()
        {
            InnerConnection.Open();
        }

        /// <summary>
        /// Asynchronously opens the database connection.
        /// </summary>
        /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public override Task OpenAsync(CancellationToken cancellationToken)
        {
            return InnerConnection.OpenAsync(cancellationToken);
        }

        /// <summary>
        /// Closes the database connection.
        /// </summary>
        public override void Close()
        {
            InnerConnection.Close();
        }

        /// <summary>
        /// Asynchronously closes the database connection.
        /// </summary>
        /// <returns>A task representing the asynchronous operation.</returns>
        public override Task CloseAsync()
        {
            return InnerConnection.CloseAsync();
        }

        #endregion

        #region Protected-Methods

        /// <summary>
        /// Begins a database transaction.
        /// </summary>
        /// <param name="isolationLevel">The isolation level for the transaction.</param>
        /// <returns>A database transaction object.</returns>
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            return InnerConnection.BeginTransaction(isolationLevel);
        }

        /// <summary>
        /// Creates a database command object.
        /// </summary>
        /// <returns>A database command object.</returns>
        protected override DbCommand CreateDbCommand()
        {
            return InnerConnection.CreateCommand();
        }

        /// <summary>
        /// Disposes the connection handle and returns the underlying connection to the pool.
        /// </summary>
        /// <param name="disposing">True if disposing managed resources.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                lock (_Lock)
                {
                    if (!_Returned && _InnerConnection != null)
                    {
                        _Returned = true;
                        DbConnection conn = _InnerConnection;
                        _InnerConnection = null;
                        _Pool.ReturnConnection(conn);
                    }
                }
            }
            base.Dispose(disposing);
        }

        /// <summary>
        /// Asynchronously disposes the connection handle and returns the underlying connection to the pool.
        /// </summary>
        /// <returns>A task representing the asynchronous disposal operation.</returns>
        public override async ValueTask DisposeAsync()
        {
            lock (_Lock)
            {
                if (!_Returned && _InnerConnection != null)
                {
                    _Returned = true;
                    DbConnection conn = _InnerConnection;
                    _InnerConnection = null;
                    // ConnectionPool doesn't have async ReturnConnection, use sync version
                    _Pool.ReturnConnection(conn);
                }
            }
            await base.DisposeAsync().ConfigureAwait(false);
        }

        #endregion

#pragma warning restore CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).
    }
}
