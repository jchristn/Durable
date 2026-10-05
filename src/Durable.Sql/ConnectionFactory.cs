namespace Durable.Sql
{
    using System;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Base connection factory relying on the ADO.NET driver's pooling, with an optional cap on concurrently open
    /// connections. Each connection handed out releases its slot when disposed.
    /// Thread safety: safe for concurrent use.
    /// </summary>
    public abstract class ConnectionFactory : IConnectionFactory
    {
        #region Public-Members

        /// <summary>
        /// Gets the maximum number of connections this factory allows open at once, or null for no limit (the default).
        /// The driver's own pool size still applies.
        /// </summary>
        public int? MaxConcurrentConnections { get; }

        /// <summary>
        /// Gets or sets how long to wait for a free slot when <see cref="MaxConcurrentConnections"/> is set.
        /// Default: 30 seconds. Minimum: 1 millisecond.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when set to less than 1 millisecond.</exception>
        public TimeSpan AcquireTimeout
        {
            get => _AcquireTimeout;
            set
            {
                if (value < TimeSpan.FromMilliseconds(1)) throw new ArgumentOutOfRangeException(nameof(value), "AcquireTimeout must be at least 1 millisecond.");
                _AcquireTimeout = value;
            }
        }

        #endregion

        #region Private-Members

        private readonly SemaphoreSlim? _Limiter;
        private TimeSpan _AcquireTimeout = TimeSpan.FromSeconds(30);
        private int _Disposed = 0;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the factory.
        /// </summary>
        /// <param name="maxConcurrentConnections">Optional cap on concurrently open connections; null for none. Minimum: 1.</param>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when maxConcurrentConnections is less than 1.</exception>
        protected ConnectionFactory(int? maxConcurrentConnections = null)
        {
            if (maxConcurrentConnections.HasValue)
            {
                if (maxConcurrentConnections.Value < 1) throw new ArgumentOutOfRangeException(nameof(maxConcurrentConnections), "Must be at least 1.");
                _Limiter = new SemaphoreSlim(maxConcurrentConnections.Value, maxConcurrentConnections.Value);
            }

            MaxConcurrentConnections = maxConcurrentConnections;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public DbConnection OpenConnection()
        {
            ThrowIfDisposed();
            if (_Limiter != null && !_Limiter.Wait(_AcquireTimeout))
                throw new TimeoutException("Timed out after " + _AcquireTimeout + " waiting for a connection slot (MaxConcurrentConnections = " + MaxConcurrentConnections + ").");

            DbConnection? connection = null;
            try
            {
                connection = CreateConnection();
                connection.Open();
                OnConnectionOpened(connection);
                TrackSlot(connection);
                return connection;
            }
            catch
            {
                connection?.Dispose();
                _Limiter?.Release();
                throw;
            }
        }

        /// <inheritdoc />
        public async Task<DbConnection> OpenConnectionAsync(CancellationToken token = default)
        {
            ThrowIfDisposed();
            if (_Limiter != null && !await _Limiter.WaitAsync(_AcquireTimeout, token).ConfigureAwait(false))
                throw new TimeoutException("Timed out after " + _AcquireTimeout + " waiting for a connection slot (MaxConcurrentConnections = " + MaxConcurrentConnections + ").");

            DbConnection? connection = null;
            try
            {
                connection = CreateConnection();
                await connection.OpenAsync(token).ConfigureAwait(false);
                await OnConnectionOpenedAsync(connection, token).ConfigureAwait(false);
                TrackSlot(connection);
                return connection;
            }
            catch
            {
                if (connection != null) await connection.DisposeAsync().ConfigureAwait(false);
                _Limiter?.Release();
                throw;
            }
        }

        /// <summary>
        /// Disposes the factory.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Disposes the factory asynchronously.
        /// </summary>
        /// <returns>A task.</returns>
        public virtual ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Creates a new, unopened connection.
        /// </summary>
        /// <returns>The connection.</returns>
        protected abstract DbConnection CreateConnection();

        /// <summary>
        /// Called after a connection opens, for per-connection setup (for example, SQLite pragmas). Default: no-op.
        /// </summary>
        /// <param name="connection">The open connection.</param>
        protected virtual void OnConnectionOpened(DbConnection connection)
        {
        }

        /// <summary>
        /// Called after a connection opens asynchronously. Default: calls <see cref="OnConnectionOpened"/>.
        /// </summary>
        /// <param name="connection">The open connection.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task.</returns>
        protected virtual Task OnConnectionOpenedAsync(DbConnection connection, CancellationToken token)
        {
            OnConnectionOpened(connection);
            return Task.CompletedTask;
        }

        /// <summary>
        /// Releases resources.
        /// </summary>
        /// <param name="disposing">True when called from <see cref="Dispose()"/>.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (Interlocked.Exchange(ref _Disposed, 1) == 1) return;
            if (disposing) _Limiter?.Dispose();
        }

        /// <summary>
        /// Throws when the factory has been disposed.
        /// </summary>
        /// <exception cref="ObjectDisposedException">Thrown when disposed.</exception>
        protected void ThrowIfDisposed()
        {
            if (Volatile.Read(ref _Disposed) == 1) throw new ObjectDisposedException(GetType().Name);
        }


        private void TrackSlot(DbConnection connection)
        {
            if (_Limiter == null) return;
            int released = 0;
            void Release()
            {
                if (Interlocked.Exchange(ref released, 1) == 0 && Volatile.Read(ref _Disposed) == 0)
                    _Limiter.Release();
            }

            // Some drivers close without raising Component.Disposed from DisposeAsync, so release on whichever comes first.
            connection.Disposed += (_, _) => Release();
            connection.StateChange += (_, e) =>
            {
                if (e.CurrentState == System.Data.ConnectionState.Closed) Release();
            };
        }

        #endregion
    }
}
