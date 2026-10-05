namespace Durable.Sql
{
    using System;
    using System.Data.Common;
    using System.Threading.Tasks;

    /// <summary>
    /// A connection borrowed for one operation: either a fresh connection owned by the lease (disposed with it)
    /// or the connection of an active transaction (left open).
    /// Thread safety: not thread-safe.
    /// </summary>
    public sealed class ConnectionLease : IDisposable, IAsyncDisposable
    {
        #region Public-Members

        /// <summary>
        /// Gets the open connection. Never null.
        /// </summary>
        public DbConnection Connection { get; }

        /// <summary>
        /// Gets the transaction to enlist commands in, or null.
        /// </summary>
        public DbTransaction? Transaction { get; }

        /// <summary>
        /// Gets the transaction context the lease came from, or null for an owned connection.
        /// </summary>
        public ISqlTransaction? TransactionContext { get; }

        /// <summary>
        /// Gets whether disposing the lease disposes the connection.
        /// </summary>
        public bool OwnsConnection { get; }

        #endregion

        #region Private-Members

        private bool _Disposed;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a lease.
        /// </summary>
        /// <param name="connection">Open connection. Must not be null.</param>
        /// <param name="transactionContext">Transaction context, or null for an owned connection.</param>
        /// <exception cref="ArgumentNullException">Thrown when connection is null.</exception>
        public ConnectionLease(DbConnection connection, ISqlTransaction? transactionContext)
        {
            Connection = connection ?? throw new ArgumentNullException(nameof(connection));
            TransactionContext = transactionContext;
            Transaction = transactionContext?.Transaction;
            OwnsConnection = transactionContext == null;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Disposes the connection when owned.
        /// </summary>
        public void Dispose()
        {
            if (_Disposed) return;
            _Disposed = true;
            if (OwnsConnection) Connection.Dispose();
        }

        /// <summary>
        /// Disposes the connection asynchronously when owned.
        /// </summary>
        /// <returns>A task.</returns>
        public async ValueTask DisposeAsync()
        {
            if (_Disposed) return;
            _Disposed = true;
            if (OwnsConnection) await Connection.DisposeAsync().ConfigureAwait(false);
        }

        #endregion
    }
}
