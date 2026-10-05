namespace Durable.Sql
{
    using System;
    using System.Data.Common;
    using System.Globalization;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;

    /// <summary>
    /// Default <see cref="ISqlTransaction"/>. Repositories create owning instances from <c>BeginTransaction</c>;
    /// <see cref="Wrap"/> creates non-owning instances around externally managed connections and transactions.
    /// Disposing an owning, uncommitted transaction rolls it back and disposes the connection.
    /// Thread safety: not thread-safe.
    /// </summary>
    public sealed class SqlTransactionContext : ISqlTransaction
    {
        #region Public-Members

        /// <inheritdoc />
        public DbConnection Connection { get; }

        /// <inheritdoc />
        public DbTransaction? Transaction { get; }

        /// <inheritdoc />
        public bool OwnsConnection { get; }

        /// <inheritdoc />
        public bool IsCompleted => _Completed;

        /// <summary>
        /// Gets the dialect used for savepoint SQL; null when wrapping without one (driver savepoint APIs are used).
        /// </summary>
        public ISqlDialect? Dialect { get; }

        #endregion

        #region Private-Members

        private bool _Completed;
        private bool _Disposed;
        private int _SavepointCounter;

        #endregion

        #region Constructors-and-Factories

        internal SqlTransactionContext(DbConnection connection, DbTransaction? transaction, bool ownsConnection, ISqlDialect? dialect)
        {
            Connection = connection;
            Transaction = transaction;
            OwnsConnection = ownsConnection;
            Dialect = dialect;
        }

        /// <summary>
        /// Wraps an externally managed connection (and optional transaction) so Durable repositories execute on it.
        /// The caller keeps ownership: committing, rolling back and disposing remain the caller's responsibility, and
        /// <see cref="Commit"/>/<see cref="Rollback"/> on the wrapper act on the wrapped transaction.
        /// </summary>
        /// <param name="connection">An open connection. Must not be null.</param>
        /// <param name="transaction">The active transaction on that connection; null to run without a transaction.</param>
        /// <param name="dialect">Dialect for savepoint SQL; null to use the driver's savepoint APIs.</param>
        /// <returns>A non-owning transaction context.</returns>
        /// <exception cref="ArgumentNullException">Thrown when connection is null.</exception>
        /// <exception cref="ArgumentException">Thrown when the transaction belongs to a different connection.</exception>
        public static SqlTransactionContext Wrap(DbConnection connection, DbTransaction? transaction = null, ISqlDialect? dialect = null)
        {
            ArgumentNullException.ThrowIfNull(connection);
            if (transaction != null && transaction.Connection != null && !ReferenceEquals(transaction.Connection, connection))
                throw new ArgumentException("The transaction belongs to a different connection.", nameof(transaction));
            return new SqlTransactionContext(connection, transaction, false, dialect);
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public void Commit()
        {
            ThrowIfCompleted();
            Transaction?.Commit();
            _Completed = true;
        }

        /// <inheritdoc />
        public void Rollback()
        {
            ThrowIfCompleted();
            Transaction?.Rollback();
            _Completed = true;
        }

        /// <inheritdoc />
        public async Task CommitAsync(CancellationToken token = default)
        {
            ThrowIfCompleted();
            if (Transaction != null) await Transaction.CommitAsync(token).ConfigureAwait(false);
            _Completed = true;
        }

        /// <inheritdoc />
        public async Task RollbackAsync(CancellationToken token = default)
        {
            ThrowIfCompleted();
            if (Transaction != null) await Transaction.RollbackAsync(token).ConfigureAwait(false);
            _Completed = true;
        }

        /// <inheritdoc />
        public ISavepoint CreateSavepoint(string? name = null)
        {
            ThrowIfNoTransaction();
            string savepointName = ResolveSavepointName(name);
            if (Dialect == null)
            {
                Transaction!.Save(savepointName);
                return new DriverSavepoint(Transaction, savepointName);
            }

            SqlSavepoint savepoint = new SqlSavepoint(this, Dialect, savepointName);
            savepoint.Create();
            return savepoint;
        }

        /// <inheritdoc />
        public async Task<ISavepoint> CreateSavepointAsync(string? name = null, CancellationToken token = default)
        {
            ThrowIfNoTransaction();
            string savepointName = ResolveSavepointName(name);
            if (Dialect == null)
            {
                await Transaction!.SaveAsync(savepointName, token).ConfigureAwait(false);
                return new DriverSavepoint(Transaction, savepointName);
            }

            SqlSavepoint savepoint = new SqlSavepoint(this, Dialect, savepointName);
            await savepoint.CreateAsync(token).ConfigureAwait(false);
            return savepoint;
        }

        /// <summary>
        /// Disposes the transaction. For owning instances, rolls back if not completed and disposes the connection.
        /// </summary>
        public void Dispose()
        {
            if (_Disposed) return;
            _Disposed = true;
            if (!OwnsConnection) return;
            try
            {
                if (!_Completed && Transaction != null && Transaction.Connection != null)
                    Transaction.Rollback();
            }
            catch (InvalidOperationException)
            {
            }
            catch (DbException)
            {
            }
            finally
            {
                Transaction?.Dispose();
                Connection.Dispose();
            }
        }

        /// <summary>
        /// Disposes the transaction asynchronously. For owning instances, rolls back if not completed and disposes the connection.
        /// </summary>
        /// <returns>A task.</returns>
        public async ValueTask DisposeAsync()
        {
            if (_Disposed) return;
            _Disposed = true;
            if (!OwnsConnection) return;
            try
            {
                if (!_Completed && Transaction != null && Transaction.Connection != null)
                    await Transaction.RollbackAsync().ConfigureAwait(false);
            }
            catch (InvalidOperationException)
            {
            }
            catch (DbException)
            {
            }
            finally
            {
                if (Transaction != null) await Transaction.DisposeAsync().ConfigureAwait(false);
                await Connection.DisposeAsync().ConfigureAwait(false);
            }
        }

        #endregion

        #region Private-Methods

        private void ThrowIfCompleted()
        {
            if (_Disposed) throw new ObjectDisposedException(nameof(SqlTransactionContext));
            if (_Completed) throw new InvalidOperationException("The transaction has already been committed or rolled back.");
        }

        private void ThrowIfNoTransaction()
        {
            ThrowIfCompleted();
            if (Transaction == null) throw new InvalidOperationException("Savepoints require an active transaction.");
        }

        private string ResolveSavepointName(string? name)
        {
            if (name == null)
            {
                _SavepointCounter++;
                return "sp_" + _SavepointCounter.ToString(CultureInfo.InvariantCulture);
            }

            foreach (char c in name)
            {
                if (!char.IsLetterOrDigit(c) && c != '_')
                    throw new ArgumentException("Savepoint names may contain only letters, digits and underscores.", nameof(name));
            }

            if (name.Length == 0) throw new ArgumentException("Savepoint name cannot be empty.", nameof(name));
            return name;
        }

        #endregion
    }
}
