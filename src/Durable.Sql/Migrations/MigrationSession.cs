namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Diagnostics;
    using System.Globalization;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// One dedicated connection used by the migrator: executes statements through a <see cref="SqlCommandExecutor"/> (so
    /// interceptors, logging, tracing and capture apply), manages an optional local transaction and holds the session lock.
    /// Thread safety: not thread-safe; used by one operation at a time.
    /// </summary>
    internal sealed class MigrationSession : IDisposable, IAsyncDisposable
    {
        #region Public-Members

        internal SqlCommandExecutor Executor { get; }

        internal ISqlDialect Dialect => Executor.Dialect;

        internal DbConnection Connection { get; }

        internal DbTransaction? Transaction { get; private set; }

        internal SqlTransactionContext Context { get; private set; }

        internal ConnectionLease Lease { get; private set; }

        #endregion

        #region Private-Members

        private string? _HeldLock;
        private bool _Disposed;

        #endregion

        #region Constructors-and-Factories

        private MigrationSession(SqlCommandExecutor executor, DbConnection connection)
        {
            Executor = executor;
            Connection = connection;
            Context = SqlTransactionContext.Wrap(connection, null, executor.Dialect);
            Lease = new ConnectionLease(connection, Context);
        }

        internal static MigrationSession Open(SqlCommandExecutor executor)
        {
            return new MigrationSession(executor, executor.ConnectionFactory.OpenConnection());
        }

        internal static async Task<MigrationSession> OpenAsync(SqlCommandExecutor executor, CancellationToken token)
        {
            DbConnection connection = await executor.ConnectionFactory.OpenConnectionAsync(token).ConfigureAwait(false);
            return new MigrationSession(executor, connection);
        }

        #endregion

        #region Public-Methods

        internal void Begin()
        {
            if (Transaction != null) throw new InvalidOperationException("A migration transaction is already active.");
            SetTransaction(Connection.BeginTransaction());
        }

        internal async Task BeginAsync(CancellationToken token)
        {
            if (Transaction != null) throw new InvalidOperationException("A migration transaction is already active.");
            SetTransaction(await Connection.BeginTransactionAsync(token).ConfigureAwait(false));
        }

        internal void Commit()
        {
            DbTransaction? transaction = Transaction;
            if (transaction == null) return;
            try
            {
                transaction.Commit();
            }
            finally
            {
                transaction.Dispose();
                SetTransaction(null);
            }
        }

        internal async Task CommitAsync(CancellationToken token)
        {
            DbTransaction? transaction = Transaction;
            if (transaction == null) return;
            try
            {
                await transaction.CommitAsync(token).ConfigureAwait(false);
            }
            finally
            {
                await transaction.DisposeAsync().ConfigureAwait(false);
                SetTransaction(null);
            }
        }

        internal void Rollback()
        {
            DbTransaction? transaction = Transaction;
            if (transaction == null) return;
            try
            {
                if (transaction.Connection != null) transaction.Rollback();
            }
            catch (InvalidOperationException)
            {
            }
            catch (DbException)
            {
            }
            finally
            {
                transaction.Dispose();
                SetTransaction(null);
            }
        }

        internal async Task RollbackAsync(CancellationToken token)
        {
            DbTransaction? transaction = Transaction;
            if (transaction == null) return;
            try
            {
                if (transaction.Connection != null) await transaction.RollbackAsync(token).ConfigureAwait(false);
            }
            catch (InvalidOperationException)
            {
            }
            catch (DbException)
            {
            }
            finally
            {
                await transaction.DisposeAsync().ConfigureAwait(false);
                SetTransaction(null);
            }
        }

        internal int Execute(SqlStatement statement, string operation)
        {
            return Executor.ExecuteNonQuery(Lease, statement, operation);
        }

        internal Task<int> ExecuteAsync(SqlStatement statement, string operation, CancellationToken token)
        {
            return Executor.ExecuteNonQueryAsync(Lease, statement, operation, token);
        }

        internal List<TResult> Query<TResult>(SqlStatement statement, string operation, Func<DbDataReader, TResult> map)
        {
            return Executor.ExecuteReader(Lease, statement, operation, reader =>
            {
                List<TResult> rows = new List<TResult>();
                while (reader.Read()) rows.Add(map(reader));
                return rows;
            });
        }

        internal Task<List<TResult>> QueryAsync<TResult>(SqlStatement statement, string operation, Func<DbDataReader, TResult> map, CancellationToken token)
        {
            return Executor.ExecuteReaderAsync(Lease, statement, operation, async (reader, ct) =>
            {
                List<TResult> rows = new List<TResult>();
                while (await reader.ReadAsync(ct).ConfigureAwait(false)) rows.Add(map(reader));
                return rows;
            }, token);
        }

        internal object? Scalar(SqlStatement statement, string operation)
        {
            return Executor.ExecuteReader(Lease, statement, operation, reader => ReadScalar(reader));
        }

        internal Task<object?> ScalarAsync(SqlStatement statement, string operation, CancellationToken token)
        {
            return Executor.ExecuteReaderAsync(Lease, statement, operation, async (reader, ct) =>
            {
                do
                {
                    if (reader.FieldCount > 0)
                    {
                        if (!await reader.ReadAsync(ct).ConfigureAwait(false)) return null;
                        object value = reader.GetValue(0);
                        return value == DBNull.Value ? null : value;
                    }
                }
                while (await reader.NextResultAsync(ct).ConfigureAwait(false));
                return null;
            }, token);
        }

        internal void AcquireLock(string lockName, int timeoutSeconds, int pollMilliseconds, CancellationToken token)
        {
            if (Dialect.AcquireMigrationLockSql(lockName, 0) == null) return;
            Stopwatch elapsed = Stopwatch.StartNew();
            while (true)
            {
                token.ThrowIfCancellationRequested();
                int wait = Math.Max(0, Math.Min(5, timeoutSeconds - (int)elapsed.Elapsed.TotalSeconds));
                SqlStatement statement = Dialect.AcquireMigrationLockSql(lockName, wait)!;
                if (IsAcquired(Scalar(statement, "MIGRATION LOCK")))
                {
                    _HeldLock = lockName;
                    return;
                }

                if (elapsed.Elapsed.TotalSeconds >= timeoutSeconds) throw LockTimeout(lockName, timeoutSeconds);
                token.WaitHandle.WaitOne(pollMilliseconds);
            }
        }

        internal async Task AcquireLockAsync(string lockName, int timeoutSeconds, int pollMilliseconds, CancellationToken token)
        {
            if (Dialect.AcquireMigrationLockSql(lockName, 0) == null) return;
            Stopwatch elapsed = Stopwatch.StartNew();
            while (true)
            {
                token.ThrowIfCancellationRequested();
                int wait = Math.Max(0, Math.Min(5, timeoutSeconds - (int)elapsed.Elapsed.TotalSeconds));
                SqlStatement statement = Dialect.AcquireMigrationLockSql(lockName, wait)!;
                if (IsAcquired(await ScalarAsync(statement, "MIGRATION LOCK", token).ConfigureAwait(false)))
                {
                    _HeldLock = lockName;
                    return;
                }

                if (elapsed.Elapsed.TotalSeconds >= timeoutSeconds) throw LockTimeout(lockName, timeoutSeconds);
                await Task.Delay(pollMilliseconds, token).ConfigureAwait(false);
            }
        }

        internal void ReleaseLock()
        {
            string? lockName = _HeldLock;
            if (lockName == null) return;
            _HeldLock = null;
            SqlStatement? statement = Dialect.ReleaseMigrationLockSql(lockName);
            if (statement != null) Scalar(statement, "MIGRATION UNLOCK");
        }

        internal async Task ReleaseLockAsync(CancellationToken token)
        {
            string? lockName = _HeldLock;
            if (lockName == null) return;
            _HeldLock = null;
            SqlStatement? statement = Dialect.ReleaseMigrationLockSql(lockName);
            if (statement != null) await ScalarAsync(statement, "MIGRATION UNLOCK", token).ConfigureAwait(false);
        }

        public void Dispose()
        {
            if (_Disposed) return;
            _Disposed = true;
            Rollback();
            Connection.Dispose();
        }

        public async ValueTask DisposeAsync()
        {
            if (_Disposed) return;
            _Disposed = true;
            await RollbackAsync(CancellationToken.None).ConfigureAwait(false);
            await Connection.DisposeAsync().ConfigureAwait(false);
        }

        #endregion

        #region Private-Methods

        private void SetTransaction(DbTransaction? transaction)
        {
            Transaction = transaction;
            Context = SqlTransactionContext.Wrap(Connection, transaction, Dialect);
            Lease = new ConnectionLease(Connection, Context);
        }

        private static object? ReadScalar(DbDataReader reader)
        {
            do
            {
                if (reader.FieldCount > 0)
                {
                    if (!reader.Read()) return null;
                    object value = reader.GetValue(0);
                    return value == DBNull.Value ? null : value;
                }
            }
            while (reader.NextResult());
            return null;
        }

        private static bool IsAcquired(object? value)
        {
            return value != null && Convert.ToInt64(value, CultureInfo.InvariantCulture) == 1;
        }

        private static TimeoutException LockTimeout(string lockName, int timeoutSeconds)
        {
            return new TimeoutException("Could not acquire the migration lock '" + lockName + "' within " + timeoutSeconds + " second(s); another migrator may be running.");
        }

        #endregion
    }
}
