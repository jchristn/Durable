namespace Durable.Sqlite
{
    using Microsoft.Data.Sqlite;
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    internal class SqliteRepositoryTransaction : ITransaction
    {
        private readonly SqliteConnection _connection;
        private readonly SqliteTransaction _transaction;
        private bool _disposed;

        public SqliteRepositoryTransaction(SqliteConnection connection, SqliteTransaction transaction)
        {
            _connection = connection;
            _transaction = transaction;
        }

        public DbConnection Connection => _connection;
        public DbTransaction Transaction => _transaction;

        public void Commit()
        {
            _transaction.Commit();
        }

        public async Task CommitAsync(CancellationToken token = default)
        {
            await _transaction.CommitAsync(token);
        }

        public void Rollback()
        {
            _transaction.Rollback();
        }

        public async Task RollbackAsync(CancellationToken token = default)
        {
            await _transaction.RollbackAsync(token);
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _transaction?.Dispose();
                _connection?.Dispose();
                _disposed = true;
            }
        }
    }
}
