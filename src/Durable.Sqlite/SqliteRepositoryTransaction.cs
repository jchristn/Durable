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
        private readonly SqliteConnection _Connection;
        private readonly SqliteTransaction _Transaction;
        private bool _Disposed;

        public SqliteRepositoryTransaction(SqliteConnection connection, SqliteTransaction transaction)
        {
            _Connection = connection;
            _Transaction = transaction;
        }

        public DbConnection Connection => _Connection;
        public DbTransaction Transaction => _Transaction;

        public void Commit()
        {
            _Transaction.Commit();
        }

        public async Task CommitAsync(CancellationToken token = default)
        {
            await _Transaction.CommitAsync(token);
        }

        public void Rollback()
        {
            _Transaction.Rollback();
        }

        public async Task RollbackAsync(CancellationToken token = default)
        {
            await _Transaction.RollbackAsync(token);
        }

        public void Dispose()
        {
            if (!_Disposed)
            {
                _Transaction?.Dispose();
                _Connection?.Dispose();
                _Disposed = true;
            }
        }
    }
}
