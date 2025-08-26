namespace Durable.Sqlite
{
    using Microsoft.Data.Sqlite;
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    internal class SqliteRepositoryTransaction : ITransaction
    {
        #region Public-Members

        public DbConnection Connection => _Connection;
        public DbTransaction Transaction => _Transaction;

        #endregion

        #region Private-Members

        private readonly SqliteConnection _Connection;
        private readonly SqliteTransaction _Transaction;
        private bool _Disposed;
        private int _SavepointCounter;

        #endregion

        #region Constructors-and-Factories

        public SqliteRepositoryTransaction(SqliteConnection connection, SqliteTransaction transaction)
        {
            _Connection = connection;
            _Transaction = transaction;
        }

        #endregion

        #region Public-Methods

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

        public ISavepoint CreateSavepoint(string? name = null)
        {
            if (_Disposed)
                throw new ObjectDisposedException(nameof(SqliteRepositoryTransaction));

            ValidateConnectionState();

            name = name ?? $"sp_{Interlocked.Increment(ref _SavepointCounter)}";
            return new SqliteSavepoint(_Connection, _Transaction, name);
        }

        public async Task<ISavepoint> CreateSavepointAsync(string? name = null, CancellationToken token = default)
        {
            if (_Disposed)
                throw new ObjectDisposedException(nameof(SqliteRepositoryTransaction));

            ValidateConnectionState();

            name = name ?? $"sp_{Interlocked.Increment(ref _SavepointCounter)}";
            
            // Create savepoint asynchronously
            using var command = new SqliteCommand($"SAVEPOINT {name};", _Connection, _Transaction);
            await command.ExecuteNonQueryAsync(token);
            
            return new SqliteSavepoint(_Connection, _Transaction, name, false);
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

        #endregion

        #region Private-Methods

        private void ValidateConnectionState()
        {
            if (_Connection?.State != ConnectionState.Open)
                throw new InvalidOperationException("Connection must be open to create savepoints");
        }

        #endregion
    }
}
