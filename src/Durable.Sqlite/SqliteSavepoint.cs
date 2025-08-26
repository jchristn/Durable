namespace Durable.Sqlite
{
    using System;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;

    internal class SqliteSavepoint : ISavepoint
    {
        #region Public-Members

        public string Name => _name;

        #endregion

        #region Private-Members

        private readonly SqliteConnection _connection;
        private readonly SqliteTransaction _transaction;
        private readonly string _name;
        private bool _disposed;
        private bool _released;

        #endregion

        #region Constructors-and-Factories

        public SqliteSavepoint(SqliteConnection connection, SqliteTransaction transaction, string name, bool createSavepoint = true)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _transaction = transaction ?? throw new ArgumentNullException(nameof(transaction));
            _name = name ?? throw new ArgumentNullException(nameof(name));
            
            if (createSavepoint)
            {
                // Create the savepoint
                using var command = new SqliteCommand($"SAVEPOINT {_name};", _connection, _transaction);
                command.ExecuteNonQuery();
            }
        }

        #endregion

        #region Public-Methods

        public void Release()
        {
            if (_disposed || _released)
                return;

            try
            {
                using var command = new SqliteCommand($"RELEASE SAVEPOINT {_name};", _connection, _transaction);
                command.ExecuteNonQuery();
                _released = true;
            }
            catch (Exception ex)
            {
                // Log but ignore errors when releasing savepoint during cleanup
                System.Diagnostics.Debug.WriteLine($"Warning: Failed to release savepoint '{_name}' during cleanup: {ex.Message}");
            }
        }

        public async Task ReleaseAsync(CancellationToken token = default)
        {
            if (_disposed || _released)
                return;

            try
            {
                using var command = new SqliteCommand($"RELEASE SAVEPOINT {_name};", _connection, _transaction);
                await command.ExecuteNonQueryAsync(token);
                _released = true;
            }
            catch (Exception ex)
            {
                // Log but ignore errors when releasing savepoint during cleanup
                System.Diagnostics.Debug.WriteLine($"Warning: Failed to release savepoint '{_name}' during cleanup: {ex.Message}");
            }
        }

        public void Rollback()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(SqliteSavepoint));

            if (_released)
                throw new InvalidOperationException("Savepoint has already been released");

            using var command = new SqliteCommand($"ROLLBACK TO SAVEPOINT {_name};", _connection, _transaction);
            command.ExecuteNonQuery();
        }

        public async Task RollbackAsync(CancellationToken token = default)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(SqliteSavepoint));

            if (_released)
                throw new InvalidOperationException("Savepoint has already been released");

            using var command = new SqliteCommand($"ROLLBACK TO SAVEPOINT {_name};", _connection, _transaction);
            await command.ExecuteNonQueryAsync(token);
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                if (!_released)
                {
                    // Auto-rollback if not explicitly released
                    try
                    {
                        Rollback();
                    }
                    catch (Exception ex)
                    {
                        // Log but swallow rollback exceptions during dispose
                        System.Diagnostics.Debug.WriteLine($"Warning: Failed to rollback savepoint '{_name}' during dispose: {ex.Message}");
                    }
                }
                _disposed = true;
            }
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}