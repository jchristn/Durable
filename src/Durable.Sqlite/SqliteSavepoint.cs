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

        public string Name => _Name;

        #endregion

        #region Private-Members

        private readonly SqliteConnection _Connection;
        private readonly SqliteTransaction _Transaction;
        private readonly string _Name;
        private bool _Disposed;
        private bool _Released;

        #endregion

        #region Constructors-and-Factories

        public SqliteSavepoint(SqliteConnection connection, SqliteTransaction transaction, string name, bool createSavepoint = true)
        {
            _Connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _Transaction = transaction ?? throw new ArgumentNullException(nameof(transaction));
            _Name = name ?? throw new ArgumentNullException(nameof(name));
            
            if (createSavepoint)
            {
                // Create the savepoint
                using SqliteCommand command = new SqliteCommand($"SAVEPOINT {_Name};", _Connection, _Transaction);
                command.ExecuteNonQuery();
            }
        }

        #endregion

        #region Public-Methods

        public void Release()
        {
            if (_Disposed || _Released)
                return;

            try
            {
                using SqliteCommand command = new SqliteCommand($"RELEASE SAVEPOINT {_Name};", _Connection, _Transaction);
                command.ExecuteNonQuery();
                _Released = true;
            }
            catch (Exception ex)
            {
                // Log but ignore errors when releasing savepoint during cleanup
                System.Diagnostics.Debug.WriteLine($"Warning: Failed to release savepoint '{_Name}' during cleanup: {ex.Message}");
            }
        }

        public async Task ReleaseAsync(CancellationToken token = default)
        {
            if (_Disposed || _Released)
                return;

            try
            {
                using SqliteCommand command = new SqliteCommand($"RELEASE SAVEPOINT {_Name};", _Connection, _Transaction);
                await command.ExecuteNonQueryAsync(token);
                _Released = true;
            }
            catch (Exception ex)
            {
                // Log but ignore errors when releasing savepoint during cleanup
                System.Diagnostics.Debug.WriteLine($"Warning: Failed to release savepoint '{_Name}' during cleanup: {ex.Message}");
            }
        }

        public void Rollback()
        {
            if (_Disposed)
                throw new ObjectDisposedException(nameof(SqliteSavepoint));

            if (_Released)
                throw new InvalidOperationException("Savepoint has already been released");

            using SqliteCommand command = new SqliteCommand($"ROLLBACK TO SAVEPOINT {_Name};", _Connection, _Transaction);
            command.ExecuteNonQuery();
        }

        public async Task RollbackAsync(CancellationToken token = default)
        {
            if (_Disposed)
                throw new ObjectDisposedException(nameof(SqliteSavepoint));

            if (_Released)
                throw new InvalidOperationException("Savepoint has already been released");

            using SqliteCommand command = new SqliteCommand($"ROLLBACK TO SAVEPOINT {_Name};", _Connection, _Transaction);
            await command.ExecuteNonQueryAsync(token);
        }

        public void Dispose()
        {
            if (!_Disposed)
            {
                if (!_Released)
                {
                    // Auto-rollback if not explicitly released
                    try
                    {
                        Rollback();
                    }
                    catch (Exception ex)
                    {
                        // Log but swallow rollback exceptions during dispose
                        System.Diagnostics.Debug.WriteLine($"Warning: Failed to rollback savepoint '{_Name}' during dispose: {ex.Message}");
                    }
                }
                _Disposed = true;
            }
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}