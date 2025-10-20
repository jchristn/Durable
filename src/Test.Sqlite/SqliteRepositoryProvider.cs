namespace Test.Sqlite
{
    using System;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sqlite;
    using Microsoft.Data.Sqlite;
    using Test.Shared;

    /// <summary>
    /// SQLite implementation of the repository provider for shared testing infrastructure.
    /// </summary>
    public class SqliteRepositoryProvider : IRepositoryProvider
    {
        #region Private-Members

        private const string TestConnectionString = "Data Source=InMemorySharedTest;Mode=Memory;Cache=Shared";
        private SqliteConnection? _KeepAliveConnection;
        private bool _Disposed = false;

        #endregion

        #region Public-Members

        /// <summary>
        /// Gets the name of the database provider.
        /// </summary>
        public string ProviderName => "SQLite";

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="SqliteRepositoryProvider"/> class.
        /// </summary>
        public SqliteRepositoryProvider()
        {
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Creates and configures a repository for the specified entity type.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <returns>A configured repository instance.</returns>
        public IRepository<T> CreateRepository<T>() where T : class, new()
        {
            return new SqliteRepository<T>(TestConnectionString);
        }

        /// <summary>
        /// Sets up the database schema for testing.
        /// </summary>
        /// <returns>A task representing the asynchronous setup operation.</returns>
        public async Task SetupDatabaseAsync()
        {
            _KeepAliveConnection = new SqliteConnection(TestConnectionString);
            _KeepAliveConnection.Open();

            IRepository<Person> personRepo = CreateRepository<Person>();

            await personRepo.ExecuteSqlAsync(@"
                CREATE TABLE IF NOT EXISTS people (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    first TEXT NOT NULL,
                    last TEXT NOT NULL,
                    age INTEGER NOT NULL,
                    email TEXT,
                    salary REAL NOT NULL,
                    department TEXT
                )
            ");

            await personRepo.ExecuteSqlAsync(@"
                CREATE TABLE IF NOT EXISTS complex_entities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    created_date TEXT NOT NULL,
                    updated_date TEXT,
                    unique_id TEXT NOT NULL,
                    duration TEXT NOT NULL,
                    status TEXT NOT NULL,
                    status_int INTEGER NOT NULL,
                    tags TEXT,
                    scores TEXT,
                    metadata TEXT,
                    address TEXT,
                    is_active INTEGER NOT NULL,
                    nullable_int INTEGER,
                    price REAL NOT NULL
                )
            ");

            await personRepo.ExecuteSqlAsync(@"
                CREATE TABLE IF NOT EXISTS authors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    company_id INTEGER,
                    version INTEGER NOT NULL DEFAULT 1
                )
            ");

            await personRepo.ExecuteSqlAsync(@"
                CREATE TABLE IF NOT EXISTS books (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    author_id INTEGER NOT NULL,
                    publisher_id INTEGER
                )
            ");
        }

        /// <summary>
        /// Cleans up the database after testing.
        /// </summary>
        /// <returns>A task representing the asynchronous cleanup operation.</returns>
        public async Task CleanupDatabaseAsync()
        {
            if (_KeepAliveConnection != null)
            {
                _KeepAliveConnection.Close();
                _KeepAliveConnection.Dispose();
                _KeepAliveConnection = null;
            }

            await Task.CompletedTask;
        }

        /// <summary>
        /// Checks if the database connection is available and working.
        /// </summary>
        /// <returns>A task that returns true if the database is available, false otherwise.</returns>
        public async Task<bool> IsDatabaseAvailableAsync()
        {
            await Task.CompletedTask;
            return true;
        }

        /// <summary>
        /// Disposes resources used by the provider.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Disposes resources used by the provider.
        /// </summary>
        /// <param name="disposing">Whether to dispose managed resources.</param>
        private void Dispose(bool disposing)
        {
            if (_Disposed) return;

            if (disposing)
            {
                if (_KeepAliveConnection != null)
                {
                    _KeepAliveConnection.Close();
                    _KeepAliveConnection.Dispose();
                    _KeepAliveConnection = null;
                }
            }

            _Disposed = true;
        }

        #endregion
    }
}
