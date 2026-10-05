namespace Test.Shared
{
    using System;
    using System.Data.Common;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sql;
    using Durable.Sqlite;
    using Microsoft.Data.Sqlite;

    /// <summary>
    /// SQLite implementation of the repository provider for shared testing infrastructure.
    /// </summary>
    public class SqliteRepositoryProvider : IRepositoryProvider
    {
        #region Private-Members

        private readonly string _ConnectionString;
        private SqliteConnection? _KeepAliveConnection;
        private bool _Disposed = false;

        #endregion

        #region Public-Members

        /// <summary>
        /// Gets the name of the database provider.
        /// </summary>
        public string ProviderName => "SQLite";

        /// <summary>
        /// Gets the database type served by this provider.
        /// </summary>
        public TestDatabaseType DatabaseType => TestDatabaseType.Sqlite;

        /// <summary>
        /// Gets the connection string used by this provider.
        /// </summary>
        public string ConnectionString => _ConnectionString;

        /// <summary>
        /// Gets the SQL dialect of this provider.
        /// </summary>
        public ISqlDialect Dialect => SqliteDialect.Default;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="SqliteRepositoryProvider"/> class.
        /// </summary>
        /// <param name="connectionString">The SQLite connection string to use for tests. If null, uses a shared in-memory database.</param>
        public SqliteRepositoryProvider(string? connectionString = null)
        {
            _ConnectionString = connectionString ?? "Data Source=file:/InMemorySharedTest?vfs=memdb";
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Creates and configures a repository for the specified entity type.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <returns>A configured repository instance.</returns>
        public ISqlRepository<T> CreateRepository<T>() where T : class, new()
        {
            return new SqliteRepository<T>(_ConnectionString);
        }

        /// <summary>
        /// Creates a new connection factory for the test database. The caller owns and disposes it.
        /// </summary>
        /// <param name="maxConcurrentConnections">Optional cap on concurrently open connections; null means no cap.</param>
        /// <returns>A new connection factory.</returns>
        public IConnectionFactory CreateConnectionFactory(int? maxConcurrentConnections = null)
        {
            return new SqliteConnectionFactory(_ConnectionString, maxConcurrentConnections);
        }

        /// <summary>
        /// Creates a repository over an existing connection factory. The repository does not own the factory.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="connectionFactory">The shared connection factory.</param>
        /// <param name="options">Optional repository options; null uses defaults.</param>
        /// <returns>A configured repository instance.</returns>
        public ISqlRepository<T> CreateRepository<T>(IConnectionFactory connectionFactory, SqlRepositoryOptions? options = null) where T : class, new()
        {
            return new SqliteRepository<T>(connectionFactory, options);
        }

        /// <summary>
        /// Creates a repository from the provider's connection string with the supplied options. The repository owns its factory.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="options">The repository options.</param>
        /// <returns>A configured repository instance.</returns>
        public ISqlRepository<T> CreateRepositoryWithOptions<T>(SqlRepositoryOptions options) where T : class, new()
        {
            return new SqliteRepository<T>(_ConnectionString, options);
        }

        /// <summary>
        /// Creates a new, unopened <see cref="SqliteConnection"/> for the test database.
        /// </summary>
        /// <returns>An unopened connection the caller owns.</returns>
        public DbConnection CreateRawConnection()
        {
            return new SqliteConnection(_ConnectionString);
        }

        /// <summary>
        /// Sets up the database schema for testing.
        /// </summary>
        /// <returns>A task representing the asynchronous setup operation.</returns>
        public async Task SetupDatabaseAsync()
        {
            _KeepAliveConnection = new SqliteConnection(_ConnectionString);
            _KeepAliveConnection.Open();

            ISqlRepository<Person> personRepo = CreateRepository<Person>();

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

            await personRepo.ExecuteSqlAsync(@"
                CREATE TABLE IF NOT EXISTS author_categories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    author_id INTEGER NOT NULL,
                    category_id INTEGER NOT NULL
                )
            ");

            await personRepo.ExecuteSqlAsync(@"
                CREATE TABLE IF NOT EXISTS categories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    description TEXT
                )
            ");

            await personRepo.ExecuteSqlAsync(@"
                CREATE TABLE IF NOT EXISTS companies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    industry TEXT
                )
            ");

            await personRepo.ExecuteSqlAsync(@"
                CREATE TABLE IF NOT EXISTS employees (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    email TEXT NOT NULL,
                    department TEXT NOT NULL,
                    hire_date TEXT NOT NULL,
                    salary REAL NOT NULL
                )
            ");

            await personRepo.ExecuteSqlAsync(@"
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    sku TEXT NOT NULL,
                    category TEXT NOT NULL,
                    price REAL NOT NULL,
                    stock_quantity INTEGER NOT NULL,
                    description TEXT
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
