namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Threading.Tasks;
    using Durable;
    using Durable.DuckDb;
    using Durable.Sql;
    using DuckDB.NET.Data;

    /// <summary>
    /// DuckDB implementation of the repository provider for shared testing infrastructure. DuckDB runs in-process: the
    /// default database is the process-wide shared in-memory database (<c>:memory:?cache=shared</c>), kept alive by a
    /// connection held for the provider's lifetime so every repository and raw connection sees the same data.
    /// </summary>
    public class DuckDbRepositoryProvider : IRepositoryProvider
    {
        #region Public-Members

        /// <summary>
        /// The connection string used when none is supplied: the process-wide shared in-memory database.
        /// </summary>
        public const string DefaultConnectionString = "Data Source=:memory:?cache=shared";

        /// <summary>
        /// Gets the name of the database provider.
        /// </summary>
        public string ProviderName => "DuckDB";

        /// <summary>
        /// Gets the database type served by this provider.
        /// </summary>
        public TestDatabaseType DatabaseType => TestDatabaseType.DuckDb;

        /// <summary>
        /// Gets the connection string used by this provider.
        /// </summary>
        public string ConnectionString => _ConnectionString;

        /// <summary>
        /// Gets the SQL dialect of this provider.
        /// </summary>
        public ISqlDialect Dialect => DuckDbDialect.Default;

        #endregion

        #region Private-Members

        private static readonly string[] _Tables = new[]
        {
            "author_categories", "books", "authors_with_version", "authors", "categories", "companies", "complex_entities", "employees", "people", "products"
        };

        private readonly string _ConnectionString;
        private DuckDBConnection? _KeepAliveConnection;
        private bool _Disposed = false;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="DuckDbRepositoryProvider"/> class.
        /// </summary>
        /// <param name="connectionString">The DuckDB connection string; null uses <see cref="DefaultConnectionString"/>.</param>
        public DuckDbRepositoryProvider(string? connectionString = null)
        {
            _ConnectionString = connectionString ?? DefaultConnectionString;
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
            EnsureKeepAlive();
            return new DuckDbRepository<T>(_ConnectionString);
        }

        /// <summary>
        /// Creates a new connection factory for the test database. The caller owns and disposes it.
        /// </summary>
        /// <param name="maxConcurrentConnections">Optional cap on concurrently open connections; null means no cap.</param>
        /// <returns>A new connection factory.</returns>
        public IConnectionFactory CreateConnectionFactory(int? maxConcurrentConnections = null)
        {
            EnsureKeepAlive();
            return new DuckDbConnectionFactory(_ConnectionString, maxConcurrentConnections);
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
            return new DuckDbRepository<T>(connectionFactory, options);
        }

        /// <summary>
        /// Creates a repository from the provider's connection string with the supplied options. The repository owns its factory.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="options">The repository options.</param>
        /// <returns>A configured repository instance.</returns>
        public ISqlRepository<T> CreateRepositoryWithOptions<T>(SqlRepositoryOptions options) where T : class, new()
        {
            EnsureKeepAlive();
            return new DuckDbRepository<T>(_ConnectionString, options);
        }

        /// <summary>
        /// Creates a new, unopened <see cref="DuckDBConnection"/> for the test database.
        /// </summary>
        /// <returns>An unopened connection the caller owns.</returns>
        public DbConnection CreateRawConnection()
        {
            EnsureKeepAlive();
            return new DuckDBConnection(_ConnectionString);
        }

        /// <summary>
        /// Sets up the database schema for testing.
        /// </summary>
        /// <returns>A task representing the asynchronous setup operation.</returns>
        public async Task SetupDatabaseAsync()
        {
            EnsureKeepAlive();
            using ISqlRepository<Person> personRepo = CreateRepository<Person>();
            await DropTablesAsync(personRepo);

            await CreateTableAsync(personRepo, "people", @"
                    first VARCHAR NOT NULL,
                    last VARCHAR NOT NULL,
                    age INTEGER NOT NULL,
                    email VARCHAR,
                    salary DECIMAL(15,2) NOT NULL,
                    department VARCHAR");

            await CreateTableAsync(personRepo, "complex_entities", @"
                    name VARCHAR NOT NULL,
                    created_date TIMESTAMP NOT NULL,
                    updated_date TIMESTAMPTZ,
                    unique_id UUID NOT NULL,
                    duration INTERVAL NOT NULL,
                    status VARCHAR NOT NULL,
                    status_int INTEGER NOT NULL,
                    tags JSON,
                    scores JSON,
                    metadata JSON,
                    address JSON,
                    is_active BOOLEAN NOT NULL,
                    nullable_int INTEGER,
                    price DECIMAL(15,2) NOT NULL");

            await CreateTableAsync(personRepo, "authors", @"
                    name VARCHAR NOT NULL,
                    company_id INTEGER,
                    version INTEGER NOT NULL DEFAULT 1");

            await CreateTableAsync(personRepo, "books", @"
                    title VARCHAR NOT NULL,
                    author_id INTEGER NOT NULL,
                    publisher_id INTEGER");

            await CreateTableAsync(personRepo, "author_categories", @"
                    author_id INTEGER NOT NULL,
                    category_id INTEGER NOT NULL");

            await CreateTableAsync(personRepo, "categories", @"
                    name VARCHAR NOT NULL,
                    description VARCHAR");

            await CreateTableAsync(personRepo, "companies", @"
                    name VARCHAR NOT NULL,
                    industry VARCHAR");

            await CreateTableAsync(personRepo, "employees", @"
                    first_name VARCHAR NOT NULL,
                    last_name VARCHAR NOT NULL,
                    email VARCHAR NOT NULL,
                    department VARCHAR NOT NULL,
                    hire_date TIMESTAMP NOT NULL,
                    salary DECIMAL(15,2) NOT NULL");

            await CreateTableAsync(personRepo, "products", @"
                    name VARCHAR NOT NULL,
                    sku VARCHAR NOT NULL,
                    category VARCHAR NOT NULL,
                    price DECIMAL(15,2) NOT NULL,
                    stock_quantity INTEGER NOT NULL,
                    description VARCHAR");
        }

        /// <summary>
        /// Cleans up the database after testing and releases the keep-alive connection.
        /// </summary>
        /// <returns>A task representing the asynchronous cleanup operation.</returns>
        public async Task CleanupDatabaseAsync()
        {
            try
            {
                using ISqlRepository<Person> personRepo = CreateRepository<Person>();
                await DropTablesAsync(personRepo);
            }
            catch (DbException)
            {
            }

            ReleaseKeepAlive();
        }

        /// <summary>
        /// Checks if the database is available (always true: DuckDB runs in-process).
        /// </summary>
        /// <returns>A task that returns true when a connection can be opened.</returns>
        public async Task<bool> IsDatabaseAvailableAsync()
        {
            try
            {
                using DuckDBConnection connection = new DuckDBConnection(_ConnectionString);
                await connection.OpenAsync();
                return true;
            }
            catch (DbException)
            {
                return false;
            }
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

        private void EnsureKeepAlive()
        {
            if (_KeepAliveConnection != null || _Disposed) return;
            DuckDBConnection connection = new DuckDBConnection(_ConnectionString);
            connection.Open();
            _KeepAliveConnection = connection;
        }

        private void ReleaseKeepAlive()
        {
            if (_KeepAliveConnection == null) return;
            _KeepAliveConnection.Dispose();
            _KeepAliveConnection = null;
        }

        private static async Task DropTablesAsync(ISqlRepository<Person> repository)
        {
            foreach (string table in _Tables)
            {
                await repository.ExecuteSqlRawAsync("DROP TABLE IF EXISTS " + table);
                await repository.ExecuteSqlRawAsync("DROP SEQUENCE IF EXISTS " + table + "_id_seq");
            }
        }

        private static async Task CreateTableAsync(ISqlRepository<Person> repository, string table, string columns)
        {
            await repository.ExecuteSqlRawAsync("CREATE SEQUENCE " + table + "_id_seq");
            await repository.ExecuteSqlRawAsync(
                "CREATE TABLE " + table + " (id INTEGER PRIMARY KEY DEFAULT nextval('" + table + "_id_seq'), " + columns + ")");
        }

        private void Dispose(bool disposing)
        {
            if (_Disposed) return;
            if (disposing) ReleaseKeepAlive();
            _Disposed = true;
        }

        #endregion
    }
}
