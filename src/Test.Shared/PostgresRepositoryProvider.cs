namespace Test.Shared
{
    using System;
    using System.Data.Common;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sql;
    using Durable.Postgres;
    using Npgsql;

    /// <summary>
    /// PostgreSQL implementation of the repository provider for shared testing infrastructure.
    /// </summary>
    public class PostgresRepositoryProvider : IRepositoryProvider
    {
        #region Private-Members

        private readonly string _ConnectionString;
        private readonly TestDatabaseType _DatabaseType;
        private readonly PostgresFlavor _Flavor;
        private bool _Disposed = false;

        #endregion

        #region Public-Members

        /// <summary>
        /// Gets the name of the database provider (for example "PostgreSQL").
        /// </summary>
        public string ProviderName => TestDatabaseTypes.ProviderName(_DatabaseType);

        /// <summary>
        /// Gets the database type served by this provider (PostgreSQL, CockroachDB or YugabyteDB). Default: <see cref="TestDatabaseType.Postgres"/>.
        /// </summary>
        public TestDatabaseType DatabaseType => _DatabaseType;

        /// <summary>
        /// Gets the connection string used by this provider.
        /// </summary>
        public string ConnectionString => _ConnectionString;

        /// <summary>
        /// Gets the SQL dialect of this provider.
        /// </summary>
        public ISqlDialect Dialect => PostgresDialect.For(_Flavor);

        /// <summary>
        /// Gets the driver flavor used for the served database type.
        /// </summary>
        public PostgresFlavor Flavor => _Flavor;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="PostgresRepositoryProvider"/> class.
        /// </summary>
        /// <param name="connectionString">The PostgreSQL connection string to use for tests.</param>
        /// <param name="databaseType">The database served (PostgreSQL, CockroachDB or YugabyteDB); wire-compatible databases reuse this provider. Default: <see cref="TestDatabaseType.Postgres"/>.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="connectionString"/> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="databaseType"/> is not served by this provider.</exception>
        public PostgresRepositoryProvider(string connectionString, TestDatabaseType databaseType = TestDatabaseType.Postgres)
        {
            _ConnectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            if (!TestDatabaseTypes.IsPostgresFamily(databaseType)) throw new ArgumentOutOfRangeException(nameof(databaseType), databaseType, "Not served by PostgresRepositoryProvider.");
            _DatabaseType = databaseType;
            _Flavor = FlavorFor(databaseType);
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns the driver flavor for a PostgreSQL-family database type.
        /// </summary>
        /// <param name="databaseType">Database type.</param>
        /// <returns>The flavor; <see cref="PostgresFlavor.PostgreSql"/> for anything that is not CockroachDB or YugabyteDB.</returns>
        public static PostgresFlavor FlavorFor(TestDatabaseType databaseType)
        {
            if (databaseType == TestDatabaseType.CockroachDb) return PostgresFlavor.CockroachDb;
            if (databaseType == TestDatabaseType.YugabyteDb) return PostgresFlavor.YugabyteDb;
            return PostgresFlavor.PostgreSql;
        }

        /// <summary>
        /// Creates and configures a repository for the specified entity type.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <returns>A configured repository instance.</returns>
        public ISqlRepository<T> CreateRepository<T>() where T : class, new()
        {
            return new PostgresRepository<T>(_ConnectionString, _Flavor);
        }

        /// <summary>
        /// Creates a new connection factory for the test database. The caller owns and disposes it.
        /// </summary>
        /// <param name="maxConcurrentConnections">Optional cap on concurrently open connections; null means no cap.</param>
        /// <returns>A new connection factory.</returns>
        public IConnectionFactory CreateConnectionFactory(int? maxConcurrentConnections = null)
        {
            return new PostgresConnectionFactory(_ConnectionString, maxConcurrentConnections) { Flavor = _Flavor };
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
            return new PostgresRepository<T>(connectionFactory, options);
        }

        /// <summary>
        /// Creates a repository from the provider's connection string with the supplied options. The repository owns its factory.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="options">The repository options.</param>
        /// <returns>A configured repository instance.</returns>
        public ISqlRepository<T> CreateRepositoryWithOptions<T>(SqlRepositoryOptions options) where T : class, new()
        {
            return new PostgresRepository<T>(_ConnectionString, _Flavor, options);
        }

        /// <summary>
        /// Creates a new, unopened <see cref="NpgsqlConnection"/> for the test database.
        /// </summary>
        /// <returns>An unopened connection the caller owns.</returns>
        public DbConnection CreateRawConnection()
        {
            return new NpgsqlConnection(_ConnectionString);
        }

        /// <summary>
        /// Sets up the database schema for testing.
        /// </summary>
        /// <returns>A task representing the asynchronous setup operation.</returns>
        public async Task SetupDatabaseAsync()
        {
            ISqlRepository<Person> personRepo = CreateRepository<Person>();

            await personRepo.ExecuteSqlRawAsync("DROP TABLE IF EXISTS author_categories CASCADE");
            await personRepo.ExecuteSqlRawAsync("DROP TABLE IF EXISTS books CASCADE");
            await personRepo.ExecuteSqlRawAsync("DROP TABLE IF EXISTS authors_with_version CASCADE");
            await personRepo.ExecuteSqlRawAsync("DROP TABLE IF EXISTS authors CASCADE");
            await personRepo.ExecuteSqlRawAsync("DROP TABLE IF EXISTS categories CASCADE");
            await personRepo.ExecuteSqlRawAsync("DROP TABLE IF EXISTS companies CASCADE");
            await personRepo.ExecuteSqlRawAsync("DROP TABLE IF EXISTS complex_entities CASCADE");
            await personRepo.ExecuteSqlRawAsync("DROP TABLE IF EXISTS employees CASCADE");
            await personRepo.ExecuteSqlRawAsync("DROP TABLE IF EXISTS people CASCADE");
            await personRepo.ExecuteSqlRawAsync("DROP TABLE IF EXISTS products CASCADE");

            await personRepo.ExecuteSqlRawAsync(@"
                CREATE TABLE people (
                    id " + KeyColumnType + @",
                    first VARCHAR(64) NOT NULL,
                    last VARCHAR(64) NOT NULL,
                    age INT4 NOT NULL,
                    email VARCHAR(128),
                    salary NUMERIC(15,2) NOT NULL,
                    department VARCHAR(32)
                )
            ");

            await personRepo.ExecuteSqlRawAsync(@"
                CREATE TABLE complex_entities (
                    id " + KeyColumnType + @",
                    name VARCHAR(100) NOT NULL,
                    created_date TIMESTAMP NOT NULL,
                    updated_date TIMESTAMPTZ,
                    unique_id UUID NOT NULL,
                    duration INTERVAL NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    status_int INT4 NOT NULL,
                    tags JSONB,
                    scores JSONB,
                    metadata JSONB,
                    address JSONB,
                    is_active BOOLEAN NOT NULL,
                    nullable_int INT4,
                    price NUMERIC(15,2) NOT NULL
                )
            ");

            await personRepo.ExecuteSqlRawAsync(@"
                CREATE TABLE authors (
                    id " + KeyColumnType + @",
                    name VARCHAR(200) NOT NULL,
                    company_id INT4,
                    version INT4 NOT NULL DEFAULT 1
                )
            ");

            await personRepo.ExecuteSqlRawAsync(@"
                CREATE TABLE books (
                    id " + KeyColumnType + @",
                    title VARCHAR(200) NOT NULL,
                    author_id INT4 NOT NULL,
                    publisher_id INT4
                )
            ");

            await personRepo.ExecuteSqlRawAsync(@"
                CREATE TABLE author_categories (
                    id " + KeyColumnType + @",
                    author_id INT4 NOT NULL,
                    category_id INT4 NOT NULL
                )
            ");

            await personRepo.ExecuteSqlRawAsync(@"
                CREATE TABLE categories (
                    id " + KeyColumnType + @",
                    name VARCHAR(100) NOT NULL,
                    description VARCHAR(255)
                )
            ");

            await personRepo.ExecuteSqlRawAsync(@"
                CREATE TABLE companies (
                    id " + KeyColumnType + @",
                    name VARCHAR(100) NOT NULL,
                    industry VARCHAR(50)
                )
            ");

            await personRepo.ExecuteSqlRawAsync(@"
                CREATE TABLE employees (
                    id " + KeyColumnType + @",
                    first_name VARCHAR(100) NOT NULL,
                    last_name VARCHAR(100) NOT NULL,
                    email VARCHAR(255) NOT NULL,
                    department VARCHAR(100) NOT NULL,
                    hire_date TIMESTAMP NOT NULL,
                    salary NUMERIC(15,2) NOT NULL
                )
            ");

            await personRepo.ExecuteSqlRawAsync(@"
                CREATE TABLE products (
                    id " + KeyColumnType + @",
                    name VARCHAR(200) NOT NULL,
                    sku VARCHAR(50) NOT NULL,
                    category VARCHAR(100) NOT NULL,
                    price NUMERIC(15,2) NOT NULL,
                    stock_quantity INT4 NOT NULL,
                    description VARCHAR(1000)
                )
            ");
        }

        /// <summary>
        /// Cleans up the database after testing.
        /// </summary>
        /// <returns>A task representing the asynchronous cleanup operation.</returns>
        public async Task CleanupDatabaseAsync()
        {
            try
            {
                ISqlRepository<Person> personRepo = CreateRepository<Person>();

                await personRepo.ExecuteSqlRawAsync("DROP TABLE IF EXISTS author_categories CASCADE");
                await personRepo.ExecuteSqlRawAsync("DROP TABLE IF EXISTS books CASCADE");
                await personRepo.ExecuteSqlRawAsync("DROP TABLE IF EXISTS authors_with_version CASCADE");
                await personRepo.ExecuteSqlRawAsync("DROP TABLE IF EXISTS authors CASCADE");
                await personRepo.ExecuteSqlRawAsync("DROP TABLE IF EXISTS categories CASCADE");
                await personRepo.ExecuteSqlRawAsync("DROP TABLE IF EXISTS companies CASCADE");
                await personRepo.ExecuteSqlRawAsync("DROP TABLE IF EXISTS complex_entities CASCADE");
                await personRepo.ExecuteSqlRawAsync("DROP TABLE IF EXISTS employees CASCADE");
                await personRepo.ExecuteSqlRawAsync("DROP TABLE IF EXISTS people CASCADE");
                await personRepo.ExecuteSqlRawAsync("DROP TABLE IF EXISTS products CASCADE");
            }
            catch
            {
            }
        }

        /// <summary>
        /// Checks if the database connection is available and working.
        /// </summary>
        /// <returns>A task that returns true if the database is available, false otherwise.</returns>
        public async Task<bool> IsDatabaseAvailableAsync()
        {
            try
            {
                using NpgsqlConnection connection = new NpgsqlConnection(_ConnectionString);
                await connection.OpenAsync();
                return true;
            }
            catch
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

        // Integer columns are INT4 (PostgreSQL's INT; CockroachDB's INT is 64-bit and binary COPY needs exact types).
        // PostgreSQL keeps SERIAL; CockroachDB's SERIAL is a 64-bit unique_rowid() (not sequential, too large for int keys),
        // so the wire-compatible databases use a 32-bit identity column.
        private string KeyColumnType => _Flavor == PostgresFlavor.PostgreSql ? "SERIAL PRIMARY KEY" : "INT4 GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY";

        private void Dispose(bool disposing)
        {
            if (_Disposed) return;

            _Disposed = true;
        }

        #endregion
    }
}
