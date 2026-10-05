namespace Test.Shared
{
    using System;
    using System.Data.Common;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sql;
    using Durable.MySql;
    using MySqlConnector;

    /// <summary>
    /// MySQL implementation of the repository provider for shared testing infrastructure.
    /// </summary>
    public class MySqlRepositoryProvider : IRepositoryProvider
    {
        #region Private-Members

        private readonly string _ConnectionString;
        private bool _Disposed = false;

        #endregion

        #region Public-Members

        /// <summary>
        /// Gets the name of the database provider.
        /// </summary>
        public string ProviderName => "MySQL";

        /// <summary>
        /// Gets the database type served by this provider.
        /// </summary>
        public TestDatabaseType DatabaseType => TestDatabaseType.MySql;

        /// <summary>
        /// Gets the connection string used by this provider.
        /// </summary>
        public string ConnectionString => _ConnectionString;

        /// <summary>
        /// Gets the SQL dialect of this provider.
        /// </summary>
        public ISqlDialect Dialect => MySqlDialect.Default;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="MySqlRepositoryProvider"/> class.
        /// </summary>
        /// <param name="connectionString">The MySQL connection string to use for tests.</param>
        public MySqlRepositoryProvider(string connectionString)
        {
            _ConnectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
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
            return new MySqlRepository<T>(_ConnectionString);
        }

        /// <summary>
        /// Creates a new connection factory for the test database. The caller owns and disposes it.
        /// </summary>
        /// <param name="maxConcurrentConnections">Optional cap on concurrently open connections; null means no cap.</param>
        /// <returns>A new connection factory.</returns>
        public IConnectionFactory CreateConnectionFactory(int? maxConcurrentConnections = null)
        {
            return new MySqlConnectionFactory(_ConnectionString, maxConcurrentConnections);
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
            return new MySqlRepository<T>(connectionFactory, options);
        }

        /// <summary>
        /// Creates a repository from the provider's connection string with the supplied options. The repository owns its factory.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="options">The repository options.</param>
        /// <returns>A configured repository instance.</returns>
        public ISqlRepository<T> CreateRepositoryWithOptions<T>(SqlRepositoryOptions options) where T : class, new()
        {
            return new MySqlRepository<T>(_ConnectionString, options);
        }

        /// <summary>
        /// Creates a new, unopened <see cref="MySqlConnection"/> for the test database.
        /// </summary>
        /// <returns>An unopened connection the caller owns.</returns>
        public DbConnection CreateRawConnection()
        {
            return new MySqlConnection(_ConnectionString);
        }

        /// <summary>
        /// Sets up the database schema for testing.
        /// </summary>
        /// <returns>A task representing the asynchronous setup operation.</returns>
        public async Task SetupDatabaseAsync()
        {
            ISqlRepository<Person> personRepo = CreateRepository<Person>();

            await personRepo.ExecuteSqlAsync(@"
                CREATE TABLE IF NOT EXISTS people (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    first VARCHAR(64) NOT NULL,
                    last VARCHAR(64) NOT NULL,
                    age INT NOT NULL,
                    email VARCHAR(128),
                    salary DECIMAL(15,2) NOT NULL,
                    department VARCHAR(32)
                ) ENGINE=InnoDB
            ");

            await personRepo.ExecuteSqlAsync(@"
                CREATE TABLE IF NOT EXISTS complex_entities (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    created_date DATETIME NOT NULL,
                    updated_date DATETIME,
                    unique_id CHAR(36) NOT NULL,
                    duration BIGINT NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    status_int INT NOT NULL,
                    tags JSON,
                    scores JSON,
                    metadata JSON,
                    address JSON,
                    is_active BOOLEAN NOT NULL,
                    nullable_int INT,
                    price DECIMAL(15,2) NOT NULL
                ) ENGINE=InnoDB
            ");

            await personRepo.ExecuteSqlAsync(@"
                CREATE TABLE IF NOT EXISTS authors (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    company_id INT,
                    version INT NOT NULL DEFAULT 1
                ) ENGINE=InnoDB
            ");

            await personRepo.ExecuteSqlAsync(@"
                CREATE TABLE IF NOT EXISTS books (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(200) NOT NULL,
                    author_id INT NOT NULL,
                    publisher_id INT
                ) ENGINE=InnoDB
            ");

            await personRepo.ExecuteSqlAsync(@"
                CREATE TABLE IF NOT EXISTS author_categories (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    author_id INT NOT NULL,
                    category_id INT NOT NULL
                ) ENGINE=InnoDB
            ");

            await personRepo.ExecuteSqlAsync(@"
                CREATE TABLE IF NOT EXISTS categories (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    description VARCHAR(255)
                ) ENGINE=InnoDB
            ");

            await personRepo.ExecuteSqlAsync(@"
                CREATE TABLE IF NOT EXISTS companies (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    industry VARCHAR(50)
                ) ENGINE=InnoDB
            ");

            await personRepo.ExecuteSqlAsync(@"
                CREATE TABLE IF NOT EXISTS employees (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    first_name VARCHAR(100) NOT NULL,
                    last_name VARCHAR(100) NOT NULL,
                    email VARCHAR(255) NOT NULL,
                    department VARCHAR(100) NOT NULL,
                    hire_date DATETIME NOT NULL,
                    salary DECIMAL(15,2) NOT NULL
                ) ENGINE=InnoDB
            ");

            await personRepo.ExecuteSqlAsync(@"
                CREATE TABLE IF NOT EXISTS products (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    sku VARCHAR(50) NOT NULL,
                    category VARCHAR(100) NOT NULL,
                    price DECIMAL(15,2) NOT NULL,
                    stock_quantity INT NOT NULL,
                    description VARCHAR(1000)
                ) ENGINE=InnoDB
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

                await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS author_categories");
                await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS books");
                await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS authors_with_version");
                await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS authors");
                await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS categories");
                await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS companies");
                await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS complex_entities");
                await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS employees");
                await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS people");
                await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS products");
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
                using MySqlConnection connection = new MySqlConnection(_ConnectionString);
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

        private void Dispose(bool disposing)
        {
            if (_Disposed) return;

            _Disposed = true;
        }

        #endregion
    }
}
