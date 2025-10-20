namespace Test.MySql
{
    using System;
    using System.Threading.Tasks;
    using Durable;
    using Durable.MySql;
    using MySqlConnector;
    using Test.Shared;

    /// <summary>
    /// MySQL implementation of the repository provider for shared testing infrastructure.
    /// </summary>
    public class MySqlRepositoryProvider : IRepositoryProvider
    {
        #region Private-Members

        private const string TestConnectionString = "Server=localhost;Database=durable_test;User=test_user;Password=test_password;";
        private const string TestDatabaseName = "durable_test";
        private bool _Disposed = false;
        private bool _DatabaseAvailable = false;

        #endregion

        #region Public-Members

        /// <summary>
        /// Gets the name of the database provider.
        /// </summary>
        public string ProviderName => "MySQL";

        #endregion

        #region Public-Methods

        /// <summary>
        /// Creates and configures a repository for the specified entity type.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <returns>A configured repository instance.</returns>
        public IRepository<T> CreateRepository<T>() where T : class, new()
        {
            return new MySqlRepository<T>(TestConnectionString);
        }

        /// <summary>
        /// Sets up the database schema for testing.
        /// </summary>
        /// <returns>A task representing the asynchronous setup operation.</returns>
        public async Task SetupDatabaseAsync()
        {
            IRepository<Person> personRepo = CreateRepository<Person>();

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
                    isbn VARCHAR(20) NOT NULL,
                    published_date DATETIME NOT NULL,
                    author_id INT NOT NULL
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
                IRepository<Person> personRepo = CreateRepository<Person>();

                await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS books");
                await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS authors_with_version");
                await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS authors");
                await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS complex_entities");
                await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS people");
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
                using MySqlConnection connection = new MySqlConnection(TestConnectionString);
                await connection.OpenAsync();
                _DatabaseAvailable = true;
                return true;
            }
            catch
            {
                _DatabaseAvailable = false;
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

        /// <summary>
        /// Disposes resources used by the provider.
        /// </summary>
        /// <param name="disposing">Whether to dispose managed resources.</param>
        private void Dispose(bool disposing)
        {
            if (_Disposed) return;

            if (disposing)
            {
            }

            _Disposed = true;
        }

        #endregion
    }
}
