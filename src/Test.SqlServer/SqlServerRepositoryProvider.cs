namespace Test.SqlServer
{
    using System;
    using System.Threading.Tasks;
    using Durable;
    using Durable.SqlServer;
    using Microsoft.Data.SqlClient;
    using Test.Shared;

    /// <summary>
    /// SQL Server implementation of the repository provider for shared testing infrastructure.
    /// </summary>
    public class SqlServerRepositoryProvider : IRepositoryProvider
    {
        #region Private-Members

        private const string TestConnectionString = "Server=localhost;Database=durable_test;Trusted_Connection=True;";
        private bool _Disposed = false;
        private bool _DatabaseAvailable = false;

        #endregion

        #region Public-Members

        /// <summary>
        /// Gets the name of the database provider.
        /// </summary>
        public string ProviderName => "SQL Server";

        #endregion

        #region Public-Methods

        /// <summary>
        /// Creates and configures a repository for the specified entity type.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <returns>A configured repository instance.</returns>
        public IRepository<T> CreateRepository<T>() where T : class, new()
        {
            return new SqlServerRepository<T>(TestConnectionString);
        }

        /// <summary>
        /// Sets up the database schema for testing.
        /// </summary>
        /// <returns>A task representing the asynchronous setup operation.</returns>
        public async Task SetupDatabaseAsync()
        {
            IRepository<Person> personRepo = CreateRepository<Person>();

            await personRepo.ExecuteSqlAsync(@"
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'people')
                CREATE TABLE people (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    first NVARCHAR(64) NOT NULL,
                    last NVARCHAR(64) NOT NULL,
                    age INT NOT NULL,
                    email NVARCHAR(128),
                    salary DECIMAL(15,2) NOT NULL,
                    department NVARCHAR(32)
                )
            ");

            await personRepo.ExecuteSqlAsync(@"
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'complex_entities')
                CREATE TABLE complex_entities (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(100) NOT NULL,
                    created_date DATETIME2 NOT NULL,
                    updated_date DATETIMEOFFSET,
                    unique_id UNIQUEIDENTIFIER NOT NULL,
                    duration BIGINT NOT NULL,
                    status NVARCHAR(50) NOT NULL,
                    status_int INT NOT NULL,
                    tags NVARCHAR(MAX),
                    scores NVARCHAR(MAX),
                    metadata NVARCHAR(MAX),
                    address NVARCHAR(MAX),
                    is_active BIT NOT NULL,
                    nullable_int INT,
                    price DECIMAL(15,2) NOT NULL
                )
            ");

            await personRepo.ExecuteSqlAsync(@"
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'authors')
                CREATE TABLE authors (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(200) NOT NULL,
                    company_id INT,
                    version INT NOT NULL DEFAULT 1
                )
            ");

            await personRepo.ExecuteSqlAsync(@"
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'books')
                CREATE TABLE books (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    title NVARCHAR(200) NOT NULL,
                    isbn NVARCHAR(20) NOT NULL,
                    published_date DATETIME2 NOT NULL,
                    author_id INT NOT NULL
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
                IRepository<Person> personRepo = CreateRepository<Person>();

                await personRepo.ExecuteSqlAsync("IF EXISTS (SELECT * FROM sys.tables WHERE name = 'books') DROP TABLE books");
                await personRepo.ExecuteSqlAsync("IF EXISTS (SELECT * FROM sys.tables WHERE name = 'authors_with_version') DROP TABLE authors_with_version");
                await personRepo.ExecuteSqlAsync("IF EXISTS (SELECT * FROM sys.tables WHERE name = 'authors') DROP TABLE authors");
                await personRepo.ExecuteSqlAsync("IF EXISTS (SELECT * FROM sys.tables WHERE name = 'complex_entities') DROP TABLE complex_entities");
                await personRepo.ExecuteSqlAsync("IF EXISTS (SELECT * FROM sys.tables WHERE name = 'people') DROP TABLE people");
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
                using SqlConnection connection = new SqlConnection(TestConnectionString);
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
