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

        private readonly string _ConnectionString;
        private bool _Disposed = false;

        #endregion

        #region Public-Members

        /// <summary>
        /// Gets the name of the database provider.
        /// </summary>
        public string ProviderName => "SQL Server";

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlServerRepositoryProvider"/> class.
        /// </summary>
        /// <param name="connectionString">The SQL Server connection string to use for tests.</param>
        public SqlServerRepositoryProvider(string connectionString)
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
        public IRepository<T> CreateRepository<T>() where T : class, new()
        {
            return new SqlServerRepository<T>(_ConnectionString);
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

            // Drop and recreate complex_entities table to ensure correct schema
            await personRepo.ExecuteSqlAsync("IF EXISTS (SELECT * FROM sys.tables WHERE name = 'complex_entities') DROP TABLE complex_entities");
            await personRepo.ExecuteSqlAsync(@"
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
                    author_id INT NOT NULL,
                    publisher_id INT
                )
            ");

            await personRepo.ExecuteSqlAsync(@"
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'author_categories')
                CREATE TABLE author_categories (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    author_id INT NOT NULL,
                    category_id INT NOT NULL
                )
            ");

            await personRepo.ExecuteSqlAsync(@"
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'categories')
                CREATE TABLE categories (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(100) NOT NULL,
                    description NVARCHAR(255)
                )
            ");

            await personRepo.ExecuteSqlAsync(@"
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'companies')
                CREATE TABLE companies (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(100) NOT NULL,
                    industry NVARCHAR(50)
                )
            ");

            await personRepo.ExecuteSqlAsync(@"
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'employees')
                CREATE TABLE employees (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    first_name NVARCHAR(100) NOT NULL,
                    last_name NVARCHAR(100) NOT NULL,
                    email NVARCHAR(255) NOT NULL,
                    department NVARCHAR(100) NOT NULL,
                    hire_date DATETIME2 NOT NULL,
                    salary DECIMAL(15,2) NOT NULL
                )
            ");

            await personRepo.ExecuteSqlAsync(@"
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'products')
                CREATE TABLE products (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(200) NOT NULL,
                    sku NVARCHAR(50) NOT NULL,
                    category NVARCHAR(100) NOT NULL,
                    price DECIMAL(15,2) NOT NULL,
                    stock_quantity INT NOT NULL,
                    description NVARCHAR(1000)
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

                await personRepo.ExecuteSqlAsync("IF EXISTS (SELECT * FROM sys.tables WHERE name = 'author_categories') DROP TABLE author_categories");
                await personRepo.ExecuteSqlAsync("IF EXISTS (SELECT * FROM sys.tables WHERE name = 'books') DROP TABLE books");
                await personRepo.ExecuteSqlAsync("IF EXISTS (SELECT * FROM sys.tables WHERE name = 'authors_with_version') DROP TABLE authors_with_version");
                await personRepo.ExecuteSqlAsync("IF EXISTS (SELECT * FROM sys.tables WHERE name = 'authors') DROP TABLE authors");
                await personRepo.ExecuteSqlAsync("IF EXISTS (SELECT * FROM sys.tables WHERE name = 'categories') DROP TABLE categories");
                await personRepo.ExecuteSqlAsync("IF EXISTS (SELECT * FROM sys.tables WHERE name = 'companies') DROP TABLE companies");
                await personRepo.ExecuteSqlAsync("IF EXISTS (SELECT * FROM sys.tables WHERE name = 'complex_entities') DROP TABLE complex_entities");
                await personRepo.ExecuteSqlAsync("IF EXISTS (SELECT * FROM sys.tables WHERE name = 'employees') DROP TABLE employees");
                await personRepo.ExecuteSqlAsync("IF EXISTS (SELECT * FROM sys.tables WHERE name = 'people') DROP TABLE people");
                await personRepo.ExecuteSqlAsync("IF EXISTS (SELECT * FROM sys.tables WHERE name = 'products') DROP TABLE products");
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
                using SqlConnection connection = new SqlConnection(_ConnectionString);
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
