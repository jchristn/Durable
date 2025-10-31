namespace Test.Postgres
{
    using System;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Postgres;
    using Npgsql;
    using Test.Shared;

    /// <summary>
    /// PostgreSQL implementation of the repository provider for shared testing infrastructure.
    /// </summary>
    public class PostgresRepositoryProvider : IRepositoryProvider
    {
        #region Private-Members

        private readonly string _ConnectionString;
        private bool _Disposed = false;

        #endregion

        #region Public-Members

        /// <summary>
        /// Gets the name of the database provider.
        /// </summary>
        public string ProviderName => "PostgreSQL";

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="PostgresRepositoryProvider"/> class.
        /// </summary>
        /// <param name="connectionString">The PostgreSQL connection string to use for tests.</param>
        public PostgresRepositoryProvider(string connectionString)
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
            return new PostgresRepository<T>(_ConnectionString);
        }

        /// <summary>
        /// Sets up the database schema for testing.
        /// </summary>
        /// <returns>A task representing the asynchronous setup operation.</returns>
        public async Task SetupDatabaseAsync()
        {
            IRepository<Person> personRepo = CreateRepository<Person>();

            await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS author_categories CASCADE");
            await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS books CASCADE");
            await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS authors_with_version CASCADE");
            await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS authors CASCADE");
            await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS categories CASCADE");
            await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS companies CASCADE");
            await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS complex_entities CASCADE");
            await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS employees CASCADE");
            await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS people CASCADE");
            await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS products CASCADE");

            await personRepo.ExecuteSqlAsync(@"
                CREATE TABLE people (
                    id SERIAL PRIMARY KEY,
                    first VARCHAR(64) NOT NULL,
                    last VARCHAR(64) NOT NULL,
                    age INT NOT NULL,
                    email VARCHAR(128),
                    salary NUMERIC(15,2) NOT NULL,
                    department VARCHAR(32)
                )
            ");

            await personRepo.ExecuteSqlAsync(@"
                CREATE TABLE complex_entities (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    created_date TIMESTAMP NOT NULL,
                    updated_date TIMESTAMPTZ,
                    unique_id UUID NOT NULL,
                    duration INTERVAL NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    status_int INT NOT NULL,
                    tags JSONB,
                    scores JSONB,
                    metadata JSONB,
                    address JSONB,
                    is_active BOOLEAN NOT NULL,
                    nullable_int INT,
                    price NUMERIC(15,2) NOT NULL
                )
            ");

            await personRepo.ExecuteSqlAsync(@"
                CREATE TABLE authors (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    company_id INT,
                    version INT NOT NULL DEFAULT 1
                )
            ");

            await personRepo.ExecuteSqlAsync(@"
                CREATE TABLE books (
                    id SERIAL PRIMARY KEY,
                    title VARCHAR(200) NOT NULL,
                    author_id INT NOT NULL,
                    publisher_id INT
                )
            ");

            await personRepo.ExecuteSqlAsync(@"
                CREATE TABLE author_categories (
                    id SERIAL PRIMARY KEY,
                    author_id INT NOT NULL,
                    category_id INT NOT NULL
                )
            ");

            await personRepo.ExecuteSqlAsync(@"
                CREATE TABLE categories (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    description VARCHAR(255)
                )
            ");

            await personRepo.ExecuteSqlAsync(@"
                CREATE TABLE companies (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    industry VARCHAR(50)
                )
            ");

            await personRepo.ExecuteSqlAsync(@"
                CREATE TABLE employees (
                    id SERIAL PRIMARY KEY,
                    first_name VARCHAR(100) NOT NULL,
                    last_name VARCHAR(100) NOT NULL,
                    email VARCHAR(255) NOT NULL,
                    department VARCHAR(100) NOT NULL,
                    hire_date TIMESTAMP NOT NULL,
                    salary NUMERIC(15,2) NOT NULL
                )
            ");

            await personRepo.ExecuteSqlAsync(@"
                CREATE TABLE products (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    sku VARCHAR(50) NOT NULL,
                    category VARCHAR(100) NOT NULL,
                    price NUMERIC(15,2) NOT NULL,
                    stock_quantity INT NOT NULL,
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
                IRepository<Person> personRepo = CreateRepository<Person>();

                await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS author_categories CASCADE");
                await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS books CASCADE");
                await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS authors_with_version CASCADE");
                await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS authors CASCADE");
                await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS categories CASCADE");
                await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS companies CASCADE");
                await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS complex_entities CASCADE");
                await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS employees CASCADE");
                await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS people CASCADE");
                await personRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS products CASCADE");
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
