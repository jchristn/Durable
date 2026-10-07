namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Oracle;
    using Durable.Sql;
    using global::Oracle.ManagedDataAccess.Client;

    /// <summary>
    /// Oracle implementation of the repository provider for shared testing infrastructure. Tables are created with
    /// unquoted (upper-case) names, which is what <see cref="OracleDialect.Default"/> generates, and string columns are
    /// nullable as <see cref="OracleDialect.ColumnAllowsNull"/> declares them (Oracle stores an empty string as NULL).
    /// </summary>
    public class OracleRepositoryProvider : IRepositoryProvider
    {
        #region Private-Members

        private static readonly string[] _Tables = new[]
        {
            "author_categories", "books", "authors_with_version", "authors", "categories", "companies",
            "complex_entities", "employees", "people", "products"
        };

        private readonly string _ConnectionString;
        private bool _Disposed = false;

        #endregion

        #region Public-Members

        /// <summary>
        /// Gets the name of the database provider.
        /// </summary>
        public string ProviderName => "Oracle";

        /// <summary>
        /// Gets the database type served by this provider.
        /// </summary>
        public TestDatabaseType DatabaseType => TestDatabaseType.Oracle;

        /// <summary>
        /// Gets the connection string used by this provider.
        /// </summary>
        public string ConnectionString => _ConnectionString;

        /// <summary>
        /// Gets the SQL dialect of this provider.
        /// </summary>
        public ISqlDialect Dialect => OracleDialect.Default;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="OracleRepositoryProvider"/> class.
        /// </summary>
        /// <param name="connectionString">The ODP.NET connection string to use for tests.</param>
        public OracleRepositoryProvider(string connectionString)
        {
            _ConnectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns a PL/SQL block that drops a table (with PURGE, so it skips the recycle bin) and ignores ORA-00942.
        /// </summary>
        /// <param name="table">Table name (folded to upper case unless quoted by the caller).</param>
        /// <returns>The block.</returns>
        public static string DropTableIfExistsSql(string table)
        {
            return "BEGIN EXECUTE IMMEDIATE 'DROP TABLE " + table.Replace("'", "''") + " CASCADE CONSTRAINTS PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE <> -942 THEN RAISE; END IF; END;";
        }

        /// <summary>
        /// Creates and configures a repository for the specified entity type.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <returns>A configured repository instance.</returns>
        public ISqlRepository<T> CreateRepository<T>() where T : class, new()
        {
            return new OracleRepository<T>(_ConnectionString);
        }

        /// <summary>
        /// Creates a new connection factory for the test database. The caller owns and disposes it.
        /// </summary>
        /// <param name="maxConcurrentConnections">Optional cap on concurrently open connections; null means no cap.</param>
        /// <returns>A new connection factory.</returns>
        public IConnectionFactory CreateConnectionFactory(int? maxConcurrentConnections = null)
        {
            return new OracleConnectionFactory(_ConnectionString, maxConcurrentConnections);
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
            return new OracleRepository<T>(connectionFactory, options);
        }

        /// <summary>
        /// Creates a repository from the provider's connection string with the supplied options. The repository owns its factory.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="options">The repository options.</param>
        /// <returns>A configured repository instance.</returns>
        public ISqlRepository<T> CreateRepositoryWithOptions<T>(SqlRepositoryOptions options) where T : class, new()
        {
            return new OracleRepository<T>(_ConnectionString, options);
        }

        /// <summary>
        /// Creates a new, unopened <see cref="OracleConnection"/> for the test database (binding by name).
        /// </summary>
        /// <returns>An unopened connection the caller owns.</returns>
        public DbConnection CreateRawConnection()
        {
            return OracleConnectionFactory.CreateRawConnection(_ConnectionString);
        }

        /// <summary>
        /// Sets up the database schema for testing.
        /// </summary>
        /// <returns>A task representing the asynchronous setup operation.</returns>
        public async Task SetupDatabaseAsync()
        {
            ISqlRepository<Person> personRepo = CreateRepository<Person>();
            foreach (string table in _Tables) await personRepo.ExecuteSqlRawAsync(DropTableIfExistsSql(table));

            List<string> statements = new List<string>
            {
                @"CREATE TABLE people (
                    id NUMBER(10) GENERATED BY DEFAULT ON NULL AS IDENTITY PRIMARY KEY,
                    first VARCHAR2(64 CHAR),
                    last VARCHAR2(64 CHAR),
                    age NUMBER(10) NOT NULL,
                    email VARCHAR2(128 CHAR),
                    salary NUMBER(15,2) NOT NULL,
                    department VARCHAR2(32 CHAR)
                )",
                @"CREATE TABLE complex_entities (
                    id NUMBER(10) GENERATED BY DEFAULT ON NULL AS IDENTITY PRIMARY KEY,
                    name VARCHAR2(100 CHAR),
                    created_date TIMESTAMP(7) NOT NULL,
                    updated_date TIMESTAMP(7) WITH TIME ZONE,
                    unique_id RAW(16) NOT NULL,
                    duration INTERVAL DAY(9) TO SECOND(7) NOT NULL,
                    status VARCHAR2(50 CHAR),
                    status_int NUMBER(10) NOT NULL,
                    tags VARCHAR2(4000 CHAR),
                    scores VARCHAR2(4000 CHAR),
                    metadata VARCHAR2(4000 CHAR),
                    address VARCHAR2(4000 CHAR),
                    is_active NUMBER(1) NOT NULL,
                    nullable_int NUMBER(10),
                    price NUMBER(15,2) NOT NULL
                )",
                @"CREATE TABLE authors (
                    id NUMBER(10) GENERATED BY DEFAULT ON NULL AS IDENTITY PRIMARY KEY,
                    name VARCHAR2(200 CHAR),
                    company_id NUMBER(10),
                    version NUMBER(10) DEFAULT 1 NOT NULL
                )",
                @"CREATE TABLE books (
                    id NUMBER(10) GENERATED BY DEFAULT ON NULL AS IDENTITY PRIMARY KEY,
                    title VARCHAR2(200 CHAR),
                    author_id NUMBER(10) NOT NULL,
                    publisher_id NUMBER(10)
                )",
                @"CREATE TABLE author_categories (
                    id NUMBER(10) GENERATED BY DEFAULT ON NULL AS IDENTITY PRIMARY KEY,
                    author_id NUMBER(10) NOT NULL,
                    category_id NUMBER(10) NOT NULL
                )",
                @"CREATE TABLE categories (
                    id NUMBER(10) GENERATED BY DEFAULT ON NULL AS IDENTITY PRIMARY KEY,
                    name VARCHAR2(100 CHAR),
                    description VARCHAR2(255 CHAR)
                )",
                @"CREATE TABLE companies (
                    id NUMBER(10) GENERATED BY DEFAULT ON NULL AS IDENTITY PRIMARY KEY,
                    name VARCHAR2(100 CHAR),
                    industry VARCHAR2(50 CHAR)
                )",
                @"CREATE TABLE employees (
                    id NUMBER(10) GENERATED BY DEFAULT ON NULL AS IDENTITY PRIMARY KEY,
                    first_name VARCHAR2(100 CHAR),
                    last_name VARCHAR2(100 CHAR),
                    email VARCHAR2(255 CHAR),
                    department VARCHAR2(100 CHAR),
                    hire_date TIMESTAMP(7) NOT NULL,
                    salary NUMBER(15,2) NOT NULL
                )",
                @"CREATE TABLE products (
                    id NUMBER(10) GENERATED BY DEFAULT ON NULL AS IDENTITY PRIMARY KEY,
                    name VARCHAR2(200 CHAR),
                    sku VARCHAR2(50 CHAR),
                    category VARCHAR2(100 CHAR),
                    price NUMBER(15,2) NOT NULL,
                    stock_quantity NUMBER(10) NOT NULL,
                    description VARCHAR2(1000 CHAR)
                )"
            };

            foreach (string statement in statements) await personRepo.ExecuteSqlRawAsync(statement);
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
                foreach (string table in _Tables) await personRepo.ExecuteSqlRawAsync(DropTableIfExistsSql(table));
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
                await using DbConnection connection = CreateRawConnection();
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
