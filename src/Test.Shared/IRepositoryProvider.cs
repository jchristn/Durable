namespace Test.Shared
{
    using System;
    using System.Data.Common;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sql;

    /// <summary>
    /// Provides database-specific repository instances for shared testing infrastructure.
    /// Each database provider (SQLite, MySQL, PostgreSQL, SQL Server) implements this interface
    /// to supply repositories configured for their specific database.
    /// </summary>
    public interface IRepositoryProvider : IDisposable
    {
        #region Public-Members

        /// <summary>
        /// Gets the name of the database provider (e.g., "SQLite", "MySQL", "PostgreSQL", "SQL Server").
        /// </summary>
        string ProviderName { get; }

        /// <summary>
        /// Gets the database type served by this provider.
        /// </summary>
        TestDatabaseType DatabaseType { get; }

        /// <summary>
        /// Gets the connection string used by this provider. Never null.
        /// </summary>
        string ConnectionString { get; }

        /// <summary>
        /// Gets the SQL dialect of this provider (for example <c>SqliteDialect.Default</c>). Never null.
        /// </summary>
        ISqlDialect Dialect { get; }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Creates a new provider-specific connection factory for the test database. The caller owns and disposes it.
        /// </summary>
        /// <param name="maxConcurrentConnections">Optional cap on concurrently open connections; null means no cap.</param>
        /// <returns>A new connection factory.</returns>
        IConnectionFactory CreateConnectionFactory(int? maxConcurrentConnections = null);

        /// <summary>
        /// Creates a repository over an existing connection factory. The repository does not own the factory.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="connectionFactory">The shared connection factory.</param>
        /// <param name="options">Optional repository options; null uses defaults.</param>
        /// <returns>A configured repository instance.</returns>
        ISqlRepository<T> CreateRepository<T>(IConnectionFactory connectionFactory, SqlRepositoryOptions? options = null) where T : class, new();

        /// <summary>
        /// Creates a repository from the provider's connection string with the supplied options. The repository owns its factory.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="options">The repository options.</param>
        /// <returns>A configured repository instance.</returns>
        ISqlRepository<T> CreateRepositoryWithOptions<T>(SqlRepositoryOptions options) where T : class, new();

        /// <summary>
        /// Creates a new, unopened raw ADO.NET connection of the provider's native type for the test database.
        /// </summary>
        /// <returns>An unopened connection the caller owns.</returns>
        DbConnection CreateRawConnection();

        /// <summary>
        /// Creates and configures a repository for the specified entity type.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <returns>A configured repository instance.</returns>
        ISqlRepository<T> CreateRepository<T>() where T : class, new();

        /// <summary>
        /// Sets up the database schema for testing.
        /// This should create all necessary tables and prepare the database for test execution.
        /// </summary>
        /// <returns>A task representing the asynchronous setup operation.</returns>
        Task SetupDatabaseAsync();

        /// <summary>
        /// Cleans up the database after testing.
        /// This may drop tables, delete data, or perform other cleanup operations.
        /// </summary>
        /// <returns>A task representing the asynchronous cleanup operation.</returns>
        Task CleanupDatabaseAsync();

        /// <summary>
        /// Checks if the database connection is available and working.
        /// Returns true if the database is accessible, false otherwise.
        /// This allows tests to be skipped if the database is not available.
        /// </summary>
        /// <returns>A task that returns true if the database is available, false otherwise.</returns>
        Task<bool> IsDatabaseAvailableAsync();

        #endregion
    }
}
