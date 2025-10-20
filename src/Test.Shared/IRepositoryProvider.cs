namespace Test.Shared
{
    using System;
    using System.Threading.Tasks;
    using Durable;

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

        #endregion

        #region Public-Methods

        /// <summary>
        /// Creates and configures a repository for the specified entity type.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <returns>A configured repository instance.</returns>
        IRepository<T> CreateRepository<T>() where T : class, new();

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
