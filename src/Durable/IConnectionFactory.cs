namespace Durable
{
    using System;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Provides a factory interface for creating and managing database connections with support for connection pooling.
    /// </summary>
    public interface IConnectionFactory : IDisposable
    {
        /// <summary>
        /// Synchronously retrieves a database connection from the factory.
        /// </summary>
        /// <returns>A database connection ready for use.</returns>
        DbConnection GetConnection();
        
        /// <summary>
        /// Asynchronously retrieves a database connection from the factory.
        /// </summary>
        /// <param name="cancellationToken">Token to cancel the connection retrieval operation.</param>
        /// <returns>A task representing the asynchronous operation, containing a database connection ready for use.</returns>
        Task<DbConnection> GetConnectionAsync(CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Returns a database connection to the factory for potential reuse or disposal.
        /// </summary>
        /// <param name="connection">The database connection to return to the factory.</param>
        void ReturnConnection(DbConnection connection);
        
        /// <summary>
        /// Asynchronously returns a database connection to the factory for potential reuse or disposal.
        /// </summary>
        /// <param name="connection">The database connection to return to the factory.</param>
        /// <returns>A task representing the asynchronous return operation.</returns>
        Task ReturnConnectionAsync(DbConnection connection);
    }
}