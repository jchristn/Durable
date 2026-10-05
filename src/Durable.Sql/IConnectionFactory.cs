namespace Durable.Sql
{
    using System;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Supplies open database connections. Callers dispose connections when done, which returns them to the
    /// ADO.NET driver's pool; Durable does not keep its own pool.
    /// Thread safety: implementations must be safe for concurrent use.
    /// </summary>
    public interface IConnectionFactory : IDisposable, IAsyncDisposable
    {
        /// <summary>
        /// Opens a connection.
        /// </summary>
        /// <returns>An open connection owned by the caller.</returns>
        /// <exception cref="ObjectDisposedException">Thrown when the factory is disposed.</exception>
        /// <exception cref="TimeoutException">Thrown when a concurrency limit is configured and no slot frees up in time.</exception>
        DbConnection OpenConnection();

        /// <summary>
        /// Opens a connection.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>An open connection owned by the caller.</returns>
        /// <exception cref="ObjectDisposedException">Thrown when the factory is disposed.</exception>
        /// <exception cref="TimeoutException">Thrown when a concurrency limit is configured and no slot frees up in time.</exception>
        Task<DbConnection> OpenConnectionAsync(CancellationToken token = default);
    }
}
