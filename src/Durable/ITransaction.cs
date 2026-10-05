namespace Durable
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// A backend-neutral unit of work. Pass an instance to repository methods to run them inside the transaction.
    /// SQL providers return <c>Durable.Sql.ISqlTransaction</c>, which also exposes the underlying connection and savepoints.
    /// Thread safety: a transaction must not be used concurrently from multiple threads.
    /// </summary>
    public interface ITransaction : IDisposable, IAsyncDisposable
    {
        /// <summary>
        /// Gets whether <see cref="Commit"/> or <see cref="Rollback"/> has completed.
        /// </summary>
        bool IsCompleted { get; }

        /// <summary>
        /// Commits the transaction.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown when the transaction has already completed.</exception>
        void Commit();

        /// <summary>
        /// Rolls back the transaction.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown when the transaction has already completed.</exception>
        void Rollback();

        /// <summary>
        /// Commits the transaction asynchronously.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the transaction has already completed.</exception>
        Task CommitAsync(CancellationToken token = default);

        /// <summary>
        /// Rolls back the transaction asynchronously.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the transaction has already completed.</exception>
        Task RollbackAsync(CancellationToken token = default);
    }
}
