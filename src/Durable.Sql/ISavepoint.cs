namespace Durable.Sql
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// A savepoint within an <see cref="ISqlTransaction"/>. Disposing an unreleased savepoint does nothing; roll back explicitly.
    /// Thread safety: not thread-safe.
    /// </summary>
    public interface ISavepoint : IDisposable
    {
        /// <summary>
        /// Gets the savepoint name. Never null.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Releases the savepoint, keeping its changes. A no-op on databases without a release statement.
        /// </summary>
        void Release();

        /// <summary>
        /// Rolls back changes made after the savepoint.
        /// </summary>
        void Rollback();

        /// <summary>
        /// Releases the savepoint, keeping its changes.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task.</returns>
        Task ReleaseAsync(CancellationToken token = default);

        /// <summary>
        /// Rolls back changes made after the savepoint.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task.</returns>
        Task RollbackAsync(CancellationToken token = default);
    }
}
