namespace Durable.Sql
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// A savepoint within an <see cref="ISqlTransaction"/>. A savepoint is not a resource: it is not disposable and ends
    /// with its transaction. Call <see cref="Rollback"/> (or <see cref="RollbackAsync"/>) to undo the work done after it,
    /// or <see cref="Release"/> (or <see cref="ReleaseAsync"/>) to keep it; doing neither keeps the work, which then
    /// commits or rolls back with the transaction.
    /// Thread safety: not thread-safe.
    /// </summary>
    public interface ISavepoint
    {
        /// <summary>
        /// Gets the savepoint name. Never null.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Releases the savepoint, keeping its changes. A no-op on databases without a release statement.
        /// </summary>
        /// <exception cref="System.Data.Common.DbException">Thrown when the database rejects the statement (for example the transaction ended).</exception>
        void Release();

        /// <summary>
        /// Rolls back changes made after the savepoint. The transaction stays active.
        /// </summary>
        /// <exception cref="System.Data.Common.DbException">Thrown when the database rejects the statement (for example the savepoint was released).</exception>
        void Rollback();

        /// <summary>
        /// Releases the savepoint, keeping its changes. A no-op on databases without a release statement.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task.</returns>
        /// <exception cref="System.Data.Common.DbException">Thrown when the database rejects the statement (for example the transaction ended).</exception>
        /// <exception cref="OperationCanceledException">Thrown when <paramref name="token"/> is canceled.</exception>
        Task ReleaseAsync(CancellationToken token = default);

        /// <summary>
        /// Rolls back changes made after the savepoint. The transaction stays active.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task.</returns>
        /// <exception cref="System.Data.Common.DbException">Thrown when the database rejects the statement (for example the savepoint was released).</exception>
        /// <exception cref="OperationCanceledException">Thrown when <paramref name="token"/> is canceled.</exception>
        Task RollbackAsync(CancellationToken token = default);
    }
}
