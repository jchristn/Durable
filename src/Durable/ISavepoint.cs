namespace Durable
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Represents a database savepoint that can be released or rolled back within a transaction.
    /// </summary>
    public interface ISavepoint : IDisposable
    {
        /// <summary>
        /// Gets the name of the savepoint.
        /// </summary>
        string Name { get; }
        
        /// <summary>
        /// Releases the savepoint, making it no longer available for rollback.
        /// </summary>
        void Release();
        
        /// <summary>
        /// Rolls back the transaction to this savepoint, undoing all changes made after the savepoint was created.
        /// </summary>
        void Rollback();
        
        /// <summary>
        /// Asynchronously releases the savepoint, making it no longer available for rollback.
        /// </summary>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous release operation.</returns>
        Task ReleaseAsync(CancellationToken token = default);
        
        /// <summary>
        /// Asynchronously rolls back the transaction to this savepoint, undoing all changes made after the savepoint was created.
        /// </summary>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous rollback operation.</returns>
        Task RollbackAsync(CancellationToken token = default);
    }
}