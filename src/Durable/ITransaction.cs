namespace Durable
{
    using System;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Represents a database transaction that can be committed or rolled back.
    /// </summary>
    public interface ITransaction : IDisposable
    {
        /// <summary>
        /// Gets the database connection associated with this transaction.
        /// </summary>
        DbConnection Connection { get; }
        
        /// <summary>
        /// Gets the underlying database transaction.
        /// </summary>
        DbTransaction Transaction { get; }
        
        /// <summary>
        /// Commits the transaction, making all changes permanent.
        /// </summary>
        void Commit();
        
        /// <summary>
        /// Rolls back the transaction, undoing all changes made within the transaction.
        /// </summary>
        void Rollback();
        
        /// <summary>
        /// Asynchronously commits the transaction, making all changes permanent.
        /// </summary>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous commit operation.</returns>
        Task CommitAsync(CancellationToken token = default);
        
        /// <summary>
        /// Asynchronously rolls back the transaction, undoing all changes made within the transaction.
        /// </summary>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous rollback operation.</returns>
        Task RollbackAsync(CancellationToken token = default);
        
        /// <summary>
        /// Creates a savepoint within the transaction.
        /// </summary>
        /// <param name="name">The optional name for the savepoint. If null, a name will be generated.</param>
        /// <returns>An <see cref="ISavepoint"/> representing the created savepoint.</returns>
        ISavepoint CreateSavepoint(string? name = null);
        
        /// <summary>
        /// Asynchronously creates a savepoint within the transaction.
        /// </summary>
        /// <param name="name">The optional name for the savepoint. If null, a name will be generated.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that returns an <see cref="ISavepoint"/> representing the created savepoint.</returns>
        Task<ISavepoint> CreateSavepointAsync(string? name = null, CancellationToken token = default);
    }
}