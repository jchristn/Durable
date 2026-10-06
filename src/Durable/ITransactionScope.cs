namespace Durable
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// A scope that owns or shares a transaction: completing it commits (when it owns the transaction) and disposing it
    /// without completing rolls back. Dispose with <c>await using</c> so the rollback is asynchronous.
    /// See <see cref="AmbientTransactionScope"/>.
    /// </summary>
    public interface ITransactionScope : IDisposable, IAsyncDisposable
    {
        /// <summary>
        /// Gets the transaction associated with this scope.
        /// </summary>
        ITransaction Transaction { get; }
        
        /// <summary>
        /// Gets a value indicating whether the transaction scope has been completed.
        /// </summary>
        bool IsCompleted { get; }
        
        /// <summary>
        /// Marks the transaction scope as complete, indicating that the transaction should be committed.
        /// </summary>
        void Complete();
        
        /// <summary>
        /// Asynchronously marks the transaction scope as complete, indicating that the transaction should be committed.
        /// </summary>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous complete operation.</returns>
        Task CompleteAsync(CancellationToken token = default);
    }
}