namespace Durable
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Represents a transaction scope that manages the lifecycle of a transaction.
    /// </summary>
    public interface ITransactionScope : IDisposable
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