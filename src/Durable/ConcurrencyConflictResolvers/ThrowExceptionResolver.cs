namespace Durable.ConcurrencyConflictResolvers
{
    using System;
    using System.Threading.Tasks;
    
    /// <summary>
    /// A concurrency conflict resolver that throws exceptions when conflicts are detected.
    /// This resolver never attempts to merge or resolve conflicts automatically.
    /// </summary>
    /// <typeparam name="T">The entity type that must be a reference type</typeparam>
    public class ThrowExceptionResolver<T> : IConcurrencyConflictResolver<T> where T : class
    {
        /// <summary>
        /// Gets or sets the default strategy used for conflict resolution. Always set to ThrowException.
        /// </summary>
        public ConflictResolutionStrategy DefaultStrategy { get; set; } = ConflictResolutionStrategy.ThrowException;
        
        /// <summary>
        /// Throws a ConcurrencyConflictException when called, as this resolver does not attempt to resolve conflicts.
        /// </summary>
        /// <param name="currentEntity">The current entity state</param>
        /// <param name="incomingEntity">The incoming entity state</param>
        /// <param name="originalEntity">The original entity state</param>
        /// <param name="strategy">The conflict resolution strategy (ignored)</param>
        /// <returns>Never returns normally, always throws an exception</returns>
        /// <exception cref="ConcurrencyConflictException">Always thrown when conflicts are detected</exception>
        public T ResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            throw new ConcurrencyConflictException($"Concurrency conflict detected for entity of type {typeof(T).Name}");
        }
        
        /// <summary>
        /// Throws a ConcurrencyConflictException when called, as this resolver does not attempt to resolve conflicts.
        /// </summary>
        /// <param name="currentEntity">The current entity state</param>
        /// <param name="incomingEntity">The incoming entity state</param>
        /// <param name="originalEntity">The original entity state</param>
        /// <param name="strategy">The conflict resolution strategy (ignored)</param>
        /// <returns>Never returns normally, always throws an exception</returns>
        /// <exception cref="ConcurrencyConflictException">Always thrown when conflicts are detected</exception>
        public Task<T> ResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            throw new ConcurrencyConflictException($"Concurrency conflict detected for entity of type {typeof(T).Name}");
        }
        
        /// <summary>
        /// Attempts to resolve conflicts but always returns false as this resolver does not resolve conflicts.
        /// </summary>
        /// <param name="currentEntity">The current entity state</param>
        /// <param name="incomingEntity">The incoming entity state</param>
        /// <param name="originalEntity">The original entity state</param>
        /// <param name="strategy">The conflict resolution strategy (ignored)</param>
        /// <param name="resolvedEntity">Always set to null</param>
        /// <returns>Always returns false</returns>
        public bool TryResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy, out T resolvedEntity)
        {
            resolvedEntity = null!;
            return false;
        }
        
        /// <summary>
        /// Asynchronously attempts to resolve conflicts but always returns a failure result.
        /// </summary>
        /// <param name="currentEntity">The current entity state</param>
        /// <param name="incomingEntity">The incoming entity state</param>
        /// <param name="originalEntity">The original entity state</param>
        /// <param name="strategy">The conflict resolution strategy (ignored)</param>
        /// <returns>A task containing a failure result with Success set to false</returns>
        public Task<TryResolveConflictResult<T>> TryResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            return Task.FromResult(new TryResolveConflictResult<T> { Success = false, ResolvedEntity = null! });
        }
    }
}