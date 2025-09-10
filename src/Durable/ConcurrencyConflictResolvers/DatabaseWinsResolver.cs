namespace Durable.ConcurrencyConflictResolvers
{
    using System.Threading.Tasks;
    
    /// <summary>
    /// A concurrency conflict resolver that always chooses the current database entity as the winner.
    /// This resolver implements a "database wins" strategy where the database's current version always takes precedence over the incoming client version.
    /// </summary>
    /// <typeparam name="T">The type of entity being resolved. Must be a reference type.</typeparam>
    public class DatabaseWinsResolver<T> : IConcurrencyConflictResolver<T> where T : class
    {
        /// <summary>
        /// Gets or sets the default conflict resolution strategy used by this resolver.
        /// </summary>
        public ConflictResolutionStrategy DefaultStrategy { get; set; } = ConflictResolutionStrategy.DatabaseWins;
        
        /// <summary>
        /// Resolves a concurrency conflict by returning the current database entity (database wins).
        /// </summary>
        /// <param name="currentEntity">The current entity state in the data store.</param>
        /// <param name="incomingEntity">The incoming entity from the client.</param>
        /// <param name="originalEntity">The original entity state used as baseline for the update.</param>
        /// <param name="strategy">The conflict resolution strategy to apply.</param>
        /// <returns>The current database entity as the resolved conflict winner.</returns>
        public T ResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            return currentEntity;
        }
        
        /// <summary>
        /// Asynchronously resolves a concurrency conflict by returning the current database entity (database wins).
        /// </summary>
        /// <param name="currentEntity">The current entity state in the data store.</param>
        /// <param name="incomingEntity">The incoming entity from the client.</param>
        /// <param name="originalEntity">The original entity state used as baseline for the update.</param>
        /// <param name="strategy">The conflict resolution strategy to apply.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the current database entity as the resolved conflict winner.</returns>
        public Task<T> ResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            return Task.FromResult(currentEntity);
        }
        
        /// <summary>
        /// Attempts to resolve a concurrency conflict by returning the current database entity (database wins).
        /// This method always succeeds for the DatabaseWinsResolver.
        /// </summary>
        /// <param name="currentEntity">The current entity state in the data store.</param>
        /// <param name="incomingEntity">The incoming entity from the client.</param>
        /// <param name="originalEntity">The original entity state used as baseline for the update.</param>
        /// <param name="strategy">The conflict resolution strategy to apply.</param>
        /// <param name="resolvedEntity">When this method returns, contains the resolved entity (always the current database entity).</param>
        /// <returns>Always returns true, indicating successful conflict resolution.</returns>
        public bool TryResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy, out T resolvedEntity)
        {
            resolvedEntity = currentEntity;
            return true;
        }
        
        /// <summary>
        /// Asynchronously attempts to resolve a concurrency conflict by returning the current database entity (database wins).
        /// This method always succeeds for the DatabaseWinsResolver.
        /// </summary>
        /// <param name="currentEntity">The current entity state in the data store.</param>
        /// <param name="incomingEntity">The incoming entity from the client.</param>
        /// <param name="originalEntity">The original entity state used as baseline for the update.</param>
        /// <param name="strategy">The conflict resolution strategy to apply.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a successful resolution result with the current database entity.</returns>
        public Task<TryResolveConflictResult<T>> TryResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            return Task.FromResult(new TryResolveConflictResult<T> { Success = true, ResolvedEntity = currentEntity });
        }
    }
}