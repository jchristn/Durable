using System.Threading.Tasks;

namespace Durable.ConcurrencyConflictResolvers
{
    /// <summary>
    /// A concurrency conflict resolver that always chooses the incoming (client-side) entity as the winner.
    /// This resolver implements a "client wins" strategy where the client's version always takes precedence over the current database version.
    /// </summary>
    /// <typeparam name="T">The type of entity being resolved. Must be a reference type.</typeparam>
    public class ClientWinsResolver<T> : IConcurrencyConflictResolver<T> where T : class
    {
        /// <summary>
        /// Gets or sets the default conflict resolution strategy used by this resolver.
        /// </summary>
        public ConflictResolutionStrategy DefaultStrategy { get; set; } = ConflictResolutionStrategy.ClientWins;
        
        /// <summary>
        /// Resolves a concurrency conflict by returning the incoming entity (client wins).
        /// </summary>
        /// <param name="currentEntity">The current entity state in the data store.</param>
        /// <param name="incomingEntity">The incoming entity from the client.</param>
        /// <param name="originalEntity">The original entity state used as baseline for the update.</param>
        /// <param name="strategy">The conflict resolution strategy to apply.</param>
        /// <returns>The incoming entity as the resolved conflict winner.</returns>
        public T ResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            return incomingEntity;
        }
        
        /// <summary>
        /// Asynchronously resolves a concurrency conflict by returning the incoming entity (client wins).
        /// </summary>
        /// <param name="currentEntity">The current entity state in the data store.</param>
        /// <param name="incomingEntity">The incoming entity from the client.</param>
        /// <param name="originalEntity">The original entity state used as baseline for the update.</param>
        /// <param name="strategy">The conflict resolution strategy to apply.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the incoming entity as the resolved conflict winner.</returns>
        public Task<T> ResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            return Task.FromResult(incomingEntity);
        }
        
        /// <summary>
        /// Attempts to resolve a concurrency conflict by returning the incoming entity (client wins).
        /// This method always succeeds for the ClientWinsResolver.
        /// </summary>
        /// <param name="currentEntity">The current entity state in the data store.</param>
        /// <param name="incomingEntity">The incoming entity from the client.</param>
        /// <param name="originalEntity">The original entity state used as baseline for the update.</param>
        /// <param name="strategy">The conflict resolution strategy to apply.</param>
        /// <param name="resolvedEntity">When this method returns, contains the resolved entity (always the incoming entity).</param>
        /// <returns>Always returns true, indicating successful conflict resolution.</returns>
        public bool TryResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy, out T resolvedEntity)
        {
            resolvedEntity = incomingEntity;
            return true;
        }
        
        /// <summary>
        /// Asynchronously attempts to resolve a concurrency conflict by returning the incoming entity (client wins).
        /// This method always succeeds for the ClientWinsResolver.
        /// </summary>
        /// <param name="currentEntity">The current entity state in the data store.</param>
        /// <param name="incomingEntity">The incoming entity from the client.</param>
        /// <param name="originalEntity">The original entity state used as baseline for the update.</param>
        /// <param name="strategy">The conflict resolution strategy to apply.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a successful resolution result with the incoming entity.</returns>
        public Task<TryResolveConflictResult<T>> TryResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            return Task.FromResult(new TryResolveConflictResult<T> { Success = true, ResolvedEntity = incomingEntity });
        }
    }
}