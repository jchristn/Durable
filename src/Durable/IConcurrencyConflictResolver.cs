namespace Durable
{
    using System.Threading.Tasks;
    
    /// <summary>
    /// Provides methods for resolving conflicts that occur during concurrent entity operations.
    /// </summary>
    /// <typeparam name="T">The entity type for which conflicts are resolved.</typeparam>
    public interface IConcurrencyConflictResolver<T> where T : class
    {
        /// <summary>
        /// Gets or sets the default strategy to use when resolving conflicts.
        /// </summary>
        ConflictResolutionStrategy DefaultStrategy { get; set; }
        
        /// <summary>
        /// Resolves a concurrency conflict between entities using the specified strategy.
        /// </summary>
        /// <param name="currentEntity">The current state of the entity in the database.</param>
        /// <param name="incomingEntity">The entity with changes to be applied.</param>
        /// <param name="originalEntity">The original state of the entity when it was first loaded.</param>
        /// <param name="strategy">The strategy to use for conflict resolution.</param>
        /// <returns>The resolved entity after applying the conflict resolution strategy.</returns>
        T ResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy);
        
        /// <summary>
        /// Asynchronously resolves a concurrency conflict between entities using the specified strategy.
        /// </summary>
        /// <param name="currentEntity">The current state of the entity in the database.</param>
        /// <param name="incomingEntity">The entity with changes to be applied.</param>
        /// <param name="originalEntity">The original state of the entity when it was first loaded.</param>
        /// <param name="strategy">The strategy to use for conflict resolution.</param>
        /// <returns>A task representing the asynchronous operation, containing the resolved entity.</returns>
        Task<T> ResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy);
        
        /// <summary>
        /// Attempts to resolve a concurrency conflict between entities using the specified strategy.
        /// </summary>
        /// <param name="currentEntity">The current state of the entity in the database.</param>
        /// <param name="incomingEntity">The entity with changes to be applied.</param>
        /// <param name="originalEntity">The original state of the entity when it was first loaded.</param>
        /// <param name="strategy">The strategy to use for conflict resolution.</param>
        /// <param name="resolvedEntity">When this method returns, contains the resolved entity if resolution was successful; otherwise, the default value.</param>
        /// <returns>true if the conflict was successfully resolved; otherwise, false.</returns>
        bool TryResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy, out T resolvedEntity);
        
        /// <summary>
        /// Asynchronously attempts to resolve a concurrency conflict between entities using the specified strategy.
        /// </summary>
        /// <param name="currentEntity">The current state of the entity in the database.</param>
        /// <param name="incomingEntity">The entity with changes to be applied.</param>
        /// <param name="originalEntity">The original state of the entity when it was first loaded.</param>
        /// <param name="strategy">The strategy to use for conflict resolution.</param>
        /// <returns>A task representing the asynchronous operation, containing the result of the conflict resolution attempt.</returns>
        Task<TryResolveConflictResult<T>> TryResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy);
    }
}