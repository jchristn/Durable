namespace Durable
{
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Resolves an optimistic-concurrency conflict detected by an update: the row in storage no longer carries the
    /// version the caller read. A repository calls <see cref="TryResolveConflict"/> (or
    /// <see cref="TryResolveConflictAsync"/>) with the current stored entity, the caller's entity and the
    /// original snapshot; when resolution succeeds the resolved entity is written with the current version,
    /// otherwise the repository throws <see cref="OptimisticConcurrencyException"/>.
    /// Built-in implementations live in <c>Durable.ConcurrencyConflictResolvers</c>; repositories default to
    /// <c>DefaultConflictResolver&lt;T&gt;</c> with <see cref="ConflictResolutionStrategy.ThrowException"/>.
    /// Implementations should be stateless or thread-safe, because one resolver instance may serve concurrent updates.
    /// </summary>
    /// <typeparam name="T">The entity type for which conflicts are resolved.</typeparam>
    public interface IConcurrencyConflictResolver<T> where T : class
    {
        /// <summary>
        /// Gets or sets the strategy the repository passes to the resolve methods. Fixed-strategy resolvers
        /// (client wins, database wins, merge, throw) ignore it; <c>DefaultConflictResolver&lt;T&gt;</c> uses it to pick the
        /// strategy, and also when <see cref="ConflictResolutionStrategy.Custom"/> is passed.
        /// </summary>
        ConflictResolutionStrategy DefaultStrategy { get; set; }

        /// <summary>
        /// Resolves a concurrency conflict between entities using the specified strategy.
        /// </summary>
        /// <param name="currentEntity">The current state of the entity in storage. Never null when called by a repository.</param>
        /// <param name="incomingEntity">The entity with changes to be applied. Never null when called by a repository.</param>
        /// <param name="originalEntity">The original state of the entity when it was first loaded. Never null when called by a repository.</param>
        /// <param name="strategy">The strategy to use for conflict resolution.</param>
        /// <returns>The resolved entity after applying the conflict resolution strategy.</returns>
        /// <exception cref="ConcurrencyConflictException">Thrown when the resolver cannot or will not resolve the conflict.</exception>
        T ResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy);

        /// <summary>
        /// Asynchronously resolves a concurrency conflict between entities using the specified strategy.
        /// </summary>
        /// <param name="currentEntity">The current state of the entity in storage. Never null when called by a repository.</param>
        /// <param name="incomingEntity">The entity with changes to be applied. Never null when called by a repository.</param>
        /// <param name="originalEntity">The original state of the entity when it was first loaded. Never null when called by a repository.</param>
        /// <param name="strategy">The strategy to use for conflict resolution.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task representing the asynchronous operation, containing the resolved entity.</returns>
        /// <exception cref="ConcurrencyConflictException">Thrown when the resolver cannot or will not resolve the conflict.</exception>
        /// <exception cref="System.OperationCanceledException">Thrown when <paramref name="token"/> is canceled.</exception>
        Task<T> ResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy, CancellationToken token = default);

        /// <summary>
        /// Attempts to resolve a concurrency conflict between entities using the specified strategy, without throwing
        /// when resolution is not possible.
        /// </summary>
        /// <param name="currentEntity">The current state of the entity in storage. Never null when called by a repository.</param>
        /// <param name="incomingEntity">The entity with changes to be applied. Never null when called by a repository.</param>
        /// <param name="originalEntity">The original state of the entity when it was first loaded. Never null when called by a repository.</param>
        /// <param name="strategy">The strategy to use for conflict resolution.</param>
        /// <param name="resolvedEntity">When this method returns true, the resolved entity; otherwise null.</param>
        /// <returns>True if the conflict was successfully resolved; otherwise, false.</returns>
        bool TryResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy, out T resolvedEntity);

        /// <summary>
        /// Asynchronously attempts to resolve a concurrency conflict between entities using the specified strategy,
        /// without throwing when resolution is not possible.
        /// </summary>
        /// <param name="currentEntity">The current state of the entity in storage. Never null when called by a repository.</param>
        /// <param name="incomingEntity">The entity with changes to be applied. Never null when called by a repository.</param>
        /// <param name="originalEntity">The original state of the entity when it was first loaded. Never null when called by a repository.</param>
        /// <param name="strategy">The strategy to use for conflict resolution.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task containing the result of the conflict resolution attempt.</returns>
        /// <exception cref="System.OperationCanceledException">Thrown when <paramref name="token"/> is canceled.</exception>
        Task<TryResolveConflictResult<T>> TryResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy, CancellationToken token = default);
    }
}
