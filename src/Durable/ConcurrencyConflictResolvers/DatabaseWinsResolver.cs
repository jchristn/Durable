namespace Durable.ConcurrencyConflictResolvers
{
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// A concurrency conflict resolver that always chooses the current stored entity as the winner: the caller's changes
    /// are discarded and the stored row is kept. Stateless and thread-safe.
    /// </summary>
    /// <typeparam name="T">The type of entity being resolved. Must be a reference type.</typeparam>
    public class DatabaseWinsResolver<T> : IConcurrencyConflictResolver<T> where T : class
    {
        /// <summary>
        /// Gets or sets the strategy reported to callers. Default: <see cref="ConflictResolutionStrategy.DatabaseWins"/>.
        /// This resolver ignores it and always lets the stored entity win.
        /// </summary>
        public ConflictResolutionStrategy DefaultStrategy { get; set; } = ConflictResolutionStrategy.DatabaseWins;

        /// <summary>
        /// Initializes a new instance of the <see cref="DatabaseWinsResolver{T}"/> class.
        /// </summary>
        public DatabaseWinsResolver()
        {
        }

        /// <summary>
        /// Resolves a concurrency conflict by returning the current stored entity (database wins).
        /// </summary>
        /// <param name="currentEntity">The current entity state in the data store.</param>
        /// <param name="incomingEntity">The incoming entity from the client.</param>
        /// <param name="originalEntity">The original entity state used as baseline for the update.</param>
        /// <param name="strategy">The conflict resolution strategy (ignored).</param>
        /// <returns>The current stored entity.</returns>
        public T ResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            return currentEntity;
        }

        /// <summary>
        /// Asynchronously resolves a concurrency conflict by returning the current stored entity (database wins).
        /// </summary>
        /// <param name="currentEntity">The current entity state in the data store.</param>
        /// <param name="incomingEntity">The incoming entity from the client.</param>
        /// <param name="originalEntity">The original entity state used as baseline for the update.</param>
        /// <param name="strategy">The conflict resolution strategy (ignored).</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task whose result is the current stored entity.</returns>
        /// <exception cref="System.OperationCanceledException">Thrown when <paramref name="token"/> is canceled.</exception>
        public Task<T> ResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            return Task.FromResult(currentEntity);
        }

        /// <summary>
        /// Attempts to resolve a concurrency conflict by returning the current stored entity (database wins). Always succeeds.
        /// </summary>
        /// <param name="currentEntity">The current entity state in the data store.</param>
        /// <param name="incomingEntity">The incoming entity from the client.</param>
        /// <param name="originalEntity">The original entity state used as baseline for the update.</param>
        /// <param name="strategy">The conflict resolution strategy (ignored).</param>
        /// <param name="resolvedEntity">When this method returns, the current stored entity.</param>
        /// <returns>Always true.</returns>
        public bool TryResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy, out T resolvedEntity)
        {
            resolvedEntity = currentEntity;
            return true;
        }

        /// <summary>
        /// Asynchronously attempts to resolve a concurrency conflict by returning the current stored entity (database wins). Always succeeds.
        /// </summary>
        /// <param name="currentEntity">The current entity state in the data store.</param>
        /// <param name="incomingEntity">The incoming entity from the client.</param>
        /// <param name="originalEntity">The original entity state used as baseline for the update.</param>
        /// <param name="strategy">The conflict resolution strategy (ignored).</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task whose result is a successful resolution carrying the current stored entity.</returns>
        /// <exception cref="System.OperationCanceledException">Thrown when <paramref name="token"/> is canceled.</exception>
        public Task<TryResolveConflictResult<T>> TryResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            return Task.FromResult(new TryResolveConflictResult<T> { Success = true, ResolvedEntity = currentEntity });
        }
    }
}
