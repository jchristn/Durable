namespace Durable.ConcurrencyConflictResolvers
{
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// A concurrency conflict resolver that always chooses the incoming (client-side) entity as the winner: the caller's
    /// entity is written over whatever another writer stored. Stateless and thread-safe.
    /// </summary>
    /// <typeparam name="T">The type of entity being resolved. Must be a reference type.</typeparam>
    public class ClientWinsResolver<T> : IConcurrencyConflictResolver<T> where T : class
    {
        /// <summary>
        /// Gets or sets the strategy reported to callers. Default: <see cref="ConflictResolutionStrategy.ClientWins"/>.
        /// This resolver ignores it and always lets the client win.
        /// </summary>
        public ConflictResolutionStrategy DefaultStrategy { get; set; } = ConflictResolutionStrategy.ClientWins;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientWinsResolver{T}"/> class.
        /// </summary>
        public ClientWinsResolver()
        {
        }

        /// <summary>
        /// Resolves a concurrency conflict by returning the incoming entity (client wins).
        /// </summary>
        /// <param name="currentEntity">The current entity state in the data store.</param>
        /// <param name="incomingEntity">The incoming entity from the client.</param>
        /// <param name="originalEntity">The original entity state used as baseline for the update.</param>
        /// <param name="strategy">The conflict resolution strategy (ignored).</param>
        /// <returns>The incoming entity.</returns>
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
        /// <param name="strategy">The conflict resolution strategy (ignored).</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task whose result is the incoming entity.</returns>
        /// <exception cref="System.OperationCanceledException">Thrown when <paramref name="token"/> is canceled.</exception>
        public Task<T> ResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            return Task.FromResult(incomingEntity);
        }

        /// <summary>
        /// Attempts to resolve a concurrency conflict by returning the incoming entity (client wins). Always succeeds.
        /// </summary>
        /// <param name="currentEntity">The current entity state in the data store.</param>
        /// <param name="incomingEntity">The incoming entity from the client.</param>
        /// <param name="originalEntity">The original entity state used as baseline for the update.</param>
        /// <param name="strategy">The conflict resolution strategy (ignored).</param>
        /// <param name="resolvedEntity">When this method returns, the incoming entity.</param>
        /// <returns>Always true.</returns>
        public bool TryResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy, out T resolvedEntity)
        {
            resolvedEntity = incomingEntity;
            return true;
        }

        /// <summary>
        /// Asynchronously attempts to resolve a concurrency conflict by returning the incoming entity (client wins). Always succeeds.
        /// </summary>
        /// <param name="currentEntity">The current entity state in the data store.</param>
        /// <param name="incomingEntity">The incoming entity from the client.</param>
        /// <param name="originalEntity">The original entity state used as baseline for the update.</param>
        /// <param name="strategy">The conflict resolution strategy (ignored).</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task whose result is a successful resolution carrying the incoming entity.</returns>
        /// <exception cref="System.OperationCanceledException">Thrown when <paramref name="token"/> is canceled.</exception>
        public Task<TryResolveConflictResult<T>> TryResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            return Task.FromResult(new TryResolveConflictResult<T> { Success = true, ResolvedEntity = incomingEntity });
        }
    }
}
