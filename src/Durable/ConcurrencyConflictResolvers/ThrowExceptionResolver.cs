namespace Durable.ConcurrencyConflictResolvers
{
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// A concurrency conflict resolver that never resolves a conflict: the resolve methods throw
    /// <see cref="ConcurrencyConflictException"/> and the try methods report failure, so the repository surfaces an
    /// <see cref="OptimisticConcurrencyException"/>. This is the behavior of the repository default
    /// (<c>DefaultConflictResolver&lt;T&gt;</c> with <see cref="ConflictResolutionStrategy.ThrowException"/>).
    /// Stateless and thread-safe.
    /// </summary>
    /// <typeparam name="T">The entity type that must be a reference type.</typeparam>
    public class ThrowExceptionResolver<T> : IConcurrencyConflictResolver<T> where T : class
    {
        /// <summary>
        /// Gets or sets the strategy reported to callers. Default: <see cref="ConflictResolutionStrategy.ThrowException"/>.
        /// This resolver ignores it.
        /// </summary>
        public ConflictResolutionStrategy DefaultStrategy { get; set; } = ConflictResolutionStrategy.ThrowException;

        /// <summary>
        /// Initializes a new instance of the <see cref="ThrowExceptionResolver{T}"/> class.
        /// </summary>
        public ThrowExceptionResolver()
        {
        }

        /// <summary>
        /// Always throws, because this resolver does not resolve conflicts.
        /// </summary>
        /// <param name="currentEntity">The current entity state.</param>
        /// <param name="incomingEntity">The incoming entity state.</param>
        /// <param name="originalEntity">The original entity state.</param>
        /// <param name="strategy">The conflict resolution strategy (ignored).</param>
        /// <returns>Never returns normally.</returns>
        /// <exception cref="ConcurrencyConflictException">Always thrown; the exception carries the three entities.</exception>
        public T ResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            throw CreateException(currentEntity, incomingEntity, originalEntity);
        }

        /// <summary>
        /// Always fails, because this resolver does not resolve conflicts. The returned task is faulted.
        /// </summary>
        /// <param name="currentEntity">The current entity state.</param>
        /// <param name="incomingEntity">The incoming entity state.</param>
        /// <param name="originalEntity">The original entity state.</param>
        /// <param name="strategy">The conflict resolution strategy (ignored).</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A faulted task.</returns>
        /// <exception cref="ConcurrencyConflictException">Always thrown when the task is awaited.</exception>
        /// <exception cref="System.OperationCanceledException">Thrown when <paramref name="token"/> is canceled.</exception>
        public Task<T> ResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            return Task.FromException<T>(CreateException(currentEntity, incomingEntity, originalEntity));
        }

        /// <summary>
        /// Always reports failure, because this resolver does not resolve conflicts.
        /// </summary>
        /// <param name="currentEntity">The current entity state.</param>
        /// <param name="incomingEntity">The incoming entity state.</param>
        /// <param name="originalEntity">The original entity state.</param>
        /// <param name="strategy">The conflict resolution strategy (ignored).</param>
        /// <param name="resolvedEntity">Always null.</param>
        /// <returns>Always false.</returns>
        public bool TryResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy, out T resolvedEntity)
        {
            resolvedEntity = null!;
            return false;
        }

        /// <summary>
        /// Always reports failure, because this resolver does not resolve conflicts.
        /// </summary>
        /// <param name="currentEntity">The current entity state.</param>
        /// <param name="incomingEntity">The incoming entity state.</param>
        /// <param name="originalEntity">The original entity state.</param>
        /// <param name="strategy">The conflict resolution strategy (ignored).</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task whose result has <see cref="TryResolveConflictResult{T}.Success"/> set to false.</returns>
        /// <exception cref="System.OperationCanceledException">Thrown when <paramref name="token"/> is canceled.</exception>
        public Task<TryResolveConflictResult<T>> TryResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            return Task.FromResult(new TryResolveConflictResult<T> { Success = false, ResolvedEntity = null });
        }

        private static ConcurrencyConflictException CreateException(T currentEntity, T incomingEntity, T originalEntity)
        {
            return new ConcurrencyConflictException(
                $"Concurrency conflict detected for entity of type {typeof(T).Name}",
                currentEntity,
                incomingEntity,
                originalEntity);
        }
    }
}
