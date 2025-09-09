namespace Durable.ConcurrencyConflictResolvers
{
    using System;
    using System.Threading.Tasks;
    
    /// <summary>
    /// A default concurrency conflict resolver that delegates to specialized resolvers based on the conflict resolution strategy.
    /// This resolver acts as a factory and dispatcher, routing conflict resolution to the appropriate strategy-specific resolver.
    /// </summary>
    /// <typeparam name="T">The type of entity being resolved. Must be a reference type with a parameterless constructor.</typeparam>
    public class DefaultConflictResolver<T> : IConcurrencyConflictResolver<T> where T : class, new()
    {
        #region Public-Members

        /// <summary>
        /// Gets or sets the default conflict resolution strategy to use when the strategy is set to Custom.
        /// </summary>
        public ConflictResolutionStrategy DefaultStrategy { get; set; }

        #endregion

        #region Private-Members

        private readonly IConcurrencyConflictResolver<T> _ThrowExceptionResolver;
        private readonly IConcurrencyConflictResolver<T> _ClientWinsResolver;
        private readonly IConcurrencyConflictResolver<T> _DatabaseWinsResolver;
        private readonly IConcurrencyConflictResolver<T> _MergeChangesResolver;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="DefaultConflictResolver{T}"/> class with the specified default strategy.
        /// </summary>
        /// <param name="defaultStrategy">The default conflict resolution strategy to use. Defaults to ThrowException.</param>
        public DefaultConflictResolver(ConflictResolutionStrategy defaultStrategy = ConflictResolutionStrategy.ThrowException)
        {
            DefaultStrategy = defaultStrategy;
            _ThrowExceptionResolver = new ThrowExceptionResolver<T>();
            _ClientWinsResolver = new ClientWinsResolver<T>();
            _DatabaseWinsResolver = new DatabaseWinsResolver<T>();
            _MergeChangesResolver = new MergeChangesResolver<T>();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Resolves a concurrency conflict by delegating to the appropriate strategy-specific resolver.
        /// </summary>
        /// <param name="currentEntity">The current entity state in the data store.</param>
        /// <param name="incomingEntity">The incoming entity from the client.</param>
        /// <param name="originalEntity">The original entity state used as baseline for the update.</param>
        /// <param name="strategy">The conflict resolution strategy to apply.</param>
        /// <returns>The resolved entity according to the specified strategy.</returns>
        public T ResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            IConcurrencyConflictResolver<T> resolver = GetResolver(strategy);
            return resolver.ResolveConflict(currentEntity, incomingEntity, originalEntity, strategy);
        }
        
        /// <summary>
        /// Asynchronously resolves a concurrency conflict by delegating to the appropriate strategy-specific resolver.
        /// </summary>
        /// <param name="currentEntity">The current entity state in the data store.</param>
        /// <param name="incomingEntity">The incoming entity from the client.</param>
        /// <param name="originalEntity">The original entity state used as baseline for the update.</param>
        /// <param name="strategy">The conflict resolution strategy to apply.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the resolved entity according to the specified strategy.</returns>
        public Task<T> ResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            IConcurrencyConflictResolver<T> resolver = GetResolver(strategy);
            return resolver.ResolveConflictAsync(currentEntity, incomingEntity, originalEntity, strategy);
        }
        
        /// <summary>
        /// Attempts to resolve a concurrency conflict by delegating to the appropriate strategy-specific resolver.
        /// </summary>
        /// <param name="currentEntity">The current entity state in the data store.</param>
        /// <param name="incomingEntity">The incoming entity from the client.</param>
        /// <param name="originalEntity">The original entity state used as baseline for the update.</param>
        /// <param name="strategy">The conflict resolution strategy to apply.</param>
        /// <param name="resolvedEntity">When this method returns, contains the resolved entity if the resolution was successful.</param>
        /// <returns>True if the conflict was successfully resolved; otherwise, false.</returns>
        public bool TryResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy, out T resolvedEntity)
        {
            IConcurrencyConflictResolver<T> resolver = GetResolver(strategy);
            return resolver.TryResolveConflict(currentEntity, incomingEntity, originalEntity, strategy, out resolvedEntity);
        }
        
        /// <summary>
        /// Asynchronously attempts to resolve a concurrency conflict by delegating to the appropriate strategy-specific resolver.
        /// </summary>
        /// <param name="currentEntity">The current entity state in the data store.</param>
        /// <param name="incomingEntity">The incoming entity from the client.</param>
        /// <param name="originalEntity">The original entity state used as baseline for the update.</param>
        /// <param name="strategy">The conflict resolution strategy to apply.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the conflict resolution result.</returns>
        public Task<TryResolveConflictResult<T>> TryResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            IConcurrencyConflictResolver<T> resolver = GetResolver(strategy);
            return resolver.TryResolveConflictAsync(currentEntity, incomingEntity, originalEntity, strategy);
        }

        #endregion

        #region Private-Methods

        private IConcurrencyConflictResolver<T> GetResolver(ConflictResolutionStrategy strategy)
        {
            ConflictResolutionStrategy effectiveStrategy = strategy == ConflictResolutionStrategy.Custom ? DefaultStrategy : strategy;
            
            switch (effectiveStrategy)
            {
                case ConflictResolutionStrategy.ClientWins:
                    return _ClientWinsResolver;
                case ConflictResolutionStrategy.DatabaseWins:
                    return _DatabaseWinsResolver;
                case ConflictResolutionStrategy.MergeChanges:
                    return _MergeChangesResolver;
                case ConflictResolutionStrategy.ThrowException:
                default:
                    return _ThrowExceptionResolver;
            }
        }

        #endregion
    }
}