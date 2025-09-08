using System;
using System.Threading.Tasks;

namespace Durable.ConcurrencyConflictResolvers
{
    public class DefaultConflictResolver<T> : IConcurrencyConflictResolver<T> where T : class, new()
    {
        #region Public-Members

        public ConflictResolutionStrategy DefaultStrategy { get; set; }

        #endregion

        #region Private-Members

        private readonly IConcurrencyConflictResolver<T> _ThrowExceptionResolver;
        private readonly IConcurrencyConflictResolver<T> _ClientWinsResolver;
        private readonly IConcurrencyConflictResolver<T> _DatabaseWinsResolver;
        private readonly IConcurrencyConflictResolver<T> _MergeChangesResolver;

        #endregion

        #region Constructors-and-Factories

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

        public T ResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            IConcurrencyConflictResolver<T> resolver = GetResolver(strategy);
            return resolver.ResolveConflict(currentEntity, incomingEntity, originalEntity, strategy);
        }
        
        public Task<T> ResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            IConcurrencyConflictResolver<T> resolver = GetResolver(strategy);
            return resolver.ResolveConflictAsync(currentEntity, incomingEntity, originalEntity, strategy);
        }
        
        public bool TryResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy, out T resolvedEntity)
        {
            IConcurrencyConflictResolver<T> resolver = GetResolver(strategy);
            return resolver.TryResolveConflict(currentEntity, incomingEntity, originalEntity, strategy, out resolvedEntity);
        }
        
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