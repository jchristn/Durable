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

        private readonly IConcurrencyConflictResolver<T> _throwExceptionResolver;
        private readonly IConcurrencyConflictResolver<T> _clientWinsResolver;
        private readonly IConcurrencyConflictResolver<T> _databaseWinsResolver;
        private readonly IConcurrencyConflictResolver<T> _mergeChangesResolver;

        #endregion

        #region Constructors-and-Factories

        public DefaultConflictResolver(ConflictResolutionStrategy defaultStrategy = ConflictResolutionStrategy.ThrowException)
        {
            DefaultStrategy = defaultStrategy;
            _throwExceptionResolver = new ThrowExceptionResolver<T>();
            _clientWinsResolver = new ClientWinsResolver<T>();
            _databaseWinsResolver = new DatabaseWinsResolver<T>();
            _mergeChangesResolver = new MergeChangesResolver<T>();
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
        
        public Task<IConcurrencyConflictResolver<T>.TryResolveConflictResult> TryResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
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
                    return _clientWinsResolver;
                case ConflictResolutionStrategy.DatabaseWins:
                    return _databaseWinsResolver;
                case ConflictResolutionStrategy.MergeChanges:
                    return _mergeChangesResolver;
                case ConflictResolutionStrategy.ThrowException:
                default:
                    return _throwExceptionResolver;
            }
        }

        #endregion
    }
}