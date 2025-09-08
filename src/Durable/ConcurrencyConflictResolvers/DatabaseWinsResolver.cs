using System.Threading.Tasks;

namespace Durable.ConcurrencyConflictResolvers
{
    public class DatabaseWinsResolver<T> : IConcurrencyConflictResolver<T> where T : class
    {
        public ConflictResolutionStrategy DefaultStrategy { get; set; } = ConflictResolutionStrategy.DatabaseWins;
        
        public T ResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            return currentEntity;
        }
        
        public Task<T> ResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            return Task.FromResult(currentEntity);
        }
        
        public bool TryResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy, out T resolvedEntity)
        {
            resolvedEntity = currentEntity;
            return true;
        }
        
        public Task<TryResolveConflictResult<T>> TryResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            return Task.FromResult(new TryResolveConflictResult<T> { Success = true, ResolvedEntity = currentEntity });
        }
    }
}