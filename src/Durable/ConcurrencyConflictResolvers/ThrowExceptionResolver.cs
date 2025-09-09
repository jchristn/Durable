namespace Durable.ConcurrencyConflictResolvers
{
    using System;
    using System.Threading.Tasks;
    
    public class ThrowExceptionResolver<T> : IConcurrencyConflictResolver<T> where T : class
    {
        public ConflictResolutionStrategy DefaultStrategy { get; set; } = ConflictResolutionStrategy.ThrowException;
        
        public T ResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            throw new ConcurrencyConflictException($"Concurrency conflict detected for entity of type {typeof(T).Name}");
        }
        
        public Task<T> ResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            throw new ConcurrencyConflictException($"Concurrency conflict detected for entity of type {typeof(T).Name}");
        }
        
        public bool TryResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy, out T resolvedEntity)
        {
            resolvedEntity = null!;
            return false;
        }
        
        public Task<TryResolveConflictResult<T>> TryResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            return Task.FromResult(new TryResolveConflictResult<T> { Success = false, ResolvedEntity = null! });
        }
    }
}