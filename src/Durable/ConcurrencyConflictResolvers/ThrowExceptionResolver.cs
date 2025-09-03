using System;
using System.Threading.Tasks;

namespace Durable.ConcurrencyConflictResolvers
{
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
        
        public Task<IConcurrencyConflictResolver<T>.TryResolveConflictResult> TryResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            return Task.FromResult(new IConcurrencyConflictResolver<T>.TryResolveConflictResult { Success = false, ResolvedEntity = null! });
        }
    }
}