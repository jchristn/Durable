using System.Threading.Tasks;

namespace Durable
{
    public interface IConcurrencyConflictResolver<T> where T : class
    {
        ConflictResolutionStrategy DefaultStrategy { get; set; }
        
        T ResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy);
        
        Task<T> ResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy);
        
        bool TryResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy, out T resolvedEntity);
        
        Task<TryResolveConflictResult<T>> TryResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy);
    }
}