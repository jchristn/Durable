namespace Durable
{
    public class TryResolveConflictResult<T> where T : class
    {
        public bool Success { get; set; }
        public T? ResolvedEntity { get; set; }
    }
}