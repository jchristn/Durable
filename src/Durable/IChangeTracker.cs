namespace Durable
{
    public interface IChangeTracker<T> where T : class
    {
        void TrackEntity(T entity);
        T? GetOriginalValues(T entity);
        bool HasChanges(T entity);
        void StopTracking(T entity);
        void Clear();
    }
}