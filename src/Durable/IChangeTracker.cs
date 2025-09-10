namespace Durable
{
    /// <summary>
    /// Provides change tracking functionality for entities to detect modifications.
    /// </summary>
    /// <typeparam name="T">The entity type to track changes for.</typeparam>
    public interface IChangeTracker<T> where T : class
    {
        /// <summary>
        /// Starts tracking changes for the specified entity.
        /// </summary>
        /// <param name="entity">The entity to begin tracking.</param>
        void TrackEntity(T entity);

        /// <summary>
        /// Gets the original values of a tracked entity before any modifications were made.
        /// </summary>
        /// <param name="entity">The entity to get original values for.</param>
        /// <returns>The original values of the entity, or null if the entity is not being tracked.</returns>
        T? GetOriginalValues(T entity);

        /// <summary>
        /// Determines whether the specified entity has any pending changes.
        /// </summary>
        /// <param name="entity">The entity to check for changes.</param>
        /// <returns>True if the entity has changes; otherwise, false.</returns>
        bool HasChanges(T entity);

        /// <summary>
        /// Stops tracking changes for the specified entity.
        /// </summary>
        /// <param name="entity">The entity to stop tracking.</param>
        void StopTracking(T entity);

        /// <summary>
        /// Clears all tracked entities from the change tracker.
        /// </summary>
        void Clear();
    }
}