namespace Durable
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Reflection;
    
    /// <summary>
    /// A simple implementation of IChangeTracker that tracks entity changes by comparing current values with original values.
    /// </summary>
    /// <typeparam name="T">The entity type to track changes for.</typeparam>
    public class SimpleChangeTracker<T> : IChangeTracker<T> where T : class, new()
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private readonly ConcurrentDictionary<T, T> _originalValues;
        private readonly Dictionary<string, PropertyInfo> _columnMappings;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the SimpleChangeTracker class.
        /// </summary>
        /// <param name="columnMappings">Dictionary mapping column names to property information.</param>
        public SimpleChangeTracker(Dictionary<string, PropertyInfo> columnMappings)
        {
            _originalValues = new ConcurrentDictionary<T, T>();
            _columnMappings = columnMappings ?? throw new ArgumentNullException(nameof(columnMappings));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Starts tracking changes for the specified entity by creating a copy of its current state.
        /// </summary>
        /// <param name="entity">The entity to start tracking.</param>
        public void TrackEntity(T entity)
        {
            if (entity == null) return;
            
            T? originalCopy = CreateCopy(entity);
            if (originalCopy != null)
            {
                _originalValues.AddOrUpdate(entity, originalCopy, (key, existing) => originalCopy);
            }
        }

        /// <summary>
        /// Gets the original values for the specified entity.
        /// </summary>
        /// <param name="entity">The entity to get original values for.</param>
        /// <returns>The original values of the entity, or null if the entity is not being tracked.</returns>
        public T? GetOriginalValues(T entity)
        {
            if (entity == null) return null;
            return _originalValues.TryGetValue(entity, out T? original) ? original : null;
        }

        /// <summary>
        /// Determines whether the specified entity has changes by comparing current values with original values.
        /// </summary>
        /// <param name="entity">The entity to check for changes.</param>
        /// <returns>True if the entity has changes; otherwise, false.</returns>
        public bool HasChanges(T entity)
        {
            if (entity == null) return false;
            
            T? original = GetOriginalValues(entity);
            if (original == null) return false;

            foreach (KeyValuePair<string, PropertyInfo> kvp in _columnMappings)
            {
                PropertyInfo property = kvp.Value;
                if (!property.CanRead) continue;

                object? originalValue = property.GetValue(original);
                object? currentValue = property.GetValue(entity);

                if (!ObjectEquals(originalValue, currentValue))
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Stops tracking changes for the specified entity.
        /// </summary>
        /// <param name="entity">The entity to stop tracking.</param>
        public void StopTracking(T entity)
        {
            if (entity != null)
            {
                _originalValues.TryRemove(entity, out T? removed);
            }
        }

        /// <summary>
        /// Clears all tracked entities from the change tracker.
        /// </summary>
        public void Clear()
        {
            _originalValues.Clear();
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Creates a copy of the specified entity for tracking original values.
        /// </summary>
        /// <param name="entity">The entity to copy.</param>
        /// <returns>A copy of the entity, or null if the entity is null.</returns>
        private T? CreateCopy(T entity)
        {
            if (entity == null) return null;
            
            T copy = new T();
            foreach (KeyValuePair<string, PropertyInfo> kvp in _columnMappings)
            {
                PropertyInfo property = kvp.Value;
                if (property.CanRead && property.CanWrite)
                {
                    object? value = property.GetValue(entity);
                    property.SetValue(copy, value);
                }
            }
            return copy;
        }

        /// <summary>
        /// Compares two objects for equality, handling null values appropriately.
        /// </summary>
        /// <param name="obj1">The first object to compare.</param>
        /// <param name="obj2">The second object to compare.</param>
        /// <returns>True if the objects are equal; otherwise, false.</returns>
        private bool ObjectEquals(object? obj1, object? obj2)
        {
            if (obj1 == null && obj2 == null) return true;
            if (obj1 == null || obj2 == null) return false;
            return obj1.Equals(obj2);
        }

        #endregion
    }
}