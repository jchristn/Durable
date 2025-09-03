using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;

namespace Durable
{
    public class SimpleChangeTracker<T> : IChangeTracker<T> where T : class, new()
    {
        private readonly ConcurrentDictionary<T, T> _originalValues;
        private readonly Dictionary<string, PropertyInfo> _columnMappings;

        public SimpleChangeTracker(Dictionary<string, PropertyInfo> columnMappings)
        {
            _originalValues = new ConcurrentDictionary<T, T>();
            _columnMappings = columnMappings ?? throw new ArgumentNullException(nameof(columnMappings));
        }

        public void TrackEntity(T entity)
        {
            if (entity == null) return;
            
            T originalCopy = CreateCopy(entity);
            _originalValues.AddOrUpdate(entity, originalCopy, (key, existing) => originalCopy);
        }

        public T GetOriginalValues(T entity)
        {
            if (entity == null) return null;
            return _originalValues.TryGetValue(entity, out T original) ? original : null;
        }

        public bool HasChanges(T entity)
        {
            if (entity == null) return false;
            
            T original = GetOriginalValues(entity);
            if (original == null) return false;

            foreach (KeyValuePair<string, PropertyInfo> kvp in _columnMappings)
            {
                PropertyInfo property = kvp.Value;
                if (!property.CanRead) continue;

                object originalValue = property.GetValue(original);
                object currentValue = property.GetValue(entity);

                if (!ObjectEquals(originalValue, currentValue))
                {
                    return true;
                }
            }

            return false;
        }

        public void StopTracking(T entity)
        {
            if (entity != null)
            {
                _originalValues.TryRemove(entity, out T removed);
            }
        }

        public void Clear()
        {
            _originalValues.Clear();
        }

        private T CreateCopy(T entity)
        {
            if (entity == null) return null;
            
            T copy = new T();
            foreach (KeyValuePair<string, PropertyInfo> kvp in _columnMappings)
            {
                PropertyInfo property = kvp.Value;
                if (property.CanRead && property.CanWrite)
                {
                    object value = property.GetValue(entity);
                    property.SetValue(copy, value);
                }
            }
            return copy;
        }

        private bool ObjectEquals(object obj1, object obj2)
        {
            if (obj1 == null && obj2 == null) return true;
            if (obj1 == null || obj2 == null) return false;
            return obj1.Equals(obj2);
        }
    }
}