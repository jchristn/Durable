using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;

namespace Durable.ConcurrencyConflictResolvers
{
    public class ImprovedMergeChangesResolver<T> : IConcurrencyConflictResolver<T> where T : class, new()
    {
        #region Public-Members

        public ConflictResolutionStrategy DefaultStrategy { get; set; } = ConflictResolutionStrategy.MergeChanges;

        public enum ConflictBehavior
        {
            IncomingWins,
            CurrentWins, 
            ThrowException
        }

        #endregion

        #region Private-Members

        private readonly HashSet<string> _ignoredProperties;
        private readonly ConflictBehavior _conflictBehavior;

        #endregion

        #region Constructors-and-Factories

        public ImprovedMergeChangesResolver(ConflictBehavior conflictBehavior = ConflictBehavior.IncomingWins, params string[] ignoredProperties)
        {
            _ignoredProperties = new HashSet<string>(ignoredProperties ?? Array.Empty<string>());
            _conflictBehavior = conflictBehavior;
        }

        #endregion

        #region Public-Methods

        public T ResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            if (currentEntity == null || incomingEntity == null || originalEntity == null)
            {
                throw new ArgumentNullException("All entities must be non-null for merge resolution");
            }
            
            T mergedEntity = new T();
            PropertyInfo[] properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
            
            foreach (PropertyInfo property in properties)
            {
                if (!property.CanRead || !property.CanWrite)
                    continue;
                    
                if (_ignoredProperties.Contains(property.Name))
                {
                    object? currentValue = property.GetValue(currentEntity);
                    property.SetValue(mergedEntity, currentValue);
                    continue;
                }
                
                object? originalValue = property.GetValue(originalEntity);
                object? currentValue2 = property.GetValue(currentEntity);
                object? incomingValue = property.GetValue(incomingEntity);
                
                bool currentChanged = !ObjectEquals(originalValue, currentValue2);
                bool incomingChanged = !ObjectEquals(originalValue, incomingValue);
                
                if (!currentChanged && !incomingChanged)
                {
                    // Neither changed - use original value
                    property.SetValue(mergedEntity, originalValue);
                }
                else if (currentChanged && !incomingChanged)
                {
                    // Only current changed - use current value
                    property.SetValue(mergedEntity, currentValue2);
                }
                else if (!currentChanged && incomingChanged)
                {
                    // Only incoming changed - use incoming value
                    property.SetValue(mergedEntity, incomingValue);
                }
                else
                {
                    // Both changed (conflict) - use conflict resolution strategy
                    object? resolvedValue = ResolvePropertyConflict(property, currentValue2, incomingValue, originalValue);
                    property.SetValue(mergedEntity, resolvedValue);
                }
            }
            
            return mergedEntity;
        }
        
        public Task<T> ResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            T resolvedEntity = ResolveConflict(currentEntity, incomingEntity, originalEntity, strategy);
            return Task.FromResult(resolvedEntity);
        }
        
        public bool TryResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy, out T resolvedEntity)
        {
            try
            {
                resolvedEntity = ResolveConflict(currentEntity, incomingEntity, originalEntity, strategy);
                return true;
            }
            catch
            {
                resolvedEntity = null!;
                return false;
            }
        }
        
        public async Task<IConcurrencyConflictResolver<T>.TryResolveConflictResult> TryResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            try
            {
                T resolvedEntity = await ResolveConflictAsync(currentEntity, incomingEntity, originalEntity, strategy);
                return new IConcurrencyConflictResolver<T>.TryResolveConflictResult { Success = true, ResolvedEntity = resolvedEntity };
            }
            catch
            {
                return new IConcurrencyConflictResolver<T>.TryResolveConflictResult { Success = false, ResolvedEntity = null! };
            }
        }

        #endregion

        #region Private-Methods

        private object? ResolvePropertyConflict(PropertyInfo property, object? currentValue, object? incomingValue, object? originalValue)
        {
            switch (_conflictBehavior)
            {
                case ConflictBehavior.CurrentWins:
                    return currentValue;
                case ConflictBehavior.IncomingWins:
                    return incomingValue;
                case ConflictBehavior.ThrowException:
                    throw new ConcurrencyConflictException(
                        $"Conflict detected on property '{property.Name}': " +
                        $"Original='{originalValue}', Current='{currentValue}', Incoming='{incomingValue}'");
                default:
                    return incomingValue;
            }
        }
        
        private bool ObjectEquals(object? obj1, object? obj2)
        {
            if (obj1 == null && obj2 == null)
                return true;
            if (obj1 == null || obj2 == null)
                return false;
                
            Type type = obj1.GetType();
            if (type != obj2.GetType())
                return false;
                
            if (type.IsArray)
            {
                Array array1 = (Array)obj1;
                Array array2 = (Array)obj2;
                
                if (array1.Length != array2.Length)
                    return false;
                    
                for (int i = 0; i < array1.Length; i++)
                {
                    if (!ObjectEquals(array1.GetValue(i), array2.GetValue(i)))
                        return false;
                }
                return true;
            }
            
            // Enhanced comparison for common collection types
            if (obj1 is System.Collections.ICollection collection1 && obj2 is System.Collections.ICollection collection2)
            {
                if (collection1.Count != collection2.Count)
                    return false;
                    
                System.Collections.IEnumerator enum1 = collection1.GetEnumerator();
                System.Collections.IEnumerator enum2 = collection2.GetEnumerator();
                
                while (enum1.MoveNext() && enum2.MoveNext())
                {
                    if (!ObjectEquals(enum1.Current, enum2.Current))
                        return false;
                }
                return true;
            }
            
            return obj1.Equals(obj2);
        }

        #endregion
    }
}