using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace Durable.ConcurrencyConflictResolvers
{
    public class MergeChangesResolver<T> : IConcurrencyConflictResolver<T> where T : class, new()
    {
        #region Public-Members

        public ConflictResolutionStrategy DefaultStrategy { get; set; } = ConflictResolutionStrategy.MergeChanges;

        #endregion

        #region Private-Members

        private readonly HashSet<string> _IgnoredProperties;

        #endregion

        #region Constructors-and-Factories

        public MergeChangesResolver(params string[] ignoredProperties)
        {
            _IgnoredProperties = new HashSet<string>(ignoredProperties ?? Array.Empty<string>());
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
                    
                if (_IgnoredProperties.Contains(property.Name))
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
                    property.SetValue(mergedEntity, originalValue);
                }
                else if (currentChanged && !incomingChanged)
                {
                    property.SetValue(mergedEntity, currentValue2);
                }
                else if (!currentChanged && incomingChanged)
                {
                    property.SetValue(mergedEntity, incomingValue);
                }
                else
                {
                    property.SetValue(mergedEntity, incomingValue);
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
        
        public async Task<TryResolveConflictResult<T>> TryResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            try
            {
                T resolvedEntity = await ResolveConflictAsync(currentEntity, incomingEntity, originalEntity, strategy);
                return new TryResolveConflictResult<T> { Success = true, ResolvedEntity = resolvedEntity };
            }
            catch
            {
                return new TryResolveConflictResult<T> { Success = false, ResolvedEntity = null! };
            }
        }

        #endregion

        #region Private-Methods

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
            
            return obj1.Equals(obj2);
        }

        #endregion
    }
}