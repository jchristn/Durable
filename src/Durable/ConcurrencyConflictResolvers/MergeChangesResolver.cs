namespace Durable.ConcurrencyConflictResolvers
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Threading.Tasks;
    
    /// <summary>
    /// A concurrency conflict resolver that merges changes from different sources.
    /// When conflicts occur, incoming changes take precedence over current changes.
    /// </summary>
    /// <typeparam name="T">The entity type that must be a reference type with a parameterless constructor</typeparam>
    public class MergeChangesResolver<T> : IConcurrencyConflictResolver<T> where T : class, new()
    {
        #region Public-Members

        /// <summary>
        /// Gets or sets the default strategy used for conflict resolution. Defaults to MergeChanges.
        /// </summary>
        public ConflictResolutionStrategy DefaultStrategy { get; set; } = ConflictResolutionStrategy.MergeChanges;

        #endregion

        #region Private-Members

        private readonly HashSet<string> _IgnoredProperties;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the MergeChangesResolver with specified properties to ignore during merge operations.
        /// </summary>
        /// <param name="ignoredProperties">Properties to ignore during merge operations</param>
        public MergeChangesResolver(params string[] ignoredProperties)
        {
            _IgnoredProperties = new HashSet<string>(ignoredProperties ?? Array.Empty<string>());
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Resolves conflicts between current and incoming entities by merging changes based on the original entity state.
        /// When both current and incoming have changes to the same property, incoming changes take precedence.
        /// </summary>
        /// <param name="currentEntity">The current entity state</param>
        /// <param name="incomingEntity">The incoming entity state</param>
        /// <param name="originalEntity">The original entity state used as baseline for change detection</param>
        /// <param name="strategy">The conflict resolution strategy to use</param>
        /// <returns>A merged entity with resolved conflicts</returns>
        /// <exception cref="ArgumentNullException">Thrown when any of the entity parameters are null</exception>
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
        
        /// <summary>
        /// Asynchronously resolves conflicts between current and incoming entities.
        /// </summary>
        /// <param name="currentEntity">The current entity state</param>
        /// <param name="incomingEntity">The incoming entity state</param>
        /// <param name="originalEntity">The original entity state used as baseline for change detection</param>
        /// <param name="strategy">The conflict resolution strategy to use</param>
        /// <returns>A task containing the merged entity with resolved conflicts</returns>
        public Task<T> ResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            T resolvedEntity = ResolveConflict(currentEntity, incomingEntity, originalEntity, strategy);
            return Task.FromResult(resolvedEntity);
        }
        
        /// <summary>
        /// Attempts to resolve conflicts between entities without throwing exceptions.
        /// </summary>
        /// <param name="currentEntity">The current entity state</param>
        /// <param name="incomingEntity">The incoming entity state</param>
        /// <param name="originalEntity">The original entity state used as baseline for change detection</param>
        /// <param name="strategy">The conflict resolution strategy to use</param>
        /// <param name="resolvedEntity">The resolved entity if successful, null otherwise</param>
        /// <returns>True if conflict resolution succeeded, false otherwise</returns>
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
        
        /// <summary>
        /// Asynchronously attempts to resolve conflicts between entities without throwing exceptions.
        /// </summary>
        /// <param name="currentEntity">The current entity state</param>
        /// <param name="incomingEntity">The incoming entity state</param>
        /// <param name="originalEntity">The original entity state used as baseline for change detection</param>
        /// <param name="strategy">The conflict resolution strategy to use</param>
        /// <returns>A task containing the result of the conflict resolution attempt</returns>
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