namespace Durable.ConcurrencyConflictResolvers
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// A concurrency conflict resolver that merges property by property against the original snapshot: a property only
    /// the stored row changed keeps the stored value, a property only the caller changed takes the caller's value, and a
    /// property both changed is decided by <see cref="ConflictBehavior"/> (incoming wins by default). Arrays and
    /// collections are compared element by element. Properties named in the constructor are never merged and keep the
    /// stored value. Thread-safe: the resolver holds only immutable configuration.
    /// </summary>
    /// <typeparam name="T">The entity type that must be a reference type with a parameterless constructor.</typeparam>
    public class MergeChangesResolver<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T> : IConcurrencyConflictResolver<T> where T : class, new()
    {
        #region Public-Members

        /// <summary>
        /// Gets or sets the strategy reported to callers. Default: <see cref="ConflictResolutionStrategy.MergeChanges"/>.
        /// This resolver ignores it and always merges.
        /// </summary>
        public ConflictResolutionStrategy DefaultStrategy { get; set; } = ConflictResolutionStrategy.MergeChanges;

        /// <summary>
        /// Gets what happens to a property both sides changed. Default: <see cref="MergeConflictBehavior.IncomingWins"/>.
        /// </summary>
        public MergeConflictBehavior ConflictBehavior
        {
            get { return _ConflictBehavior; }
        }

        #endregion

        #region Private-Members

        private readonly HashSet<string> _IgnoredProperties;
        private readonly MergeConflictBehavior _ConflictBehavior;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="MergeChangesResolver{T}"/> class in which the incoming value wins
        /// when both sides changed a property.
        /// </summary>
        /// <param name="ignoredProperties">Names of properties that are never merged (the stored value is kept). May be null or empty.</param>
        public MergeChangesResolver(params string[] ignoredProperties)
            : this(MergeConflictBehavior.IncomingWins, ignoredProperties)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MergeChangesResolver{T}"/> class with the given behavior for
        /// properties both sides changed.
        /// </summary>
        /// <param name="conflictBehavior">What to do with a property both sides changed.</param>
        /// <param name="ignoredProperties">Names of properties that are never merged (the stored value is kept). May be null or empty.</param>
        public MergeChangesResolver(MergeConflictBehavior conflictBehavior, params string[] ignoredProperties)
        {
            _IgnoredProperties = new HashSet<string>(ignoredProperties ?? Array.Empty<string>());
            _ConflictBehavior = conflictBehavior;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Merges the current and incoming entities against the original snapshot.
        /// </summary>
        /// <param name="currentEntity">The current stored entity state.</param>
        /// <param name="incomingEntity">The incoming entity state.</param>
        /// <param name="originalEntity">The original entity state used as baseline for change detection.</param>
        /// <param name="strategy">The conflict resolution strategy (ignored).</param>
        /// <returns>A new merged entity.</returns>
        /// <exception cref="ArgumentNullException">Thrown when any of the entity parameters is null.</exception>
        /// <exception cref="ConcurrencyConflictException">Thrown when both sides changed a property and <see cref="ConflictBehavior"/> is <see cref="MergeConflictBehavior.ThrowException"/>.</exception>
        public T ResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy)
        {
            ArgumentNullException.ThrowIfNull(currentEntity);
            ArgumentNullException.ThrowIfNull(incomingEntity);
            ArgumentNullException.ThrowIfNull(originalEntity);

            T mergedEntity = new T();
            PropertyInfo[] properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);

            foreach (PropertyInfo property in properties)
            {
                if (!property.CanRead || !property.CanWrite || property.GetIndexParameters().Length > 0)
                    continue;

                object? currentValue = property.GetValue(currentEntity);
                if (_IgnoredProperties.Contains(property.Name))
                {
                    property.SetValue(mergedEntity, currentValue);
                    continue;
                }

                object? originalValue = property.GetValue(originalEntity);
                object? incomingValue = property.GetValue(incomingEntity);

                bool currentChanged = !ValuesEqual(originalValue, currentValue);
                bool incomingChanged = !ValuesEqual(originalValue, incomingValue);

                if (!currentChanged && !incomingChanged)
                    property.SetValue(mergedEntity, originalValue);
                else if (currentChanged && !incomingChanged)
                    property.SetValue(mergedEntity, currentValue);
                else if (!currentChanged)
                    property.SetValue(mergedEntity, incomingValue);
                else
                    property.SetValue(mergedEntity, ResolvePropertyConflict(property, currentValue, incomingValue, originalValue));
            }

            return mergedEntity;
        }

        /// <summary>
        /// Asynchronously merges the current and incoming entities against the original snapshot. The merge itself is
        /// synchronous; the method exists for interface symmetry.
        /// </summary>
        /// <param name="currentEntity">The current stored entity state.</param>
        /// <param name="incomingEntity">The incoming entity state.</param>
        /// <param name="originalEntity">The original entity state used as baseline for change detection.</param>
        /// <param name="strategy">The conflict resolution strategy (ignored).</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task whose result is the merged entity.</returns>
        /// <exception cref="ArgumentNullException">Thrown when any of the entity parameters is null.</exception>
        /// <exception cref="ConcurrencyConflictException">Thrown when both sides changed a property and <see cref="ConflictBehavior"/> is <see cref="MergeConflictBehavior.ThrowException"/>.</exception>
        /// <exception cref="OperationCanceledException">Thrown when <paramref name="token"/> is canceled.</exception>
        public Task<T> ResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            return Task.FromResult(ResolveConflict(currentEntity, incomingEntity, originalEntity, strategy));
        }

        /// <summary>
        /// Attempts to merge the entities without throwing when the merge is not possible.
        /// </summary>
        /// <param name="currentEntity">The current stored entity state.</param>
        /// <param name="incomingEntity">The incoming entity state.</param>
        /// <param name="originalEntity">The original entity state used as baseline for change detection.</param>
        /// <param name="strategy">The conflict resolution strategy (ignored).</param>
        /// <param name="resolvedEntity">The merged entity when successful; otherwise null.</param>
        /// <returns>True if the merge succeeded; otherwise false.</returns>
        public bool TryResolveConflict(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy, out T resolvedEntity)
        {
            try
            {
                resolvedEntity = ResolveConflict(currentEntity, incomingEntity, originalEntity, strategy);
                return true;
            }
            catch (Exception e) when (e is ConcurrencyConflictException || e is ArgumentNullException)
            {
                resolvedEntity = null!;
                return false;
            }
        }

        /// <summary>
        /// Asynchronously attempts to merge the entities without throwing when the merge is not possible.
        /// </summary>
        /// <param name="currentEntity">The current stored entity state.</param>
        /// <param name="incomingEntity">The incoming entity state.</param>
        /// <param name="originalEntity">The original entity state used as baseline for change detection.</param>
        /// <param name="strategy">The conflict resolution strategy (ignored).</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task whose result describes the merge attempt.</returns>
        /// <exception cref="OperationCanceledException">Thrown when <paramref name="token"/> is canceled.</exception>
        public Task<TryResolveConflictResult<T>> TryResolveConflictAsync(T currentEntity, T incomingEntity, T originalEntity, ConflictResolutionStrategy strategy, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            bool success = TryResolveConflict(currentEntity, incomingEntity, originalEntity, strategy, out T resolved);
            return Task.FromResult(new TryResolveConflictResult<T> { Success = success, ResolvedEntity = success ? resolved : null });
        }

        #endregion

        #region Private-Methods

        private object? ResolvePropertyConflict(PropertyInfo property, object? currentValue, object? incomingValue, object? originalValue)
        {
            switch (_ConflictBehavior)
            {
                case MergeConflictBehavior.CurrentWins:
                    return currentValue;
                case MergeConflictBehavior.ThrowException:
                    throw new ConcurrencyConflictException(
                        $"Conflict detected on property '{property.Name}': " +
                        $"Original='{originalValue}', Current='{currentValue}', Incoming='{incomingValue}'");
                case MergeConflictBehavior.IncomingWins:
                default:
                    return incomingValue;
            }
        }

        private static bool ValuesEqual(object? left, object? right)
        {
            if (left == null && right == null)
                return true;
            if (left == null || right == null)
                return false;

            Type type = left.GetType();
            if (type != right.GetType())
                return false;

            if (left is string)
                return left.Equals(right);

            if (left is ICollection leftCollection && right is ICollection rightCollection)
            {
                if (leftCollection.Count != rightCollection.Count)
                    return false;

                IEnumerator leftItems = leftCollection.GetEnumerator();
                IEnumerator rightItems = rightCollection.GetEnumerator();
                while (leftItems.MoveNext() && rightItems.MoveNext())
                {
                    if (!ValuesEqual(leftItems.Current, rightItems.Current))
                        return false;
                }
                return true;
            }

            return left.Equals(right);
        }

        #endregion
    }
}
