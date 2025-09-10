namespace Durable
{
    /// <summary>
    /// Represents the result of attempting to resolve a conflict for an entity of type T.
    /// </summary>
    /// <typeparam name="T">The type of entity being resolved.</typeparam>
    public class TryResolveConflictResult<T> where T : class
    {
        /// <summary>
        /// Gets or sets a value indicating whether the conflict resolution was successful.
        /// </summary>
        public bool Success { get; set; }
        /// <summary>
        /// Gets or sets the resolved entity if the conflict resolution was successful.
        /// </summary>
        public T? ResolvedEntity { get; set; }
    }
}