namespace Durable
{
    /// <summary>
    /// Defines strategies for resolving conflicts during concurrent operations.
    /// </summary>
    public enum ConflictResolutionStrategy
    {
        /// <summary>
        /// Throws an exception when a conflict is detected.
        /// </summary>
        ThrowException,
        
        /// <summary>
        /// Client changes take precedence over database values.
        /// </summary>
        ClientWins,
        
        /// <summary>
        /// Database values take precedence over client changes.
        /// </summary>
        DatabaseWins,
        
        /// <summary>
        /// Attempts to merge changes from both client and database.
        /// </summary>
        MergeChanges,
        
        /// <summary>
        /// Uses a custom conflict resolution strategy.
        /// </summary>
        Custom
    }
}