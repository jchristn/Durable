namespace Durable
{
    /// <summary>
    /// What <see cref="ConcurrencyConflictResolvers.MergeChangesResolver{T}"/> does with a property that both the
    /// incoming entity and the stored entity changed relative to the original snapshot.
    /// </summary>
    public enum MergeConflictBehavior
    {
        /// <summary>
        /// The incoming (caller's) value wins. This is the default.
        /// </summary>
        IncomingWins,

        /// <summary>
        /// The current stored value wins.
        /// </summary>
        CurrentWins,

        /// <summary>
        /// The merge fails with a <see cref="ConcurrencyConflictException"/> naming the property, so the repository
        /// reports the conflict instead of choosing a value.
        /// </summary>
        ThrowException
    }
}
