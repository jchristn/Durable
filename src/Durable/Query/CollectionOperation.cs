namespace Durable.Query
{
    /// <summary>
    /// Operations over a collection navigation in a <see cref="CollectionNode"/>.
    /// </summary>
    public enum CollectionOperation
    {
        /// <summary>At least one related row (matching the predicate, when present) exists.</summary>
        Any,
        /// <summary>Every related row matches the predicate.</summary>
        All,
        /// <summary>The number of related rows (matching the predicate, when present).</summary>
        Count
    }
}
