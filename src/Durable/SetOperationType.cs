namespace Durable
{
    /// <summary>
    /// Defines the types of set operations that can be performed on collections.
    /// </summary>
    public enum SetOperationType
    {
        /// <summary>
        /// Represents a union operation that returns distinct elements from both collections.
        /// </summary>
        Union,
        /// <summary>
        /// Represents a union operation that returns all elements from both collections including duplicates.
        /// </summary>
        UnionAll,
        /// <summary>
        /// Represents an intersect operation that returns elements common to both collections.
        /// </summary>
        Intersect,
        /// <summary>
        /// Represents an except operation that returns elements from the first collection not in the second.
        /// </summary>
        Except
    }
}