namespace Durable
{
    /// <summary>
    /// Represents a set operation (UNION, INTERSECT, EXCEPT) between queries.
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    public class SetOperation<T> where T : class, new()
    {
        /// <summary>
        /// Gets or sets the type of set operation.
        /// </summary>
        public SetOperationType Type { get; set; }
        
        /// <summary>
        /// Gets or sets the other query to combine with.
        /// </summary>
        public IQueryBuilder<T> OtherQuery { get; set; }

        /// <summary>
        /// Initializes a new instance of the SetOperation class.
        /// </summary>
        /// <param name="type">The type of set operation.</param>
        /// <param name="otherQuery">The other query to combine with.</param>
        public SetOperation(SetOperationType type, IQueryBuilder<T> otherQuery)
        {
            Type = type;
            OtherQuery = otherQuery;
        }
    }
}