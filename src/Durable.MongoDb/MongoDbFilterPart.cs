namespace Durable.MongoDb
{
    using System;
    using MongoDB.Bson;

    /// <summary>
    /// A translated predicate: a MongoDB query filter and whether it is exactly the predicate (true) or only a necessary
    /// condition of it (false; the predicate is then re-evaluated client-side).
    /// Thread safety: immutable; safe for concurrent use.
    /// </summary>
    internal sealed class MongoDbFilterPart
    {
        #region Public-Members

        /// <summary>
        /// Gets the filter. Never null.
        /// </summary>
        public BsonDocument Filter { get; }

        /// <summary>
        /// Gets whether the filter matches exactly the documents the predicate is true for.
        /// </summary>
        public bool Exact { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a part.
        /// </summary>
        /// <param name="filter">Filter. Must not be null.</param>
        /// <param name="exact">Whether the filter is exact.</param>
        /// <exception cref="ArgumentNullException">Thrown when filter is null.</exception>
        public MongoDbFilterPart(BsonDocument filter, bool exact)
        {
            Filter = filter ?? throw new ArgumentNullException(nameof(filter));
            Exact = exact;
        }

        #endregion
    }
}
