namespace Durable.Query
{
    using System;
    using Durable;

    /// <summary>
    /// Identifies one entity instance in a query: the root entity, or the related row of a navigation subquery.
    /// Columns (<see cref="ColumnNode"/>) refer to a source by reference identity; backends map sources to their own
    /// constructs (SQL table aliases, document paths, in-memory objects).
    /// Thread safety: immutable.
    /// </summary>
    public sealed class QuerySource
    {
        #region Public-Members

        /// <summary>
        /// Gets the entity metadata. Never null.
        /// </summary>
        public EntityMetadata Metadata { get; }

        /// <summary>
        /// Gets an optional name for diagnostics; may be null.
        /// </summary>
        public string? Name { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a source.
        /// </summary>
        /// <param name="metadata">Entity metadata. Must not be null.</param>
        /// <param name="name">Optional name for diagnostics.</param>
        /// <exception cref="ArgumentNullException">Thrown when metadata is null.</exception>
        public QuerySource(EntityMetadata metadata, string? name = null)
        {
            Metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
            Name = name;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override string ToString()
        {
            return (Name ?? "source") + ":" + Metadata.EntityType.Name;
        }

        #endregion
    }
}
