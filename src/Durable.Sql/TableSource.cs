namespace Durable.Sql
{
    using System;
    using Durable;

    /// <summary>
    /// A table bound to a lambda parameter during translation: the qualifier to prefix columns with and the entity metadata.
    /// Thread safety: immutable.
    /// </summary>
    public sealed class TableSource
    {
        /// <summary>
        /// Gets the SQL qualifier (an alias such as "t0", or a quoted table name), or null for unqualified columns.
        /// </summary>
        public string? Qualifier { get; }

        /// <summary>
        /// Gets the entity metadata. Never null.
        /// </summary>
        public EntityMetadata Metadata { get; }

        /// <summary>
        /// Instantiates a table source.
        /// </summary>
        /// <param name="qualifier">Column qualifier; null for none.</param>
        /// <param name="metadata">Metadata. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when metadata is null.</exception>
        public TableSource(string? qualifier, EntityMetadata metadata)
        {
            Qualifier = qualifier;
            Metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
        }
    }
}
