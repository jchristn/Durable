namespace Durable.LiteGraph
{
    using System;
    using Durable;
    using global::LiteGraph;

    /// <summary>
    /// One entity row as read from (or about to be written to) a LiteGraph node: the node GUID, the stored column values
    /// by ordinal, the node's creation time (which orders rows in insertion order) and a signature of the stored node used
    /// to detect concurrent changes at commit.
    /// Thread safety: not thread-safe; rows are treated as immutable once created.
    /// </summary>
    internal sealed class LiteGraphRow
    {
        #region Public-Members

        /// <summary>
        /// Gets the node GUID.
        /// </summary>
        public Guid Guid { get; }

        /// <summary>
        /// Gets the entity metadata. Never null.
        /// </summary>
        public EntityMetadata Metadata { get; }

        /// <summary>
        /// Gets the stored values by column ordinal. Never null.
        /// </summary>
        public object?[] Values { get; }

        /// <summary>
        /// Gets the node creation time (UTC); rows are read in this order unless a query orders them.
        /// </summary>
        public DateTime CreatedUtc { get; }

        /// <summary>
        /// Gets the signature of the stored node (last update time and data) when the row was read from storage; null for
        /// rows not read from storage.
        /// </summary>
        public string? Signature { get; }

        /// <summary>
        /// Gets or sets the stored node with its labels, tags and vectors, when they were loaded (so updates can carry them
        /// over); null otherwise.
        /// </summary>
        public Node? Subordinates { get; set; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a row.
        /// </summary>
        /// <param name="guid">Node GUID.</param>
        /// <param name="metadata">Entity metadata. Must not be null.</param>
        /// <param name="values">Stored values by ordinal. Must not be null.</param>
        /// <param name="createdUtc">Node creation time.</param>
        /// <param name="signature">Signature of the stored node; null when not read from storage.</param>
        /// <exception cref="ArgumentNullException">Thrown when metadata or values is null.</exception>
        public LiteGraphRow(Guid guid, EntityMetadata metadata, object?[] values, DateTime createdUtc, string? signature)
        {
            Guid = guid;
            Metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
            Values = values ?? throw new ArgumentNullException(nameof(values));
            CreatedUtc = createdUtc;
            Signature = signature;
        }

        #endregion
    }
}
