namespace Durable.LiteGraph
{
    using System;
    using System.Collections.Generic;
    using Durable;
    using ExpressionTree;

    /// <summary>
    /// What to read from LiteGraph for one entity: nodes by GUID, or the nodes of the entity's label narrowed by an
    /// optional data filter.
    /// Thread safety: immutable once built.
    /// </summary>
    internal sealed class LiteGraphReadRequest
    {
        #region Public-Members

        /// <summary>
        /// Gets the entity metadata. Never null.
        /// </summary>
        public EntityMetadata Metadata { get; }

        /// <summary>
        /// Gets the node GUIDs to read; null for a label scan.
        /// </summary>
        public IReadOnlyList<Guid>? Guids { get; }

        /// <summary>
        /// Gets the data filter for a label scan; null for none.
        /// </summary>
        public Expr? Filter { get; }

        /// <summary>
        /// Gets the strategy.
        /// </summary>
        public LiteGraphReadStrategy Strategy => Guids != null ? LiteGraphReadStrategy.KeyLookup : LiteGraphReadStrategy.LabelScan;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a request.
        /// </summary>
        /// <param name="metadata">Entity metadata. Must not be null.</param>
        /// <param name="guids">Node GUIDs; null for a label scan.</param>
        /// <param name="filter">Data filter for a label scan; null for none.</param>
        /// <exception cref="ArgumentNullException">Thrown when metadata is null.</exception>
        public LiteGraphReadRequest(EntityMetadata metadata, IReadOnlyList<Guid>? guids, Expr? filter)
        {
            Metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
            Guids = guids;
            Filter = guids != null ? null : filter;
        }

        /// <summary>
        /// Creates a request for every node of an entity.
        /// </summary>
        /// <param name="metadata">Entity metadata. Must not be null.</param>
        /// <returns>The request.</returns>
        public static LiteGraphReadRequest All(EntityMetadata metadata)
        {
            return new LiteGraphReadRequest(metadata, null, null);
        }

        #endregion
    }
}
