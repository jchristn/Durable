namespace Durable.LiteGraph
{
    using System;
    using Durable;

    /// <summary>
    /// The state a write scope (a transaction, or a single operation outside one) has given a node: its current row, or
    /// null when deleted, and what the node was in storage before the scope first wrote it, so a commit can detect that
    /// another writer changed it in the meantime.
    /// Thread safety: not thread-safe; owned by one scope.
    /// </summary>
    internal sealed class LiteGraphPendingNode
    {
        #region Public-Members

        /// <summary>
        /// Gets the entity metadata. Never null.
        /// </summary>
        public EntityMetadata Metadata { get; }

        /// <summary>
        /// Gets or sets the node's row as written by the scope; null when the scope deleted it.
        /// </summary>
        public LiteGraphRow? Current { get; set; }

        /// <summary>
        /// Gets whether the node existed in storage when the scope first wrote it.
        /// </summary>
        public bool ExistedBefore { get; }

        /// <summary>
        /// Gets the signature of the stored node when the scope first wrote it; null when it did not exist.
        /// </summary>
        public string? BaseSignature { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a pending node.
        /// </summary>
        /// <param name="metadata">Entity metadata. Must not be null.</param>
        /// <param name="current">Row written; null for a delete.</param>
        /// <param name="existedBefore">Whether the node existed in storage before the scope wrote it.</param>
        /// <param name="baseSignature">Signature of the stored node before the scope wrote it; null when it did not exist.</param>
        /// <exception cref="ArgumentNullException">Thrown when metadata is null.</exception>
        public LiteGraphPendingNode(EntityMetadata metadata, LiteGraphRow? current, bool existedBefore, string? baseSignature)
        {
            Metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
            Current = current;
            ExistedBefore = existedBefore;
            BaseSignature = baseSignature;
        }

        #endregion
    }
}
