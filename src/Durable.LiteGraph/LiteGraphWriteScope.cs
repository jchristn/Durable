namespace Durable.LiteGraph
{
    using System;
    using System.Collections.Generic;
    using Durable;
    using global::LiteGraph;

    /// <summary>
    /// The writes of a transaction, or of one operation outside a transaction: the nodes written so far (read back by
    /// reads in the same scope, which is how a transaction reads its own writes) and the LiteGraph graph-transaction
    /// operations that apply them, in order. An operation that fails is undone from the scope with
    /// <see cref="Checkpoint"/> and <see cref="Restore"/>, so a transaction keeps only complete operations.
    /// Thread safety: not thread-safe; the owner serializes access.
    /// </summary>
    internal sealed class LiteGraphWriteScope
    {
        #region Public-Members

        /// <summary>
        /// Gets the nodes written by the scope, by node GUID. Never null.
        /// </summary>
        public Dictionary<Guid, LiteGraphPendingNode> Pending { get; private set; } = new Dictionary<Guid, LiteGraphPendingNode>();

        /// <summary>
        /// Gets the graph-transaction operations, in order. Never null.
        /// </summary>
        public List<TransactionOperation> Operations { get; } = new List<TransactionOperation>();

        /// <summary>
        /// Gets cached existence checks of stored nodes (GUID to exists). Never null.
        /// </summary>
        public Dictionary<Guid, bool> StoredExistence { get; } = new Dictionary<Guid, bool>();

        #endregion

        #region Private-Members

        private Dictionary<Guid, LiteGraphPendingNode>? _SavedPending;
        private List<KeyValuePair<LiteGraphPendingNode, LiteGraphRow?>>? _SavedCurrent;
        private int _SavedOperations;

        #endregion

        #region Public-Methods

        /// <summary>
        /// Records the state before an operation so it can be undone with <see cref="Restore"/>.
        /// </summary>
        public void Checkpoint()
        {
            _SavedPending = new Dictionary<Guid, LiteGraphPendingNode>(Pending);
            _SavedCurrent = new List<KeyValuePair<LiteGraphPendingNode, LiteGraphRow?>>(Pending.Count);
            foreach (LiteGraphPendingNode node in Pending.Values) _SavedCurrent.Add(new KeyValuePair<LiteGraphPendingNode, LiteGraphRow?>(node, node.Current));
            _SavedOperations = Operations.Count;
        }

        /// <summary>
        /// Undoes everything recorded since the last <see cref="Checkpoint"/>.
        /// </summary>
        public void Restore()
        {
            if (_SavedPending == null || _SavedCurrent == null) return;
            Pending = _SavedPending;
            foreach (KeyValuePair<LiteGraphPendingNode, LiteGraphRow?> saved in _SavedCurrent) saved.Key.Current = saved.Value;
            if (Operations.Count > _SavedOperations) Operations.RemoveRange(_SavedOperations, Operations.Count - _SavedOperations);
            StoredExistence.Clear();
            _SavedPending = null;
            _SavedCurrent = null;
        }

        /// <summary>
        /// Records that the scope wrote a node.
        /// </summary>
        /// <param name="metadata">Entity metadata. Must not be null.</param>
        /// <param name="guid">Node GUID.</param>
        /// <param name="current">Row now; null for a delete.</param>
        /// <param name="stored">The node's row in storage before the scope first wrote it, when it existed; null otherwise.</param>
        public void Write(EntityMetadata metadata, Guid guid, LiteGraphRow? current, LiteGraphRow? stored)
        {
            if (Pending.TryGetValue(guid, out LiteGraphPendingNode? existing))
            {
                existing.Current = current;
                return;
            }

            Pending[guid] = new LiteGraphPendingNode(metadata, current, stored != null, stored?.Signature);
        }

        /// <summary>
        /// Clears every write.
        /// </summary>
        public void Clear()
        {
            Pending.Clear();
            Operations.Clear();
            StoredExistence.Clear();
            _SavedPending = null;
            _SavedCurrent = null;
        }

        #endregion
    }
}
