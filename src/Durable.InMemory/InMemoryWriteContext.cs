namespace Durable.InMemory
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// The working state of one write operation: the state being changed, the rows it touched (for transaction conflict
    /// detection) and the number of rows affected. A failed operation's context is discarded, so writes are atomic.
    /// Thread safety: not thread-safe; used by one operation under the backend's or transaction's lock.
    /// </summary>
    internal sealed class InMemoryWriteContext
    {
        #region Public-Members

        /// <summary>
        /// Gets or sets the state being changed. Never null.
        /// </summary>
        public InMemoryDatabaseState State { get; set; }

        /// <summary>
        /// Gets the touched rows, by entity and key. Never null.
        /// </summary>
        public List<KeyValuePair<EntityMetadata, InMemoryRowKey>> Touched { get; } = new List<KeyValuePair<EntityMetadata, InMemoryRowKey>>();

        /// <summary>
        /// Gets or sets the number of rows affected.
        /// </summary>
        public int Count { get; set; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a context.
        /// </summary>
        /// <param name="state">Initial state. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when state is null.</exception>
        public InMemoryWriteContext(InMemoryDatabaseState state)
        {
            State = state ?? throw new ArgumentNullException(nameof(state));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Records a touched row.
        /// </summary>
        /// <param name="metadata">Entity metadata. Must not be null.</param>
        /// <param name="key">Row key. Must not be null.</param>
        public void Touch(EntityMetadata metadata, InMemoryRowKey key)
        {
            Touched.Add(new KeyValuePair<EntityMetadata, InMemoryRowKey>(metadata, key));
        }

        #endregion
    }
}
