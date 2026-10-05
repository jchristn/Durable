namespace Durable.InMemory
{
    using System;

    /// <summary>
    /// One stored row: its key, its insertion sequence (the default read order) and its stored column values in
    /// <see cref="EntityMetadata.Columns"/> order. Rows are never mutated; an update stores a new row with the same
    /// sequence, so a row reference identifies one version of a row (used for transaction conflict detection).
    /// Thread safety: immutable; safe for concurrent use.
    /// </summary>
    internal sealed class InMemoryRow
    {
        #region Public-Members

        /// <summary>
        /// Gets the row key. Never null.
        /// </summary>
        public InMemoryRowKey Key { get; }

        /// <summary>
        /// Gets the insertion sequence.
        /// </summary>
        public long Sequence { get; }

        /// <summary>
        /// Gets the stored values in column order. Never null; must not be modified.
        /// </summary>
        public object?[] Values { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a row.
        /// </summary>
        /// <param name="key">Key. Must not be null.</param>
        /// <param name="sequence">Insertion sequence.</param>
        /// <param name="values">Stored values in column order. Must not be null; ownership passes to the row.</param>
        /// <exception cref="ArgumentNullException">Thrown when key or values is null.</exception>
        public InMemoryRow(InMemoryRowKey key, long sequence, object?[] values)
        {
            Key = key ?? throw new ArgumentNullException(nameof(key));
            Sequence = sequence;
            Values = values ?? throw new ArgumentNullException(nameof(values));
        }

        #endregion
    }
}
