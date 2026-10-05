namespace Durable.InMemory
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using Durable;

    /// <summary>
    /// An immutable table: rows indexed by key and ordered by insertion sequence. Every change returns a new table that
    /// shares structure with the old one, so snapshots are O(1).
    /// Thread safety: immutable; safe for concurrent use.
    /// </summary>
    internal sealed class InMemoryTable
    {
        #region Public-Members

        /// <summary>
        /// Gets the entity metadata. Never null.
        /// </summary>
        public EntityMetadata Metadata { get; }

        /// <summary>
        /// Gets the rows in insertion order. Never null.
        /// </summary>
        public IEnumerable<InMemoryRow> Rows => _BySequence.Values;

        /// <summary>
        /// Gets the number of rows.
        /// </summary>
        public int Count => _ByKey.Count;

        #endregion

        #region Private-Members

        private readonly ImmutableDictionary<InMemoryRowKey, InMemoryRow> _ByKey;
        private readonly ImmutableSortedDictionary<long, InMemoryRow> _BySequence;

        #endregion

        #region Constructors-and-Factories

        private InMemoryTable(EntityMetadata metadata, ImmutableDictionary<InMemoryRowKey, InMemoryRow> byKey, ImmutableSortedDictionary<long, InMemoryRow> bySequence)
        {
            Metadata = metadata;
            _ByKey = byKey;
            _BySequence = bySequence;
        }

        /// <summary>
        /// Creates an empty table.
        /// </summary>
        /// <param name="metadata">Entity metadata. Must not be null.</param>
        /// <returns>The table.</returns>
        /// <exception cref="ArgumentNullException">Thrown when metadata is null.</exception>
        public static InMemoryTable Empty(EntityMetadata metadata)
        {
            ArgumentNullException.ThrowIfNull(metadata);
            return new InMemoryTable(metadata, ImmutableDictionary<InMemoryRowKey, InMemoryRow>.Empty, ImmutableSortedDictionary<long, InMemoryRow>.Empty);
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Finds a row by key.
        /// </summary>
        /// <param name="key">Key. Must not be null.</param>
        /// <returns>The row, or null.</returns>
        public InMemoryRow? Find(InMemoryRowKey key)
        {
            return _ByKey.TryGetValue(key, out InMemoryRow? row) ? row : null;
        }

        /// <summary>
        /// Returns a table with a row added, or replacing the row with the same key.
        /// </summary>
        /// <param name="row">Row. Must not be null.</param>
        /// <returns>The new table.</returns>
        public InMemoryTable With(InMemoryRow row)
        {
            ImmutableSortedDictionary<long, InMemoryRow> bySequence = _BySequence;
            if (_ByKey.TryGetValue(row.Key, out InMemoryRow? existing)) bySequence = bySequence.Remove(existing.Sequence);
            return new InMemoryTable(Metadata, _ByKey.SetItem(row.Key, row), bySequence.SetItem(row.Sequence, row));
        }

        /// <summary>
        /// Returns a table without the row with a key.
        /// </summary>
        /// <param name="key">Key. Must not be null.</param>
        /// <returns>The new table (this table when the key is absent).</returns>
        public InMemoryTable Without(InMemoryRowKey key)
        {
            if (!_ByKey.TryGetValue(key, out InMemoryRow? existing)) return this;
            return new InMemoryTable(Metadata, _ByKey.Remove(key), _BySequence.Remove(existing.Sequence));
        }

        #endregion
    }
}
