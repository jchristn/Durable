namespace Durable.InMemory
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using Durable;

    /// <summary>
    /// An immutable snapshot of every table in an <see cref="InMemoryBackend"/>. The committed database and each
    /// transaction's working copy are states; taking a snapshot is a reference copy.
    /// Thread safety: immutable; safe for concurrent use.
    /// </summary>
    internal sealed class InMemoryDatabaseState
    {
        #region Public-Members

        /// <summary>
        /// Gets the empty state. Never null.
        /// </summary>
        public static InMemoryDatabaseState Empty { get; } = new InMemoryDatabaseState(ImmutableDictionary<Type, InMemoryTable>.Empty);

        /// <summary>
        /// Gets the tables that have been written, by entity type. Never null.
        /// </summary>
        public IEnumerable<InMemoryTable> Tables => _Tables.Values;

        #endregion

        #region Private-Members

        private readonly ImmutableDictionary<Type, InMemoryTable> _Tables;

        #endregion

        #region Constructors-and-Factories

        private InMemoryDatabaseState(ImmutableDictionary<Type, InMemoryTable> tables)
        {
            _Tables = tables;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns the table of an entity, or an empty table when it has never been written.
        /// </summary>
        /// <param name="metadata">Entity metadata. Must not be null.</param>
        /// <returns>The table. Never null.</returns>
        public InMemoryTable Table(EntityMetadata metadata)
        {
            ArgumentNullException.ThrowIfNull(metadata);
            return _Tables.TryGetValue(metadata.EntityType, out InMemoryTable? table) ? table : InMemoryTable.Empty(metadata);
        }

        /// <summary>
        /// Finds a row by entity type and key.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="key">Key. Must not be null.</param>
        /// <returns>The row, or null.</returns>
        public InMemoryRow? Find(Type entityType, InMemoryRowKey key)
        {
            return _Tables.TryGetValue(entityType, out InMemoryTable? table) ? table.Find(key) : null;
        }

        /// <summary>
        /// Returns a state with a table replaced.
        /// </summary>
        /// <param name="table">Table. Must not be null.</param>
        /// <returns>The new state.</returns>
        public InMemoryDatabaseState With(InMemoryTable table)
        {
            ArgumentNullException.ThrowIfNull(table);
            return new InMemoryDatabaseState(_Tables.SetItem(table.Metadata.EntityType, table));
        }

        #endregion
    }
}
