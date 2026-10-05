namespace Durable.InMemory
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// Cached column ordinals of an entity's stored rows (the position of each column in <see cref="InMemoryRow.Values"/>)
    /// and of its key columns.
    /// Thread safety: immutable once built; <see cref="For"/> is thread-safe.
    /// </summary>
    internal sealed class InMemoryTableSchema
    {
        #region Public-Members

        /// <summary>
        /// Gets the entity metadata. Never null.
        /// </summary>
        public EntityMetadata Metadata { get; }

        /// <summary>
        /// Gets the ordinals of the key columns, in key order. Never null.
        /// </summary>
        public IReadOnlyList<int> KeyOrdinals { get; }

        #endregion

        #region Private-Members

        private static readonly ConcurrentDictionary<Type, InMemoryTableSchema> _Cache = new ConcurrentDictionary<Type, InMemoryTableSchema>();
        private readonly Dictionary<ColumnMetadata, int> _Ordinals = new Dictionary<ColumnMetadata, int>(ReferenceEqualityComparer.Instance);
        private readonly Dictionary<string, int> _OrdinalsByProperty = new Dictionary<string, int>(StringComparer.Ordinal);

        #endregion

        #region Constructors-and-Factories

        private InMemoryTableSchema(EntityMetadata metadata)
        {
            Metadata = metadata;
            for (int i = 0; i < metadata.Columns.Count; i++)
            {
                _Ordinals[metadata.Columns[i]] = i;
                _OrdinalsByProperty[metadata.Columns[i].Property.Name] = i;
            }

            List<int> keys = new List<int>();
            foreach (ColumnMetadata key in metadata.KeyColumns) keys.Add(_Ordinals[key]);
            KeyOrdinals = keys;
        }

        /// <summary>
        /// Returns the cached schema of an entity.
        /// </summary>
        /// <param name="metadata">Entity metadata. Must not be null.</param>
        /// <returns>The schema.</returns>
        public static InMemoryTableSchema For(EntityMetadata metadata)
        {
            ArgumentNullException.ThrowIfNull(metadata);
            return _Cache.GetOrAdd(metadata.EntityType, _ => new InMemoryTableSchema(metadata));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns the ordinal of a column.
        /// </summary>
        /// <param name="column">Column of this entity. Must not be null.</param>
        /// <returns>The ordinal.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the column does not belong to the entity.</exception>
        public int Ordinal(ColumnMetadata column)
        {
            if (_Ordinals.TryGetValue(column, out int ordinal)) return ordinal;
            if (_OrdinalsByProperty.TryGetValue(column.Property.Name, out ordinal)) return ordinal;
            throw new InvalidOperationException("Column '" + column.Name + "' is not mapped by " + Metadata.EntityType.Name + ".");
        }

        /// <summary>
        /// Builds the key of stored values.
        /// </summary>
        /// <param name="values">Stored values in column order. Must not be null.</param>
        /// <returns>The key.</returns>
        /// <exception cref="InvalidOperationException">Thrown when a key value is null.</exception>
        public InMemoryRowKey KeyOf(object?[] values)
        {
            object?[] parts = new object?[KeyOrdinals.Count];
            for (int i = 0; i < parts.Length; i++)
            {
                parts[i] = values[KeyOrdinals[i]];
                if (parts[i] == null)
                    throw new InvalidOperationException("Primary key column '" + Metadata.KeyColumns[i].Name + "' of " + Metadata.EntityType.Name + " is null.");
            }

            return new InMemoryRowKey(parts);
        }

        #endregion
    }
}
