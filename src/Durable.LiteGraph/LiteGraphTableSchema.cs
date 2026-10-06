namespace Durable.LiteGraph
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// The node layout of one entity type: the node label (<see cref="EntityMetadata.TableName"/>), column ordinals in
    /// <see cref="LiteGraphRow.Values"/>, each column's stored type and the ordinals of the key columns. Cached per type.
    /// Thread safety: immutable; safe for concurrent use.
    /// </summary>
    internal sealed class LiteGraphTableSchema
    {
        #region Public-Members

        /// <summary>
        /// Gets the entity metadata. Never null.
        /// </summary>
        public EntityMetadata Metadata { get; }

        /// <summary>
        /// Gets the node label. Never null.
        /// </summary>
        public string Label { get; }

        /// <summary>
        /// Gets the stored type of each column, by ordinal. Never null.
        /// </summary>
        public IReadOnlyList<Type> StoredTypes { get; }

        /// <summary>
        /// Gets the ordinals of the key columns in key order. Never null.
        /// </summary>
        public IReadOnlyList<int> KeyOrdinals { get; }

        #endregion

        #region Private-Members

        private static readonly ConcurrentDictionary<Type, LiteGraphTableSchema> _Cache = new ConcurrentDictionary<Type, LiteGraphTableSchema>();
        private readonly Dictionary<ColumnMetadata, int> _Ordinals;

        #endregion

        #region Constructors-and-Factories

        private LiteGraphTableSchema(EntityMetadata metadata)
        {
            Metadata = metadata;
            Label = metadata.TableName;
            _Ordinals = new Dictionary<ColumnMetadata, int>();
            Type[] stored = new Type[metadata.Columns.Count];
            for (int i = 0; i < metadata.Columns.Count; i++)
            {
                _Ordinals[metadata.Columns[i]] = i;
                stored[i] = LiteGraphValueConverter.StoredType(metadata.Columns[i]);
            }

            StoredTypes = stored;
            int[] keys = new int[metadata.KeyColumns.Count];
            for (int i = 0; i < keys.Length; i++) keys[i] = _Ordinals[metadata.KeyColumns[i]];
            KeyOrdinals = keys;
        }

        /// <summary>
        /// Gets the cached schema of an entity.
        /// </summary>
        /// <param name="metadata">Entity metadata. Must not be null.</param>
        /// <returns>The schema. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when metadata is null.</exception>
        public static LiteGraphTableSchema For(EntityMetadata metadata)
        {
            ArgumentNullException.ThrowIfNull(metadata);
            return _Cache.GetOrAdd(metadata.EntityType, _ => new LiteGraphTableSchema(metadata));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns the ordinal of a column.
        /// </summary>
        /// <param name="column">Column of <see cref="Metadata"/>. Must not be null.</param>
        /// <returns>The ordinal.</returns>
        /// <exception cref="ArgumentException">Thrown when the column does not belong to the entity.</exception>
        public int Ordinal(ColumnMetadata column)
        {
            if (!_Ordinals.TryGetValue(column, out int ordinal))
                throw new ArgumentException("Column '" + column.Name + "' does not belong to " + Metadata.EntityType.Name + ".", nameof(column));
            return ordinal;
        }

        /// <summary>
        /// Extracts the key values of a row's values.
        /// </summary>
        /// <param name="values">Row values by ordinal. Must not be null.</param>
        /// <returns>The key values in key order. Never null.</returns>
        public object?[] KeyOf(object?[] values)
        {
            object?[] key = new object?[KeyOrdinals.Count];
            for (int i = 0; i < key.Length; i++) key[i] = values[KeyOrdinals[i]];
            return key;
        }

        #endregion
    }
}
