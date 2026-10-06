namespace Durable.LiteGraph
{
    using System;
    using System.Collections.Generic;
    using Durable;
    using Durable.Query;

    /// <summary>
    /// The rows of related entity types loaded once for an operation, so navigation members, collection predicates and
    /// many-to-many junctions are resolved from one consistent read per type instead of one store round trip per row.
    /// Lookups by column are served from hash indexes built on first use, with the key equality of
    /// <see cref="QueryValueComparer.Ordinal"/>.
    /// Thread safety: not thread-safe; create and use one per operation.
    /// </summary>
    internal sealed class LiteGraphRowSet
    {
        #region Private-Members

        private readonly Dictionary<Type, List<LiteGraphRow>> _Rows = new Dictionary<Type, List<LiteGraphRow>>();
        private readonly Dictionary<ColumnMetadata, Dictionary<object, List<LiteGraphRow>>> _Indexes = new Dictionary<ColumnMetadata, Dictionary<object, List<LiteGraphRow>>>();

        #endregion

        #region Public-Methods

        /// <summary>
        /// Determines whether the rows of an entity type are loaded.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <returns>True when loaded.</returns>
        public bool Contains(Type entityType)
        {
            return _Rows.ContainsKey(entityType);
        }

        /// <summary>
        /// Adds the rows of an entity type.
        /// </summary>
        /// <param name="metadata">Entity metadata. Must not be null.</param>
        /// <param name="rows">Every row of the entity (including soft-deleted rows). Must not be null.</param>
        public void Add(EntityMetadata metadata, List<LiteGraphRow> rows)
        {
            _Rows[metadata.EntityType] = rows;
        }

        /// <summary>
        /// Returns the rows of an entity whose column equals a key.
        /// </summary>
        /// <param name="metadata">Entity metadata. Must not be null.</param>
        /// <param name="column">Column of the entity. Must not be null.</param>
        /// <param name="key">Key in stored form. Must not be null.</param>
        /// <returns>The matching rows. Never null.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the entity's rows were not loaded.</exception>
        public IReadOnlyList<LiteGraphRow> Find(EntityMetadata metadata, ColumnMetadata column, object key)
        {
            if (!_Rows.TryGetValue(metadata.EntityType, out List<LiteGraphRow>? rows))
                throw new InvalidOperationException("Rows of " + metadata.EntityType.Name + " were not loaded for this operation.");

            if (!_Indexes.TryGetValue(column, out Dictionary<object, List<LiteGraphRow>>? index))
            {
                index = new Dictionary<object, List<LiteGraphRow>>(QueryValueComparer.Ordinal);
                int ordinal = LiteGraphTableSchema.For(metadata).Ordinal(column);
                foreach (LiteGraphRow row in rows)
                {
                    object? value = row.Values[ordinal];
                    if (value == null) continue;
                    if (!index.TryGetValue(value, out List<LiteGraphRow>? bucket))
                    {
                        bucket = new List<LiteGraphRow>();
                        index[value] = bucket;
                    }

                    bucket.Add(row);
                }

                _Indexes[column] = index;
            }

            return index.TryGetValue(key, out List<LiteGraphRow>? found) ? found : Array.Empty<LiteGraphRow>();
        }

        #endregion
    }
}
