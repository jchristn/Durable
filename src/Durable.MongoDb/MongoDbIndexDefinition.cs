namespace Durable.MongoDb
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// One MongoDB index of an entity: its name, its fields in key order (ascending), whether it is unique, and the columns
    /// it covers. A unique index only constrains documents whose indexed fields are all non-null (a partial index filtered on
    /// the fields' BSON types), so, as in SQL, any number of rows may hold null in a unique column.
    /// Thread safety: immutable; safe for concurrent use.
    /// </summary>
    internal sealed class MongoDbIndexDefinition
    {
        #region Public-Members

        /// <summary>
        /// Gets the index name. Never null.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the indexed columns in key order. Never null or empty.
        /// </summary>
        public IReadOnlyList<ColumnMetadata> Columns { get; }

        /// <summary>
        /// Gets whether the index is unique.
        /// </summary>
        public bool IsUnique { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a definition.
        /// </summary>
        /// <param name="name">Index name. Must not be null.</param>
        /// <param name="columns">Columns in key order. Must not be null or empty.</param>
        /// <param name="isUnique">Whether the index is unique.</param>
        /// <exception cref="ArgumentNullException">Thrown when name or columns is null.</exception>
        /// <exception cref="ArgumentException">Thrown when columns is empty.</exception>
        public MongoDbIndexDefinition(string name, IReadOnlyList<ColumnMetadata> columns, bool isUnique)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            Columns = columns ?? throw new ArgumentNullException(nameof(columns));
            if (columns.Count == 0) throw new ArgumentException("An index needs at least one column.", nameof(columns));
            IsUnique = isUnique;
        }

        #endregion
    }
}
