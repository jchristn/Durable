namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// A live database table: its columns and secondary indexes, as read by <see cref="DatabaseSchemaReader"/>.
    /// Thread safety: immutable; safe to share.
    /// </summary>
    public sealed class TableSchema
    {
        #region Public-Members

        /// <summary>
        /// Gets the table name. Never null.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the columns in ordinal order. Never null.
        /// </summary>
        public IReadOnlyList<ColumnSchema> Columns { get; }

        /// <summary>
        /// Gets the secondary indexes (excluding the primary key and constraint-backed indexes). Never null.
        /// </summary>
        public IReadOnlyList<IndexSchema> Indexes { get; }

        /// <summary>
        /// Gets the primary key column names in ordinal order. Never null.
        /// </summary>
        public IReadOnlyList<string> PrimaryKeyColumns => Columns.Where(c => c.IsPrimaryKey).Select(c => c.Name).ToList();

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a table description.
        /// </summary>
        /// <param name="name">Table name. Must not be null or empty.</param>
        /// <param name="columns">Columns. Must not be null.</param>
        /// <param name="indexes">Indexes. Must not be null.</param>
        /// <exception cref="ArgumentException">Thrown when name is null or empty.</exception>
        /// <exception cref="ArgumentNullException">Thrown when columns or indexes is null.</exception>
        public TableSchema(string name, IEnumerable<ColumnSchema> columns, IEnumerable<IndexSchema> indexes)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentException("Table name cannot be null or empty.", nameof(name));
            ArgumentNullException.ThrowIfNull(columns);
            ArgumentNullException.ThrowIfNull(indexes);
            Name = name;
            Columns = columns.OrderBy(c => c.Ordinal).ToList();
            Indexes = indexes.ToList();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Finds a column by name (case-insensitive).
        /// </summary>
        /// <param name="columnName">Column name. Must not be null.</param>
        /// <returns>The column, or null when absent.</returns>
        /// <exception cref="ArgumentNullException">Thrown when columnName is null.</exception>
        public ColumnSchema? FindColumn(string columnName)
        {
            ArgumentNullException.ThrowIfNull(columnName);
            return Columns.FirstOrDefault(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));
        }

        /// <summary>
        /// Finds an index by name (case-insensitive).
        /// </summary>
        /// <param name="indexName">Index name. Must not be null.</param>
        /// <returns>The index, or null when absent.</returns>
        /// <exception cref="ArgumentNullException">Thrown when indexName is null.</exception>
        public IndexSchema? FindIndex(string indexName)
        {
            ArgumentNullException.ThrowIfNull(indexName);
            return Indexes.FirstOrDefault(i => string.Equals(i.Name, indexName, StringComparison.OrdinalIgnoreCase));
        }

        /// <summary>
        /// Returns the table name.
        /// </summary>
        /// <returns>The name.</returns>
        public override string ToString()
        {
            return Name;
        }

        #endregion
    }
}
