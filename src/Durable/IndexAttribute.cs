#nullable enable

namespace Durable
{
    using System;

    /// <summary>
    /// Attribute to mark a property for indexing in the database.
    /// Can be applied to individual properties for single-column indexes or composite indexes.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true, Inherited = true)]
    public class IndexAttribute : Attribute
    {
        /// <summary>
        /// Gets or sets the name of the index.
        /// If null, the index name will be auto-generated based on the table and column names.
        /// Default: null (auto-generated).
        /// </summary>
        public string? Name { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether this is a unique index.
        /// Unique indexes enforce uniqueness constraints on the indexed column(s).
        /// Default: false.
        /// </summary>
        public bool IsUnique { get; set; }

        /// <summary>
        /// Gets or sets the order of this column in a composite index.
        /// Used when multiple properties have the same index name to create a multi-column index.
        /// Lower values appear first in the index column order.
        /// Default: 0.
        /// </summary>
        public int Order { get; set; }

        /// <summary>
        /// Gets or sets additional columns to include in the index (covering index).
        /// This is a database-specific optimization feature.
        /// Included columns are not part of the index key but can improve query performance.
        /// Not all database engines support this feature.
        /// Default: null.
        /// </summary>
        public string[]? IncludedColumns { get; set; }

        /// <summary>
        /// Initializes a new instance of the IndexAttribute class with default settings.
        /// Creates a non-unique index with auto-generated name.
        /// </summary>
        public IndexAttribute()
        {
            Name = null;
            IsUnique = false;
            Order = 0;
            IncludedColumns = null;
        }

        /// <summary>
        /// Initializes a new instance of the IndexAttribute class with a specific index name.
        /// </summary>
        /// <param name="name">The name of the index. Use the same name on multiple properties to create a composite index.</param>
        public IndexAttribute(string name)
        {
            Name = name;
            IsUnique = false;
            Order = 0;
            IncludedColumns = null;
        }

        /// <summary>
        /// Initializes a new instance of the IndexAttribute class with a specific index name and unique constraint.
        /// </summary>
        /// <param name="name">The name of the index. Use the same name on multiple properties to create a composite index.</param>
        /// <param name="isUnique">If true, creates a unique index that enforces uniqueness constraints.</param>
        public IndexAttribute(string name, bool isUnique)
        {
            Name = name;
            IsUnique = isUnique;
            Order = 0;
            IncludedColumns = null;
        }
    }
}
