#nullable enable

namespace Durable
{
    using System;

    /// <summary>
    /// Attribute to define a composite (multi-column) index at the class level.
    /// This provides an alternative to using multiple IndexAttribute declarations with the same name.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true, Inherited = true)]
    public class CompositeIndexAttribute : Attribute
    {
        /// <summary>
        /// Gets the name of the composite index.
        /// This is a required field.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the column names that make up this composite index, in order.
        /// Column names should match the Property attribute names, not the C# property names.
        /// This is a required field.
        /// </summary>
        public string[] ColumnNames { get; }

        /// <summary>
        /// Gets or sets a value indicating whether this is a unique composite index.
        /// Unique indexes enforce uniqueness constraints on the combination of indexed columns.
        /// Default: false.
        /// </summary>
        public bool IsUnique { get; set; }

        /// <summary>
        /// Gets or sets additional columns to include in the index (covering index).
        /// This is a database-specific optimization feature.
        /// Included columns are not part of the index key but can improve query performance.
        /// Not all database engines support this feature.
        /// Default: null.
        /// </summary>
        public string[]? IncludedColumns { get; set; }

        /// <summary>
        /// Initializes a new instance of the CompositeIndexAttribute class.
        /// </summary>
        /// <param name="name">The name of the composite index.</param>
        /// <param name="columnNames">The column names that make up this index, in order. Must contain at least 2 columns.</param>
        /// <exception cref="ArgumentNullException">Thrown when name or columnNames is null.</exception>
        /// <exception cref="ArgumentException">Thrown when columnNames contains fewer than 2 columns.</exception>
        public CompositeIndexAttribute(string name, params string[] columnNames)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name), "Index name cannot be null or empty");

            if (columnNames == null)
                throw new ArgumentNullException(nameof(columnNames), "Column names cannot be null");

            if (columnNames.Length < 2)
                throw new ArgumentException("Composite index must contain at least 2 columns", nameof(columnNames));

            Name = name;
            ColumnNames = columnNames;
            IsUnique = false;
            IncludedColumns = null;
        }
    }
}
