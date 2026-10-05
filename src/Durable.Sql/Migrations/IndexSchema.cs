namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// A secondary index, either read from a live table by <see cref="DatabaseSchemaReader"/> or expected from entity
    /// attributes (<see cref="IndexAttribute"/>, <see cref="CompositeIndexAttribute"/>).
    /// Thread safety: immutable; safe to share.
    /// </summary>
    public sealed class IndexSchema
    {
        #region Public-Members

        /// <summary>
        /// Gets the index name. Never null.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the key column names in index order. Never null.
        /// </summary>
        public IReadOnlyList<string> Columns { get; }

        /// <summary>
        /// Gets the included (covering, non-key) column names; empty when none or unsupported. Never null.
        /// </summary>
        public IReadOnlyList<string> IncludedColumns { get; }

        /// <summary>
        /// Gets whether the index is unique.
        /// </summary>
        public bool IsUnique { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates an index description.
        /// </summary>
        /// <param name="name">Index name. Must not be null or empty.</param>
        /// <param name="columns">Key column names in order. Must not be null.</param>
        /// <param name="isUnique">Whether the index is unique.</param>
        /// <param name="includedColumns">Included column names; null for none.</param>
        /// <exception cref="ArgumentException">Thrown when name is null or empty.</exception>
        /// <exception cref="ArgumentNullException">Thrown when columns is null.</exception>
        public IndexSchema(string name, IEnumerable<string> columns, bool isUnique, IEnumerable<string>? includedColumns = null)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentException("Index name cannot be null or empty.", nameof(name));
            ArgumentNullException.ThrowIfNull(columns);
            Name = name;
            Columns = columns.ToList();
            IsUnique = isUnique;
            IncludedColumns = includedColumns?.ToList() ?? new List<string>();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns true when another index has the same key columns (in order, case-insensitive) and uniqueness.
        /// Names and included columns are not compared.
        /// </summary>
        /// <param name="other">Other index. Must not be null.</param>
        /// <returns>True when the definitions match.</returns>
        /// <exception cref="ArgumentNullException">Thrown when other is null.</exception>
        public bool HasSameDefinition(IndexSchema other)
        {
            ArgumentNullException.ThrowIfNull(other);
            return IsUnique == other.IsUnique && Columns.SequenceEqual(other.Columns, StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Returns a readable description, for example "UNIQUE idx_users_email (email)".
        /// </summary>
        /// <returns>The description.</returns>
        public override string ToString()
        {
            return (IsUnique ? "UNIQUE " : string.Empty) + Name + " (" + string.Join(", ", Columns) + ")";
        }

        #endregion
    }
}
