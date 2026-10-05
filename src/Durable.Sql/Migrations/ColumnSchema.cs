namespace Durable.Sql
{
    using System;

    /// <summary>
    /// A column of a live database table, as read by <see cref="DatabaseSchemaReader"/>.
    /// Thread safety: immutable; safe to share.
    /// </summary>
    public sealed class ColumnSchema
    {
        #region Public-Members

        /// <summary>
        /// Gets the column name. Never null.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the declared type as reported by the database, including length or precision (for example "varchar(64)").
        /// Never null.
        /// </summary>
        public string DataType { get; }

        /// <summary>
        /// Gets whether the column accepts null.
        /// </summary>
        public bool IsNullable { get; }

        /// <summary>
        /// Gets the maximum character length; -1 for unbounded (MAX) types; null when not applicable or unknown.
        /// </summary>
        public int? MaxLength { get; }

        /// <summary>
        /// Gets whether the column is part of the primary key.
        /// </summary>
        public bool IsPrimaryKey { get; }

        /// <summary>
        /// Gets the zero-based position of the column in the table.
        /// </summary>
        public int Ordinal { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a column description.
        /// </summary>
        /// <param name="name">Column name. Must not be null or empty.</param>
        /// <param name="dataType">Declared type. Must not be null.</param>
        /// <param name="isNullable">Whether the column accepts null.</param>
        /// <param name="maxLength">Maximum character length; -1 for unbounded; null when not applicable.</param>
        /// <param name="isPrimaryKey">Whether the column is part of the primary key.</param>
        /// <param name="ordinal">Zero-based position. Minimum: 0.</param>
        /// <exception cref="ArgumentException">Thrown when name is null or empty.</exception>
        /// <exception cref="ArgumentNullException">Thrown when dataType is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when ordinal is negative.</exception>
        public ColumnSchema(string name, string dataType, bool isNullable, int? maxLength, bool isPrimaryKey, int ordinal)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentException("Column name cannot be null or empty.", nameof(name));
            if (ordinal < 0) throw new ArgumentOutOfRangeException(nameof(ordinal), "Ordinal cannot be negative.");
            Name = name;
            DataType = dataType ?? throw new ArgumentNullException(nameof(dataType));
            IsNullable = isNullable;
            MaxLength = maxLength;
            IsPrimaryKey = isPrimaryKey;
            Ordinal = ordinal;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns a readable description, for example "email varchar(128) NULL".
        /// </summary>
        /// <returns>The description.</returns>
        public override string ToString()
        {
            return Name + " " + DataType + (IsNullable ? " NULL" : " NOT NULL") + (IsPrimaryKey ? " PK" : string.Empty);
        }

        #endregion
    }
}
