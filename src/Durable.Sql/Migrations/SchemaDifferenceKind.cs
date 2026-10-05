namespace Durable.Sql
{
    /// <summary>
    /// Kind of a schema difference that is detected but never applied automatically; each needs a manual step
    /// (typically a versioned <see cref="Migration"/>).
    /// </summary>
    public enum SchemaDifferenceKind
    {
        /// <summary>
        /// The column's type differs from the type the mapping declares.
        /// </summary>
        TypeMismatch = 0,

        /// <summary>
        /// The column's maximum length differs from the mapping's MaxLength.
        /// </summary>
        MaxLengthMismatch = 1,

        /// <summary>
        /// The column's nullability differs from the mapping.
        /// </summary>
        NullabilityMismatch = 2,

        /// <summary>
        /// The column's primary key membership differs from the mapping, or a key column would have to be added or dropped.
        /// </summary>
        PrimaryKeyMismatch = 3,

        /// <summary>
        /// A NOT NULL column is missing and no default value can be derived for the existing rows.
        /// </summary>
        NotNullColumnWithoutDefault = 4,

        /// <summary>
        /// A required change cannot be expressed on this database (for example dropping a column where unsupported,
        /// or adding an auto-increment column to an existing table).
        /// </summary>
        UnsupportedChange = 5
    }
}
