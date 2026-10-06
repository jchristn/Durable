namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// The outcome of validating one entity mapping against the database
    /// (<see cref="ISqlRepository{T}.ValidateTableAsync"/>): mapping errors (no key, duplicate columns, invalid
    /// auto-increment type), mapped columns missing from an existing table (errors), and table columns the entity does not
    /// map (warnings). A table that does not exist yet is not an error.
    /// Thread safety: immutable; safe to share.
    /// </summary>
    public sealed class TableValidationResult
    {
        #region Public-Members

        /// <summary>
        /// Gets the validated entity type. Never null.
        /// </summary>
        public Type EntityType { get; }

        /// <summary>
        /// Gets the mapped table name, or null when the entity's mapping could not be read.
        /// </summary>
        public string? TableName { get; }

        /// <summary>
        /// Gets whether the table exists in the database. False when it does not exist or the mapping is invalid
        /// (the database is only queried for a valid mapping).
        /// </summary>
        public bool TableExists { get; }

        /// <summary>
        /// Gets the errors. Empty when valid. Never null.
        /// </summary>
        public IReadOnlyList<string> Errors { get; }

        /// <summary>
        /// Gets the warnings (table columns not mapped by the entity). Warnings do not make the result invalid. Never null.
        /// </summary>
        public IReadOnlyList<string> Warnings { get; }

        /// <summary>
        /// Gets whether there are no errors.
        /// </summary>
        public bool IsValid => Errors.Count == 0;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a result.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="tableName">Table name; null when the mapping could not be read.</param>
        /// <param name="tableExists">Whether the table exists.</param>
        /// <param name="errors">Errors; null is treated as empty.</param>
        /// <param name="warnings">Warnings; null is treated as empty.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="entityType"/> is null.</exception>
        public TableValidationResult(Type entityType, string? tableName, bool tableExists, IReadOnlyList<string>? errors, IReadOnlyList<string>? warnings)
        {
            EntityType = entityType ?? throw new ArgumentNullException(nameof(entityType));
            TableName = tableName;
            TableExists = tableExists;
            Errors = errors ?? Array.Empty<string>();
            Warnings = warnings ?? Array.Empty<string>();
        }

        #endregion
    }
}
