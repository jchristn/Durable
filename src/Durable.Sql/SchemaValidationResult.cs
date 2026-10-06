namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// The outcome of validating several entity mappings (<see cref="ISqlRepository{T}.ValidateTablesAsync"/>): one
    /// <see cref="TableValidationResult"/> per entity type, plus the combined errors and warnings, each prefixed with the
    /// entity type name.
    /// Thread safety: immutable; safe to share.
    /// </summary>
    public sealed class SchemaValidationResult
    {
        #region Public-Members

        /// <summary>
        /// Gets the per-table results, in the order the entity types were given. Never null.
        /// </summary>
        public IReadOnlyList<TableValidationResult> Tables { get; }

        /// <summary>
        /// Gets all errors, each prefixed with "EntityTypeName: ". Never null.
        /// </summary>
        public IReadOnlyList<string> Errors { get; }

        /// <summary>
        /// Gets all warnings, each prefixed with "EntityTypeName: ". Never null.
        /// </summary>
        public IReadOnlyList<string> Warnings { get; }

        /// <summary>
        /// Gets whether every table is valid.
        /// </summary>
        public bool IsValid => Errors.Count == 0;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a result from per-table results.
        /// </summary>
        /// <param name="tables">Per-table results. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="tables"/> is null.</exception>
        public SchemaValidationResult(IReadOnlyList<TableValidationResult> tables)
        {
            Tables = tables ?? throw new ArgumentNullException(nameof(tables));
            Errors = tables.SelectMany(t => t.Errors.Select(e => t.EntityType.Name + ": " + e)).ToList();
            Warnings = tables.SelectMany(t => t.Warnings.Select(w => t.EntityType.Name + ": " + w)).ToList();
        }

        #endregion
    }
}
