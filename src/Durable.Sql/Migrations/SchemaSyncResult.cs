namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// The outcome of a schema synchronization: the diff it was based on, the operations applied, and the destructive
    /// operations skipped because they were not allowed.
    /// Thread safety: immutable; safe to share.
    /// </summary>
    public sealed class SchemaSyncResult
    {
        #region Public-Members

        /// <summary>
        /// Gets the diff computed against the live schema before applying. Never null.
        /// </summary>
        public SchemaDiff Diff { get; }

        /// <summary>
        /// Gets the operations that were applied (or written to the script when scripting), in order. Never null.
        /// </summary>
        public IReadOnlyList<MigrationOperation> AppliedOperations { get; }

        /// <summary>
        /// Gets the destructive operations that were not applied because <see cref="SchemaSyncOptions.AllowDestructive"/>
        /// was false. Never null.
        /// </summary>
        public IReadOnlyList<MigrationOperation> SkippedOperations { get; }

        /// <summary>
        /// Gets the differences that need a manual step (same as <see cref="SchemaDiff.Differences"/>). Never null.
        /// </summary>
        public IReadOnlyList<SchemaDifference> Differences => Diff.Differences;

        /// <summary>
        /// Gets whether the schema now matches the mappings: nothing was skipped and no differences remain.
        /// </summary>
        public bool IsInSync => SkippedOperations.Count == 0 && Differences.Count == 0;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a result.
        /// </summary>
        /// <param name="diff">Diff. Must not be null.</param>
        /// <param name="appliedOperations">Applied operations. Must not be null.</param>
        /// <param name="skippedOperations">Skipped operations. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public SchemaSyncResult(SchemaDiff diff, IEnumerable<MigrationOperation> appliedOperations, IEnumerable<MigrationOperation> skippedOperations)
        {
            Diff = diff ?? throw new ArgumentNullException(nameof(diff));
            ArgumentNullException.ThrowIfNull(appliedOperations);
            ArgumentNullException.ThrowIfNull(skippedOperations);
            AppliedOperations = appliedOperations.ToList();
            SkippedOperations = skippedOperations.ToList();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns a one-line summary of the counts.
        /// </summary>
        /// <returns>The summary.</returns>
        public override string ToString()
        {
            return AppliedOperations.Count + " applied, " + SkippedOperations.Count + " skipped, " + Differences.Count + " difference(s)";
        }

        #endregion
    }
}
