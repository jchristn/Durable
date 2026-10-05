namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// The outcome of <see cref="SqlMigrator.Migrate"/> or <see cref="SqlMigrator.RollbackTo(string?)"/>.
    /// Thread safety: immutable; safe to share.
    /// </summary>
    public sealed class MigrationRunResult
    {
        #region Public-Members

        /// <summary>
        /// Gets the migrations applied by this run, in order. Never null.
        /// </summary>
        public IReadOnlyList<AppliedMigration> Applied { get; }

        /// <summary>
        /// Gets the identifiers of migrations reverted by this run, in order. Never null.
        /// </summary>
        public IReadOnlyList<string> Reverted { get; }

        /// <summary>
        /// Gets the identifiers of migrations that were pending when the run started but had already been applied (or
        /// reverted) by a concurrent migrator when their turn came. Never null.
        /// </summary>
        public IReadOnlyList<string> Skipped { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a result.
        /// </summary>
        /// <param name="applied">Applied migrations. Must not be null.</param>
        /// <param name="reverted">Reverted migration identifiers. Must not be null.</param>
        /// <param name="skipped">Skipped migration identifiers. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public MigrationRunResult(IEnumerable<AppliedMigration> applied, IEnumerable<string> reverted, IEnumerable<string> skipped)
        {
            ArgumentNullException.ThrowIfNull(applied);
            ArgumentNullException.ThrowIfNull(reverted);
            ArgumentNullException.ThrowIfNull(skipped);
            Applied = applied.ToList();
            Reverted = reverted.ToList();
            Skipped = skipped.ToList();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns a one-line summary of the counts.
        /// </summary>
        /// <returns>The summary.</returns>
        public override string ToString()
        {
            return Applied.Count + " applied, " + Reverted.Count + " reverted, " + Skipped.Count + " skipped";
        }

        #endregion
    }
}
