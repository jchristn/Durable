namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    /// <summary>
    /// The result of comparing entity mappings with the live schema: ordered operations (additive and destructive) and
    /// differences that need a manual step.
    /// Thread safety: immutable after construction; safe to share.
    /// </summary>
    public sealed class SchemaDiff
    {
        #region Public-Members

        /// <summary>
        /// Gets the dialect the statements were rendered for. Never null.
        /// </summary>
        public ISqlDialect Dialect { get; }

        /// <summary>
        /// Gets all operations in application order (DropIndex, CreateTable, AddColumn, DropColumn, CreateIndex). Never null.
        /// </summary>
        public IReadOnlyList<MigrationOperation> Operations { get; }

        /// <summary>
        /// Gets the differences that are reported but never applied automatically. Never null.
        /// </summary>
        public IReadOnlyList<SchemaDifference> Differences { get; }

        /// <summary>
        /// Gets the additive (safe) operations. Never null.
        /// </summary>
        public IReadOnlyList<MigrationOperation> AdditiveOperations => Operations.Where(o => !o.IsDestructive).ToList();

        /// <summary>
        /// Gets the destructive operations. Never null.
        /// </summary>
        public IReadOnlyList<MigrationOperation> DestructiveOperations => Operations.Where(o => o.IsDestructive).ToList();

        /// <summary>
        /// Gets whether the schema matches the mappings exactly (no operations and no differences).
        /// </summary>
        public bool IsEmpty => Operations.Count == 0 && Differences.Count == 0;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a diff.
        /// </summary>
        /// <param name="dialect">Dialect. Must not be null.</param>
        /// <param name="operations">Operations in application order. Must not be null.</param>
        /// <param name="differences">Differences. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public SchemaDiff(ISqlDialect dialect, IEnumerable<MigrationOperation> operations, IEnumerable<SchemaDifference> differences)
        {
            Dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
            ArgumentNullException.ThrowIfNull(operations);
            ArgumentNullException.ThrowIfNull(differences);
            Operations = operations.ToList();
            Differences = differences.ToList();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Renders the diff as a SQL script for review or CI. Parameters are inlined as literals. Destructive operations
        /// are included only when requested; otherwise they are listed as comments. Differences are listed as comments.
        /// </summary>
        /// <param name="includeDestructive">Whether destructive operations are emitted as statements. Default: false.</param>
        /// <returns>The script; contains only comments when there is nothing to apply.</returns>
        public string ToScript(bool includeDestructive = false)
        {
            StringBuilder script = new StringBuilder();
            MigrationScriptWriter.AppendComment(script, "Durable schema sync script (" + Dialect.RepositoryType.DisplayName + ")");
            foreach (MigrationOperation operation in Operations)
            {
                if (operation.IsDestructive && !includeDestructive)
                {
                    MigrationScriptWriter.AppendComment(script, "SKIPPED (destructive): " + operation.Description);
                    continue;
                }

                MigrationScriptWriter.AppendComment(script, operation.ToString());
                if (operation.Warning != null) MigrationScriptWriter.AppendComment(script, "WARNING: " + operation.Warning);
                foreach (SqlStatement statement in operation.Statements)
                    MigrationScriptWriter.AppendStatement(script, Dialect, statement);
            }

            foreach (SchemaDifference difference in Differences)
                MigrationScriptWriter.AppendComment(script, "MANUAL STEP REQUIRED: " + difference);

            return script.ToString();
        }

        /// <summary>
        /// Returns a one-line summary of the counts.
        /// </summary>
        /// <returns>The summary.</returns>
        public override string ToString()
        {
            int destructive = Operations.Count(o => o.IsDestructive);
            return (Operations.Count - destructive) + " additive, " + destructive + " destructive operation(s), " + Differences.Count + " difference(s)";
        }

        #endregion
    }
}
