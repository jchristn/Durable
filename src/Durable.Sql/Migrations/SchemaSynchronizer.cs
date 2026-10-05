namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;

    /// <summary>
    /// Reads the live schema for entity types on a session, diffs it, and applies (or scripts) the permitted operations.
    /// Thread safety: stateless.
    /// </summary>
    internal static class SchemaSynchronizer
    {
        #region Public-Methods

        internal static SchemaDiff Diff(MigrationSession session, IReadOnlyList<Type> entityTypes, SchemaSyncOptions options)
        {
            Dictionary<string, TableSchema> live = DatabaseSchemaReader.ReadTables(session, TableNames(entityTypes));
            return SchemaDiffer.Compare(session.Dialect, entityTypes, live, options);
        }

        internal static async Task<SchemaDiff> DiffAsync(MigrationSession session, IReadOnlyList<Type> entityTypes, SchemaSyncOptions options, CancellationToken token)
        {
            Dictionary<string, TableSchema> live = await DatabaseSchemaReader.ReadTablesAsync(session, TableNames(entityTypes), token).ConfigureAwait(false);
            return SchemaDiffer.Compare(session.Dialect, entityTypes, live, options);
        }

        internal static SchemaSyncResult Apply(MigrationSession session, SchemaDiff diff, SchemaSyncOptions options, List<MigrationOperation> applied)
        {
            List<MigrationOperation> skipped = new List<MigrationOperation>();
            foreach (MigrationOperation operation in diff.Operations)
            {
                if (operation.IsDestructive && !options.AllowDestructive)
                {
                    skipped.Add(operation);
                    continue;
                }

                foreach (SqlStatement statement in operation.Statements) session.Execute(statement, "DDL");
                applied.Add(operation);
            }

            return new SchemaSyncResult(diff, applied, skipped);
        }

        internal static async Task<SchemaSyncResult> ApplyAsync(MigrationSession session, SchemaDiff diff, SchemaSyncOptions options, List<MigrationOperation> applied, CancellationToken token)
        {
            List<MigrationOperation> skipped = new List<MigrationOperation>();
            foreach (MigrationOperation operation in diff.Operations)
            {
                token.ThrowIfCancellationRequested();
                if (operation.IsDestructive && !options.AllowDestructive)
                {
                    skipped.Add(operation);
                    continue;
                }

                foreach (SqlStatement statement in operation.Statements)
                    await session.ExecuteAsync(statement, "DDL", token).ConfigureAwait(false);
                applied.Add(operation);
            }

            return new SchemaSyncResult(diff, applied, skipped);
        }

        internal static SchemaSyncResult Script(SchemaDiff diff, SchemaSyncOptions options, StringBuilder script)
        {
            List<MigrationOperation> applied = new List<MigrationOperation>();
            List<MigrationOperation> skipped = new List<MigrationOperation>();
            foreach (MigrationOperation operation in diff.Operations)
            {
                if (operation.IsDestructive && !options.AllowDestructive)
                {
                    skipped.Add(operation);
                    MigrationScriptWriter.AppendComment(script, "SKIPPED (destructive): " + operation.Description);
                    continue;
                }

                MigrationScriptWriter.AppendComment(script, operation.ToString());
                if (operation.Warning != null) MigrationScriptWriter.AppendComment(script, "WARNING: " + operation.Warning);
                foreach (SqlStatement statement in operation.Statements) MigrationScriptWriter.AppendStatement(script, diff.Dialect, statement);
                applied.Add(operation);
            }

            foreach (SchemaDifference difference in diff.Differences)
                MigrationScriptWriter.AppendComment(script, "MANUAL STEP REQUIRED: " + difference);

            return new SchemaSyncResult(diff, applied, skipped);
        }

        #endregion

        #region Private-Methods

        private static List<string> TableNames(IReadOnlyList<Type> entityTypes)
        {
            return entityTypes.Select(t => EntityMetadata.For(t).TableName).ToList();
        }

        #endregion
    }
}
