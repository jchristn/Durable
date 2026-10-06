namespace Durable.Tool
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable.Sql;

    /// <summary>
    /// <c>durable schema sync</c>: brings the database schema up to date with the entity types.
    /// </summary>
    internal static class SchemaSyncCommand
    {
        /// <summary>
        /// Gets the command definition.
        /// </summary>
        public static CommandDefinition Definition { get; } = new CommandDefinition(
            "schema sync",
            "Schema",
            "Bring the database schema up to date with the entity types",
            "Creates missing tables, adds missing columns and creates missing indexes for the [Entity] types in your\n" +
            "assembly, in one transaction where the database supports transactional DDL. Destructive operations (dropping\n" +
            "unmapped columns and indexes) run only with --allow-destructive. Type, length, nullability and key changes are\n" +
            "reported, never applied. --dry-run prints the SQL instead.",
            string.Empty,
            null,
            0,
            new[]
            {
                new OptionGroup("Options", CliOptions.DryRun, CliOptions.AllowDestructive),
                CommandOptionGroups.Database,
                CommandOptionGroups.ProjectWithEntities,
                CommandOptionGroups.General
            },
            new[]
            {
                "durable schema sync --dry-run",
                "durable schema sync --allow-destructive"
            },
            RunAsync);

        /// <summary>
        /// Runs the command.
        /// </summary>
        /// <param name="invocation">Invocation. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The exit code.</returns>
        public static async Task<int> RunAsync(CommandInvocation invocation, CancellationToken token)
        {
            SchemaSyncOptions options = new SchemaSyncOptions { AllowDestructive = invocation.Arguments.HasFlag(CliOptions.AllowDestructive) };
            await using DatabaseTarget database = invocation.OpenDatabase();
            List<Type> entities = await invocation.LoadEntitiesAsync(token).ConfigureAwait(false);
            SqlMigrator migrator = invocation.CreateMigrator(database, Array.Empty<Migration>());

            if (invocation.Arguments.HasFlag(CliOptions.DryRun))
            {
                string script = await migrator.GenerateSyncScriptAsync(entities, options, token).ConfigureAwait(false);
                invocation.Output.Write(script);
                return ExitCodes.Success;
            }

            SchemaSyncResult result = await migrator.SyncSchemaAsync(entities, options, token).ConfigureAwait(false);
            foreach (MigrationOperation operation in result.AppliedOperations)
                invocation.Output.WriteLine("Applied  " + operation.Description);
            foreach (MigrationOperation operation in result.SkippedOperations)
                invocation.Output.WriteLine("Skipped  " + operation.Description + " (destructive; pass --allow-destructive)");
            foreach (SchemaDifference difference in result.Differences)
                invocation.Output.WriteLine("Manual   " + difference.Message);

            if (result.AppliedOperations.Count == 0 && result.IsInSync)
                invocation.Output.WriteLine("The " + database.DisplayName + " schema already matches the " + entities.Count + " entity type(s).");
            else
                invocation.Output.WriteLine("Applied " + result.AppliedOperations.Count + " operation(s); " + result.SkippedOperations.Count + " skipped, " +
                    result.Differences.Count + " difference(s) needing a manual migration.");
            return ExitCodes.Success;
        }
    }
}
