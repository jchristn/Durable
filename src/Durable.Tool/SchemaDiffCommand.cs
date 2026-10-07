namespace Durable.Tool
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable.Sql;

    /// <summary>
    /// <c>durable schema diff</c>: prints the differences between the entity types and the database.
    /// </summary>
    internal static class SchemaDiffCommand
    {
        /// <summary>
        /// Gets the command definition.
        /// </summary>
        public static CommandDefinition Definition { get; } = new CommandDefinition(
            "schema diff",
            "Schema",
            "Show differences between entity types and the database",
            "Compares the [Entity] types in your assembly with the live schema and lists what 'schema sync' would do:\n" +
            "  +  additive operations (create tables, add columns, create indexes)\n" +
            "  -  destructive operations (drop unmapped columns and indexes; applied only with --allow-destructive)\n" +
            "  !  differences that need a hand-written migration (type, length, nullability or key changes)\n" +
            "Nothing is changed.",
            string.Empty,
            null,
            0,
            new[]
            {
                new OptionGroup("Options", CliOptions.Sql, CliOptions.AllowDestructive),
                CommandOptionGroups.Database,
                CommandOptionGroups.ProjectWithEntities,
                CommandOptionGroups.General
            },
            new[]
            {
                "durable schema diff",
                "durable schema diff --entities-namespace MyApp.Entities --sql"
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
            bool allowDestructive = invocation.Arguments.HasFlag(CliOptions.AllowDestructive);
            await using DatabaseTarget database = await invocation.OpenDatabaseAsync(true, token).ConfigureAwait(false);
            List<Type> entities = await invocation.LoadEntitiesAsync(token).ConfigureAwait(false);
            SqlMigrator migrator = invocation.CreateMigrator(database, Array.Empty<Migration>());
            SchemaDiff diff = await migrator.DiffSchemaAsync(entities, new SchemaSyncOptions { AllowDestructive = allowDestructive }, token).ConfigureAwait(false);

            if (invocation.Arguments.HasFlag(CliOptions.Sql))
            {
                invocation.Output.Write(diff.ToScript(allowDestructive));
                return ExitCodes.Success;
            }

            WriteDiff(invocation.Output, diff, entities.Count, database.DisplayName);
            return ExitCodes.Success;
        }

        /// <summary>
        /// Writes a readable summary of a diff.
        /// </summary>
        /// <param name="output">Output stream. Must not be null.</param>
        /// <param name="diff">Diff. Must not be null.</param>
        /// <param name="entityCount">Number of entity types compared.</param>
        /// <param name="databaseName">Database display name. Must not be null.</param>
        public static void WriteDiff(TextWriter output, SchemaDiff diff, int entityCount, string databaseName)
        {
            if (diff.IsEmpty)
            {
                output.WriteLine("The " + databaseName + " schema matches the " + entityCount + " entity type(s); nothing to do.");
                return;
            }

            output.WriteLine("Schema differences between " + entityCount + " entity type(s) and the " + databaseName + " database:");
            foreach (MigrationOperation operation in diff.Operations)
            {
                output.WriteLine("  " + (operation.IsDestructive ? "- " : "+ ") + operation.Description);
                if (operation.Warning != null) output.WriteLine("      warning: " + operation.Warning);
            }

            foreach (SchemaDifference difference in diff.Differences)
                output.WriteLine("  ! " + difference.Message);

            output.WriteLine(diff.AdditiveOperations.Count + " additive, " + diff.DestructiveOperations.Count + " destructive operation(s), " +
                diff.Differences.Count + " difference(s) needing a manual migration.");
        }
    }
}
