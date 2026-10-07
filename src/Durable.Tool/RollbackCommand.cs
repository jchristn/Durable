namespace Durable.Tool
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable.Sql;

    /// <summary>
    /// <c>durable rollback</c>: reverts applied migrations with their Down methods.
    /// </summary>
    internal static class RollbackCommand
    {
        /// <summary>
        /// Gets the command definition.
        /// </summary>
        public static CommandDefinition Definition { get; } = new CommandDefinition(
            "rollback",
            "Migrations",
            "Revert applied migrations down to a target migration",
            "Reverts, newest first, every applied migration whose Id sorts after --target by calling its Down method, and\n" +
            "removes it from the history table. --target 0 reverts every migration. A migration without a Down override\n" +
            "stops the rollback before anything is reverted.",
            string.Empty,
            null,
            0,
            new[]
            {
                new OptionGroup("Options", CliOptions.RollbackTarget),
                CommandOptionGroups.DatabaseWithHistory,
                CommandOptionGroups.ProjectWithMigrations,
                CommandOptionGroups.General
            },
            new[]
            {
                "durable rollback --target 20261005120000_AddOrders",
                "durable rollback --target 0"
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
            string? target = invocation.Arguments.GetValue(CliOptions.RollbackTarget);
            if (target == null)
                throw new DurableCliException("rollback requires --target <id> (the migration to keep) or --target 0 (revert everything).", null, true);

            await using DatabaseTarget database = await invocation.OpenDatabaseAsync(true, token).ConfigureAwait(false);
            List<Migration> migrations = await invocation.LoadMigrationsAsync(token).ConfigureAwait(false);
            SqlMigrator migrator = invocation.CreateMigrator(database, migrations);
            List<AppliedMigration> history = await migrator.GetAppliedMigrationsAsync(token).ConfigureAwait(false);

            string? targetId = target == "0" ? null : target;
            if (targetId != null && !migrations.Any(m => m.Id == targetId) && !history.Any(a => a.Id == targetId))
                throw new DurableCliException("Unknown migration '" + targetId + "'. Run 'durable status' to list migration Ids.");

            MigrationRunResult result = await migrator.RollbackToAsync(targetId, token).ConfigureAwait(false);
            foreach (string reverted in result.Reverted) invocation.Output.WriteLine("Reverted " + reverted);
            foreach (string skipped in result.Skipped) invocation.Output.WriteLine("Skipped  " + skipped + " (reverted concurrently by another process)");

            if (result.Reverted.Count == 0)
                invocation.Output.WriteLine(targetId == null ? "No applied migrations to revert." : "Nothing to revert; no applied migration sorts after " + targetId + ".");
            else
                invocation.Output.WriteLine("Reverted " + result.Reverted.Count + " migration(s) on " + database.DisplayName + ".");
            return ExitCodes.Success;
        }
    }
}
