namespace Durable.Tool
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable.Sql;

    /// <summary>
    /// <c>durable migrate</c>: applies pending migrations.
    /// </summary>
    internal static class MigrateCommand
    {
        /// <summary>
        /// Gets the command definition.
        /// </summary>
        public static CommandDefinition Definition { get; } = new CommandDefinition(
            "migrate",
            "Migrations",
            "Apply pending migrations from your assembly",
            "Applies, in Id order, every migration in your assembly that is not recorded in the history table. Each migration\n" +
            "runs in its own transaction where the database supports transactional DDL, under a database lock so several\n" +
            "processes can migrate at once. With --target, stops after that migration.",
            string.Empty,
            null,
            0,
            new[]
            {
                new OptionGroup("Options", CliOptions.MigrateTarget),
                CommandOptionGroups.DatabaseWithHistory,
                CommandOptionGroups.ProjectWithMigrations,
                CommandOptionGroups.General
            },
            new[]
            {
                "durable migrate --provider sqlite --connection \"Data Source=app.db\"",
                "durable migrate --target 20261005120000_AddOrders",
                "durable migrate --assembly bin/Release/net8.0/App.dll"
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
            string? target = invocation.Arguments.GetValue(CliOptions.MigrateTarget);
            await using DatabaseTarget database = invocation.OpenDatabase();
            List<Migration> migrations = await invocation.LoadMigrationsAsync(token).ConfigureAwait(false);
            if (target != null)
            {
                if (!migrations.Any(m => string.Equals(m.Id, target, StringComparison.Ordinal)))
                    throw new DurableCliException("Unknown migration '" + target + "'. Run 'durable status' to list migration Ids.");
                migrations = migrations.Where(m => string.CompareOrdinal(m.Id, target) <= 0).ToList();
            }

            SqlMigrator migrator = invocation.CreateMigrator(database, migrations);
            List<AppliedMigration> history = await migrator.GetAppliedMigrationsAsync(token).ConfigureAwait(false);
            if (target != null && history.Any(a => string.CompareOrdinal(a.Id, target) > 0))
                invocation.Error.WriteLine("warning: migrations after " + target + " are already applied; use 'durable rollback --target " + target + "' to revert them.");

            MigrationRunResult result = await migrator.MigrateAsync(token).ConfigureAwait(false);
            foreach (AppliedMigration applied in result.Applied)
                invocation.Output.WriteLine("Applied  " + applied.Id + " (" + applied.DurationMs + " ms)");
            foreach (string skipped in result.Skipped)
                invocation.Output.WriteLine("Skipped  " + skipped + " (applied concurrently by another process)");

            if (result.Applied.Count == 0) invocation.Output.WriteLine("No pending migrations; the database is up to date.");
            else invocation.Output.WriteLine("Applied " + result.Applied.Count + " migration(s) to " + database.DisplayName + ".");
            return ExitCodes.Success;
        }
    }
}
