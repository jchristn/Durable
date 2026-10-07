namespace Durable.Tool
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable.Sql;

    /// <summary>
    /// <c>durable status</c> (alias <c>durable migrations list</c>): lists applied and pending migrations.
    /// </summary>
    internal static class StatusCommand
    {
        /// <summary>
        /// Gets the command definition.
        /// </summary>
        public static CommandDefinition Definition { get; } = Create("status", null);

        /// <summary>
        /// Gets the alias definition.
        /// </summary>
        public static CommandDefinition ListDefinition { get; } = Create("migrations list", "status");

        /// <summary>
        /// Runs the command.
        /// </summary>
        /// <param name="invocation">Invocation. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The exit code.</returns>
        public static async Task<int> RunAsync(CommandInvocation invocation, CancellationToken token)
        {
            await using DatabaseTarget database = await invocation.OpenDatabaseAsync(true, token).ConfigureAwait(false);
            List<Migration> migrations = await invocation.LoadMigrationsAsync(token).ConfigureAwait(false);
            SqlMigrator migrator = invocation.CreateMigrator(database, migrations);
            List<AppliedMigration> history = await migrator.GetAppliedMigrationsAsync(token).ConfigureAwait(false);
            Dictionary<string, AppliedMigration> applied = history.ToDictionary(a => a.Id, StringComparer.Ordinal);
            Dictionary<string, Migration> known = migrations.ToDictionary(m => m.Id, StringComparer.Ordinal);
            List<string> ids = known.Keys.Union(applied.Keys).OrderBy(id => id, StringComparer.Ordinal).ToList();

            invocation.Output.WriteLine("Migrations on " + database.DisplayName + " (history table " + invocation.HistoryTableName + "):");
            if (ids.Count == 0) invocation.Output.WriteLine("  (none)");
            int width = ids.Count == 0 ? 0 : ids.Max(id => id.Length);
            int pending = 0;
            int missing = 0;
            foreach (string id in ids)
            {
                bool isApplied = applied.TryGetValue(id, out AppliedMigration? record);
                known.TryGetValue(id, out Migration? migration);
                string state;
                string detail;
                if (isApplied && migration != null)
                {
                    state = "Applied";
                    detail = record!.AppliedUtc.ToString("yyyy-MM-dd HH:mm:ss'Z'", CultureInfo.InvariantCulture);
                }
                else if (isApplied)
                {
                    state = "Missing";
                    detail = "applied " + record!.AppliedUtc.ToString("yyyy-MM-dd HH:mm:ss'Z'", CultureInfo.InvariantCulture) + ", not found in the assembly";
                    missing++;
                }
                else
                {
                    state = "Pending";
                    detail = migration!.SupportsDown ? string.Empty : "(no Down)";
                    pending++;
                }

                invocation.Output.WriteLine(("  " + state.PadRight(8) + " " + id.PadRight(width) + "  " + detail).TrimEnd());
            }

            invocation.Output.WriteLine((ids.Count - pending - missing) + " applied, " + pending + " pending" + (missing > 0 ? ", " + missing + " applied but missing from the assembly" : string.Empty) + ".");
            return ExitCodes.Success;
        }

        private static CommandDefinition Create(string name, string? aliasOf)
        {
            return new CommandDefinition(
                name,
                "Migrations",
                aliasOf == null ? "Show applied and pending migrations" : "Same as 'durable status'",
                "Lists every migration in your assembly and in the history table with its state: Applied (with the time it\n" +
                "was applied), Pending, or Missing (recorded in the history but no longer in the assembly).",
                string.Empty,
                null,
                0,
                new[]
                {
                    CommandOptionGroups.DatabaseWithHistory,
                    CommandOptionGroups.ProjectWithMigrations,
                    CommandOptionGroups.General
                },
                new[]
                {
                    "durable status",
                    "durable " + name + " --provider postgres --connection \"Host=localhost;Database=app;Username=app;Password=secret\""
                },
                RunAsync,
                aliasOf);
        }
    }
}
