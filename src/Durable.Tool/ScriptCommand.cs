namespace Durable.Tool
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable.Sql;

    /// <summary>
    /// <c>durable script</c>: writes a SQL script for migrations.
    /// </summary>
    internal static class ScriptCommand
    {
        /// <summary>
        /// Gets the command definition.
        /// </summary>
        public static CommandDefinition Definition { get; } = new CommandDefinition(
            "script",
            "Migrations",
            "Generate a SQL script for migrations",
            "Without --from/--to, scripts the migrations that are pending on the database. With --from and/or --to, scripts\n" +
            "that range of migrations regardless of the history (for example to upgrade another environment between two\n" +
            "releases). The script creates the history table if needed and records each migration; nothing is executed.\n" +
            "A database connection is still required because migrations can inspect the current schema.",
            string.Empty,
            null,
            0,
            new[]
            {
                new OptionGroup("Options", CliOptions.From, CliOptions.To, CliOptions.Output),
                CommandOptionGroups.DatabaseWithHistory,
                CommandOptionGroups.ProjectWithMigrations,
                CommandOptionGroups.General
            },
            new[]
            {
                "durable script --output pending.sql",
                "durable script --from 20261001090000_Initial --to 20261005120000_AddOrders",
                "durable script --from 0 --output full.sql"
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
            string? from = invocation.Arguments.GetValue(CliOptions.From);
            string? to = invocation.Arguments.GetValue(CliOptions.To);
            string? output = invocation.Arguments.GetValue(CliOptions.Output);

            await using DatabaseTarget database = invocation.OpenDatabase();
            List<Migration> migrations = await invocation.LoadMigrationsAsync(token).ConfigureAwait(false);
            string? fromId = from == "0" ? null : from;
            RequireKnown(migrations, fromId, "--from");
            RequireKnown(migrations, to, "--to");
            if (fromId != null && to != null && string.CompareOrdinal(fromId, to) >= 0)
                throw new DurableCliException("--from " + fromId + " must sort before --to " + to + ".");

            SqlMigrator migrator = invocation.CreateMigrator(database, migrations);
            string script = from != null || to != null
                ? await migrator.GenerateScriptAsync(fromId, to, token).ConfigureAwait(false)
                : await migrator.GenerateScriptAsync(token).ConfigureAwait(false);

            if (output == null)
            {
                invocation.Output.Write(script);
                return ExitCodes.Success;
            }

            string path = invocation.Context.ResolvePath(output);
            string? directory = Path.GetDirectoryName(path);
            if (directory != null) Directory.CreateDirectory(directory);
            await File.WriteAllTextAsync(path, script, token).ConfigureAwait(false);
            invocation.Output.WriteLine("Wrote " + database.DisplayName + " migration script to " + path + ".");
            return ExitCodes.Success;
        }

        private static void RequireKnown(List<Migration> migrations, string? id, string option)
        {
            if (id != null && !migrations.Any(m => string.Equals(m.Id, id, StringComparison.Ordinal)))
                throw new DurableCliException("Unknown migration '" + id + "' for " + option + ". Run 'durable status' to list migration Ids.");
        }
    }
}
