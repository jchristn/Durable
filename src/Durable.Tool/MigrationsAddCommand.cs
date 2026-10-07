namespace Durable.Tool
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable.Sql;

    /// <summary>
    /// <c>durable migrations add &lt;Name&gt;</c>: creates a migration class, generated from the schema differences when a
    /// database is configured.
    /// </summary>
    internal static class MigrationsAddCommand
    {
        /// <summary>
        /// Gets the command definition.
        /// </summary>
        public static CommandDefinition Definition { get; } = new CommandDefinition(
            "migrations add",
            "Migrations",
            "Create a new migration class",
            "Creates <output-dir>/<Id>.cs containing a Migration subclass named <Name> with the Id <UTC yyyyMMddHHmmss>_<Name>,\n" +
            "so migrations sort chronologically. When a database connection is configured (and --empty is not given), the\n" +
            "Up method applies the differences between your [Entity] types and the database (what 'schema diff' reports) and\n" +
            "Down reverses them when every operation is reversible; otherwise an empty template is created. Build your project\n" +
            "afterwards and run 'durable migrate'.",
            "<Name>",
            "Class name of the migration (a C# identifier), for example AddOrders",
            1,
            new[]
            {
                new OptionGroup("Options", CliOptions.MigrationsOutputDir, CliOptions.Namespace, CliOptions.Empty, CliOptions.AllowDestructive),
                CommandOptionGroups.DatabaseWithHistory,
                new OptionGroup(
                    "Project", CliOptions.Project, CliOptions.Assembly, CliOptions.Framework, CliOptions.Configuration, CliOptions.NoBuild,
                    CliOptions.EntitiesNamespace, CliOptions.Entities, CliOptions.MappingSource, CliOptions.MigrationsNamespace),
                CommandOptionGroups.General
            },
            new[]
            {
                "durable migrations add AddOrders",
                "durable migrations add AddOrders --entities-namespace MyApp.Entities --output-dir Data/Migrations",
                "durable migrations add BackfillEmails --empty"
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
            string name = invocation.Arguments.Positionals[0];
            if (!CSharpNames.IsValidIdentifier(name))
                throw new DurableCliException("'" + name + "' is not a valid migration name; use a C# identifier such as AddOrders.", null, true);

            GeneratedOutput output = await GeneratedOutput.ResolveAsync(invocation, CliOptions.MigrationsOutputDir, "Migrations", token).ConfigureAwait(false);
            if (Directory.Exists(output.Directory))
            {
                string? existing = Directory.GetFiles(output.Directory, "*_" + name + ".cs").FirstOrDefault();
                if (existing != null) throw new DurableCliException("A migration named " + name + " already exists: " + existing + ". Choose another name.");
            }

            string id = DateTime.UtcNow.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture) + "_" + name;
            string code;
            string summary;
            if (invocation.Arguments.HasFlag(CliOptions.Empty) || !invocation.HasDatabase)
            {
                code = MigrationCodeGenerator.GenerateEmpty(output.Namespace, name, id);
                summary = "empty template" + (invocation.HasDatabase ? string.Empty : "; configure a database connection to generate it from the schema differences");
            }
            else
            {
                bool allowDestructive = invocation.Arguments.HasFlag(CliOptions.AllowDestructive);
                await using DatabaseTarget database = await invocation.OpenDatabaseAsync(true, token).ConfigureAwait(false);
                List<Type> entities = await invocation.LoadEntitiesAsync(token).ConfigureAwait(false);
                UserAssembly assembly = await invocation.LoadAssemblyAsync(token).ConfigureAwait(false);
                List<Migration> migrations = assembly.DiscoverMigrations(invocation.Settings.MigrationsNamespace);
                if (migrations.Any(m => m.GetType().Name == name && m.GetType().Namespace == output.Namespace))
                    throw new DurableCliException("A migration class " + output.Namespace + "." + name + " already exists in the assembly. Choose another name.");
                Migration? latest = migrations.LastOrDefault();
                if (latest != null && string.CompareOrdinal(latest.Id, id) >= 0)
                    invocation.Error.WriteLine("warning: existing migration " + latest.Id + " sorts after " + id + "; the new migration will run before it.");

                SqlMigrator migrator = invocation.CreateMigrator(database, Array.Empty<Migration>());
                SchemaDiff diff = await migrator.DiffSchemaAsync(entities, new SchemaSyncOptions { AllowDestructive = allowDestructive }, token).ConfigureAwait(false);
                code = MigrationCodeGenerator.GenerateFromDiff(output.Namespace, name, id, diff, allowDestructive);
                int operations = diff.Operations.Count(o => allowDestructive || !o.IsDestructive);
                summary = operations + " operation(s) from the differences between " + entities.Count + " entity type(s) and the " + database.DisplayName + " database";
                if (diff.Differences.Count > 0) summary += "; " + diff.Differences.Count + " manual step(s) noted as comments";
            }

            Directory.CreateDirectory(output.Directory);
            string path = Path.Combine(output.Directory, id + ".cs");
            await File.WriteAllTextAsync(path, code, token).ConfigureAwait(false);
            invocation.Output.WriteLine("Created migration " + id + " (" + summary + "):");
            invocation.Output.WriteLine("  " + path);
            return ExitCodes.Success;
        }
    }
}
