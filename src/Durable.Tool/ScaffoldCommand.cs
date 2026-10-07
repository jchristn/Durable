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
    /// <c>durable scaffold</c>: generates entity classes from existing tables.
    /// </summary>
    internal static class ScaffoldCommand
    {
        /// <summary>
        /// Gets the command definition.
        /// </summary>
        public static CommandDefinition Definition { get; } = new CommandDefinition(
            "scaffold",
            "Schema",
            "Generate entity classes from existing database tables",
            "Reads the tables of the database and writes one entity class per table to <output-dir>/<Class>.cs, with\n" +
            "[Entity], [Property] (column name, PrimaryKey/AutoIncrement/String flags, MaxLength), nullable types for\n" +
            "nullable columns, and [Index]/[CompositeIndex] for secondary indexes. Class names are the PascalCase singular of\n" +
            "the table name. Existing files are not overwritten unless --force is given. Generated code assumes nullable\n" +
            "reference types are enabled.",
            string.Empty,
            null,
            0,
            new[]
            {
                new OptionGroup("Options", CliOptions.EntitiesOutputDir, CliOptions.Namespace, CliOptions.Tables, CliOptions.Force, CliOptions.NoSingularize),
                CommandOptionGroups.DatabaseWithHistory,
                CommandOptionGroups.ProjectForOutput,
                CommandOptionGroups.General
            },
            new[]
            {
                "durable scaffold --provider postgres --connection \"Host=localhost;Database=shop;Username=app;Password=secret\"",
                "durable scaffold --tables customers,orders --namespace Shop.Data --output-dir Data",
                "durable scaffold --force"
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
            string? tablesOption = invocation.Arguments.GetValue(CliOptions.Tables);
            bool force = invocation.Arguments.HasFlag(CliOptions.Force);
            bool singularize = !invocation.Arguments.HasFlag(CliOptions.NoSingularize);

            await using DatabaseTarget database = await invocation.OpenDatabaseAsync(false, token).ConfigureAwait(false);
            DatabaseSchemaReader reader = new DatabaseSchemaReader(database.ConnectionFactory, database.Dialect);
            List<string> tableNames;
            if (tablesOption != null)
            {
                tableNames = ToolSettings.SplitList(tablesOption);
                if (tableNames.Count == 0) throw new DurableCliException("--tables needs at least one table name.", null, true);
            }
            else
            {
                tableNames = (await reader.ReadTableNamesAsync(token).ConfigureAwait(false))
                    .Where(t => !string.Equals(t, invocation.HistoryTableName, StringComparison.OrdinalIgnoreCase))
                    .ToList();
                if (tableNames.Count == 0)
                {
                    invocation.Output.WriteLine("The " + database.DisplayName + " database has no tables to scaffold.");
                    return ExitCodes.Success;
                }
            }

            List<TableSchema> tables = new List<TableSchema>();
            foreach (string tableName in tableNames)
            {
                TableSchema? table = await reader.ReadTableAsync(tableName, token).ConfigureAwait(false);
                if (table == null) throw new DurableCliException("Table '" + tableName + "' was not found in the " + database.DisplayName + " database.");
                tables.Add(table);
            }

            GeneratedOutput output = await GeneratedOutput.ResolveAsync(invocation, CliOptions.EntitiesOutputDir, "Entities", token).ConfigureAwait(false);
            HashSet<string> classNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            List<KeyValuePair<string, string>> files = new List<KeyValuePair<string, string>>();
            List<string> generatedFor = new List<string>();
            foreach (TableSchema table in tables)
            {
                string className = CSharpNames.MakeUnique(CSharpNames.ToPascalCase(singularize ? CSharpNames.Singularize(table.Name) : table.Name, "Table"), classNames);
                string path = Path.Combine(output.Directory, className + ".cs");
                files.Add(new KeyValuePair<string, string>(path, EntityCodeGenerator.Generate(database.Dialect, table, output.Namespace, className)));
                generatedFor.Add(table.Name);
                if (table.PrimaryKeyColumns.Count == 0)
                    invocation.Error.WriteLine("warning: table " + table.Name + " has no primary key; mark one property of " + className + " with Flags.PrimaryKey.");
            }

            List<string> existing = files.Select(f => f.Key).Where(File.Exists).ToList();
            if (existing.Count > 0 && !force)
                throw new DurableCliException("These files already exist: " + string.Join(", ", existing) + ". Pass --force to overwrite them or choose another --output-dir.");

            Directory.CreateDirectory(output.Directory);
            for (int i = 0; i < files.Count; i++)
            {
                await File.WriteAllTextAsync(files[i].Key, files[i].Value, token).ConfigureAwait(false);
                invocation.Output.WriteLine("Created " + files[i].Key + " (table " + generatedFor[i] + ")");
            }

            invocation.Output.WriteLine("Scaffolded " + files.Count + " entity class(es) in namespace " + output.Namespace + " from the " + database.DisplayName + " database.");
            return ExitCodes.Success;
        }
    }
}
