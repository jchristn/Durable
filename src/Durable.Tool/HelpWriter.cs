namespace Durable.Tool
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;

    /// <summary>
    /// Writes the tool's help pages.
    /// </summary>
    internal static class HelpWriter
    {
        /// <summary>
        /// Writes the overview: usage, commands by category, and how settings are found.
        /// </summary>
        /// <param name="output">Output stream. Must not be null.</param>
        public static void WriteOverview(TextWriter output)
        {
            ArgumentNullException.ThrowIfNull(output);
            output.WriteLine("durable " + DurableCli.Version + " - command-line tool for the Durable ORM");
            output.WriteLine();
            output.WriteLine("Usage: durable <command> [arguments] [options]");
            int width = CommandCatalog.Commands.Max(c => Label(c).Length) + 2;
            foreach (IGrouping<string, CommandDefinition> category in CommandCatalog.Commands.GroupBy(c => c.Category))
            {
                output.WriteLine();
                output.WriteLine(category.Key + ":");
                foreach (CommandDefinition command in category) output.WriteLine("  " + Label(command).PadRight(width) + command.Summary);
            }

            output.WriteLine();
            output.WriteLine("Other:");
            output.WriteLine("  " + "help [command]".PadRight(width) + "Show help for a command");
            output.WriteLine("  " + "--version".PadRight(width) + "Show the tool version");
            output.WriteLine();
            output.WriteLine("Settings: --provider (sqlite|postgres|mysql|sqlserver) and --connection select the database; they default to");
            output.WriteLine("the DURABLE_PROVIDER and DURABLE_CONNECTION environment variables, then to ./durable.json. Commands that");
            output.WriteLine("read your code build the project in the current directory (or --project) or load --assembly.");
            output.WriteLine();
            output.WriteLine("Run 'durable <command> --help' for the options of a command.");
        }

        /// <summary>
        /// Writes the help of a command group ("migrations", "schema").
        /// </summary>
        /// <param name="output">Output stream. Must not be null.</param>
        /// <param name="group">Group word. Must not be null.</param>
        public static void WriteGroup(TextWriter output, string group)
        {
            ArgumentNullException.ThrowIfNull(output);
            List<CommandDefinition> commands = CommandCatalog.InGroup(group);
            output.WriteLine("Usage: durable " + group + " <command> [arguments] [options]");
            output.WriteLine();
            output.WriteLine("Commands:");
            int width = commands.Max(c => Label(c).Length) + 2;
            foreach (CommandDefinition command in commands) output.WriteLine("  " + Label(command).PadRight(width) + command.Summary);
            output.WriteLine();
            output.WriteLine("Run 'durable " + group + " <command> --help' for the options of a command.");
        }

        /// <summary>
        /// Writes the help of a command.
        /// </summary>
        /// <param name="output">Output stream. Must not be null.</param>
        /// <param name="command">Command. Must not be null.</param>
        public static void WriteCommand(TextWriter output, CommandDefinition command)
        {
            ArgumentNullException.ThrowIfNull(output);
            ArgumentNullException.ThrowIfNull(command);
            output.WriteLine("durable " + command.Name + " - " + command.Summary);
            output.WriteLine();
            output.WriteLine("Usage: durable " + Label(command) + " [options]");
            output.WriteLine();
            foreach (string line in command.Description.Split('\n')) output.WriteLine(line);
            if (command.AliasOf != null)
            {
                output.WriteLine();
                output.WriteLine("This command is an alias of 'durable " + command.AliasOf + "'.");
            }

            int width = Math.Max(
                command.AllOptions.Max(o => o.Syntax.Length),
                command.ArgumentDescription != null ? command.ArgumentSyntax.Length : 0) + 2;
            if (command.ArgumentDescription != null)
            {
                output.WriteLine();
                output.WriteLine("Arguments:");
                output.WriteLine("  " + command.ArgumentSyntax.PadRight(width) + command.ArgumentDescription);
            }

            foreach (OptionGroup group in command.OptionGroups)
            {
                output.WriteLine();
                output.WriteLine(group.Title + ":");
                foreach (OptionDefinition option in group.Options) output.WriteLine("  " + option.Syntax.PadRight(width) + option.Description);
            }

            if (command.Examples.Count > 0)
            {
                output.WriteLine();
                output.WriteLine("Examples:");
                foreach (string example in command.Examples) output.WriteLine("  " + example);
            }
        }

        private static string Label(CommandDefinition command)
        {
            return command.Name + (command.ArgumentSyntax.Length > 0 ? " " + command.ArgumentSyntax : string.Empty);
        }
    }
}
